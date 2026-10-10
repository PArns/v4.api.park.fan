"""Turn a run's per-origin aggregates into the BENCH-SPEC outputs.

Writes into the run directory (``tables[_<from>_<to>]/*.csv`` and
``summary[_tables_<from>_<to>].md``):

* per use case (UC1–UC4) and per frontend decision (D1–D9): every model's
  value with a park-day bootstrap CI, and the PAIRED difference against the
  per-lead reference with two CIs — park-day bootstrap and park-cluster
  bootstrap (all days of a park resampled together). **A model wins only when
  the park-cluster CI excludes 0**; it is the stricter of the two, because days
  of one park are not independent;
* ``horizon_curve.csv`` (metric vs lead), ``usable_horizon.csv`` and
  ``handover.csv`` (the serving router's input);
* every comparison is paired at the level the metric lives on: slots for MAE,
  ride-days for best time / dayPeak / rope drop (only ride-days a model covers
  in full), ride pairs for the dayPeak ordering, park-days for the optimiser,
  day pairs for the day comparison.

Reference per lead (BENCH-SPEC "Horizon"): the candidate naives are compared to
each other in a PAIRED round robin (every ordered pair on the rows both cover,
park-cluster CI). A candidate is dropped only when another candidate beats it
significantly; among the survivors the reference is the one that comes first in
BENCH-SPEC's ladder (persistence → seasonal-naive → weekday-median →
climatology). So the reference is pre-specified unless the data significantly
says otherwise — it is never the argmin of several unpaired values measured on
different row sets, which is selection on noise and biases "nothing beats X"
claims (PAR-827 critic B1).

Every (model, reference-candidate) pair is published in ``cells_all_refs.csv``
and the choice itself, with its reason, in ``ref_choice.csv``; ``cells.csv``
keeps one row per model, against the selected reference.

Gating (hand-over and usable horizon): a cell counts only with enough units for
its own unit of analysis — ≥ 30 origin days for day-unit metrics, ≥ 30
park-months for UC4, whose unit is the park-MONTH (a 3-month window has 3
distinct month labels but ~200 park-months, so gating it on "origin days" is a
category error that silently disables the use case) — and when the model's
paired rows are ≥ 30 % of the reference's own rows. A win additionally needs
≥ 10 distinct bootstrap units and a strictly positive CI width, because a
one-unit paired set yields a zero-width CI that "excludes 0" trivially.
Winners are ranked by their PAIRED margin, never by the unpaired value.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from . import baselines as B
from .config import UC4_LEADS, BenchConfig
from .stats import Bootstrapper, ratio_ci, significant_diff

UC1_KEYS = ["m015", "m030", "m045", "m060", "m075", "m090", "m105", "m120"]
UC2_INTRADAY_KEYS = ["h2-4", "h4-8", "h8+"]
NON_COMPETING = set(B.ORACLES) | {"prod_ropedrop_hist"}
# A model that opts into ORACLE weather is scored as `<name>_owx` (and its composed
# level curve as `<name>_owx_x_h5`). BENCH-SPEC says oracle weather is "reported
# separately"; the hand-over table is the serving router's INPUT, so an oracle
# configuration must never be able to win a row in it. This is a suffix test rather
# than an enumerated list so it also holds for models that do not exist yet.
# (Latent in `20261009-baselines-v2` — no baseline reads weather — but PAR-828
# measured 34 of 280 full-period hand-over rows going to `chronos2_owx` /
# `chronos2_owx_x_h5`, including all of D4's dayPeak ordering at d0–d21.)
_OWX = re.compile(r"(?:^|_)owx(?:$|_)")


def is_oracle(model: str) -> bool:
    """Not servable, so it may appear in the tables but never win a hand-over row."""
    return model in NON_COMPETING or bool(_OWX.search(str(model)))
NAIVE_LEVELS = ["lvl_naive4", "lvl_wt56", "lvl_snaive7", "lvl_clim"]
MIN_COVERAGE = 0.3
# DuckDB caps for the report. They used to be hard-coded at 3 GB / 4 threads, which
# does not fit in a 3 GB container and was the sole cause of a sibling workstream's
# rc=137 OOM. Defaults are the old literals, so nothing changes unless asked.
REPORT_MEMORY = os.environ.get("MLBENCH_REPORT_MEMORY", "3GB")
REPORT_THREADS = int(os.environ.get("MLBENCH_REPORT_THREADS", "4"))
# model columns of the "vs each naive candidate" tables (keeps summary.md readable)
CANDIDATE_TABLE_MODELS = ["persistence", "snaive7", "wt_med", "clim", "h5", "lvlh5_naive",
                          "lvlh5_tft", "lvlh5_cbd", "prod_served", "prod_served_lin"]
# BENCH-SPEC's reference ladder, in order: persistence -> seasonal-naive ->
# weekday-median -> climatology, for slot models and for the daily level sources.
# Ties between candidates are broken by THIS order, never by the point estimate.
# `prod_ropedrop_hist` is only a candidate for the rope-drop decision and is not
# part of the ladder, so it sorts last.
REF_LADDER = ["persistence", "snaive7", "wt_med", "clim",
              "lvl_snaive7", "lvl_naive4", "lvl_wt56", "lvl_clim",
              "prod_ropedrop_hist"]
# metrics whose bootstrap unit is the park-MONTH, not the park-day
PARK_MONTH_UNIT = "park-month"
# one weight column per unit for the whole report (see stats.Bootstrapper)
BOOT: dict[str, Bootstrapper] = {}
# every (model, reference-candidate) pair, and the per-cell reference choice;
# filled by `evaluate`, written as cells_all_refs.csv / ref_choice.csv
ALL_REF_ROWS: list[dict] = []
REF_CHOICES: list[dict] = []


def _ladder_rank(name: str, refs: list[str]) -> int:
    if name in REF_LADDER:
        return REF_LADDER.index(name)
    return len(REF_LADDER) + (refs.index(name) if name in refs else len(refs))


def min_units(unit: str, cfg: BenchConfig) -> int:
    """LOW-N threshold for the unit the metric actually lives on."""
    return cfg.low_n_park_months if unit == PARK_MONTH_UNIT else cfg.low_n_origin_days


def _num(row, key: str) -> float:
    """``row[key]`` as a float, with missing / None / NaN all becoming nan."""
    v = row.get(key)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return np.nan
    return f


def _str(row, key: str, default: str) -> str:
    v = row.get(key)
    return v if isinstance(v, str) and v else default


def unit_count(row, unit: str) -> int:
    """The count the LOW-N gate must look at: origin days for day-unit metrics,
    park-months for UC4 (where ``n_origin_days`` counts month LABELS, not sample size)."""
    v = _num(row, "env_min_units_park_day" if unit == PARK_MONTH_UNIT else "n_origin_days")
    return 0 if not np.isfinite(v) else int(v)


def tested(row, cfg: BenchConfig, model: str | None = None) -> tuple[bool, str]:
    """Was this cell a real test of the model against the whole naive envelope?

    ``model`` is accepted for call-site symmetry and is not used: a naive
    candidate is allowed to beat the envelope of the OTHER candidates, which is
    a real finding ("climatology beats every other naive here").
    """
    unit = _str(row, "unit", "park-day")
    n_env = _num(row, "n_env")
    if not np.isfinite(n_env) or n_env < 1:
        return False, "no naive candidate to compare against"
    n, need = unit_count(row, unit), min_units(unit, cfg)
    if n < need:
        return False, f"LOW-N: {n} {unit}s < {need}"
    share = _num(row, "env_min_share")
    share = 0.0 if not np.isfinite(share) else share
    if share < MIN_COVERAGE:
        return False, f"coverage {share:.2f} < {MIN_COVERAGE:.2f} against one of the candidates"
    return True, "ok"


def _boot(kind: str, cfg: BenchConfig) -> Bootstrapper:
    if kind not in BOOT:
        BOOT[kind] = Bootstrapper(cfg.bootstrap_reps, cfg.bootstrap_seed + (1 if kind == "park" else 0))
    return BOOT[kind]

_T0 = [None]


def _log(msg: str) -> None:
    if _T0[0] is None:
        _T0[0] = time.monotonic()
    print(f"[report {time.monotonic() - _T0[0]:7.1f}s] {msg}", file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- io

class Data:
    TABLES = ("slot", "rideday", "pairs", "optim", "intraday", "nextbest", "levels", "openness", "d8tft")

    # Which expression is the ORIGIN of a row, per parts table. A target-window filter
    # pins when the truth happened; it does NOT pin when the forecast was made, and for
    # a horizon curve the origin season is the confounded axis: inside a fixed 54-day
    # target window the d1 cells come from autumn origins and the d90 cells from summer
    # ones. PAR-830 measured that (its `driver_level` gap flips sign between the two
    # passes), so the harness needs to be able to fix the origin window too.
    @staticmethod
    def origin_expr(cols: set[str]) -> str | None:
        if "origin" in cols:
            return "origin"                       # levels, d8tft carry it directly
        if {"L", "date"} <= cols:
            return "(date - L)"                   # slot, rideday, pairs, optim, openness
        if "date" in cols:
            return "date"                         # intraday / nextbest: origin day = target day
        return None

    def __init__(self, run: Path, export: Path | None, target_from: str | None, target_to: str | None,
                 memory: str = "3GB", threads: int = 4, origin_from: str | None = None,
                 origin_to: str | None = None):
        import duckdb

        self.run = run
        self.con = duckdb.connect()
        self.con.execute(f"SET threads={int(threads)}")
        self.con.execute(f"SET memory_limit='{memory}'")
        tmp = run / "work" / f"report-tmp-{target_from or 'all'}"
        tmp.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"SET temp_directory='{tmp}'")
        self.meta = [json.loads(p.read_text()) for p in sorted(run.glob("run-*.json"))]
        self.export = Path(export or self.meta[0]["export"])
        pq = self.export / "parquet"
        self.con.execute(f"CREATE VIEW parks AS SELECT * FROM read_parquet('{pq}/parks.parquet')")
        self.con.execute(f"""CREATE VIEW rides AS SELECT a.id aid, a.park_id, coalesce(a.is_headliner, false) hl
                              FROM read_parquet('{pq}/attractions.parquet') a""")
        self.con.execute(f"CREATE VIEW ride_day AS SELECT * FROM read_parquet('{pq}/ride_day.parquet')")
        self.con.execute("""CREATE TABLE region AS SELECT id park_id, CASE split_part(timezone, '/', 1)
              WHEN 'Europe' THEN 'EU' WHEN 'America' THEN 'NA' WHEN 'Asia' THEN 'Asia'
              WHEN 'Australia' THEN 'Asia' WHEN 'Pacific' THEN 'Asia' ELSE 'other' END region FROM parks""")
        self.target_from, self.target_to = target_from, target_to
        self.origin_from, self.origin_to = origin_from, origin_to
        self.tables = set()
        for t in self.TABLES:
            if not list((run / "parts" / t).glob("*.parquet")):
                continue
            src = f"read_parquet('{run}/parts/{t}/*.parquet', union_by_name=true)"
            cols = {c[0] for c in self.con.execute(f"DESCRIBE SELECT * FROM {src} LIMIT 0").fetchall()}
            flt = []
            # d8tft has no `date`, only `origin` (its "stated" rows look 45 days BACK
            # from the origin, its "realised" rows up to 60 days forward), so the
            # target-day filter cannot apply to it. Without this the windowed pass was a
            # byte-identical copy of the full-period table and the headline D8 claim was
            # a 231-origin result presented as a 54-origin one (critic S13).
            tcol = "date" if "date" in cols else ("origin" if t == "d8tft" else None)
            if tcol:
                if target_from:
                    flt.append(f"{tcol} >= DATE '{target_from}'")
                if target_to:
                    flt.append(f"{tcol} <= DATE '{target_to}'")
            ocol = self.origin_expr(cols)
            if ocol and (origin_from or origin_to):
                if origin_from:
                    flt.append(f"{ocol} >= DATE '{origin_from}'")
                if origin_to:
                    flt.append(f"{ocol} <= DATE '{origin_to}'")
            elif (origin_from or origin_to) and not ocol:
                _log(f"WARNING: {t} has no origin column — origin window not applied to it")
            where = (" WHERE " + " AND ".join(flt)) if flt else ""
            if t == "d8tft":
                self.con.execute(f"CREATE TABLE d8tft AS SELECT * FROM {src}{where}")
            else:
                self.con.execute(f"""CREATE TABLE {t} AS SELECT x.*, r.region FROM {src} x
                    LEFT JOIN region r USING (park_id) {where}""")
            self.tables.add(t)

    def df(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()

    def cols(self, table: str) -> list[str]:
        return [r[0] for r in self.con.execute(f"DESCRIBE {table}").fetchall()]


# --------------------------------------------------------------------------- paired engine

def _paired_stats(base: pd.DataFrame, m: str, r: str, cfg: BenchConfig, lower: bool,
                  own_den: dict) -> dict | None:
    """Paired difference of ``m`` against ``r`` on the rows BOTH cover, with the
    park-day and the park-cluster CI and the unit counts behind each."""
    p = base[(base["model"] == m) & (base["ref"] == r)]
    p = p[(p["den_m"] > 0) & (p["den_r"] > 0)]
    if not len(p):
        return None
    cols = [p[c].to_numpy() for c in ("num_m", "den_m", "num_r", "den_r")]
    pd_key = p["park_id"].astype(str) + "|" + p["date"].astype(str)
    d_, dlo, dhi, n_pd = _boot("pd", cfg).diff(pd_key, *cols)
    _, plo, phi, n_pk = _boot("park", cfg).diff(p["park_id"].astype(str), *cols)
    den_m, den_r = float(p["den_m"].sum()), float(p["den_r"].sum())
    share = den_r / own_den[r] if own_den.get(r) else np.nan
    return dict(diff_vs_ref=d_, diff_lo=dlo, diff_hi=dhi, diff_lo_park=plo, diff_hi_park=phi,
                n_paired=den_m, n_paired_ref=den_r, paired_share=share,
                n_units_park_day=n_pd, n_units_park=n_pk,
                n_paired_origin_days=int(p["date"].nunique()),
                wins=significant_diff(plo, phi, lower, n_pk, cfg.min_bootstrap_units),
                wins_park_day=significant_diff(dlo, dhi, lower, n_pd, cfg.min_bootstrap_units))


def choose_reference(refs: list[str], vals: dict, pair: dict) -> tuple[str | None, list[str], str]:
    """The DISPLAY reference: BENCH-SPEC's ladder, overridden only by a significant
    paired loss to another candidate.

    The old rule took ``min`` over the candidates' own-coverage values, i.e. an
    argmin of three MAEs measured on three different slot sets, re-chosen at
    every lead. On this data the gaps between `wt_med` and `clim` are 0.02–0.19
    min against a park-cluster half-width of ±0.24, so the reference flipped
    `wt_med -> clim -> wt_med` across the lead axis on noise, and the "nothing
    beats the reference in d7–d60" headline was a statement about the pointwise
    minimum of three naives rather than about any one of them.

    Note what this function is NOT: it is not the win criterion. A ladder
    tie-break necessarily prefers the *simpler* candidate, which on this data
    hands the reference to `snaive7` at d4–d6 (8.44 against `wt_med`'s 8.33, a
    gap the paired CI cannot resolve) — and scoring models against the weakest
    naive would inflate every win, which is B1's error with the sign flipped.
    So wins are decided by ``beats_envelope`` below, against EVERY candidate;
    this reference only fixes which single column `cells.csv` shows.

    Among the candidates with the fewest significant losses the ladder order
    decides. Returns (ref, candidates, why).
    """
    cover_max = max((vals[r][1] for r in refs if r in vals), default=0)
    cands = [r for r in refs if r in vals and vals[r][1] >= 0.5 * cover_max]
    cands.sort(key=lambda r: _ladder_rank(r, refs))
    if len(cands) > 1:
        # a candidate with no paired sums against any other candidate cannot be
        # compared to anything, so it is not a candidate
        cands = [a for a in cands if any(pair.get((a, b)) for b in cands if b != a)]
    if not cands:
        return None, [], "no candidate covers >= 50 % of the best-covered one"
    losses = {r: 0 for r in cands}
    beaten_by: dict[str, list[str]] = {r: [] for r in cands}
    for a in cands:
        for b in cands:
            if a == b:
                continue
            st = pair.get((a, b))
            if st and st["wins"]:
                losses[b] += 1
                beaten_by[b].append(a)
    ref = min(cands, key=lambda r: (losses[r], _ladder_rank(r, refs)))
    first = cands[0]
    if ref == first:
        why = (f"ladder order ({first} first of {'/'.join(cands)}); no candidate beats it significantly"
               if len(cands) > 1 else "only candidate")
    else:
        why = (f"{first} is the ladder-first candidate but is significantly beaten by "
               f"{'/'.join(beaten_by[first])}; {ref} has {losses[ref]} significant loss(es)")
    return ref, cands, why


def evaluate(base: pd.DataFrame, cfg: BenchConfig, refs: list[str], lower: bool, keys: dict,
             unit: str = "park-day") -> list[dict]:
    """``base``: rows park_id, date, model, ref, num_m, den_m, num_r, den_r — one cell
    (one lead, one segment). ``ref == model`` rows carry the model's own figure.

    Returns one row per model, paired against the SELECTED reference (``cells.csv``).
    Every (model, candidate) pair is also appended to ``ALL_REF_ROWS`` and the
    reference choice to ``REF_CHOICES``, so a reader can see what the result is
    against each individual naive and not only against the chosen one.
    """
    if base.empty:
        return []
    own = base[base["model"] == base["ref"]]
    vals, own_rows = {}, {}
    for m, g in own.groupby("model"):
        if g["den_m"].sum() > 0:
            vals[m] = (g["num_m"].sum() / g["den_m"].sum(), g["den_m"].sum())
            own_rows[m] = g
    own_den = {m: v[1] for m, v in vals.items()}
    # every pair we might need: candidate-vs-candidate for the choice, model-vs-candidate
    # for the published table. diff(b, a) = -diff(a, b) on the same rows, so compute once.
    pair: dict[tuple[str, str], dict] = {}

    def get(m: str, r: str) -> dict | None:
        if m == r or m not in vals or r not in vals:
            return None
        if (m, r) in pair:
            return pair[(m, r)]
        st = _paired_stats(base, m, r, cfg, lower, own_den)
        pair[(m, r)] = st
        if st is not None and (r, m) not in pair:
            # the mirrored pair is the SAME row set with the two arms swapped, so every
            # bootstrap replicate is exactly negated: point and CI mirror, only the
            # row counts and the coverage share belong to the other arm.
            pair[(r, m)] = dict(
                st, diff_vs_ref=-st["diff_vs_ref"], diff_lo=-st["diff_hi"], diff_hi=-st["diff_lo"],
                diff_lo_park=-st["diff_hi_park"], diff_hi_park=-st["diff_lo_park"],
                n_paired=st["n_paired_ref"], n_paired_ref=st["n_paired"],
                paired_share=(st["n_paired"] / own_den[m]) if own_den.get(m) else np.nan,
                wins=significant_diff(-st["diff_hi_park"], -st["diff_lo_park"], lower,
                                      st["n_units_park"], cfg.min_bootstrap_units),
                wins_park_day=significant_diff(-st["diff_hi"], -st["diff_lo"], lower,
                                               st["n_units_park_day"], cfg.min_bootstrap_units))
        return pair[(m, r)]

    for a in refs:
        for b in refs:
            get(a, b)
    ref, cands, why = choose_reference(refs, vals, pair)
    if cands:
        REF_CHOICES.append(dict(keys, unit=unit, selected=ref, candidates="/".join(cands),
                                reason=why, lower=lower,
                                **{f"value_{c}": vals[c][0] for c in cands},
                                **{f"n_{c}": float(vals[c][1]) for c in cands}))
    out = []
    for m in sorted(vals):
        g = own_rows[m]
        v, lo, hi = _boot("pd", cfg).ratio(g["park_id"].astype(str) + "|" + g["date"].astype(str),
                                           g["num_m"].to_numpy(), g["den_m"].to_numpy())
        row = dict(keys, model=m, value=v, lo=lo, hi=hi, n=float(vals[m][1]), lower=lower,
                   n_park_days=int((g["den_m"] > 0).sum()), n_origin_days=int(g["date"].nunique()),
                   unit=unit, ref=ref)
        env = []
        for c in cands:
            st = get(m, c)
            if st is None:
                continue
            ALL_REF_ROWS.append(dict(keys, model=m, ref_candidate=c, unit=unit, lower=lower,
                                     value=vals[m][0], value_ref=vals[c][0],
                                     is_selected_ref=(c == ref), **st))
            env.append((c, st))
        st = get(m, ref) if ref else None
        if st is not None:
            row.update(dict(st, wins_vs_ref=st["wins"]))
        row.update(envelope_verdict(env, lower))
        out.append(row)
    return out


def envelope_verdict(env: list[tuple[str, dict]], lower: bool) -> dict:
    """"Beats the naive envelope": better than EVERY candidate naive, each paired on
    its own shared rows with the park-cluster CI excluding 0.

    This replaces "beats the selected reference" as the win criterion, and it is the
    part of critic B1 that matters most. A single selected reference is a choice, and
    any rule for making it is wrong in one direction or the other: an argmin over
    own-coverage values picks on noise and understates models (B1 as filed), a ladder
    tie-break picks the *simplest* candidate and overstates them (`snaive7` would be
    the reference at d4–d6 here at 8.44 against `wt_med`'s 8.33). A conjunction over
    a FIXED candidate set is neither — nothing is selected on the data, and it is
    strictly harder than beating any one member, so "usable horizon" now means
    "better than every naive we have", which is the claim a serving decision needs.
    """
    if not env:
        return {"n_env": 0, "wins": False, "envelope": ""}
    worst = max(env, key=lambda cs: cs[1]["diff_vs_ref"] if lower else -cs[1]["diff_vs_ref"])
    return {
        "n_env": len(env),
        "envelope": "/".join(c for c, _ in env),
        "wins": all(st["wins"] for _, st in env),
        "wins_park_day": all(st["wins_park_day"] for _, st in env),
        "hardest_naive": worst[0],
        "diff_vs_hardest": worst[1]["diff_vs_ref"],
        "hardest_lo_park": worst[1]["diff_lo_park"], "hardest_hi_park": worst[1]["diff_hi_park"],
        "env_min_share": min(st["paired_share"] if np.isfinite(st["paired_share"]) else 0.0
                             for _, st in env),
        "env_min_units_park": min(st["n_units_park"] for _, st in env),
        "env_min_units_park_day": min(st["n_units_park_day"] for _, st in env),
    }


def slot_base(d: Data, table: str, where: str, refs: list[str], group: str = "park_id, date") -> pd.DataFrame:
    """Long base rows for slot MAE (or bias with ``se``) from a slot-style table."""
    cols = d.cols(table)
    models = [c[3:] for c in cols if c.startswith("n__")]
    parts = []
    for m in models:
        parts.append(f"SELECT {group}, '{m}' AS model, '{m}' AS ref, sum(sae__{m}) num_m, sum(n__{m}) den_m, "
                     f"0 num_r, 0 den_r FROM {table} WHERE {where} GROUP BY ALL")
        for r in refs:
            if r != m and f"pn__{m}__{r}" in cols:
                parts.append(f"SELECT {group}, '{m}', '{r}', sum(psm__{m}__{r}), sum(pn__{m}__{r}), "
                             f"sum(psr__{m}__{r}), sum(pn__{m}__{r}) FROM {table} WHERE {where} GROUP BY ALL")
    return d.df(" UNION ALL ".join(parts))


def bias_table(d: Data, table: str, where: str, key: str, keys: list) -> pd.DataFrame:
    models = [c[3:] for c in d.cols(table) if c.startswith("n__")]
    sel = ", ".join(f"sum(se__{m}) / nullif(sum(n__{m}), 0) AS \"{m}\"" for m in models)
    df = d.df(f"SELECT {key} AS lead, {sel} FROM {table} WHERE {where} GROUP BY 1")
    order = {k: i for i, k in enumerate(keys)}
    return df.assign(o=df["lead"].map(order)).sort_values("o").drop(columns="o")


def run_cells(d: Data, cfg: BenchConfig, table: str, key: str, keys: list, refs: list[str], uc_of,
              extra: str = "TRUE", segments=(("all", "TRUE"), ("busy", "busy")),
              regions=("all", "EU", "NA", "Asia"), metric: str = "MAE") -> pd.DataFrame:
    rows = []
    for k in keys:
        # one narrow copy per lead, so the per-model UNION below scans ~1/15 of the table
        d.con.execute(f"CREATE OR REPLACE TEMP TABLE cell_src AS SELECT * FROM {table} "
                      f"WHERE {key} = {k!r} AND ({extra})")
        for seg, cond in segments:
            for region in regions:
                rc = "TRUE" if region == "all" else f"region = '{region}'"
                where = f"({cond}) AND {rc}"
                base = slot_base(d, "cell_src", where, refs)
                rows += evaluate(base, cfg, refs, True, {"uc": uc_of(k), "lead": k, "segment": seg,
                                                         "region": region, "metric": metric})
    return pd.DataFrame(rows)


def long_base(d: Data, sql: str) -> pd.DataFrame:
    return d.df(sql)


def decision_cells(d: Data, cfg: BenchConfig, sql: str, lead_col: str, metric: str, uc: str, lower: bool,
                   refs: list[str]) -> pd.DataFrame:
    """``sql`` returns lead_col, park_id, date, model, ref, num_m, den_m, num_r, den_r."""
    df = d.df(sql)
    rows = []
    for k, g in df.groupby(lead_col, sort=True):
        rows += evaluate(g, cfg, refs, lower, {"uc": uc, "lead": k, "segment": "all", "region": "all",
                                               "metric": metric})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- horizon summaries

def usable_horizon(cells: pd.DataFrame, order: dict, cfg: BenchConfig) -> pd.DataFrame:
    """Largest lead up to which the model wins (park-cluster CI excludes 0), contiguous
    from its first scored lead; a lead where it is the reference, is untested or fails
    the coverage gate breaks the run.

    ``leads_tested = 0`` is NOT "nothing won": it means nothing was ever tested, so
    the reason the gate fired is reported alongside it (critic B4).
    """
    out = []
    c = cells[(cells["region"] == "all") & ~cells["model"].map(is_oracle)]
    for (metric, uc, seg, model), g in c.groupby(["metric", "uc", "segment", "model"]):
        g = g.assign(o=g["lead"].map(order)).sort_values("o")
        contiguous, any_sig, n_tested, broken, reasons = None, None, 0, False, []
        for _, r in g.iterrows():
            ok, why = tested(r, cfg, model)
            if not ok:
                broken = True
                reasons.append(f"{r['lead']}: {why}")
                continue
            n_tested += 1
            if bool(r.get("wins")):
                any_sig = r["lead"]
                if not broken:
                    contiguous = r["lead"]
            else:
                broken = True
        out.append({"metric": metric, "uc": uc, "segment": seg, "model": model,
                    "unit": _str(g.iloc[0], "unit", "park-day"), "leads_scored": len(g),
                    "leads_tested": n_tested,
                    "usable_horizon": "" if contiguous is None else str(contiguous),
                    "max_significant_lead": "" if any_sig is None else str(any_sig),
                    "not_tested_because": "; ".join(reasons[:6]) if n_tested == 0 else ""})
    return pd.DataFrame(out)


def handover(cells: pd.DataFrame, cfg: BenchConfig) -> pd.DataFrame:
    out = []
    c = cells[(cells["region"] == "all") & ~cells["model"].map(is_oracle)]
    for (metric, uc, seg, lead), g in c.groupby(["metric", "uc", "segment", "lead"], sort=False):
        ref = g["ref"].dropna().iloc[0] if g["ref"].notna().any() else None
        gate = g.apply(lambda r: tested(r, cfg, r["model"])[0], axis=1)
        wins = g["wins"].fillna(False).astype(bool) if "wins" in g else pd.Series(False, index=g.index)
        ok = g[wins & gate]
        lower = bool(g["lower"].iloc[0])
        extra: dict = {}
        if len(ok):
            # rank by the margin against the HARDEST naive, not against the displayed
            # reference: that is the margin a serving decision can rely on
            best = ok.sort_values("diff_vs_hardest", ascending=lower).iloc[0]
            winner, val, margin, n_win = best["model"], best["value"], best["diff_vs_ref"], len(ok)
            share = float(best.get("env_min_share", np.nan))
            cis = {k: float(best.get(k, np.nan)) for k in
                   ("diff_lo_park", "diff_hi_park", "diff_lo", "diff_hi")}
            # does BENCH-SPEC's literal unit (park-DAY) agree with the park-cluster call?
            agree = bool(best.get("wins_park_day", False))
            extra = {"hardest_naive": best.get("hardest_naive"),
                     "margin_vs_hardest": best.get("diff_vs_hardest"),
                     "hardest_lo_park": best.get("hardest_lo_park"),
                     "hardest_hi_park": best.get("hardest_hi_park")}
        else:
            winner, margin, n_win, share = ref, 0.0, 0, np.nan
            val = g.loc[g["model"] == ref, "value"].iloc[0] if ref in set(g["model"]) else np.nan
            cis = dict.fromkeys(("diff_lo_park", "diff_hi_park", "diff_lo", "diff_hi"), np.nan)
            pdw = g["wins_park_day"].fillna(False).astype(bool) if "wins_park_day" in g else None
            agree = not bool((pdw & gate).any()) if pdw is not None else True
            extra = dict.fromkeys(("hardest_naive", "margin_vs_hardest", "hardest_lo_park",
                                   "hardest_hi_park"), np.nan)
        out.append({"metric": metric, "uc": uc, "segment": seg, "lead": lead, "reference": ref,
                    "envelope": _str(g.iloc[0], "envelope", ""),
                    "winner": winner, "winner_value": val, "paired_margin": margin,
                    "margin_lo_park": cis["diff_lo_park"], "margin_hi_park": cis["diff_hi_park"],
                    "margin_lo_park_day": cis["diff_lo"], "margin_hi_park_day": cis["diff_hi"],
                    **extra, "park_day_unit_agrees": agree,
                    "winner_paired_share": share, "significant_models": n_win,
                    "n_tested": int(gate.sum()), "unit": _str(g.iloc[0], "unit", "park-day"),
                    "n_origin_days": int(g["n_origin_days"].max())})
    return pd.DataFrame(out)


def low_coverage_cells(cells: pd.DataFrame, thresh: float = 0.5) -> pd.DataFrame:
    """Every published cell whose paired rows are below ``thresh`` of the reference's.

    These values are not comparable with the rest of the column — `lvlh5_tft` reads
    1.337 min on D5 at d60 because TFT's horizon ends there and only 2 % of the
    reference's slots remain. The gates keep most of them out of the hand-over;
    this table is so a reader of `cells.csv` can see all of them at once (critic S4).
    """
    if cells.empty or "paired_share" not in cells:
        return pd.DataFrame()
    c = cells[(cells["region"] == "all") & cells["paired_share"].notna()
              & (cells["paired_share"] < thresh)]
    cols = ["metric", "uc", "segment", "lead", "model", "ref", "value", "diff_vs_ref",
            "diff_lo_park", "diff_hi_park", "paired_share", "n_paired", "n_units_park",
            "n_origin_days", "wins"]
    c = c[[x for x in cols if x in c.columns]].sort_values(["metric", "uc", "segment", "model"])
    return c.assign(reaches_handover=c["paired_share"] >= MIN_COVERAGE)


# --------------------------------------------------------------------------- crowd levels (UC4 / D6 / D7)

def crowd_levels(d: Data, cfg: BenchConfig) -> list[str] | None:
    """Park-day levels per origin and lead (headliner mean on matched headliners) and
    the typical-day peak as of the origin's month."""
    if "levels" not in d.tables:
        return None
    srcs = [c for c in d.cols("levels") if c.startswith("lvl_")]
    d.con.execute("""CREATE OR REPLACE TABLE hl_day AS SELECT r.park_id, r.date, avg(r.p90) lvl
        FROM ride_day r JOIN rides a USING (aid) WHERE a.hl GROUP BY ALL""")
    months = d.df("SELECT DISTINCT date_trunc('month', origin)::DATE m0 FROM levels")["m0"].tolist()
    parts = [f"""SELECT park_id, DATE '{pd.Timestamp(m0).date()}' m0, median(lvl) typ, count(*) nd
                 FROM hl_day WHERE date < DATE '{pd.Timestamp(m0).date()}' GROUP BY park_id
                 HAVING count(*) >= {cfg.typical_peak_min_days}""" for m0 in months]
    if not parts:
        return None
    d.con.execute(f"CREATE OR REPLACE TABLE typ AS {' UNION ALL '.join(parts)}")
    sel = ", ".join(f"avg({s}) FILTER (WHERE true_p90 IS NOT NULL) AS {s}" for s in srcs)
    d.con.execute(f"""CREATE OR REPLACE TABLE pdl AS
        SELECT l.origin, l.park_id, any_value(l.region) region, l.date, l.L, avg(l.true_p90) true_lvl, {sel}
        FROM levels l GROUP BY l.origin, l.park_id, l.date, l.L HAVING count(l.true_p90) > 0""")
    d.con.execute("""CREATE OR REPLACE TABLE pdl2 AS SELECT p.*, t.typ FROM pdl p
        JOIN typ t ON t.park_id = p.park_id AND t.m0 = date_trunc('month', p.origin)::DATE
        ORDER BY p.origin, p.park_id, p.date""")
    return srcs


def bucket_np(pct: np.ndarray) -> np.ndarray:
    out = np.full(pct.shape, np.nan)
    ok = np.isfinite(pct)
    p = pct[ok]
    out[ok] = np.select([p <= 60, p <= 89, p <= 110, p <= 150, p <= 200], [0, 1, 2, 3, 4], 5)
    return out


def _paired_from_units(units: pd.DataFrame, srcs: list[str], refs: list[str], lower: bool, keys: dict,
                       cfg: BenchConfig, unit_cols=("park_id", "date"),
                       unit: str = "park-day") -> list[dict]:
    """units: one row per unit with num_<s>, den_<s> per source and paired columns
    pnum_<m>__<r>/pden_<m>__<r> prepared by the caller."""
    base = []
    for s in srcs:
        if f"den_{s}" not in units:
            continue
        b = units[list(unit_cols)].copy()
        b["model"], b["ref"] = s, s
        b["num_m"], b["den_m"], b["num_r"], b["den_r"] = units[f"num_{s}"], units[f"den_{s}"], 0.0, 0.0
        base.append(b)
        for r in refs:
            if r == s or f"pden_{s}__{r}" not in units:
                continue
            b = units[list(unit_cols)].copy()
            b["model"], b["ref"] = s, r
            b["num_m"], b["den_m"] = units[f"pnum_{s}__{r}"], units[f"pden_{s}__{r}"]
            b["num_r"], b["den_r"] = units[f"pnumr_{s}__{r}"], units[f"pden_{s}__{r}"]
            base.append(b)
    if not base:
        return []
    return evaluate(pd.concat(base), cfg, refs, lower, keys, unit=unit)


def uc4(d: Data, cfg: BenchConfig, srcs: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Spearman of the daily park level within park-month (paired on the same days),
    busy-day recall with ties broken fractionally, precision of the >= q75 rule (ties
    included, so it shows what ties cost), spread."""
    lv = d.df("SELECT * FROM pdl2")
    rows, extra = [], []
    for L in UC4_LEADS:
        g = lv[lv["L"] == L]
        if g.empty:
            continue
        g = g.assign(mon=pd.to_datetime(g["date"]).dt.to_period("M").astype(str))
        units = []
        for (park, mon), h in g.groupby(["park_id", "mon"], sort=True):
            u = {"park_id": park, "date": mon}
            t = h["true_lvl"].to_numpy()
            k = int(np.ceil(len(h) / 4))
            top_t = h["true_lvl"].rank(method="first", ascending=False).to_numpy() <= k
            for s in srcs:
                p = h[s].to_numpy()
                okm = np.isfinite(p)
                if okm.sum() >= 8:
                    sp = pd.Series(t[okm]).rank().corr(pd.Series(p[okm]).rank())
                    u[f"num_{s}"], u[f"den_{s}"] = (sp, 1.0) if np.isfinite(sp) else (0.0, 0.0)
                    pv = np.where(okm, p, -np.inf)
                    vk = np.sort(pv)[::-1][k - 1]
                    above, eq = pv > vk, pv == vk
                    psel = above.astype(float) + eq * (k - above.sum()) / max(eq.sum(), 1)
                    q75 = np.quantile(p[okm], 0.75)
                    pred_set = okm & (p >= q75)
                    extra.append({"lead": L, "model": s, "hit": float((psel * top_t).sum()), "k": k,
                                  "prec_hit": float((pred_set & top_t).sum()), "pred_n": int(pred_set.sum()),
                                  "iqr": float(np.subtract(*np.quantile(p[okm], [0.75, 0.25]))),
                                  "iqr_true": float(np.subtract(*np.quantile(t, [0.75, 0.25])))})
                    for r in NAIVE_LEVELS:
                        if r == s:
                            continue
                        pr = h[r].to_numpy()
                        both = okm & np.isfinite(pr)
                        if both.sum() >= 8:
                            a = pd.Series(t[both]).rank().corr(pd.Series(p[both]).rank())
                            b = pd.Series(t[both]).rank().corr(pd.Series(pr[both]).rank())
                            if np.isfinite(a) and np.isfinite(b):
                                u[f"pnum_{s}__{r}"], u[f"pnumr_{s}__{r}"], u[f"pden_{s}__{r}"] = a, b, 1.0
            units.append(u)
        U = pd.DataFrame(units).fillna(0.0)
        rows += _paired_from_units(U, srcs, NAIVE_LEVELS, False,
                                   {"uc": "UC4", "lead": L, "segment": "all", "region": "all",
                                    "metric": "UC4 Spearman within park-month"}, cfg,
                                   unit=PARK_MONTH_UNIT)
    e = pd.DataFrame(extra)
    if not e.empty:
        e = e.groupby(["lead", "model"]).agg(hit=("hit", "sum"), k=("k", "sum"), prec_hit=("prec_hit", "sum"),
                                             pred_n=("pred_n", "sum"), iqr=("iqr", "median"),
                                             iqr_true=("iqr_true", "median"), park_months=("k", "size")).reset_index()
        e["busy_day_recall"] = e["hit"] / e["k"]
        e["busy_day_precision_q75_rule"] = e["prec_hit"] / e["pred_n"]
    return pd.DataFrame(rows), e


def d6_d7(d: Data, cfg: BenchConfig, srcs: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame,
                                                               pd.DataFrame, pd.DataFrame]:
    df = d.df("SELECT * FROM pdl2")
    if df.empty:
        return (pd.DataFrame(),) * 5
    df["true_pct"] = df["true_lvl"] / df["typ"] * 100
    df["true_b"] = bucket_np(df["true_pct"].to_numpy())
    df["true_rank"] = df["true_b"] + np.minimum(0.99, np.maximum(0, df["true_lvl"]) / 120)
    for s in srcs:
        pct = df[s] / df["typ"] * 100
        df[f"b_{s}"] = bucket_np(pct.to_numpy())
        df[f"pct_{s}"] = pct
        df[f"rank_{s}"] = df[f"b_{s}"] + np.minimum(0.99, np.maximum(0, df[s]) / 120)
    # D6 bucket accuracy, paired per park-day
    d6_rows, d6_extra = [], []
    for L in UC4_LEADS:
        g = df[df["L"] == L].copy()
        if g.empty:
            continue
        for s in srcs:
            ok = g[f"b_{s}"].notna()
            g[f"num_{s}"] = np.where(ok, (g[f"b_{s}"] == g["true_b"]).astype(float), 0.0)
            g[f"den_{s}"] = ok.astype(float)
            g[f"pm1_{s}"] = np.where(ok, ((g[f"b_{s}"] - g["true_b"]).abs() <= 1).astype(float), 0.0)
            for r in NAIVE_LEVELS:
                if r != s:
                    both = ok & g[f"b_{r}"].notna()
                    g[f"pnum_{s}__{r}"] = np.where(both, g[f"num_{s}"], 0.0)
                    g[f"pnumr_{s}__{r}"] = np.where(both, (g[f"b_{r}"] == g["true_b"]).astype(float), 0.0)
                    g[f"pden_{s}__{r}"] = both.astype(float)
            busy_t, busy_p = (g["true_b"] >= 3) & ok, (g[f"b_{s}"] >= 3) & ok
            d6_extra.append({"lead": L, "model": s, "park_days": int(ok.sum()),
                             "pm1_acc": g[f"pm1_{s}"].sum() / max(ok.sum(), 1),
                             "busy_recall": (busy_t & busy_p).sum() / max(busy_t.sum(), 1),
                             "busy_precision": (busy_t & busy_p).sum() / max(busy_p.sum(), 1)})
        g["date"] = g["date"].astype(str) + "@" + g["origin"].astype(str)
        d6_rows += _paired_from_units(g, srcs, NAIVE_LEVELS, False,
                                      {"uc": "D6", "lead": L, "segment": "all", "region": "all",
                                       "metric": "crowd bucket exact"}, cfg)
    # cross-park ordering on the same (origin, date): true gap >= 10 points
    cp = []
    for (o, dte, L), g in df[df["L"].isin(UC4_LEADS)].groupby(["origin", "date", "L"]):
        if len(g) < 2:
            continue
        t = g["true_pct"].to_numpy()
        i, j = np.triu_indices(len(g), 1)
        gap = t[i] - t[j]
        keep = np.abs(gap) >= 10
        if not keep.any():
            continue
        for s in srcs:
            p = g[f"pct_{s}"].to_numpy()
            pp = p[i] - p[j]
            okp = keep & np.isfinite(pp)
            cp.append({"L": L, "model": s, "pairs": int(okp.sum()),
                       "correct": int((np.sign(pp[okp]) == np.sign(gap[okp])).sum())})
    cross = pd.DataFrame(cp)
    if not cross.empty:
        cross = cross.groupby(["L", "model"]).sum().reset_index()
        cross["accuracy"] = cross["correct"] / cross["pairs"]
    # D7 day comparison, paired on the same day pairs; units = (origin, park)
    d7_units = {b: [] for b in ("d1-7", "d8-30", "d31-90")}
    star = []
    for (o, park), g in df.groupby(["origin", "park_id"], sort=True):
        g = g.sort_values("date")
        tr = g["true_rank"].to_numpy()
        Ls = g["L"].to_numpy()
        i, j = np.triu_indices(len(g), 1)
        tgap = tr[i] - tr[j]
        clear = np.abs(tgap) >= cfg.day_compare_clear
        maxL = np.maximum(Ls[i], Ls[j])
        res = {}
        for s in srcs:
            r = g[f"rank_{s}"].to_numpy()
            pg = r[i] - r[j]
            have = np.isfinite(pg)
            win = have & (np.abs(pg) >= cfg.day_compare_tie) & (np.sign(pg) == np.sign(tgap))
            tie = have & (np.abs(pg) < cfg.day_compare_tie)
            res[s] = (have, win, tie)
        for lb, lo_, hi_ in (("d1-7", 1, 7), ("d8-30", 8, 30), ("d31-90", 31, 90)):
            sel = clear & (maxL >= lo_) & (maxL <= hi_)
            if not sel.any():
                continue
            u = {"park_id": park, "date": str(o)}
            for s in srcs:
                have, win, tie = res[s]
                u[f"num_{s}"], u[f"den_{s}"] = float((sel & win).sum()), float((sel & have).sum())
                u[f"tie_{s}"] = float((sel & tie).sum())
                for r in NAIVE_LEVELS:
                    if r != s:
                        both = sel & have & res[r][0]
                        u[f"pnum_{s}__{r}"] = float((both & win).sum())
                        u[f"pnumr_{s}__{r}"] = float((both & res[r][1]).sum())
                        u[f"pden_{s}__{r}"] = float(both.sum())
            d7_units[lb].append(u)
        if pd.Timestamp(o).day not in (1, 15):
            continue
        tm = (pd.Timestamp(o) + pd.Timedelta(days=30)).to_period("M")
        mm = g[pd.to_datetime(g["date"]).dt.to_period("M") == tm]
        for s in srcs:
            c = mm[mm[f"rank_{s}"].notna() & mm["true_rank"].notna()]
            if len(c) < cfg.star_min_candidates:
                continue
            ps = c[f"rank_{s}"] <= c[f"rank_{s}"].median() - cfg.star_margin
            ts = c["true_rank"] <= c["true_rank"].median() - cfg.star_margin
            star.append({"park_id": park, "date": str(o), "model": s, "pred_star": int(ps.sum()),
                         "true_star": int(ts.sum()), "both": int((ps & ts).sum())})
    d7_rows, ties = [], []
    for lb, us in d7_units.items():
        if not us:
            continue
        U = pd.DataFrame(us).fillna(0.0)
        d7_rows += _paired_from_units(U, srcs, NAIVE_LEVELS, False,
                                      {"uc": "D7", "lead": lb, "segment": "all", "region": "all",
                                       "metric": "day comparison winner accuracy"}, cfg,
                                      unit="park-origin")
        for s in srcs:
            if f"den_{s}" in U and U[f"den_{s}"].sum() > 0:
                ties.append({"lead": lb, "model": s, "tie_rate": U[f"tie_{s}"].sum() / U[f"den_{s}"].sum()})
    st = pd.DataFrame(star)
    star_rows = []
    if not st.empty:
        for s, g in st.groupby("model"):
            g = g.sort_values(["park_id", "date"])
            v, lo, hi = ratio_ci(g["both"].to_numpy(), g["pred_star"].to_numpy(), cfg.bootstrap_reps,
                                 cfg.bootstrap_seed)
            rv, rlo, rhi = ratio_ci(g["both"].to_numpy(), g["true_star"].to_numpy(), cfg.bootstrap_reps,
                                    cfg.bootstrap_seed)
            star_rows.append({"model": s, "star_precision": ci(v, lo, hi), "star_recall": ci(rv, rlo, rhi),
                              "pred_stars": int(g["pred_star"].sum()), "true_stars": int(g["true_star"].sum())})
    return (pd.DataFrame(d6_rows + d7_rows), pd.DataFrame(d6_extra), cross, pd.DataFrame(ties),
            pd.DataFrame(star_rows))


# --------------------------------------------------------------------------- inputs / availability

def coverage_horizon(d: Data) -> pd.DataFrame:
    pq = d.export / "parquet"
    rows = []
    for name in ("tft", "cbd"):
        r = d.df(f"""SELECT max(target_date - forecast_date) max_lead, max(forecast_date) last_run
                      FROM read_parquet('{pq}/{name}.parquet')
                      WHERE forecast_date >= (SELECT max(forecast_date) - 7 FROM read_parquet('{pq}/{name}.parquet'))""")
        rows.append({"input": {"tft": "TFT daily level", "cbd": "CatBoost daily level"}[name],
                     "horizon_days": int(r["max_lead"].iloc[0]) if pd.notna(r["max_lead"].iloc[0]) else None,
                     "note": f"last run {r['last_run'].iloc[0]}; served only if <= 3 days stale"})
    w = d.df(f"""SELECT max(date) mx, min(date) mn FROM read_parquet('{pq}/weather.parquet')
                 WHERE data_type = 'forecast'""")
    rows.append({"input": "weather forecast (Open-Meteo)", "horizon_days": 16,
                 "note": f"rows {w['mn'].iloc[0]}..{w['mx'].iloc[0]} only; NO archive -> only actuals exist "
                         "(ORACLE, stripped unless a plug-in opts in)"})
    s = d.df(f"""WITH m AS (SELECT park_id, max(date) mx FROM read_parquet('{pq}/windows.parquet') GROUP BY park_id)
                 SELECT quantile_cont(mx - (SELECT max(date) FROM read_parquet('{pq}/truth.parquet')), 0.1) p10,
                        quantile_cont(mx - (SELECT max(date) FROM read_parquet('{pq}/truth.parquet')), 0.5) p50,
                        quantile_cont(mx - (SELECT max(date) FROM read_parquet('{pq}/truth.parquet')), 0.9) p90
                 FROM m""")
    rows.append({"input": "published operator schedule (per park)", "horizon_days": int(s["p50"].iloc[0]),
                 "note": f"p10 {s['p10'].iloc[0]:.0f} d, median {s['p50'].iloc[0]:.0f} d, p90 {s['p90'].iloc[0]:.0f} d "
                         "past the last truth day; a window last written after the origin is treated as "
                         "unknown and projected (schedule_entries.updatedAt)"})
    return pd.DataFrame(rows)


def lead_availability(cfg: BenchConfig, first_truth: dt.date, last_truth: dt.date,
                      target_from: str | None = None, target_to: str | None = None,
                      origin_from: str | None = None, origin_to: str | None = None) -> pd.DataFrame:
    """Origin days per lead FOR THIS PASS.

    A target day ``d`` is scored at lead ``L`` only if its origin ``d − L`` exists,
    i.e. ``d ≥ first_origin + L``. In a windowed pass ``d`` is additionally confined
    to the target window, which is why the windowed table must not be a copy of the
    full-period one: the headline window gives 54 origin days at every lead up to
    d177 and fewer beyond (critic B4).
    """
    rows = []
    first_origin = first_truth + dt.timedelta(days=cfg.window_days)
    lo = max(first_origin, dt.date.fromisoformat(target_from)) if target_from else first_origin
    hi = min(last_truth, dt.date.fromisoformat(target_to)) if target_to else last_truth
    o0 = max(first_origin, dt.date.fromisoformat(origin_from)) if origin_from else first_origin
    o1 = min(last_truth, dt.date.fromisoformat(origin_to)) if origin_to else last_truth
    for L in sorted(set(cfg.slot_leads) | set(UC4_LEADS)):
        dL = dt.timedelta(days=L)
        d0 = max(lo, first_origin + dL, o0 + dL)
        n = max((min(hi, o1 + dL) - d0).days + 1, 0)
        status = "ok" if n >= cfg.low_n_origin_days else ("LOW-N" if n > 0 else "not measurable yet")
        when = first_origin + dt.timedelta(days=L + cfg.low_n_origin_days - 1)
        rows.append({"lead": L, "origin_days": n, "status": status,
                     "first_target_day": d0.isoformat() if n else "",
                     "last_target_day": hi.isoformat() if n else "",
                     "measurable_from": when.isoformat() if n < cfg.low_n_origin_days else ""})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- markdown

def md(df: pd.DataFrame, cols: list[str] | None = None, floatfmt: str = "{:.2f}", max_rows: int = 400) -> str:
    if df is None or df.empty:
        return "_no rows_\n"
    df = df[[c for c in cols if c in df.columns]] if cols else df
    df = df.head(max_rows)

    def f(v):
        if isinstance(v, (float, np.floating)):
            return "" if not np.isfinite(v) else floatfmt.format(v)
        return str(v)

    head = "| " + " | ".join(map(str, df.columns)) + " |"
    sep = "|" + "---|" * len(df.columns)
    body = ["| " + " | ".join(f(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join([head, sep, *body]) + "\n"


def ci(v, lo, hi) -> str:
    if v is None or not np.isfinite(v):
        return ""
    if not (np.isfinite(lo) and np.isfinite(hi)):
        return f"{v:.2f}"
    return f"{v:.2f} [{lo:.2f}, {hi:.2f}]"


def wide(cells: pd.DataFrame, order: dict, kind: str = "value", models: list[str] | None = None) -> pd.DataFrame:
    """kind 'value': value [park-day CI]; 'diff': paired diff [park-cluster CI], * = wins."""
    if cells.empty:
        return cells
    c = cells.copy()
    if kind == "value":
        c["cell"] = [ci(a, b, d_) for a, b, d_ in zip(c["value"], c["lo"], c["hi"])]
    else:
        c = c.dropna(subset=["diff_vs_ref"]) if "diff_vs_ref" in c else c.iloc[0:0]
        if c.empty:
            return c
        c["cell"] = [ci(a, b, d_) + ("*" if w else "") for a, b, d_, w in
                     zip(c["diff_vs_ref"], c["diff_lo_park"], c["diff_hi_park"], c["wins"].fillna(False))]
    w = c.pivot_table(index="lead", columns="model", values="cell", aggfunc="first")
    if models:
        w = w[[m for m in models if m in w.columns]]
    w = w.reset_index()
    w["o"] = w["lead"].map(order)
    w = w.sort_values("o").drop(columns="o")
    if kind == "diff":
        refs = cells.groupby("lead")["ref"].first()
        w["ref"] = w["lead"].map(refs)
    return w


def ci_both(v, plo, phi, lo, hi, win: bool, win_pd: bool) -> str:
    """``diff [park-cluster] / [park-day]`` with ``*`` per unit, and ``!`` when the two
    bootstrap units disagree about the win (BENCH-SPEC says park-day, the harness
    decides on park-cluster — critic B5)."""
    if v is None or not np.isfinite(v):
        return ""
    s = f"{v:.3f} [{plo:.3f}, {phi:.3f}]{'*' if win else ''}" if np.isfinite(plo) else f"{v:.3f}"
    if np.isfinite(lo):
        s += f" / [{lo:.3f}, {hi:.3f}]{'*' if win_pd else ''}"
    return s + ("  !" if bool(win) != bool(win_pd) else "")


def wide_vs(ar: pd.DataFrame, order: dict, metric: str, uc: str, seg: str, candidate: str,
            models: list[str] | None = None, region: str = "all") -> pd.DataFrame:
    """lead x model table of the paired difference against ONE named naive candidate,
    with both CIs. This is what makes "no model beats the best-of-N envelope" and
    "against `wt_med` the result is …" separable statements (critic B1c, B5)."""
    if ar is None or ar.empty:
        return pd.DataFrame()
    c = ar[(ar["metric"] == metric) & (ar["uc"] == uc) & (ar["segment"] == seg)
           & (ar["region"] == region) & (ar["ref_candidate"] == candidate)]
    if c.empty:
        return pd.DataFrame()
    c = c.copy()
    c["cell"] = [ci_both(a, b, d_, e, f_, w, wp) for a, b, d_, e, f_, w, wp in
                 zip(c["diff_vs_ref"], c["diff_lo_park"], c["diff_hi_park"], c["diff_lo"],
                     c["diff_hi"], c["wins"].fillna(False), c["wins_park_day"].fillna(False))]
    w = c.pivot_table(index="lead", columns="model", values="cell", aggfunc="first")
    if models:
        w = w[[m for m in models if m in w.columns]]
    w = w.reset_index()
    w["o"] = w["lead"].map(order)
    return w.sort_values("o").drop(columns="o")


# --------------------------------------------------------------------------- main

def build_report(run: Path, export: Path | None = None, target_from: str | None = None,
                 target_to: str | None = None, reps: int | None = None,
                 memory: str | None = None, threads: int | None = None,
                 origin_from: str | None = None, origin_to: str | None = None) -> Path:
    cfg = BenchConfig()
    if reps:
        cfg.bootstrap_reps = reps
    BOOT.clear()
    ALL_REF_ROWS.clear()
    REF_CHOICES.clear()
    d = Data(run, export, target_from, target_to, memory=memory or REPORT_MEMORY,
             threads=threads or REPORT_THREADS, origin_from=origin_from, origin_to=origin_to)
    windowed = bool(target_from or target_to)
    name_parts = []
    if windowed:
        name_parts.append(f"{target_from}_{target_to}")
    if origin_from or origin_to:
        name_parts.append(f"o{origin_from or 'start'}_{origin_to or 'end'}")
    tdir = run / ("tables_" + "_".join(name_parts) if name_parts else "tables")
    tdir.mkdir(parents=True, exist_ok=True)
    out: list[str] = []
    w = out.append
    stats_p = d.export / "parquet" / "build_stats.json"
    build_stats = json.loads(stats_p.read_text()) if stats_p.exists() else {}
    manifest_p = d.export / "manifest.json"
    manifest = json.loads(manifest_p.read_text()) if manifest_p.exists() else {}
    first_truth = dt.date.fromisoformat(build_stats.get("truth_dates", ["2025-12-24"])[0][:10])
    last_truth = dt.date.fromisoformat(build_stats.get("truth_dates", ["", "2026-10-08"])[1][:10])
    if manifest.get("queue_to"):
        last_truth = min(last_truth, dt.date.fromisoformat(manifest["queue_to"]) - dt.timedelta(days=1))
    if manifest.get("queue_from"):
        first_truth = max(first_truth, dt.date.fromisoformat(manifest["queue_from"]) + dt.timedelta(days=1))

    daily_keys = sorted(set(cfg.slot_leads))
    order = {**{k: i for i, k in enumerate(daily_keys)}, **{k: 100 + i for i, k in enumerate(UC1_KEYS)},
             **{k: 200 + i for i, k in enumerate(UC2_INTRADAY_KEYS)},
             **{k: 300 + i for i, k in enumerate(UC4_LEADS)},
             "0-60": 400, "0-120": 401, "w0-45": 500, "d1-7": 600, "d8-30": 601, "d31-90": 602}
    lead_av = lead_availability(cfg, first_truth, last_truth, target_from, target_to,
                                origin_from, origin_to)
    cov = coverage_horizon(d)

    _log("slot MAE")
    cells = []
    if "slot" in d.tables:
        cells.append(run_cells(d, cfg, "slot", "L", daily_keys, B.REF_CANDIDATES,
                               lambda L: "UC2" if L == 0 else "UC3"))
        d1 = cells[-1][cells[-1]["lead"] == 1].assign(uc="UC2")
        cells.append(d1)
        _log("first hour / schedule split")
        cells.append(run_cells(d, cfg, "slot", "L", daily_keys, B.REF_CANDIDATES, lambda L: "D5",
                               extra="fh", segments=(("all", "TRUE"),), regions=("all",),
                               metric="first-hour MAE (opening-aligned)"))
        for sk, lab in (("sk", "known"), ("NOT sk", "projected")):
            cells.append(run_cells(d, cfg, "slot", "L", daily_keys, B.REF_CANDIDATES, lambda L: "UC3",
                                   extra=sk, segments=(("all", "TRUE"),), regions=("all",),
                                   metric=f"MAE, schedule {lab} at origin"))
            cells.append(run_cells(d, cfg, "slot", "L", daily_keys, B.REF_CANDIDATES, lambda L: "D5",
                                   extra=f"fh AND {sk}", segments=(("all", "TRUE"),), regions=("all",),
                                   metric=f"first-hour MAE, schedule {lab} at origin"))
    if "intraday" in d.tables:
        _log("intraday")
        cells.append(run_cells(d, cfg, "intraday", "lead_key", UC1_KEYS + UC2_INTRADAY_KEYS, B.INTRADAY_REFS,
                               lambda k: "UC1" if k.startswith("m") else "UC2-intraday"))
        base = slot_base(d, "intraday", "hl AND lead_key IN ('m015','m030','m045')", B.INTRADAY_REFS)
        cells.append(pd.DataFrame(evaluate(base, cfg, B.INTRADAY_REFS, True,
                                           {"uc": "D2", "lead": "w0-45", "segment": "headliners",
                                            "region": "all", "metric": "live-window MAE (headliners)"})))

    _log("ride-day decisions")
    if "rideday" in d.tables:
        specs = [("D3", "best-time hit rate (top-2)", "hit2_m", "bt_n", "hit2_r", "bt_n", False),
                 ("D3", "best-time hit rate (±30 min)", "hit30_m", "bt_n", "hit30_r", "bt_n", False),
                 ("D3", "best-time regret (min)", "regret_m", "bt_n", "regret_r", "bt_n", True),
                 ("UC2/UC3", "Spearman of slots within ride-day", "sp_m", "sp_n", "sp_r", "sp_n", False),
                 ("UC3", "dayPeak abs error (min)", "dp_m", "dp_n", "dp_r", "dp_n", True),
                 ("D5", "rope-drop worth agreement", "w_m", "w_n", "w_r", "w_n", False)]
        for uc, metric, nm, dm, nr, dr, lower in specs:
            # `prod_ropedrop_hist` (the production rule on the window medians) used to be
            # listed as a reference candidate for the worth decision, but the runner
            # materialises paired sums only for B.REF_CANDIDATES, so no model has a
            # PAIRED comparison against it and it can never be part of the envelope.
            # Listing it as a candidate therefore only made `ref_choice.csv` disagree
            # with `envelope`. Its own value is still reported as a model. Scoring
            # models against it properly needs it in `rideday_pair_sql`'s ref list,
            # which is a re-run — tracked as a follow-up, not done here.
            refs = B.REF_CANDIDATES
            cells.append(decision_cells(d, cfg, f"""
                SELECT L, park_id, date, model, ref, sum({nm}) num_m, sum({dm}) den_m,
                       sum(coalesce({nr}, 0)) num_r, sum({dr}) den_r
                FROM rideday GROUP BY ALL ORDER BY L, park_id, date, model, ref""",
                "L", metric, uc, lower, refs))
    if "pairs" in d.tables:
        cells.append(decision_cells(d, cfg, """SELECT L, park_id, date, model, ref, sum(pw_m) num_m,
            sum(pw_n) den_m, sum(pw_r) num_r, sum(pw_n) den_r FROM pairs WHERE pw_n > 0 GROUP BY ALL
            ORDER BY ALL""", "L", "dayPeak pairwise ordering (headliners)", "D4", False, B.REF_CANDIDATES))
        cells.append(decision_cells(d, cfg, """SELECT L, park_id, date, model, ref, sum(dsp_m) num_m,
            sum(dsp_n) den_m, sum(dsp_r) num_r, sum(dsp_n) den_r FROM pairs WHERE dsp_n > 0 GROUP BY ALL
            ORDER BY ALL""", "L", "dayPeak Spearman across rides", "UC3", False, B.REF_CANDIDATES))
    if "optim" in d.tables:
        cells.append(decision_cells(d, cfg, """
            WITH o AS (SELECT L, park_id, date, model, plan_cost - opt_cost AS rg FROM optim
                       WHERE plan_cost IS NOT NULL AND isfinite(plan_cost))
            SELECT a.L, a.park_id, a.date, a.model, b.model AS ref, a.rg num_m, 1 den_m, b.rg num_r, 1 den_r
            FROM o a JOIN o b USING (L, park_id, date)
            WHERE b.model IN ('snaive7', 'wt_med', 'clim') OR b.model = a.model
            ORDER BY ALL""", "L", "optimiser regret (min per park-day)", "D4", True, B.REF_CANDIDATES))
    if "nextbest" in d.tables:
        for metric, num, den in (("next-best precision", "n_ok", "n_sug"),
                                 ("next-best precision (top-3)", "n_ok3", "n_sug3"),
                                 ("next-best recall", "n_ok", "n_opp")):
            cells.append(decision_cells(d, cfg, f"""
                WITH n AS (SELECT bucket, park_id, date, model, sum({num}) nu, sum({den}) de FROM nextbest
                           GROUP BY ALL)
                SELECT a.bucket, a.park_id, a.date, a.model, b.model AS ref, a.nu num_m, a.de den_m,
                       b.nu num_r, b.de den_r
                FROM n a JOIN n b USING (bucket, park_id, date)
                WHERE b.model IN ('snaive7', 'wt_med', 'clim') OR b.model = a.model ORDER BY ALL""",
                "bucket", metric, "D1", False, ["snaive7", "wt_med", "clim"]))

    _log("UC4 / D6 / D7")
    uc4_extra = d6x = d6e = d7t = d7s = pd.DataFrame()
    srcs = crowd_levels(d, cfg)
    if srcs:
        u4, uc4_extra = uc4(d, cfg, srcs)
        cells.append(u4)
        d67, d6e, d6x, d7t, d7s = d6_d7(d, cfg, srcs)
        cells.append(d67)
        cov_lvl = d.df(f"""SELECT L, {', '.join(f'count({s}) FILTER (WHERE true_p90 IS NOT NULL) / '
                                                   f'nullif(count(true_p90), 0) AS {s}' for s in srcs)},
                              count(true_p90) n_ride_days FROM levels
                           WHERE L IN ({','.join(map(str, UC4_LEADS))}) GROUP BY L ORDER BY L""")
    else:
        cov_lvl = pd.DataFrame()
    C = pd.concat([c for c in cells if c is not None and not c.empty], ignore_index=True)
    C.to_csv(tdir / "cells.csv", index=False)
    # the same cells against EVERY reference candidate, not only the selected one,
    # plus the per-cell choice and its reason (critic B1)
    AR = pd.DataFrame(ALL_REF_ROWS)
    AR.to_csv(tdir / "cells_all_refs.csv", index=False)
    pd.DataFrame(REF_CHOICES).to_csv(tdir / "ref_choice.csv", index=False)
    low_coverage_cells(C).to_csv(tdir / "low_coverage_cells.csv", index=False)

    _log("D8 / D9")
    d8q = pd.DataFrame()
    if "slot" in d.tables:
        qm = [c[len("qn__"):] for c in d.cols("slot") if c.startswith("qn__")]
        if qm:
            d8q = d.df(f"""SELECT L, {', '.join(f'sum(q80__{m}) / nullif(sum(qn__{m}), 0) AS "{m} cov_q80", '
                                                f'sum(q95__{m}) / nullif(sum(qn__{m}), 0) AS "{m} cov_q95", '
                                                f'sum(qn__{m}) / nullif(sum(n_truth), 0) AS "{m} field_coverage"'
                                                for m in qm)} FROM slot GROUP BY L ORDER BY L""")
    d8p = pd.DataFrame()
    if "d8tft" in d.tables:
        d8p = d.df("""
            WITH st AS (SELECT origin, band, bucket, sae / n AS stated FROM d8tft WHERE kind = 'stated'),
            rl AS (SELECT origin, band, bucket, n, sae FROM d8tft WHERE kind = 'realised')
            SELECT rl.bucket, rl.band, sum(rl.n) AS n, sum(rl.sae) / sum(rl.n) AS realised_mae,
                   sum(st.stated * rl.n) / sum(rl.n) AS stated_mae,
                   (sum(rl.sae) / sum(rl.n)) / (sum(st.stated * rl.n) / sum(rl.n)) AS ratio
            FROM rl JOIN st USING (origin, band, bucket) GROUP BY ALL ORDER BY bucket, band""")
    d8t = pd.DataFrame()
    if "slot" in d.tables:
        frames = []
        for m in [c[3:] for c in d.cols("slot") if c.startswith("n__")]:
            if m in NON_COMPETING:
                continue
            frames.append(d.df(f"""
              WITH pdm AS (SELECT L, park_id, date, sum(sae__{m}) s, sum(n__{m}) n FROM slot GROUP BY ALL),
              st AS (SELECT a.L, a.park_id, a.date, a.s, a.n, sum(b.s) / nullif(sum(b.n), 0) stated
                     FROM pdm a JOIN pdm b ON b.L = a.L AND b.park_id = a.park_id
                       AND b.date < a.date - a.L AND b.date >= a.date - a.L - 28
                     WHERE a.n > 0 GROUP BY a.L, a.park_id, a.date, a.s, a.n)
              SELECT '{m}' model, L, sum(s) / sum(n) realised, sum(stated * n) / sum(n) stated_weighted
              FROM st WHERE stated IS NOT NULL GROUP BY L ORDER BY L"""))
        d8t = pd.concat(frames) if frames else d8t
        if not d8t.empty:
            d8t["ratio"] = d8t["realised"] / d8t["stated_weighted"]
    opn = pd.DataFrame()
    if "openness" in d.tables:
        models = [c[len("op_f__"):] for c in d.cols("openness") if c.startswith("op_f__")]
        opn = d.df(f"""SELECT L, {', '.join(
            f'sum(op_f__{m}) / nullif(sum(op_f__{m} + op_nf__{m}), 0) AS "{m} P(fc|operated)", '
            f'sum(cl_f__{m}) / nullif(sum(cl_f__{m} + cl_nf__{m}), 0) AS "{m} P(fc|not operated)"' for m in models)},
            sum(op_f__wt_med + op_nf__wt_med) operated_ride_days,
            sum(cl_f__wt_med + cl_nf__wt_med) not_operated_ride_days
            FROM openness GROUP BY L ORDER BY L""")
    seasons = pd.DataFrame()
    if "slot" in d.tables:
        models = [c[3:] for c in d.cols("slot") if c.startswith("n__")]
        agg = ", ".join(f"sum(sae__{m}) / nullif(sum(n__{m}), 0) AS \"{m}\"" for m in models)
        seasons = d.df(f"""SELECT L, CASE WHEN month(date) IN (12,1,2) THEN 'winter' WHEN month(date) IN (3,4,5)
                   THEN 'spring' WHEN month(date) IN (6,7,8) THEN 'summer' ELSE 'autumn' END season,
                   sum(n_truth) n, {agg} FROM slot WHERE L IN (0, 1, 3, 7, 14, 30) GROUP BY ALL ORDER BY L, season""")

    uh = usable_horizon(C, order, cfg)
    ho = handover(C, cfg)
    for name, t in (("lead_availability", lead_av), ("coverage_horizon", cov), ("usable_horizon", uh),
                    ("handover", ho), ("uc4_busy_days", uc4_extra), ("d6_extra", d6e),
                    ("d6_cross_park_ordering", d6x), ("d7_tie_rate", d7t), ("d7_star", d7s),
                    ("d8_quantile_coverage", d8q), ("d8_production_expected_error", d8p),
                    ("d8_trailing28", d8t), ("d9_openness", opn), ("d9_level_coverage", cov_lvl),
                    ("mae_by_season", seasons)):
        t.to_csv(tdir / f"{name}.csv", index=False)
    C[(C["metric"] == "MAE") & (C["region"] == "all")].to_csv(tdir / "horizon_curve_mae.csv", index=False)
    C[C["metric"] != "MAE"].to_csv(tdir / "decision_metrics.csv", index=False)

    # ================= summary.md
    _log("summary.md")
    w("# ml-bench results\n")
    w(f"Run: `{run.name}` · generated {dt.datetime.now(dt.timezone.utc).isoformat(timespec='minutes')}")
    if windowed:
        w(f"\n**Target window {target_from} … {target_to}: every lead below is scored on the SAME target "
          "days**, so two leads are compared on the same days' outcomes (with the full period, lead L "
          "starts at first origin + L, and TFT/CatBoost only exist from late May).\n")
        w("**This does NOT remove the lead–season confound.** A lead-L cell inside a fixed target window "
          "is scored from origins at `target − L`, so the short leads come from the END of the window and "
          "the long leads from before it: the decay along the lead axis still contains an origin-season "
          "gradient. Fixing the target window pins when the truth happened, not when the forecast was "
          "made. Use `--origin-from/--origin-to` to fix the origin window instead, and read the "
          "per-season tables before reading a lead boundary as a number.\n")
    else:
        w("\nFull evaluation period. Leads start at different target days here (first origin + L), so the "
          "curves mix lead with TARGET season. The common-window pass mixes lead with ORIGIN season "
          "instead. Both are confounded; neither alone identifies the horizon curve.\n")
    if origin_from or origin_to:
        w(f"\n**Origin window {origin_from or 'start'} … {origin_to or 'end'}**: only forecasts made in "
          "this window are scored.\n")
    shas = sorted({m.get("git_sha", "?") for m in d.meta})
    w(f"- code: git `{', '.join(shas)}`, sources sha256 "
      f"`{', '.join(sorted({str(m.get('code_sha256', '?'))[:16] for m in d.meta}))}`, image "
      f"`{', '.join(sorted({str(m.get('image_id', '?'))[:19] for m in d.meta}))}`; plug-ins "
      f"{sorted({x for m in d.meta for x in m.get('models', [])}) or 'none'}")
    w(f"- export: `{d.export}` (finished {manifest.get('export_finished_utc', '?')}); truth "
      f"{build_stats.get('truth_dates')}, {build_stats.get('truth_rides')} rides, {build_stats.get('truth_parks')} "
      f"parks, {build_stats.get('truth')} truth slots")
    secs = [m.get("seconds") for m in d.meta if m.get("seconds")]
    if secs:
        w(f"- runtime: {sum(secs) / 60:.1f} shard-minutes over {len(d.meta)} shard(s)")
    w(f"- Values: point [95 % park-day bootstrap, {cfg.bootstrap_reps} reps]. Paired difference vs the per-lead "
      "reference: point [95 % **park-cluster** bootstrap] — `*` = the model wins (that CI excludes 0 in its "
      "favour). Park-day CIs of the differences are in `cells.csv`.")
    w("- **The win criterion is the park-CLUSTER bootstrap, not BENCH-SPEC's literal park-day unit.** Days of "
      "one park are not independent, so the park-day CI is too narrow; the deviation is recorded in "
      "BENCH-SPEC.md. Both CIs are in `cells.csv` (`diff_lo/hi` = park-day, `diff_lo_park/hi_park` = "
      "park-cluster) and in the `… vs each naive` tables below, where `!` marks a cell on which the two "
      "units disagree about the win.")
    w(f"- **A win also needs ≥ {cfg.min_bootstrap_units} distinct bootstrap units and a strictly positive CI "
      "width.** With one unit the resampled ratio is weight-independent, so the CI collapses to zero width "
      "and excludes 0 for free.")
    w("- **A win means the model beats EVERY naive candidate at that lead** (the \"naive envelope\"), each "
      "paired on its own shared rows. Not \"beats the selected reference\": a single selected reference is "
      "a choice made on the data, and every rule for making it errs in one direction — an argmin over the "
      "candidates' own-coverage values picks on noise and understates models (their gaps here are an order "
      "of magnitude inside the CI, so the reference flipped along the lead axis), a ladder tie-break picks "
      "the simplest candidate and overstates them. A conjunction over a FIXED candidate set selects "
      "nothing and is strictly harder than beating any single member. `handover.csv` reports the margin "
      "against the hardest candidate (`hardest_naive`), and winners are ranked by it.")
    w("- **The `ref` column is a DISPLAY reference only**: BENCH-SPEC's ladder (persistence → "
      "seasonal-naive → weekday-median → climatology), overridden only when another candidate beats it in "
      "a paired comparison with the park-cluster CI excluding 0. `ref_choice.csv` records the candidates, "
      "their values and why each one was chosen. `cells.csv` keeps `wins_vs_ref` for that single column.")
    w("- `cells_all_refs.csv` has every (model, reference-candidate) pair, so \"no model beats the best of "
      "the naives\" and \"against `wt_med` the model is X better\" can be read separately. "
      "`low_coverage_cells.csv` lists every cell whose paired rows are < 50 % of the reference's.")
    w("- **A model scored `<name>_owx` used ORACLE weather** (the target day's actuals; no forecast "
      "archive exists). It appears in the value and difference tables as an upper bound and is marked "
      "NOT SERVABLE, but it is excluded from the hand-over table and the usable horizon — those are the "
      "serving router's input.")
    w("- MAE is in minutes on 15-min slots, each model on its own coverage — compare models through the "
      "paired difference, never through two unpaired values.")
    w("- Opening-aligned forecasts use the window KNOWN at the origin: the published one if its schedule row "
      "was last written before the origin, else a projection from the last 56 days. Weather: no forecast "
      "archive exists; baselines use none, plug-ins only by opting in (scored as `<name>_owx`).\n")
    w("## Horizon — the headline\n")
    w("### Lead availability (this pass)\n")
    w(md(lead_av))
    w("### Coverage horizon of the inputs\n")
    w(md(cov))
    w(f"### Usable horizon\n\nLargest lead, contiguous from the model's first scored lead, at which the "
      f"model beats EVERY naive candidate (park-cluster CI, ≥ {cfg.min_bootstrap_units} bootstrap units); "
      f"cells need ≥ {cfg.low_n_origin_days} origin days (≥ {cfg.low_n_park_months} park-months for UC4) "
      f"and paired rows ≥ {int(MIN_COVERAGE * 100)} % of each candidate's. Empty = never.\n")
    if not uh.empty:
        w(md(uh[(uh["usable_horizon"] != "") | (uh["max_significant_lead"] != "")]
             .sort_values(["metric", "uc", "segment", "model"])))
        nt = uh[uh["leads_tested"] == 0]
        if not nt.empty:
            w("\n**Never tested** — `leads_tested = 0` means no lead passed the gates, which is NOT "
              "\"nothing won\". The reason per metric × model:\n")
            w(md(nt.sort_values(["metric", "uc", "model"]),
                 ["metric", "uc", "segment", "model", "unit", "leads_scored", "not_tested_because"]))
    w("### Reference choice per cell\n")
    w("Candidates (coverage ≥ 50 % of the best-covered), their own-coverage values, and why the reference "
      "is the one it is. `reason` says `ladder order` when no candidate significantly beats the "
      "ladder-first one — i.e. the choice was NOT made on the point estimates.\n")
    rc = pd.DataFrame(REF_CHOICES)
    if not rc.empty:
        sel = rc[(rc["region"] == "all") & rc["metric"].isin(["MAE", "first-hour MAE (opening-aligned)"])]
        w(md(sel, ["metric", "uc", "segment", "lead", "candidates", "selected", "reason"]))
    w("### Hand-over table (input for the serving router)\n")
    w("Per use case × lead: among models that win against the reference (and pass the gates), the largest "
      "PAIRED margin; otherwise the reference itself. `park_day_unit_agrees = False` marks a row whose "
      "verdict changes if BENCH-SPEC's literal park-day bootstrap is used instead of the park-cluster one.\n")
    if not ho.empty:
        w(md(ho[ho["metric"] == "MAE"],
             ["uc", "segment", "lead", "reference", "winner", "winner_value", "paired_margin",
              "margin_lo_park", "margin_hi_park", "margin_lo_park_day", "margin_hi_park_day",
              "park_day_unit_agrees", "winner_paired_share", "significant_models", "n_tested",
              "n_origin_days"], floatfmt="{:.3f}"))
        w("Decision metrics:\n")
        w(md(ho[ho["metric"] != "MAE"],
             ["metric", "uc", "lead", "reference", "winner", "winner_value", "paired_margin",
              "margin_lo_park", "margin_hi_park", "margin_lo_park_day", "margin_hi_park_day",
              "park_day_unit_agrees", "winner_paired_share", "significant_models", "n_tested"],
             floatfmt="{:.3f}"))
    lc = low_coverage_cells(C)
    if not lc.empty:
        w("### Low-coverage cells (paired rows < 50 % of the reference's)\n")
        w("These absolute values are not comparable with the rest of their column. "
          "`reaches_handover` = the cell still passes the 30 % gate and CAN appear as a winner.\n")
        w(md(lc, floatfmt="{:.3f}", max_rows=200))
    mae = C[(C["metric"] == "MAE") & (C["region"] == "all")]
    w("### Level vs shape by lead (MAE, all rides)\n")
    w(md(wide(mae[(mae["uc"] == "UC3") & (mae["segment"] == "all")], order,
              models=["lvlh5_naive", "lvlh5_tft", "oracle_level", "oracle_shape", "h5", "wt_med"])))
    w("`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape "
      "scaled to the naive level (perfect shape).\n")
    for uc, title in (("UC1", "UC1 — live / next 2 h (intraday origins)"),
                      ("UC2-intraday", "UC2 — rest of today from intraday origins"),
                      ("UC2", "UC2 — today and tomorrow from the 06:00 origin"),
                      ("UC3", "UC3 — planner day 1…90")):
        w(f"## {title}\n")
        for seg in ("all", "busy"):
            c = mae[(mae["uc"] == uc) & (mae["segment"] == seg)]
            if c.empty:
                continue
            w(f"**MAE, {seg}** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)\n")
            w(md(wide(c, order)))
            w(f"**Paired difference vs the DISPLAY reference, {seg}** (park-cluster CI). `*` means "
              "the model beats the whole naive envelope, not just this one column — see the "
              "per-candidate tables below for the individual verdicts.\n")
            w(md(wide(c, order, "diff")))
            w(f"**Paired difference vs EACH naive candidate, {seg}** — park-cluster CI / park-day CI, "
              "`*` per unit, `!` = the two units disagree. \"Nothing beats the reference\" is a statement "
              "about the best-of-N envelope; these tables are the statement about each individual naive.\n")
            for cand in sorted({x for x in AR["ref_candidate"]} if not AR.empty else set(),
                               key=lambda r: _ladder_rank(r, B.INTRADAY_REFS)):
                t = wide_vs(AR, order, "MAE", uc, seg, cand, models=CANDIDATE_TABLE_MODELS)
                if not t.empty:
                    w(f"vs `{cand}`:\n")
                    w(md(t))
        reg = C[(C["metric"] == "MAE") & (C["uc"] == uc) & (C["segment"] == "all") & (C["region"] != "all")]
        if not reg.empty:
            sel = reg[reg["lead"].isin([0, 1, 3, 7, 14, 30, "m015", "m060", "m120"])]
            w("**MAE by region (all rides)**\n")
            w(md(sel.pivot_table(index=["lead", "region"], columns="model", values="value").reset_index()))
        if uc in ("UC1", "UC2-intraday"):
            w("**Bias**\n")
            w(md(bias_table(d, "intraday", "TRUE", "lead_key", UC1_KEYS + UC2_INTRADAY_KEYS)
                 .query("lead.str.startswith('m')" if uc == "UC1" else "~lead.str.startswith('m')")))
        if uc == "UC3":
            w("**Bias (all rides)**\n")
            w(md(bias_table(d, "slot", "TRUE", "L", daily_keys)))
            for lab in ("known", "projected"):
                c = C[C["metric"] == f"MAE, schedule {lab} at origin"]
                if not c.empty:
                    w(f"**MAE where the opening hours were {lab} at the origin**\n")
                    w(md(wide(c, order, models=["wt_med", "h5", "lvlh5_naive", "lvlh5_tft", "prod_served",
                                                "prod_served_lin"])))
    w("## UC3 — by season (MAE, all rides)\n")
    w(md(seasons))
    w("## UC4 — crowd calendar (daily park level = mean headliner P90)\n")
    u4c = C[C["metric"] == "UC4 Spearman within park-month"]
    w("**Spearman of the daily level within park-month** (paired on the same days)\n")
    w(md(wide(u4c, order)))
    w(md(wide(u4c, order, "diff")))
    w("**Busy days** (true top quartile of the month): recall with ties broken fractionally, precision of the "
      "`≥ predicted q75` rule (ties included, which is what flat predictions cost)\n")
    if not uc4_extra.empty:
        w(md(uc4_extra[uc4_extra["lead"].isin(UC4_LEADS)],
             ["lead", "model", "park_months", "busy_day_recall", "busy_day_precision_q75_rule", "iqr", "iqr_true"]))

    def dec(title: str, metrics: list[str], note: str = "") -> None:
        w(f"## {title}\n")
        if note:
            w(note + "\n")
        for mname in metrics:
            g = C[C["metric"] == mname]
            if g.empty:
                continue
            w(f"**{mname}**\n")
            w(md(wide(g, order)))
            if "diff_vs_ref" in g and g["diff_vs_ref"].notna().any():
                w("paired difference vs reference (park-cluster CI; `*` = wins):\n")
                w(md(wide(g, order, "diff")))

    dec("D1 — next-best ride", ["next-best precision", "next-best precision (top-3)", "next-best recall"],
        "Candidates: every (intraday origin, ride) with a live OPERATING wait — the same set for every model. "
        "Suggest when the forecast maximum within the lookahead (60 or 120 min) is ≥ live + 10 "
        "(`next-best-ride.ts`, walking time 0); correct when the TRUE maximum there is also ≥ live + 10. "
        "False-suggestion rate = 1 − precision.")
    dec("D2 — plan-block live correction (0–45 min, headliners)", ["live-window MAE (headliners)"],
        "Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`): `persistence` "
        "is what the frontend serves in this window.")
    if "intraday" in d.tables:
        w("**live-window bias (headliners)**\n")
        w(md(bias_table(d, "intraday", "hl AND lead_key IN ('m015','m030','m045')", "lead_key", UC1_KEYS)))
    dec("D3 — best time of the day", ["best-time hit rate (top-2)", "best-time hit rate (±30 min)",
                                      "best-time regret (min)"],
        "Paired ride-day by ride-day: only ride-days a model covers in full (every truth slot), ≥ 8 slots and "
        "a non-flat truth. Hit = a true-minimum slot is among the predicted lowest two / within ±30 min of the "
        "predicted best (ties in the truth count as minima); regret = true wait at the predicted best − true "
        "minimum.")
    dec("D4 — planner optimiser", ["optimiser regret (min per park-day)", "dayPeak pairwise ordering (headliners)"],
        f"Regret: the park's top {cfg.optimiser_rides} rides (headliners first, ex-ante level), objective "
        "Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; true cost of the "
        "forecast's plan − truth-optimal cost; paired per park-day. Not simulated: overflow / headliner dropping, "
        "fixed blocks, opensAt floors, early entry, live corrections; waits are read per 15-min slot (the frontend "
        "reads the hourly point today). Ordering: headliner pairs with a true dayPeak gap ≥ 1 min, paired on "
        "the same pairs.")
    d5_head = False
    for cand in sorted({x for x in AR["ref_candidate"]} if not AR.empty else set(),
                       key=lambda r: _ladder_rank(r, B.INTRADAY_REFS)):
        t = wide_vs(AR, order, "first-hour MAE (opening-aligned)", "D5", "all", cand,
                    models=CANDIDATE_TABLE_MODELS)
        if t.empty:
            continue
        if not d5_head:
            w("## D5 first-hour MAE vs EACH naive candidate\n")
            d5_head = True
        w(f"vs `{cand}`:\n")
        w(md(t))
    dec("D5 — rope drop", ["first-hour MAE (opening-aligned)", "first-hour MAE, schedule known at origin",
                          "first-hour MAE, schedule projected at origin", "rope-drop worth agreement"],
        "First hour = the first 4 slots of the PUBLISHED window, paired slot by slot. worth = day peak ≥ 60 ∧ "
        "peak − opening wait ≥ 45 (`rope-drop.util.ts`) per ride-day, paired ride-day by ride-day; "
        "`prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).")
    dec("D6 — crowd bucket per park-day", ["crowd bucket exact"],
        "Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's "
        "month, ≥ 30 days) → the `determineCrowdLevel` ladder; paired per (origin, park-day).")
    w(md(d6e[d6e["lead"].isin([1, 7, 30, 90])] if not d6e.empty else d6e))
    w("Cross-park ordering on the same date (true gap ≥ 10 points):\n")
    w(md(d6x[d6x["L"].isin([1, 7, 30, 90])] if not d6x.empty else d6x))
    dec("D7 — day comparison and calendar star", ["day comparison winner accuracy"],
        "rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin "
        "and park with a true rank gap ≥ 0.5, paired on the same day pairs; a predicted gap < 0.1 is a tie "
        "(counted wrong); CI over (origin, park).")
    w(md(d7t))
    w("Calendar star (rank ≤ month median − 0.5 among ≥ 4 days of the month ~30 days ahead, origins on the "
      "1st and 15th):\n")
    w(md(d7s))
    w("## D8 — uncertainty\n")
    w("**What production states** (`forecast-accuracy.service.ts`): MAE of TFT's predicted peak against the "
      "day's max hourly P90, by predicted band × lead bucket over the 45 days before the origin (cells with "
      "≥ 500 comparisons). Realised = the same error of the TFT forecasts served at the origin; ratio > 1 = "
      "production understates its error.\n")
    w(md(d8p))
    w("Empirical-quantile coverage (target 0.80 / 0.95) and the share of truth slots that HAVE an interval:\n")
    w(md(d8q))
    w("Secondary diagnostic — trailing 28-day slot MAE at the same lead and park as the 'stated' error:\n")
    w(md(d8t[d8t["L"].isin([0, 1, 3, 7, 14, 30, 60, 90])] if not d8t.empty else d8t))
    w("## D9 — coverage and openness\n")
    w("Share of operating truth slots with a forecast, per lead:\n")
    cv = mae[(mae["segment"] == "all") & mae["uc"].isin(["UC2", "UC3"])]
    if not cv.empty:
        w(md(cv.pivot_table(index="lead", columns="model", values="n").div(
            d.df("SELECT L AS lead, sum(n_truth) t FROM slot GROUP BY 1").set_index("lead")["t"], axis=0)
            .reset_index()))
    w("Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):\n")
    w(md(cov_lvl))
    w("Forecast present vs ride operated. The baselines have **no openness logic** — they forecast "
      "whenever their profile exists — so `P(fc|not operated)` only shows how often a ride that never "
      "opened still got a curve; it is the bar a model with an open/closed decision has to beat:\n")
    w(md(opn))
    name = "summary.md" if not windowed else f"summary_{tdir.name}.md"
    (run / name).write_text("\n".join(out))
    return tdir


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--run", required=True, help="results/<run-id>")
    p.add_argument("--export", default=None, help="override the export dir recorded in the run")
    p.add_argument("--target-from", default=None)
    p.add_argument("--target-to", default=None)
    p.add_argument("--reps", type=int, default=None, help="bootstrap reps (default 1000)")
    p.add_argument("--memory", default=REPORT_MEMORY,
                   help=f"DuckDB memory_limit, keep below the container cap (env "
                        f"MLBENCH_REPORT_MEMORY, default {REPORT_MEMORY})")
    p.add_argument("--threads", type=int, default=REPORT_THREADS,
                   help=f"env MLBENCH_REPORT_THREADS, default {REPORT_THREADS}")
    p.add_argument("--origin-from", default=None,
                   help="restrict the ORIGINS as well as the target days: a target window "
                        "pins when the truth happened, not when the forecast was made, so a "
                        "horizon curve over a short target window still mixes origin seasons")
    p.add_argument("--origin-to", default=None)


def main(args: argparse.Namespace) -> int:
    t = build_report(Path(args.run), Path(args.export) if args.export else None, args.target_from,
                     args.target_to, args.reps, args.memory, args.threads,
                     args.origin_from, args.origin_to)
    print(f"tables in {t}")
    return 0
