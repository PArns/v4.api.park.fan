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

Reference per lead (BENCH-SPEC "Horizon"): the best naive candidate at that lead
(persistence → seasonal-naive → weekday-median → climatology), chosen on the
metric itself among candidates covering ≥ 50 % of what the best-covered
candidate covers. Choosing the best of several candidates on the same data
favours the reference slightly (winner's curse), which makes a model's win
conservative, not optimistic.

Gating (hand-over and usable horizon): a cell counts only with ≥ 30 origin days
and when the model's paired rows are ≥ 30 % of the reference's own rows;
winners are ranked by their PAIRED margin, never by the unpaired value.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from . import baselines as B
from .config import UC4_LEADS, BenchConfig
from .stats import Bootstrapper, ratio_ci, significant

UC1_KEYS = ["m015", "m030", "m045", "m060", "m075", "m090", "m105", "m120"]
UC2_INTRADAY_KEYS = ["h2-4", "h4-8", "h8+"]
NON_COMPETING = set(B.ORACLES) | {"prod_ropedrop_hist"}
NAIVE_LEVELS = ["lvl_naive4", "lvl_wt56", "lvl_snaive7", "lvl_clim"]
MIN_COVERAGE = 0.3
# one weight column per unit for the whole report (see stats.Bootstrapper)
BOOT: dict[str, Bootstrapper] = {}


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

    def __init__(self, run: Path, export: Path | None, target_from: str | None, target_to: str | None):
        import duckdb

        self.run = run
        self.con = duckdb.connect()
        # The report runs on the production host, so its DuckDB limits are overridable
        # (``MLBENCH_REPORT_MEMORY`` / ``MLBENCH_REPORT_THREADS``); the previous fixed
        # values are the defaults. A 3 GB limit inside a 3 GB container gets OOM-killed.
        self.con.execute(f"SET threads={int(os.environ.get('MLBENCH_REPORT_THREADS', 4))}")
        self.con.execute(
            f"SET memory_limit='{os.environ.get('MLBENCH_REPORT_MEMORY', '3GB')}'")
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
        flt = []
        if target_from:
            flt.append(f"date >= DATE '{target_from}'")
        if target_to:
            flt.append(f"date <= DATE '{target_to}'")
        where = (" WHERE " + " AND ".join(flt)) if flt else ""
        self.tables = set()
        for t in self.TABLES:
            if list((run / "parts" / t).glob("*.parquet")):
                if t == "d8tft":
                    self.con.execute(f"""CREATE TABLE d8tft AS SELECT * FROM read_parquet(
                        '{run}/parts/d8tft/*.parquet', union_by_name=true)""")
                else:
                    self.con.execute(f"""CREATE TABLE {t} AS SELECT x.*, r.region FROM read_parquet(
                        '{run}/parts/{t}/*.parquet', union_by_name=true) x
                        LEFT JOIN region r USING (park_id) {where}""")
                self.tables.add(t)

    def df(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()

    def cols(self, table: str) -> list[str]:
        return [r[0] for r in self.con.execute(f"DESCRIBE {table}").fetchall()]


# --------------------------------------------------------------------------- paired engine

def evaluate(base: pd.DataFrame, cfg: BenchConfig, refs: list[str], lower: bool, keys: dict) -> list[dict]:
    """``base``: rows park_id, date, model, ref, num_m, den_m, num_r, den_r — one cell
    (one lead, one segment). ``ref == model`` rows carry the model's own figure."""
    if base.empty:
        return []
    own = base[base["model"] == base["ref"]]
    vals = {}
    for m, g in own.groupby("model"):
        if g["den_m"].sum() > 0:
            vals[m] = (g["num_m"].sum() / g["den_m"].sum(), g["den_m"].sum())
    cover_max = max((vals[r][1] for r in refs if r in vals), default=0)
    cands = [(vals[r][0] if lower else -vals[r][0], r) for r in refs
             if r in vals and vals[r][1] >= 0.5 * cover_max]
    ref = min(cands)[1] if cands else None
    out = []
    for m, g in own.groupby("model"):
        if m not in vals:
            continue
        v, lo, hi = _boot("pd", cfg).ratio(g["park_id"].astype(str) + "|" + g["date"].astype(str),
                                           g["num_m"].to_numpy(), g["den_m"].to_numpy())
        row = dict(keys, model=m, value=v, lo=lo, hi=hi, n=float(vals[m][1]), lower=lower,
                   n_park_days=int((g["den_m"] > 0).sum()), n_origin_days=int(g["date"].nunique()), ref=ref)
        if ref and m != ref:
            p = base[(base["model"] == m) & (base["ref"] == ref)]
            p = p[(p["den_m"] > 0) & (p["den_r"] > 0)]
            if len(p):
                cols = ["num_m", "den_m", "num_r", "den_r"]
                d_, dlo, dhi = _boot("pd", cfg).diff(p["park_id"].astype(str) + "|" + p["date"].astype(str),
                                                     *(p[c].to_numpy() for c in cols))
                _, plo, phi = _boot("park", cfg).diff(p["park_id"].astype(str), *(p[c].to_numpy() for c in cols))
                row.update(diff_vs_ref=d_, diff_lo=dlo, diff_hi=dhi, diff_lo_park=plo, diff_hi_park=phi,
                           n_paired=float(p["den_m"].sum()),
                           paired_share=float(p["den_r"].sum()) / vals[ref][1] if ref in vals else np.nan,
                           wins=significant(plo, phi, lower))
        out.append(row)
    return out


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
    the coverage gate breaks the run."""
    out = []
    c = cells[(cells["region"] == "all") & ~cells["model"].isin(NON_COMPETING)]
    for (metric, uc, seg, model), g in c.groupby(["metric", "uc", "segment", "model"]):
        g = g.assign(o=g["lead"].map(order)).sort_values("o")
        contiguous, any_sig, tested, broken = None, None, 0, False
        for _, r in g.iterrows():
            ok = (r.get("ref") != model and pd.notna(r.get("diff_lo_park", np.nan))
                  and r.get("n_origin_days", 0) >= cfg.low_n_origin_days
                  and (r.get("paired_share") or 0) >= MIN_COVERAGE)
            if not ok:
                broken = True
                continue
            tested += 1
            if bool(r.get("wins")):
                any_sig = r["lead"]
                if not broken:
                    contiguous = r["lead"]
            else:
                broken = True
        out.append({"metric": metric, "uc": uc, "segment": seg, "model": model, "leads_tested": tested,
                    "usable_horizon": "" if contiguous is None else str(contiguous),
                    "max_significant_lead": "" if any_sig is None else str(any_sig)})
    return pd.DataFrame(out)


def handover(cells: pd.DataFrame, cfg: BenchConfig) -> pd.DataFrame:
    out = []
    c = cells[(cells["region"] == "all") & ~cells["model"].isin(NON_COMPETING)]
    for (metric, uc, seg, lead), g in c.groupby(["metric", "uc", "segment", "lead"], sort=False):
        ref = g["ref"].dropna().iloc[0] if g["ref"].notna().any() else None
        wins = g["wins"].fillna(False).astype(bool) if "wins" in g else pd.Series(False, index=g.index)
        share = g["paired_share"].fillna(0) if "paired_share" in g else pd.Series(0.0, index=g.index)
        ok = g[wins & (g["n_origin_days"] >= cfg.low_n_origin_days) & (share >= MIN_COVERAGE)]
        lower = bool(g["lower"].iloc[0])
        if len(ok):
            best = ok.sort_values("diff_vs_ref", ascending=lower).iloc[0]
            winner, val, margin, n_win = best["model"], best["value"], best["diff_vs_ref"], len(ok)
        else:
            winner, margin, n_win = ref, 0.0, 0
            val = g.loc[g["model"] == ref, "value"].iloc[0] if ref in set(g["model"]) else np.nan
        out.append({"metric": metric, "uc": uc, "segment": seg, "lead": lead, "reference": ref,
                    "winner": winner, "winner_value": val, "paired_margin": margin,
                    "significant_models": n_win, "n_origin_days": int(g["n_origin_days"].max())})
    return pd.DataFrame(out)


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
                       cfg: BenchConfig, unit_cols=("park_id", "date")) -> list[dict]:
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
    return evaluate(pd.concat(base), cfg, refs, lower, keys)


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
                                    "metric": "UC4 Spearman within park-month"}, cfg)
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
                                       "metric": "day comparison winner accuracy"}, cfg)
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


def lead_availability(cfg: BenchConfig, first_truth: dt.date, last_truth: dt.date) -> pd.DataFrame:
    rows = []
    first_origin = first_truth + dt.timedelta(days=cfg.window_days)
    for L in sorted(set(cfg.slot_leads) | set(UC4_LEADS)):
        n = max((last_truth - first_origin).days - L + 1, 0)
        status = "ok" if n >= cfg.low_n_origin_days else ("LOW-N" if n > 0 else "not measurable yet")
        when = first_origin + dt.timedelta(days=L + cfg.low_n_origin_days - 1)
        rows.append({"lead": L, "origin_days": n, "status": status,
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


# --------------------------------------------------------------------------- main

def build_report(run: Path, export: Path | None = None, target_from: str | None = None,
                 target_to: str | None = None, reps: int | None = None,
                 vs: list[str] | None = None) -> Path:
    cfg = BenchConfig()
    if reps:
        cfg.bootstrap_reps = reps
    BOOT.clear()
    d = Data(run, export, target_from, target_to)
    windowed = bool(target_from or target_to)
    tdir = run / (f"tables_{target_from}_{target_to}" if windowed else "tables")
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
    lead_av = lead_availability(cfg, first_truth, last_truth)
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
            refs = B.REF_CANDIDATES + (["prod_ropedrop_hist"] if "worth" in metric else [])
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

    # --vs <model>: the same paired engine with ONE forced comparator instead of the
    # per-lead reference, so a model can be compared against another model (layout
    # against layout, with against without covariates, candidate against H5). It needs
    # the paired sums `--pair-with` wrote at run time; a comparator without them is
    # reported as missing rather than silently dropped.
    vs_cells: dict[str, pd.DataFrame] = {}
    for cmp_model in vs or []:
        rows = []

        def paired(table: str) -> bool:
            return table in d.tables and any(
                c.startswith("pn__") and c.endswith(f"__{cmp_model}") for c in d.cols(table))

        if paired("slot"):
            _log(f"paired vs {cmp_model}")
            rows.append(run_cells(d, cfg, "slot", "L", daily_keys, [cmp_model],
                                  lambda L: "UC2" if L == 0 else "UC3"))
        if paired("intraday"):
            rows.append(run_cells(d, cfg, "intraday", "lead_key", UC1_KEYS + UC2_INTRADAY_KEYS,
                                  [cmp_model], lambda k: "UC1" if k.startswith("m") else "UC2-intraday"))
        frames = [r for r in rows if r is not None and not r.empty]
        if frames:
            t = pd.concat(frames, ignore_index=True)
            t = t[t["model"] != cmp_model]
            vs_cells[cmp_model] = t
            t.to_csv(tdir / f"cells_vs_{cmp_model}.csv", index=False)
        else:
            vs_cells[cmp_model] = pd.DataFrame()

    _log("D8 / D9")
    d8q = pd.DataFrame()
    if "slot" in d.tables:
        qm = [c[len("qn__"):] for c in d.cols("slot") if c.startswith("qn__")]
        # a model may provide q80 but not q95 (TimesFM 3.0's head stops at 0.9)
        n95 = {m: (f"qn95__{m}" if f"qn95__{m}" in d.cols("slot") else f"qn__{m}") for m in qm}
        if qm:
            d8q = d.df(f"""SELECT L, {', '.join(f'sum(q80__{m}) / nullif(sum(qn__{m}), 0) AS "{m} cov_q80", '
                                                f'sum(q95__{m}) / nullif(sum({n95[m]}), 0) AS "{m} cov_q95", '
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
        w(f"\n**Target window {target_from} … {target_to}: every lead below is scored on the SAME target days**, "
          "so the horizon curves are not confounded with season (with the full period, lead L starts at "
          "first origin + L, and TFT/CatBoost only exist from late May).\n")
    else:
        w("\nFull evaluation period. Leads start at different target days here (first origin + L), so the "
          "curves mix lead with season — the headline horizon curves come from the common-window report.\n")
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
    w("- MAE is in minutes on 15-min slots, each model on its own coverage — compare models through the "
      "paired difference, never through two unpaired values.")
    w("- Opening-aligned forecasts use the window KNOWN at the origin: the published one if its schedule row "
      "was last written before the origin, else a projection from the last 56 days. Weather: no forecast "
      "archive exists; baselines use none, plug-ins only by opting in (scored as `<name>_owx`).\n")
    w("## Horizon — the headline\n")
    w("### Lead availability (full period)\n")
    w(md(lead_av))
    w("### Coverage horizon of the inputs\n")
    w(md(cov))
    w(f"### Usable horizon\n\nLargest lead, contiguous from the model's first scored lead, at which it wins "
      f"(park-cluster CI); cells need ≥ {cfg.low_n_origin_days} origin days and paired rows ≥ "
      f"{int(MIN_COVERAGE * 100)} % of the reference's. Empty = never.\n")
    if not uh.empty:
        w(md(uh[(uh["usable_horizon"] != "") | (uh["max_significant_lead"] != "")]
             .sort_values(["metric", "uc", "segment", "model"])))
    w("### Hand-over table (input for the serving router)\n")
    w("Per use case × lead: among models that win against the reference (and pass the gates), the largest "
      "PAIRED margin; otherwise the reference itself.\n")
    if not ho.empty:
        w(md(ho[ho["metric"] == "MAE"], ["uc", "segment", "lead", "reference", "winner", "winner_value",
                                         "paired_margin", "significant_models", "n_origin_days"]))
        w("Decision metrics:\n")
        w(md(ho[ho["metric"] != "MAE"], ["metric", "uc", "lead", "reference", "winner", "winner_value",
                                         "paired_margin", "significant_models"]))
    for cmp_model, t in vs_cells.items():
        w(f"### Paired against `{cmp_model}` (not the per-lead reference)\n")
        if t.empty:
            w(f"_no paired sums against `{cmp_model}` in this run — re-run with "
              f"`--pair-with {cmp_model}` (the per-slot forecasts are not kept, so this cannot be "
              "recovered from the stored aggregates)._\n")
            continue
        w(f"MAE difference model − `{cmp_model}` on the slots BOTH cover, park-cluster CI, "
          "`*` = the model wins:\n")
        for seg in ("all", "busy"):
            g = t[(t["metric"] == "MAE") & (t["region"] == "all") & (t["segment"] == seg)]
            if g.empty:
                continue
            w(f"segment `{seg}`:\n")
            w(md(wide(g, order, kind="diff")))
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
            w(f"**Paired difference vs the reference, {seg}** (park-cluster CI; `*` = wins)\n")
            w(md(wide(c, order, "diff")))
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
    p.add_argument("--vs", default=None,
                   help="also compare every model PAIRED against these models (comma-separated) "
                        "instead of the per-lead reference; needs `run --pair-with <same models>`")


def main(args: argparse.Namespace) -> int:
    t = build_report(Path(args.run), Path(args.export) if args.export else None, args.target_from,
                     args.target_to, args.reps,
                     [v.strip() for v in (args.vs or "").split(",") if v.strip()])
    print(f"tables in {t}")
    return 0
