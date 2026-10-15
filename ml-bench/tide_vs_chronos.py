#!/usr/bin/env python3
"""Generate the TiDE vs Chronos-2 head-to-head tables from ONE run's CSVs (PAR-869).

Every number in RESULTS-TIDE-VS-CHRONOS.md and in the PR comes from here; nothing is
transcribed by hand. Input is a run directory written by `mlbench run` + two
`mlbench report` passes (`--vs nf_tide,nf_tide_x_h5,chronos2,chronos2_grid,chronos2_x_h5,h5,lvlh5_tft`):

    <run>/tables/                        full period
    <run>/tables_<from>_<to>/            headline window
    <run>/tables*/cells.csv              every cell vs the per-lead naive envelope
    <run>/tables*/cells_vs_<model>.csv   the same cells PAIRED against <model>
    <run>/tables*/cells_all_refs.csv     the same cells vs EVERY naive candidate

Reading rules (the same as every other document in this review):
* `diff` is the PAIRED difference `model − comparator` on identical rows; for MAE and
  regret negative = the model is better, for hit rates / Spearman / accuracy the
  direction is inverted (`lower` column of the harness).
* the interval is the harness's **park-cluster** bootstrap 95 % CI
  (`diff_lo_park` / `diff_hi_park`, parks resampled with replacement) — BENCH-SPEC
  recorded change #1. A verdict needs >= `MIN_UNITS` distinct park clusters and a
  strictly positive CI width (recorded change #2).
* a result counts only if BOTH passes agree; disagreements are printed as such.
* LOW-N (< 30 paired origin days, BENCH-SPEC "Horizon") is flagged, never hidden:
  a 47-origin sample cannot clear it inside the headline window, so the window pass
  is read as corroboration of the full period, not as an independent verdict.

Usage:
    python3 tide_vs_chronos.py --run results/20261012-tide-vs-chronos \
        --window tables_2026-08-15_2026-10-07 [--out <file.md>]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import pathlib
import sys
from pathlib import Path

MIN_UNITS = 10          # BENCH-SPEC recorded change #2
LOW_N_ORIGIN_DAYS = 30  # BENCH-SPEC "Horizon"
MIN_COVERAGE = 0.30     # the harness's publication gate on paired share

# The app's questions, in the order the owner reads them.
LIVE_LEADS = ["m015", "m030", "m045", "m060", "m075", "m090", "m105", "m120"]
INTRADAY_LEADS = ["h2-4", "h4-8", "h8+"]
SLOT_LEADS = [str(d) for d in range(8)]
SEGMENTS = ["all", "busy"]

# `direct` = the harness can pair the two models against EACH OTHER here. `report --vs`
# only runs its paired engine on the slot and the intraday table (report.py, "--vs <model>":
# `if paired("slot")` / `if paired("intraday")`), and the ride-day sums are written against
# `REF_CANDIDATES` alone (`runner.py`: `rideday_pair_sql(B.REF_CANDIDATES, cfg)`), so D3 has
# no model-vs-model paired margin in this run. D3 is therefore reported as both families
# against the SAME naive candidate — an indirect comparison, labelled as one.
QUESTIONS = [
    # (key, uc, metric, leads, label, direct)
    ("uc1", "UC1", "MAE", LIVE_LEADS, "UC1 live / next best ride (15–120 min)", True),
    ("uc2i", "UC2-intraday", "MAE", INTRADAY_LEADS, "UC2 rest of day (intraday origins)", True),
    ("uc2", "UC2", "MAE", ["0", "1"], "UC2 planner today / tomorrow (06:00 origin)", True),
    ("uc3", "UC3", "MAE", SLOT_LEADS, "UC2/UC3 planner slot curve d0–d7", True),
    ("d3hit", "D3", "best-time hit rate (top-2)", SLOT_LEADS, "D3 best visit time — hit rate (top-2)", False),
    ("d3pm", "D3", "best-time hit rate (±30 min)", SLOT_LEADS, "D3 best visit time — hit rate (±30 min)", False),
    ("d3reg", "D3", "best-time regret (min)", SLOT_LEADS, "D3 best visit time — regret (min)", False),
]
INDIRECT_CANDIDATES = ["clim", "wt_med"]
#: label -> metric, so a generated sentence can name the unit without being told
METRIC_OF_LABEL = {q[4]: q[2] for q in QUESTIONS}


def sample_census(run: pathlib.Path, win_from: str | None, win_to: str | None) -> list[str]:
    """The origins that were actually scored, read from the per-origin parts, plus their
    weekday census. BENCH-SPEC's weekday-aliasing guard records a census in `run-*.json`;
    this recomputes it from the origin dates themselves, which is the check the review
    asked for (the run's own census and this one must agree)."""
    names = sorted(x.stem for x in (run / "parts" / "slot").glob("*.parquet"))
    dates = [dt.date.fromisoformat(n) for n in names if len(n) == 10]
    wd = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    cnt = {d: 0 for d in wd}
    for d in dates:
        cnt[wd[d.weekday()]] += 1
    out = [f"* **{len(dates)} origins** scored, {dates[0]} … {dates[-1]} "
           f"(stride {(dates[1] - dates[0]).days} d), recomputed from `parts/slot/*.parquet`.",
           "",
           "| weekday | " + " | ".join(wd) + " |",
           "|---" * 8 + "|",
           "| origins | " + " | ".join(str(cnt[d]) for d in wd) + " |",
           ""]
    if win_from and win_to:
        a, b = dt.date.fromisoformat(win_from), dt.date.fromisoformat(win_to)
        inside = [d for d in dates if a <= d <= b]
        out.append(f"* Inside the headline target window {win_from} … {win_to} lie "
                   f"**{len(inside)} origins**: "
                   + ", ".join(f"{d} ({wd[d.weekday()]})" for d in inside)
                   + f" — {len({wd[d.weekday()] for d in inside})} distinct weekdays.")
        out.append("")
    return out


def read_cells(path: Path) -> dict:
    """(uc, metric, lead, segment, model) -> row, region 'all' only."""
    out = {}
    with path.open() as fh:
        for r in csv.DictReader(fh):
            if r["region"] != "all":
                continue
            out[(r["uc"], r["metric"], r["lead"], r["segment"], r["model"])] = r
    return out


def f(row: dict, key: str):
    v = row.get(key, "")
    if v in ("", "None", "nan"):
        return None
    return float(v)


def i(row: dict, key: str):
    v = f(row, key)
    return None if v is None else int(v)


ROWS: list[dict] = []


def record(kind: str, pass_tag: str, uc: str, metric: str, lead: str, seg: str, model: str,
           comparator: str, c: "Cell") -> None:
    if c.row is None:
        return
    ROWS.append({"kind": kind, "pass": pass_tag, "uc": uc, "metric": metric, "lead": lead,
                 "segment": seg, "model": model, "comparator": comparator,
                 "lower_is_better": c.lower, "diff": c.diff, "diff_lo_park": c.lo,
                 "diff_hi_park": c.hi, "diff_lo_park_day": c.lo_pd, "diff_hi_park_day": c.hi_pd,
                 "n_paired": c.n, "n_units_park": c.units,
                 "n_paired_origin_days": c.origin_days, "paired_share": c.share,
                 "verdict": c.verdict, "why": c.why})


class Cell:
    """One paired comparison, with its verdict and the reason it has none."""

    def __init__(self, row: dict | None, comparator: str):
        self.row, self.comparator = row, comparator
        self.diff = f(row, "diff_vs_ref") if row else None
        self.lo = f(row, "diff_lo_park") if row else None
        self.hi = f(row, "diff_hi_park") if row else None
        self.lo_pd = f(row, "diff_lo") if row else None
        self.hi_pd = f(row, "diff_hi") if row else None
        self.units = i(row, "n_units_park") if row else None
        self.origin_days = i(row, "n_paired_origin_days") if row else None
        self.n = i(row, "n_paired") if row else None
        self.share = f(row, "paired_share") if row else None
        self.lower = (row.get("lower") == "True") if row else True
        self.verdict, self.why = self._verdict()

    def _verdict(self) -> tuple[str, str]:
        if self.row is None:
            return "missing", "no paired row"
        if self.diff is None or self.lo is None or self.hi is None:
            return "missing", "no paired difference"
        if self.units is None or self.units < MIN_UNITS:
            return "not tested", f"only {self.units} park clusters (< {MIN_UNITS})"
        if self.hi - self.lo <= 0:
            return "not tested", "degenerate CI (zero width)"
        if self.share is not None and self.share < MIN_COVERAGE:
            return "not tested", f"paired share {self.share:.2f} < {MIN_COVERAGE}"
        better = self.hi < 0 if self.lower else self.lo > 0
        worse = self.lo > 0 if self.lower else self.hi < 0
        if better:
            return "better", ""
        if worse:
            return "worse", ""
        return "tie", "CI covers 0"

    @property
    def low_n(self) -> bool:
        return self.origin_days is not None and self.origin_days < LOW_N_ORIGIN_DAYS

    def fmt(self) -> str:
        if self.row is None or self.diff is None:
            return "—"
        s = f"{self.diff:+.3f} [{self.lo:+.3f}, {self.hi:+.3f}]"
        mark = {"better": " **B**", "worse": " **W**", "tie": "", "not tested": " n.t.",
                "missing": ""}[self.verdict]
        return s + mark


def pair(cells: dict, comparator: str, uc: str, metric: str, lead: str, seg: str, model: str) -> Cell:
    return Cell(cells.get((uc, metric, lead, seg, model)), comparator)


def load_pass(d: Path, models: list[str]) -> dict:
    """{'envelope': cells, '<comparator>': cells} for one report pass."""
    out = {"envelope": read_cells(d / "cells.csv")}
    for m in models:
        p = d / f"cells_vs_{m}.csv"
        if p.exists():
            out[m] = read_cells(p)
    p = d / "cells_all_refs.csv"
    if p.exists():
        rows = {}
        with p.open() as fh:
            for r in csv.DictReader(fh):
                if r["region"] != "all":
                    continue
                rows.setdefault(r["ref_candidate"], {})[(r["uc"], r["metric"], r["lead"], r["segment"],
                                               r["model"])] = r
        out["_all_refs"] = rows
    return out


def indirect_table(passes: dict, models: list[str], q: tuple, seg: str,
                   candidates: list[str]) -> list[str]:
    """D3: no model-vs-model paired margin exists, so every model is shown against the
    SAME naive candidate on its own paired rows. The gap between two rows of this table
    is NOT a paired test and carries no CI -- it is an ordering only."""
    _, uc, metric, leads, label, _direct = q
    head = ("| lead | model | " + " | ".join(f"vs {c} (full)" for c in candidates)
            + " | " + " | ".join(f"vs {c} (window)" for c in candidates) + " |")
    lines = [head, "|---" * (2 * len(candidates) + 2) + "|"]
    any_row = False
    for lead in leads:
        for m in models:
            cols, got = [], False
            for tag in ("full", "win"):
                for cand in candidates:
                    src = passes[tag].get("_all_refs", {}).get(cand, {})
                    c = pair(src, cand, uc, metric, lead, seg, m)
                    record("indirect-vs-candidate", "full" if tag == "full" else "window",
                           uc, metric, lead, seg, m, cand, c)
                    if c.row is not None:
                        got = True
                    cols.append(c.fmt())
            if not got:
                continue
            any_row = True
            lines.append(f"| {lead} | `{m}` | " + " | ".join(cols) + " |")
    return lines if any_row else []


def envelope_cell(passes: dict, tag: str, uc: str, metric: str, lead: str, seg: str,
                  model: str) -> Cell:
    """The model's own cell against the per-lead naive envelope (cells.csv)."""
    return Cell(passes[tag]["envelope"].get((uc, metric, lead, seg, model)), "envelope")


def h2h_table(passes: dict, a: str, b: str, q: tuple, seg: str) -> list[str]:
    """`a` paired against `b`, both passes side by side, one row per lead."""
    _, uc, metric, leads, label, _direct = q
    lines = [f"| lead | full period: {a} − {b} | window: {a} − {b} | n (full) | origin days (full) |",
             "|---|---|---|---|---|"]
    any_row = False
    for lead in leads:
        c_full = pair(passes["full"].get(b, {}), b, uc, metric, lead, seg, a)
        c_win = pair(passes["win"].get(b, {}), b, uc, metric, lead, seg, a)
        record("head-to-head", "full", uc, metric, lead, seg, a, b, c_full)
        record("head-to-head", "window", uc, metric, lead, seg, a, b, c_win)
        if c_full.row is None and c_win.row is None:
            continue
        any_row = True
        n = f"{c_full.n:,}" if c_full.n else "—"
        od = (f"{c_full.origin_days}" + (" LOW-N" if c_full.low_n else "")) if c_full.origin_days else "—"
        lines.append(f"| {lead} | {c_full.fmt()} | {c_win.fmt()} | {n} | {od} |")
    return lines if any_row else []


def incumbent_table(passes: dict, model: str, q: tuple, seg: str, incumbents: list[str]) -> list[str]:
    _, uc, metric, leads, label, _direct = q
    head = "| lead | " + " | ".join(f"vs {x}" for x in incumbents) + " |"
    lines = [head, "|---" * (len(incumbents) + 1) + "|"]
    any_row = False
    for lead in leads:
        cols = []
        got = False
        for inc in incumbents:
            src = passes["full"].get(inc)
            if src is None:
                src = passes["full"].get("_all_refs", {}).get(inc, {})
            c = pair(src or {}, inc, uc, metric, lead, seg, model)
            record("vs-incumbent", "full", uc, metric, lead, seg, model, inc, c)
            if c.row is not None:
                got = True
            cols.append(c.fmt())
        if not got:
            continue
        any_row = True
        lines.append(f"| {lead} | " + " | ".join(cols) + " |")
    return lines if any_row else []


def family_verdict(passes: dict, tide: list[str], chronos: list[str], q: tuple, seg: str,
                   incumbents: list[str]) -> list[dict]:
    """Per lead: who wins the head to head, and does the winner also beat the incumbents?"""
    _, uc, metric, leads, label, _direct = q
    out = []
    for lead in leads:
        rec = {"lead": lead, "h2h": {}, "inc": {}}
        for t in tide:
            for c in chronos:
                for tag, p in (("full", passes["full"]), ("win", passes["win"])):
                    rec["h2h"][(t, c, tag)] = pair(p.get(c, {}), c, uc, metric, lead, seg, t)
        for m in tide + chronos:
            for inc in incumbents:
                for tag, p in (("full", passes["full"]), ("win", passes["win"])):
                    src = p.get(inc) or p.get("_all_refs", {}).get(inc, {})
                    c = pair(src, inc, uc, metric, lead, seg, m)
                    rec["inc"][(m, inc, tag)] = c
                    record("vs-incumbent", "full" if tag == "full" else "window",
                           uc, metric, lead, seg, m, inc, c)
        out.append(rec)
    return out


def beats_all_incumbents(rec: dict, model: str, incumbents: list[str], both: bool = True) -> tuple[bool, list[str]]:
    missing = []
    ok = True
    scored_any = False
    for inc in incumbents:
        tags = ("full", "win") if both else ("full",)
        vs = [rec["inc"].get((model, inc, t)) for t in tags]
        vs = [v for v in vs if v is not None and v.row is not None]
        if not vs:
            missing.append(f"{inc}: not scored")
            continue
        scored_any = True
        if not all(v.verdict == "better" for v in vs):
            ok = False
            missing.append(f"{inc}: " + "/".join(v.verdict for v in vs))
    if not scored_any:
        # A model with no comparison at all at this lead cannot "clear the incumbents":
        # the conjunction above is then EMPTY, and an empty conjunction is vacuously
        # true. That awarded UC1 m015 to `chronos2_grid`, which produces no intraday
        # forecast at all — it and the `*_x_h5` compositions exist only on the slot
        # table. Not evaluable is not the same as passing.
        return False, [f"not scored at this lead ({', '.join(incumbents)} all absent)"]
    return ok, missing

DE = {
    "UC1 live / next best ride (15\u2013120 min)": "Live: Wartezeit in den naechsten 15\u2013120 Minuten",
    "UC2 rest of day (intraday origins)": "Rest des Tages (Intraday-Start)",
    "UC2 planner today / tomorrow (06:00 origin)": "Tagesplaner heute / morgen (Start 06:00)",
    "UC2/UC3 planner slot curve d0\u2013d7": "Tagesplaner: 15-Minuten-Verlauf",
    "D3 best visit time \u2014 hit rate (top-2)": "Beste Besuchszeit: Treffer (Top-2)",
    "D3 best visit time \u2014 hit rate (\u00b130 min)": "Beste Besuchszeit: Treffer (\u00b130 min)",
    "D3 best visit time \u2014 regret (min)": "Beste Besuchszeit: Fehlgriff in Minuten",
}


def decide(rec: dict, tide: list[str], chronos: list[str], incumbents: list[str],
           both: bool) -> tuple[str, str, str, str]:
    """(verdict, suffix, head-to-head, why). `both` = require the headline window too."""
    tags = ("full", "win") if both else ("full",)
    t_wins, c_wins = [], []
    for t in tide:
        for c in chronos:
            cs = [rec["h2h"].get((t, c, tag)) for tag in tags]
            cs = [x for x in cs if x is not None and x.row is not None]
            if len(cs) != len(tags):
                continue
            won = {x.verdict for x in cs}
            if won not in ({"better"}, {"worse"}) or cs[0].diff is None:
                # a row can exist without a difference (`not tested`, `missing`); it is
                # not a win either way, and it has no margin to rank
                continue
            # `advantage` is the margin in the direction of the winner, so the champion is
            # the strongest variant and not merely the first one. `diff` is `t - c` and its
            # good direction depends on the metric (`lower`), which is why the sign is taken
            # from the cell rather than assumed.
            adv = -cs[0].diff if cs[0].lower else cs[0].diff
            if won == {"better"}:
                t_wins.append((t, c, adv))
            else:
                c_wins.append((c, t, -adv))
    if t_wins and not c_wins:
        h2h = "TiDE"
    elif c_wins and not t_wins:
        h2h = "Chronos-2"
    elif t_wins and c_wins:
        h2h = "split"
    else:
        h2h = "Gleichstand"
    ready, why = {}, []
    for m in tide + chronos:
        ok, miss = beats_all_incumbents(rec, m, incumbents, both=both)
        ready[m] = ok
        if not ok:
            why.append(f"{m}: " + ", ".join(miss))
    t_ready = [m for m in tide if ready.get(m)]
    c_ready = [m for m in chronos if ready.get(m)]
    if h2h == "TiDE":
        champ = max(t_wins, key=lambda x: x[2])[0]
        if ready.get(champ):
            return "TiDE", f" (`{champ}`)", h2h, "beats every scored incumbent"
        if t_ready:
            return "TiDE", f" (`{t_ready[0]}`)", h2h, ("duel won by `" + champ
                                                       + "`, incumbents cleared by `"
                                                       + t_ready[0] + "`")
        return "keiner", "", h2h, "; ".join(why)
    if h2h == "Chronos-2":
        champ = max(c_wins, key=lambda x: x[2])[0]
        if ready.get(champ):
            return "Chronos-2", f" (`{champ}`)", h2h, "beats every scored incumbent"
        if c_ready:
            return "Chronos-2", f" (`{c_ready[0]}`)", h2h, ("duel won by `" + champ
                                                            + "`, incumbents cleared by `"
                                                            + c_ready[0] + "`")
        return "keiner", "", h2h, "; ".join(why)
    if t_ready and c_ready:
        return "Gleichstand", f" (`{t_ready[0]}` / `{c_ready[0]}`)", h2h, "both clear the incumbents"
    if t_ready:
        return "TiDE", f" (`{t_ready[0]}`)", h2h, "duel is a tie, only TiDE clears the incumbents"
    if c_ready:
        return "Chronos-2", f" (`{c_ready[0]}`)", h2h, ("duel is a tie, only Chronos-2 clears "
                                                        "the incumbents")
    return "keiner", "", h2h, "; ".join(why) or "no variant clears the incumbents"


def best_h2h(rec: dict, tide: list[str], chronos: list[str]) -> tuple[str, str, "Cell"] | None:
    """The strongest TiDE-vs-Chronos separation at this lead that holds in BOTH passes.
    Returns (winner, loser, the full-period cell) or None when nothing separates them.
    The margin is read in the winner's direction, so the number printed is always an
    advantage; `lower` decides the sign, the metric is never assumed."""
    best = None
    for t in tide:
        for c in chronos:
            full, win = rec["h2h"].get((t, c, "full")), rec["h2h"].get((t, c, "win"))
            if full is None or win is None or full.row is None or win.row is None:
                continue
            if full.verdict == win.verdict == "better":
                a, b, adv = t, c, (-full.diff if full.lower else full.diff)
            elif full.verdict == win.verdict == "worse":
                a, b, adv = c, t, (full.diff if full.lower else -full.diff)
            else:
                continue
            if best is None or adv > best[3]:
                best = (a, b, full, adv)
    return None if best is None else (best[0], best[1], best[2])


def collapse(leads: list[str], order: list[str]) -> str:
    """`m015, m030, m045` -> `m015-m045`, using the question's own lead order."""
    idx = sorted((order.index(x) for x in leads if x in order))
    out, i0 = [], 0
    while i0 < len(idx):
        j = i0
        while j + 1 < len(idx) and idx[j + 1] == idx[j] + 1:
            j += 1
        out.append(order[idx[i0]] if j == i0 else f"{order[idx[i0]]}-{order[idx[j]]}")
        i0 = j + 1
    return ", ".join(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--window", required=True, help="the windowed tables dir name")
    ap.add_argument("--tide", default="nf_tide,nf_tide_x_h5")
    ap.add_argument("--chronos", default="chronos2,chronos2_grid,chronos2_x_h5")
    ap.add_argument("--incumbents", default="persistence,h5,lvlh5_tft")
    ap.add_argument("--nf-cache", default=None,
                    help="the nf_precompute cache the TiDE plug-in read, so the forecasts' own "
                         "provenance (which is NOT in run-*.json) is printed with the run's")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    run = Path(a.run)
    tide = [m for m in a.tide.split(",") if m]
    chronos = [m for m in a.chronos.split(",") if m]
    incumbents = [m for m in a.incumbents.split(",") if m]
    models = tide + chronos + incumbents

    full_dir, win_dir = run / "tables", run / a.window
    for d in (full_dir, win_dir):
        if not (d / "cells.csv").exists():
            print(f"{d}/cells.csv missing — run both report passes first", file=sys.stderr)
            return 2
    passes = {"full": load_pass(full_dir, models), "win": load_pass(win_dir, models)}

    o: list[str] = []
    w = o.append
    w("# TiDE vs Chronos-2 — head to head on identical rows (PAR-869)")
    w("")
    # ---- provenance, read from the run itself
    runs = sorted(run.glob("run-*.json"))
    reps = sorted(run.glob("report-*.json"))
    w("## Provenance (read from the run, not asserted here)")
    w("")
    for p in runs:
        d = json.loads(p.read_text())
        w(f"* `{p.name}`: models {d.get('models')}, pair_with {d.get('pair_with')}, "
          f"export `{d.get('export')}`, git `{str(d.get('git_sha'))[:8]}` "
          f"({d.get('git_sha_source')}), code_sha256 `{str(d.get('code_sha256'))[:16]}`, "
          f"image `{str(d.get('image_id'))[:19]}`, {d.get('seconds')} s")
        wk = d.get("origin_weekdays")
        if wk:
            w(f"  * origin weekdays: {wk} ({d.get('origin_weekday_distinct')} distinct)")
    for p in reps:
        d = json.loads(p.read_text())
        w(f"* `{p.name}`: code_sha256 `{str(d.get('code_sha256'))[:16]}`, "
          f"git `{str(d.get('git_sha'))[:8]}`, window "
          f"{d.get('target_from')}…{d.get('target_to')}")
    w("")
    if a.nf_cache:
        used = {m.replace("_x_h5", "") for m in tide}
        for meta in sorted(pathlib.Path(a.nf_cache).glob("*/meta.json")):
            if meta.parent.name not in used:
                continue
            m = json.loads(meta.read_text())
            blocks = m.get("blocks") or []
            w(f"* TiDE forecast cache `{meta.parent}`: model `{m.get('model')}`, "
              f"{m.get('spec', {}).get('cls')}, max_steps {m.get('max_steps')}, "
              f"input {m.get('input_days')} d, horizon {m.get('horizon_days')} d, "
              f"export `{m.get('export')}`, git `{str(m.get('git_sha'))[:8]}`, "
              f"code_sha256 `{str(m.get('code_sha256'))[:16]}`, "
              f"image `{str(m.get('image_id'))[:19]}`, {len(blocks)} month blocks "
              f"{(blocks[0].get('from') if blocks else '?')} \u2026 "
              f"{(blocks[-1].get('to') if blocks else '?')}")
        w("  * the plug-in only READS this cache (pandas + pyarrow), so the TiDE forecasts "
          "scored here are the ones PAR-829 produced on the GPU, under PAR-829's provenance; "
          "the scoring code above is this run's.")
        w("")
    w("## Sample (recomputed from the run, not from its own census)")
    w("")
    rep = {}
    for pth in reps:
        rep = json.loads(pth.read_text())
        if rep.get("target_from"):
            break
    for line in sample_census(run, rep.get("target_from"), rep.get("target_to")):
        w(line)
    w(f"Gates: park-cluster CI, >= {MIN_UNITS} park clusters, positive CI width, paired share "
      f">= {MIN_COVERAGE}; **B** = better (CI entirely on the better side), **W** = worse, "
      f"blank = tie, `n.t.` = not tested. LOW-N = < {LOW_N_ORIGIN_DAYS} paired origin days.")
    w("")

    # ---- 1. the head-to-head tables
    w("## 1. Head to head, paired on identical rows")
    w("")
    for q in QUESTIONS:
        key, uc, metric, leads, label, direct = q
        if not direct:
            for seg in SEGMENTS:
                tb = indirect_table(passes, tide + chronos + incumbents, q, seg,
                                    INDIRECT_CANDIDATES)
                if not tb:
                    continue
                w(f"### {label} \u2014 segment `{seg}` (INDIRECT, no paired head to head)")
                w("")
                w("This run has no paired model-vs-model margin for this metric: "
                  "`report --vs` runs its paired engine on the slot and the intraday table "
                  "only, and the ride-day sums are written against the naive candidates alone "
                  "(`rideday_pair_sql(REF_CANDIDATES)`). Both families are therefore shown "
                  "against the SAME candidate, each on its own paired rows; the gap between "
                  "two rows of this table is an ordering, not a test.")
                w("")
                o.extend(tb)
                w("")
            continue
        for seg in SEGMENTS:
            blocks = []
            for t in tide:
                for c in chronos:
                    tb = h2h_table(passes, t, c, q, seg)
                    if tb:
                        blocks.append((f"{t} − {c}", tb))
            if not blocks:
                continue
            w(f"### {label} — segment `{seg}`")
            w("")
            lower = "hit rate" not in metric
            sign = "negative" if lower else "positive"
            for name, tb in blocks:
                first = name.split(" \u2212 ")[0]
                w(f"**{name}** ({sign} = {first} better)")
                w("")
                o.extend(tb)
                w("")

    # ---- 2. each model against the incumbents (full period)
    w("## 2. Each model against the incumbents (full period, park-cluster CI)")
    w("")
    for q in QUESTIONS:
        key, uc, metric, leads, label, direct = q
        if not direct:
            continue
        for seg in SEGMENTS:
            rows = []
            for m in tide + chronos:
                tb = incumbent_table(passes, m, q, seg, incumbents)
                if tb:
                    rows.append((m, tb))
            if not rows:
                continue
            w(f"### {label} — segment `{seg}`")
            w("")
            for m, tb in rows:
                w(f"**{m}**")
                w("")
                o.extend(tb)
                w("")

    # ---- 3. who should serve what
    de_rows: list[tuple] = []
    w("## 3. Who should serve which surface x lead")
    w("")
    w("Two verdict columns, because the two passes do not have the same power: the headline")
    w("window holds 11 of the 47 origins, so a window cell can only confirm a full-period")
    w("result, never carry one alone. **strict** = the verdict survives BOTH passes;")
    w("**full period** = the same rule on the full period only. A family is awarded a")
    w("surface when one of its variants wins the head to head AND that variant also beats")
    w("every incumbent that is scored there (`persistence` on intraday, `h5`, `lvlh5_tft`);")
    w("`Gleichstand` = the head to head does not separate them, `keiner` = no variant of")
    w("either family clears the incumbents.")
    w("")
    w("| question | lead | segment | head to head (strict) | verdict (strict) | verdict (full period) | why |")
    w("|---|---|---|---|---|---|---|")
    for q in QUESTIONS:
        key, uc, metric, leads, label, direct = q
        for seg in SEGMENTS:
            for rec in family_verdict(passes, tide, chronos, q, seg, incumbents):
                lead = rec["lead"]
                if direct:
                    scored = [c for c in rec["h2h"].values() if c.row is not None]
                    if not scored:
                        continue
                    strict = decide(rec, tide, chronos, incumbents, both=True)
                    loose = decide(rec, tide, chronos, incumbents, both=False)
                    lown = any(c.low_n for c in scored)
                    de_rows.append((label, lead, seg, strict[0], loose[0], lown,
                                    best_h2h(rec, tide, chronos), leads, True))
                    w(f"| {label} | {lead} | {seg} | {strict[2]}{' (LOW-N)' if lown else ''} "
                      f"| **{strict[0]}**{strict[1]} | {loose[0]}{loose[1]} | {strict[3]} |")
                else:
                    # D3: no paired head to head. The only testable statement per model is
                    # "beats the naive candidates it is paired against".
                    ok = {}
                    for m in tide + chronos:
                        cells_ = [(tag, cand, pair(passes[tag].get("_all_refs", {}).get(cand, {}),
                                                   cand, uc, metric, lead, seg, m))
                                  for tag in ("full", "win") for cand in INDIRECT_CANDIDATES]
                        have = [c for _, _, c in cells_ if c.row is not None]
                        if not have:
                            continue
                        ok[m] = all(c.verdict == "better" for c in have)
                    if not ok:
                        continue
                    t_ok = [m for m in tide if ok.get(m)]
                    c_ok = [m for m in chronos if ok.get(m)]
                    if t_ok and not c_ok:
                        v = "TiDE"
                    elif c_ok and not t_ok:
                        v = "Chronos-2"
                    elif t_ok and c_ok:
                        v = "Gleichstand"
                    else:
                        v = "keiner"
                    de_rows.append((label, lead, seg, v, v, True, None, leads, False))
                    w(f"| {label} | {lead} | {seg} | nicht paarbar | **{v}** | {v} | "
                      f"indirect: beats {INDIRECT_CANDIDATES} in both passes: "
                      f"{', '.join(m for m in ok if ok[m]) or 'none'} |")
    w("")
    w(f"_{len(de_rows)} question x lead x segment rows._")
    w("")

    # ---- 4. the German answer table (what the owner reads first)
    w("## 4. Antwort-Tabelle (deutsch)")
    w("")
    w("`besser` heisst: dieses Modell gewinnt das direkte Duell auf identischen Zeilen UND")
    w("schlaegt jeden Incumbent, der dort gemessen ist. `Gleichstand` = das Duell trennt sie")
    w("nicht. `keiner` = keine Variante schlaegt die Incumbents, es bleibt beim Bestand.")
    w("Spalte `nur Vollperiode` zeigt dasselbe Urteil ohne die Bestaetigung durch das")
    w("Kopf-Fenster (11 von 47 Origins, zu wenig Kraft fuer ein eigenes Urteil).")
    w("")
    w("| Frage in der App | Vorlauf | alle Fahrgeschaefte | volle Fahrgeschaefte | nur Vollperiode (alle / volle) |")
    w("|---|---|---|---|---|")
    by_q: dict = {}
    for label, lead, seg, strict_v, loose_v, lown, _best, _leads, _direct in de_rows:
        by_q.setdefault((label, lead), {})[seg] = (strict_v, loose_v)
    for (label, lead), segs in by_q.items():
        a_s, a_l = segs.get("all", ("nicht messbar", "nicht messbar"))
        b_s, b_l = segs.get("busy", ("nicht messbar", "nicht messbar"))
        w(f"| {DE.get(label, label)} | {lead} | {a_s} | {b_s} | {a_l} / {b_l} |")
    w("")

    # ---- 5. the same answer collapsed to one line per question (what goes on top of
    #         RESULTS-TIDE-VS-CHRONOS.md, generated so the prose cannot drift from the CSVs)
    w("## 5. Kurzfassung je Frage (generiert)")
    w("")
    w("Dieselben Urteile wie Abschnitt 4, nur je Frage und Segment zu Vorlauf-Baendern")
    w("zusammengefasst; `strict`, also beide Report-Paesse. Dahinter der **groesste")
    w("gesicherte** Abstand des Duells in dieser Frage: Gewinner, Verlierer, Vorlauf und")
    w("der gepaarte Margin mit Park-Cluster-CI (Vollperiode). Der Margin steht als")
    w("**Vorsprung des Gewinners**, also immer positiv — nicht als vorzeichenbehaftete")
    w("Differenz wie in Abschnitt 1, damit die Zahl dem Namen davor nicht widerspricht.")
    w("")
    by_qs: dict = {}
    for label, lead, seg, strict_v, loose_v, lown, best, leads, direct in de_rows:
        by_qs.setdefault((label, seg), {"bands": {}, "best": None, "leads": leads,
                                        "direct": direct})
        e = by_qs[(label, seg)]
        e["bands"].setdefault(strict_v, []).append(lead)
        if best is None:
            continue
        win, lose, cell = best
        # Restate the margin as the WINNER's advantage, so the printed sign cannot
        # disagree with the name in front of it. `cell.diff` is always `tide - chronos`
        # and its good direction depends on the metric, so both have to be folded in;
        # the CI endpoints swap whenever the triple is negated.
        tri = (cell.lo, cell.diff, cell.hi)
        if (win in tide) == cell.lower:
            tri = (-cell.hi, -cell.diff, -cell.lo)
        if e["best"] is None or tri[1] > e["best"][0][1]:
            e["best"] = (tri, win, lose, lead, cell)
    for (label, seg), e in by_qs.items():
        bands = "; ".join(f"{v} {collapse(ls, e['leads'])}" for v, ls in e["bands"].items())
        seg_de = "alle Fahrgeschaefte" if seg == "all" else "volle Fahrgeschaefte"
        line = f"* **{DE.get(label, label)}**, {seg_de}: {bands}."
        if e["best"] is not None:
            tri, win, lose, lead, cell = e["best"]
            unit = "" if "hit rate" in METRIC_OF_LABEL.get(label, "") else " min"
            line += (f" Groesster gesicherter Abstand: `{win}` ist bei {lead} um "
                     f"{tri[1]:.3f}{unit} besser als `{lose}` "
                     f"[{tri[0]:.3f}, {tri[2]:.3f}] "
                     f"(n={cell.n:,}, {cell.units} Park-Cluster).")
        elif e["direct"]:
            line += " Kein Vorlauf trennt die beiden in beiden Paessen."
        else:
            line += (" Kein gepaartes Duell moeglich (siehe Abschnitt 1), nur der"
                     " indirekte Vergleich gegen dieselben Naive-Kandidaten.")
        w(line)
    w("")

    out = Path(a.out) if a.out else run / "summary_tide_vs_chronos.md"
    out.write_text("\n".join(o) + "\n")
    seen, uniq = set(), []
    for r in ROWS:
        k = (r["kind"], r["pass"], r["uc"], r["metric"], r["lead"], r["segment"], r["model"],
             r["comparator"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)
    csv_out = out.with_name(out.stem + "_cells.csv")
    with csv_out.open("w", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=list(uniq[0].keys()))
        wr.writeheader()
        wr.writerows(uniq)
    print(f"wrote {out} ({len(o)} lines) and {csv_out} ({len(uniq)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
