#!/usr/bin/env python3
"""Re-derive published head-to-head cells from `parts/` and compare (PAR-869).

The verdicts in `cells_vs_<model>.csv` are recomputed at report time from the paired
sums the runner wrote per origin. This script goes back to those sums and rebuilds the
point estimate and the three counts behind each published cell **without** using the
report code, so a wrong filter, a wrong lead mapping or a silently empty arm shows up
as a mismatch instead of being taken on trust.

What is checked per cell, against `cells_vs_<comparator>.csv`:
* `diff_vs_ref` = Σ psm__a__b / Σ pn__a__b − Σ psr__a__b / Σ pn__a__b
  (`stats.Bootstrapper.diff`: Σnum_m/Σden_m − Σnum_r/Σden_r; for a paired arm
  `den_m == den_r == pn`)
* `n_paired` = Σ pn__a__b
* `n_units_park` = parks carrying weight
* `n_paired_origin_days` = distinct target dates carrying weight

Only the full-period pass is verified: the windowed pass applies its date filter inside
the report's DuckDB view, and re-implementing that filter here would re-implement the
thing under test. Run inside the bench image (needs pandas + pyarrow):

    python verify_cells_from_parts.py --run /app/results/<run-id> [--top 12]
"""
from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

import pandas as pd

TIDE = ["nf_tide", "nf_tide_x_h5"]
CHRONOS = ["chronos2", "chronos2_grid", "chronos2_x_h5"]
# uc -> (parts table, lead column in that table, cast of the published lead label)
TABLE_OF = {
    "UC1": ("intraday", "lead_key", str),
    "UC2-intraday": ("intraday", "lead_key", str),
    "UC2": ("slot", "L", int),
    "UC3": ("slot", "L", int),
}


def load_parts(run: Path, table: str, cols: list[str]) -> pd.DataFrame:
    files = sorted(glob.glob(str(run / "parts" / table / "*.parquet")))
    if not files:
        raise SystemExit(f"no parts under {run}/parts/{table}")
    frames = []
    for fp in files:
        df = pd.read_parquet(fp, columns=cols)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--tables", default="tables", help="the full-period tables dir")
    ap.add_argument("--top", type=int, default=12,
                    help="verify the N largest-|diff| published cells per comparator AND use case")
    ap.add_argument("--tol", type=float, default=1e-9)
    ap.add_argument("--a-models", default=",".join(TIDE),
                    help="the models whose published cells are re-derived")
    ap.add_argument("--b-models", default=",".join(CHRONOS),
                    help="the comparators they were paired against")
    a = ap.parse_args()
    run, tdir = Path(a.run), Path(a.run) / a.tables
    a_models = [m for m in a.a_models.split(",") if m]
    b_models = [m for m in a.b_models.split(",") if m]

    want = []
    for b in b_models:
        p = tdir / f"cells_vs_{b}.csv"
        if not p.exists():
            continue
        c = pd.read_csv(p)
        c = c[(c["region"] == "all") & (c["model"].isin(a_models)) & (c["uc"].isin(TABLE_OF))
              & c["diff_vs_ref"].notna()]
        if c.empty:
            continue
        c = c.assign(comparator=b, _abs=c["diff_vs_ref"].abs())
        # top-|diff| PER USE CASE, so the check spans the live, the intraday and the
        # planner tables instead of piling onto whichever one happens to have the
        # largest margins.
        for _uc, g in c.groupby("uc"):
            want.append(g.sort_values("_abs", ascending=False).head(a.top))
    if not want:
        print("no published head-to-head cells found — were both report passes run?",
              file=sys.stderr)
        return 2
    cells = pd.concat(want, ignore_index=True)

    cache: dict[str, pd.DataFrame] = {}
    worst, bad, checked = 0.0, [], 0
    print(f"{'uc':13} {'lead':5} {'seg':5} {'a':14} {'b':14} "
          f"{'published':>11} {'recomputed':>11} {'d':>9}")
    for _, r in cells.iterrows():
        table, keycol, cast = TABLE_OF[r["uc"]]
        pn, psm, psr = (f"pn__{r['model']}__{r['comparator']}",
                        f"psm__{r['model']}__{r['comparator']}",
                        f"psr__{r['model']}__{r['comparator']}")
        if table not in cache:
            probe = pd.read_parquet(sorted(glob.glob(str(run / "parts" / table / "*.parquet")))[0])
            keep = [c for c in probe.columns
                    if c.startswith(("pn__", "psm__", "psr__"))
                    or c in ("park_id", "date", "busy", keycol)]
            cache[table] = load_parts(run, table, keep)
        df = cache[table]
        if pn not in df.columns:
            bad.append((dict(r), "paired columns missing in parts"))
            continue
        sel = df[df[keycol] == cast(r["lead"])]
        if r["segment"] == "busy":
            sel = sel[sel["busy"].astype(bool)]
        g = sel.groupby(["park_id", "date"], observed=True)[[pn, psm, psr]].sum()
        g = g[g[pn] > 0]
        n = float(g[pn].sum())
        got = float(g[psm].sum()) / n - float(g[psr].sum()) / n
        exp = float(r["diff_vs_ref"])
        d = abs(got - exp)
        worst = max(worst, d)
        checked += 1
        counts = []
        idx = g.index.to_frame(index=False)
        for col, published in (("park_id", "n_units_park"), ("date", "n_paired_origin_days")):
            if published in r and pd.notna(r[published]):
                mine, theirs = int(idx[col].nunique()), int(r[published])
                if mine != theirs:
                    counts.append(f"{published} {mine} != {theirs}")
        if pd.notna(r.get("n_paired")) and abs(n - float(r["n_paired"])) > 0.5:
            counts.append(f"n_paired {n:.0f} != {float(r['n_paired']):.0f}")
        flag = "  OK" if d <= a.tol and not counts else "  MISMATCH " + "; ".join(counts)
        if d > a.tol or counts:
            bad.append((dict(r), f"diff d={d:.3g}; " + "; ".join(counts)))
        print(f"{r['uc']:13} {str(r['lead']):5} {r['segment']:5} {r['model']:14} "
              f"{r['comparator']:14} {exp:11.6f} {got:11.6f} {d:9.2e}{flag}")

    print(f"\n{checked} cells re-derived from parts/, max |published - recomputed| = {worst:.3g}")
    if bad:
        print(f"{len(bad)} MISMATCHES", file=sys.stderr)
        for row, why in bad:
            print(f"  {row.get('uc')} {row.get('lead')} {row.get('segment')} "
                  f"{row.get('model')} vs {row.get('comparator')}: {why}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
