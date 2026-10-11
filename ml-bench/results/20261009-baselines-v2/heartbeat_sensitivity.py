#!/usr/bin/env python3
"""BENCH-SPEC recorded change #6 / critic B3: what the heartbeat truth slots do.

`queue_data` is a change log and BENCH-SPEC's truth rule is "forward-fill the last
row, max staleness 3 h". An `is_heartbeat` row carries the previous status *and
wait* forward under a fresh `ts`, so it resets that 3 h clock and keeps producing
truth for a ride that stopped reporting. The 2026-10-09 export does not filter
those rows, so the headline run is scored against a target that includes them.

This script compares the two sensitivity runs, which differ in exactly one input:

    20261010-sens-hb    export 20261009          (heartbeats KEPT)
    20261010-sens-nohb  export 20261010-nohb     (`build --drop-heartbeats`)

Both arms: 68 daily origins 2026-08-01…2026-10-07, leads 0–14, reported on the
headline target window 2026-08-15…2026-10-07. Same raw export (the nohb export
symlinks `raw/`), same runner code, same report code — see the `report-*.json`
of each arm.

Run it from a checkout, against either worktree that holds the two runs:

    python3 ml-bench/results/20261009-baselines-v2/heartbeat_sensitivity.py \
        ml-bench/results/20261010-sens-hb ml-bench/results/20261010-sens-nohb

Nothing here reads production; it is pure CSV arithmetic over committed tables.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

WINDOW = "tables_2026-08-15_2026-10-07"
HB, NOHB = (Path(a) for a in (sys.argv[1:3] or ["20261010-sens-hb", "20261010-sens-nohb"]))


def num(x: str | None) -> float:
    try:
        return float(x)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float("nan")


def yes(x: object) -> bool:
    return str(x).strip().lower() in ("true", "1")


def cells(run: Path) -> dict[tuple, dict]:
    p = run / WINDOW / "cells.csv"
    return {(r["metric"], r["uc"], r["segment"], r["region"], str(r["lead"]), r["model"]): r
            for r in csv.DictReader(open(p))}


def seasons(run: Path) -> dict[tuple, dict]:
    p = run / WINDOW / "mae_by_season.csv"
    return {(str(r["L"]), r["season"]): r for r in csv.DictReader(open(p))}


A, B = cells(HB), cells(NOHB)
SA, SB = seasons(HB), seasons(NOHB)
common = sorted(set(A) & set(B))

print("# Heartbeat sensitivity — BENCH-SPEC change #6 / critic B3\n")
print(f"Arms: `{HB.name}` (heartbeats kept) vs `{NOHB.name}` (dropped). "
      f"{len(common)} comparable cells.\n")

# ---------------------------------------------------------------- 1. how much truth moves
print("## 1. How much of the target is carried forward\n")
print("Denominator of the all-rides slot-MAE cell (`wt_med`, region all):\n")
print("| lead | truth slots, kept | dropped | share dropped |")
print("|---|---|---|---|")
for lead in ("0", "1", "3", "7", "10", "14"):
    for uc in ("UC2", "UC3"):
        k = ("MAE", uc, "all", "all", lead, "wt_med")
        if k in A and k in B:
            a, b = num(A[k]["n"]), num(B[k]["n"])
            print(f"| d{lead} | {a:,.0f} | {a - b:,.0f} | {100 * (a - b) / a:.2f} % |")
            break

# ---------------------------------------------------------------- 2. the season structure
print("\n## 2. The contamination is entirely in the autumn half of the headline window\n")
print("`is_heartbeat` was not populated before ~2026-09-06, so the 17 summer days of the "
      "window (Aug 15–31) are bit-identical between the arms and the 37 autumn days carry "
      "all of it. That is the same half of the window that drives the season reversal in "
      "`mae_by_season.csv`, so the two confounds sit on top of each other.\n")
print("| lead | season | slots kept | share dropped | `wt_med` | `h5` | `lvlh5_tft` |")
print("|---|---|---|---|---|---|---|")
for lead in ("0", "1", "3", "7", "14"):
    for season in ("summer", "autumn"):
        k = (lead, season)
        if k not in SA or k not in SB:
            continue
        na, nb = num(SA[k]["n"]), num(SB[k]["n"])
        cols = []
        for m in ("wt_med", "h5", "lvlh5_tft"):
            x, y = num(SA[k].get(m)), num(SB[k].get(m))
            cols.append(f"{x:.3f} → {y:.3f} ({y - x:+.3f})")
        print(f"| d{lead} | {season} | {na:,.0f} | {100 * (na - nb) / na:.2f} % | "
              + " | ".join(cols) + " |")

# ---------------------------------------------------------------- 3. UC1 / persistence
print("\n## 3. The bias has a direction: it favours `persistence`\n")
print("A heartbeat slot carries a *frozen* wait, which is exactly what `persistence` "
      "predicts, so the contamination flatters it — and the longer the horizon, the more "
      "a real queue would have moved while the frozen one did not. (PAR-828 reached the "
      "same point from its Chronos-2 run; this is the measurement.)\n")
print("| UC1 lead | `persistence` | `h5` | `lvlh5_tft` | `persistence` penalty on `h5` |")
print("|---|---|---|---|---|")
for lead in ("m015", "m030", "m045", "m060", "m090", "m120"):
    row = []
    for m in ("persistence", "h5", "lvlh5_tft"):
        k = ("MAE", "UC1", "all", "all", lead, m)
        if k not in A:
            row.append("–")
            continue
        x, y = num(A[k]["value"]), num(B[k]["value"])
        row.append(f"{x:.3f} → {y:.3f} ({y - x:+.3f})")
    kh = ("MAE", "UC1", "all", "all", lead, "h5")
    gap = (f"{num(A[kh].get('diff_vs_hardest')):+.3f} → "
           f"{num(B[kh].get('diff_vs_hardest')):+.3f}" if kh in A else "–")
    print(f"| {lead} | " + " | ".join(row) + f" | {gap} |")

# ---------------------------------------------------------------- 4. verdict flips
print("\n## 4. Does any verdict flip?\n")
flips = [(k, yes(A[k].get("wins")), yes(B[k].get("wins")))
         for k in common if yes(A[k].get("wins")) != yes(B[k].get("wins"))]
won_a = sum(yes(A[k].get("wins")) for k in common)
won_b = sum(yes(B[k].get("wins")) for k in common)
print(f"`wins=True`: **{won_a}** with heartbeats kept, **{won_b}** with them dropped, over "
      f"{len(common)} cells. **{len(flips)} cells flip** ({100 * len(flips) / len(common):.1f} %), "
      f"{sum(1 for _, a, b in flips if b and not a)} of them from no-win to win.\n")
print("| uc | metric | seg | region | lead | model | kept | dropped | margin vs hardest |")
print("|---|---|---|---|---|---|---|---|---|")
for (metric, uc, seg, reg, lead, model), a, b in flips:
    k = (metric, uc, seg, reg, lead, model)
    print(f"| {uc} | {metric[:34]} | {seg} | {reg} | {lead} | `{model}` | {a} | {b} | "
          f"{num(A[k].get('diff_vs_hardest')):+.4f} → {num(B[k].get('diff_vs_hardest')):+.4f} |")
