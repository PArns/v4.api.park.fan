#!/usr/bin/env python3
"""Derive the PAR-853 hand-over tables from a run's report CSVs.

Every number quoted in the PR body, docs/ml/foundation-models.md and the review notes
comes out of this script, so each one is re-derivable from the committed CSVs instead of
being transcribed by hand.

Usage:
    python summarise.py <run-dir> [--tables tables] [--ref h5] [--md]

Reads `<run-dir>/<tables>/cells_vs_<ref>.csv` (the harness' paired table: `diff_vs_ref`
is the paired MAE difference model - ref on identical slots, `diff_lo_park`/`diff_hi_park`
the park-cluster bootstrap 95 % CI) and prints one row per use case x lead x segment.

Verdict rule (the same one the admin matrix uses):
    better = CI entirely below 0, worse = CI entirely above 0, tie = CI contains 0.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

# Lead bands of the admin model-comparison questions (src/ml/utils/model-comparison.util.ts).
# The harness labels the slot-MAE of day lead 0 "UC2" and of day leads 1..90 "UC3"
# (report.py: `lambda L: "UC2" if L == 0 else "UC3"`), so the planner questions accept both.
PLANNER_UCS = ("UC2", "UC3")
QUESTION_BANDS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "live_h0_1": (("UC1",), ("m015", "m030", "m045", "m060")),
    "live_h1_2": (("UC1",), ("m060", "m075", "m090", "m105", "m120")),
    "today_h2_6": (("UC2-intraday",), ("h2-4", "h4-8")),
    "today_h6_12": (("UC2-intraday",), ("h8+",)),
    "plan_d0_1": (PLANNER_UCS, ("0", "1")),
    "plan_d2_5": (PLANNER_UCS, ("2", "3", "4", "5")),
    "plan_d6_plus": (PLANNER_UCS, ("6", "7")),
}

PAR853_MODELS = ("tirex2", "tirex2_grid", "toto2", "toto2_nocov", "toto2_grid")


def verdict(lo: float, hi: float, lower_is_better: bool = True) -> str:
    if lo is None or hi is None:
        return "n/a"
    if lo > 0 and hi > 0:
        return "worse" if lower_is_better else "better"
    if lo < 0 and hi < 0:
        return "better" if lower_is_better else "worse"
    return "tie"


def fnum(raw: str) -> float | None:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def load(run: Path, tables: str, ref: str) -> list[dict[str, str]]:
    path = run / tables / f"cells_vs_{ref}.csv"
    if not path.exists():
        sys.exit(f"missing {path}")
    with path.open() as fh:
        return list(csv.DictReader(fh))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run", type=Path)
    ap.add_argument("--tables", default="tables")
    ap.add_argument("--ref", default="h5")
    ap.add_argument("--models", default=",".join(PAR853_MODELS))
    ap.add_argument("--segment", default="all,busy")
    ap.add_argument("--region", default="all", help="region rows to keep ('all' = the whole subset)")
    ap.add_argument("--md", action="store_true", help="markdown table instead of TSV")
    args = ap.parse_args()

    models = [m for m in args.models.split(",") if m]
    segments = [s for s in args.segment.split(",") if s]
    rows = load(args.run, args.tables, args.ref)

    by_key: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for r in rows:
        if r["region"] != args.region:
            continue
        key = (r["uc"], r["lead"], r["segment"], r["model"])
        if key in by_key:
            sys.exit(f"duplicate row for {key} in region {args.region}")
        by_key[key] = r

    header = ["question", "uc", "lead", "segment", "model", "mae", "ref_mae", "diff",
              "ci_lo", "ci_hi", "verdict", "n_paired", "parks", "park_days", "origins"]
    print(("| " + " | ".join(header) + " |") if args.md else "\t".join(header))
    if args.md:
        print("|" + "|".join(["---"] * len(header)) + "|")

    for qid, (ucs, leads) in QUESTION_BANDS.items():
        for lead in leads:
            for seg in segments:
                for model in models:
                    for uc in ucs:
                        r = by_key.get((uc, lead, seg, model))
                        if r is not None:
                            break
                    if r is None:
                        continue
                    ref_row = by_key.get((uc, lead, seg, args.ref))
                    diff = fnum(r["diff_vs_ref"])
                    lo = fnum(r["diff_lo_park"])
                    hi = fnum(r["diff_hi_park"])
                    mae = fnum(r["value"])
                    # The reference is not a candidate row in its own cells_vs_<ref>.csv,
                    # so its paired MAE is recovered from the model's MAE and the paired diff.
                    ref_mae = fnum(ref_row["value"]) if ref_row else (
                        None if mae is None or diff is None else mae - diff)
                    out = [
                        qid, uc, lead, seg, model,
                        "" if mae is None else f"{mae:.3f}",
                        "" if ref_mae is None else f"{ref_mae:.3f}",
                        "" if diff is None else f"{diff:+.3f}",
                        "" if lo is None else f"{lo:+.3f}",
                        "" if hi is None else f"{hi:+.3f}",
                        verdict(lo, hi),
                        r.get("n_paired", ""), r.get("n_units_park", ""),
                        r.get("n_units_park_day", ""),
                        r.get("n_paired_origin_days", "") or r.get("n_origin_days", ""),
                    ]
                    print(("| " + " | ".join(out) + " |") if args.md else "\t".join(out))


if __name__ == "__main__":
    main()
