"""Generate ``offline-model-verdicts.json`` entries from a run paired against ``served``
(PAR-1043). Standard library only, so it runs from a checkout with the system Python.

    python3 -m mlbench.served_verdicts --run results/<run-id>            # dry run: prints the diff
    python3 -m mlbench.served_verdicts --run results/<run-id> --write    # writes the file

Input: ``<run>/<pass>/cells_vs_served.csv`` from ``report --vs served`` (one per report
pass; by default every ``tables*`` directory that has one). Each matrix cell (question x
model column) maps to the bench cells of its leads (``QUESTIONS``) and becomes ONE entry
against ``reference: "served"``:

* ``better`` only when EVERY lead of the question wins in EVERY pass — the model's paired
  difference against served has a park-cluster CI entirely below 0, with the report's
  bootstrap-unit floor (``wins``) and at least ``--min-origin-days`` paired origin days;
* ``worse`` when every lead in every pass has that CI entirely above 0 under the same floors;
* ``unclear`` otherwise, including a lead missing from a pass.

``diff`` / ``ciLow`` / ``ciHigh`` are those of the LEAST favourable cell (the largest CI
upper bound): one number per cell, never pooled across leads, and the conservative one.
An entry is replaced whole; cells the run has no rows for are left untouched. The note is
plain German like every other note in the file, and says which lead and pass it quotes.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import math
import sys
from pathlib import Path

DEFAULT_VERDICTS = Path(__file__).resolve().parents[2] / "src" / "ml" / "data" / "offline-model-verdicts.json"
REFERENCE = "served"
MIN_UNITS = 10                      # BenchConfig.min_bootstrap_units (parks)

INTRADAY_UC = ("UC1", "UC2-intraday")
SLOT_UC = ("UC2", "UC3")
# Matrix model column -> bench model, per question kind (src/ml/utils/model-comparison.util.ts
# COMPARISON_QUESTIONS: the live/today questions use Chronos-2's live variant, the planner its
# grid variant). `fm` (two models in one column) and `peak` (no bench twin) are not generated.
LIVE = {"persist": "persistence", "chronos": "chronos2", "h5": "h5", "tfth5": "lvlh5_tft"}
PLAN = {"chronos": "chronos2_grid", "h5": "h5", "tfth5": "lvlh5_tft", "wkd": "wt_med"}
# question -> (report metric, verdict metric, bench use cases, bench lead keys, columns).
# The intraday buckets are the report's (m015…m120, h2-4, h4-8, h8+); a question that
# straddles a bucket takes every bucket it touches, which can only make `better` harder.
QUESTIONS: dict[str, tuple[str, str, tuple[str, ...], list[str], dict[str, str]]] = {
    # m015 is the slot starting AT the bench origin; the capture runs at :05 or later, so the
    # served curve never answers it (it could have partly seen it) and it can never pair
    "live_h0_1": ("MAE", "slot_mae", INTRADAY_UC, ["m030", "m045", "m060"],
                  {k: LIVE[k] for k in ("persist", "chronos", "h5")}),
    "live_h1_2": ("MAE", "slot_mae", INTRADAY_UC, ["m075", "m090", "m105", "m120"], LIVE),
    "today_h2_6": ("MAE", "slot_mae", INTRADAY_UC, ["h2-4", "h4-8"], LIVE),
    "today_h6_12": ("MAE", "slot_mae", INTRADAY_UC, ["h4-8", "h8+"],
                    {"h5": "h5", "tfth5": "lvlh5_tft", "chronos": "chronos2_grid"}),
    "plan_d0_1": ("MAE", "slot_mae", SLOT_UC, ["0", "1"], PLAN),
    "plan_d2_5": ("MAE", "slot_mae", SLOT_UC, ["2", "3", "4", "5"], PLAN),
    "plan_d6_60": ("MAE", "slot_mae", SLOT_UC, ["6", "7", "10", "14", "21", "30", "45", "60"],
                   {k: PLAN[k] for k in ("h5", "tfth5", "wkd")}),
    "plan_besttime": ("best-time regret (min)", "besttime_regret", ("D3",),
                      ["0", "1", "2", "3", "4", "5", "6", "7", "10", "14", "21", "30"], PLAN),
}


def _f(row: dict, key: str) -> float:
    try:
        v = float(row.get(key, ""))
    except ValueError:
        return math.nan
    return v


def _true(row: dict, key: str) -> bool:
    return str(row.get(key, "")).strip().lower() == "true"


def read_pass(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return [r for r in csv.DictReader(f)
                if r.get("ref") == REFERENCE and r.get("segment") == "all" and r.get("region") == "all"]


def lead_label(key: str) -> str:
    if key.startswith("m"):
        return f"{int(key[1:])} Minuten vorher"
    if key == "h8+":
        return "mehr als 8 Stunden vorher"
    if key.startswith("h"):
        a, b = key[1:].split("-")
        return f"{a}–{b} Stunden vorher"
    return "am selben Tag" if key == "0" else ("1 Tag vorher" if key == "1" else f"{key} Tage vorher")


def num(v: float) -> str:
    """German decimal comma and a real minus sign, like the hand-written notes."""
    return f"{v:+.2f}".replace("-", "−").replace(".", ",")


def cell_verdict(passes: dict[str, list[dict]], metric: str, ucs: tuple[str, ...], keys: list[str],
                 bench_model: str, min_days: int) -> dict | None:
    """The entry fields for one matrix cell, or None when no pass has a row for it."""
    found: list[tuple[str, str, dict]] = []
    complete = True
    for pname, rows in passes.items():
        for k in keys:
            hit = [r for r in rows if r.get("metric") == metric and r.get("uc") in ucs
                   and str(r.get("lead")) == k and r.get("model") == bench_model
                   and all(math.isfinite(_f(r, c)) for c in ("diff_vs_ref", "diff_lo_park", "diff_hi_park"))]
            if hit:
                found.append((pname, k, hit[0]))
            else:
                complete = False
    if not found:
        return None

    def enough(r: dict) -> bool:
        return _f(r, "n_paired_origin_days") >= min_days and _f(r, "n_units_park") >= MIN_UNITS

    if complete and all(_true(r, "wins") and enough(r) for _, _, r in found):
        state = "better"
    elif complete and all(_f(r, "diff_lo_park") > 0 and enough(r) for _, _, r in found):
        state = "worse"
    else:
        state = "unclear"
    pname, key, w = max(found, key=lambda t: _f(t[2], "diff_hi_park"))
    return {"state": state, "diff": round(_f(w, "diff_vs_ref"), 3),
            "ciLow": round(_f(w, "diff_lo_park"), 3), "ciHigh": round(_f(w, "diff_hi_park"), 3),
            "_worst": (pname, key, w), "_complete": complete, "_n": len(found)}


def run_period(run: Path) -> str | None:
    first, last = [], []
    for p in sorted(run.glob("run-*.json")):
        o = json.loads(p.read_text()).get("origins") or []
        if len(o) == 2:
            first.append(o[0])
            last.append(o[1])
    return f"{min(first)}..{max(last)}" if first else None


def entries(run: Path, passes: dict[str, list[dict]], min_days: int) -> dict[tuple[str, str], dict]:
    out = {}
    word = {"better": "besser als", "worse": "schlechter als", "unclear": "nicht sicher anders als"}
    for qid, (metric, vmetric, ucs, keys, columns) in QUESTIONS.items():
        for mid, bench_model in columns.items():
            v = cell_verdict(passes, metric, ucs, keys, bench_model, min_days)
            if v is None:
                continue
            pname, key, w = v.pop("_worst")
            complete, n = v.pop("_complete"), v.pop("_n")
            gap = "" if complete else " Nicht jeder Abstand war in jedem Durchgang messbar."
            note = (f"Offline gegen das, was heute läuft (archivierte Kurven): {word[v['state']]} "
                    f"heute. Ungünstigster Abstand {lead_label(key)} ({pname}): "
                    f"{num(v['diff'])} [{num(v['ciLow'])}; {num(v['ciHigh'])}] Minuten, "
                    f"{int(_f(w, 'n_units_park'))} Parks, {int(_f(w, 'n_paired_origin_days'))} Tage; "
                    f"{n} Zellen aus {len(passes)} Durchgängen.{gap}")
            out[(qid, mid)] = {"state": v["state"], "reference": REFERENCE, "metric": vmetric,
                               "runId": run.name, "diff": v["diff"], "ciLow": v["ciLow"],
                               "ciHigh": v["ciHigh"],
                               "source": f"ml-bench {run.name} (gegen die archivierten Kurven, PAR-1043)",
                               "period": run_period(run), "note": note}
    return out


def apply(verdicts: dict, new: dict[tuple[str, str], dict]) -> dict:
    out = json.loads(json.dumps(verdicts))
    for (qid, mid), e in new.items():
        if e["period"] is None:
            e = {k: v for k, v in e.items() if k != "period"}
        out.setdefault("questions", {}).setdefault(qid, {})[mid] = e
    return out


def dump(obj: dict) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mlbench.served_verdicts", description=__doc__.split("\n")[0])
    p.add_argument("--run", required=True, help="results/<run-id> (reported with --vs served)")
    p.add_argument("--pass", dest="passes", action="append", default=[],
                   help="report tables dir inside the run (repeatable; default: every tables* "
                        "dir with a cells_vs_served.csv)")
    p.add_argument("--verdicts", default=str(DEFAULT_VERDICTS))
    p.add_argument("--min-origin-days", type=int, default=14,
                   help="paired origin days a cell needs to count as better/worse (default 14, "
                        "about the archive's 14-day retention window)")
    p.add_argument("--write", action="store_true", help="write the file (default: print the diff)")
    a = p.parse_args(argv)
    run = Path(a.run)
    dirs = [run / x for x in a.passes] or sorted(d for d in run.glob("tables*")
                                                 if (d / "cells_vs_served.csv").exists())
    if not dirs:
        print(f"no cells_vs_served.csv under {run} — run `report --vs served` first", file=sys.stderr)
        return 2
    passes = {d.name: read_pass(d / "cells_vs_served.csv") for d in dirs}
    path = Path(a.verdicts)
    old = path.read_text()
    new = dump(apply(json.loads(old), entries(run, passes, a.min_origin_days)))
    if a.write:
        path.write_text(new)
        print(f"wrote {path}")
    else:
        sys.stdout.writelines(difflib.unified_diff(old.splitlines(True), new.splitlines(True),
                                                   str(path), str(path) + " (generated)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
