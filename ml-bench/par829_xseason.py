"""PAR-829 cross-season comparison: the autumn-shoulder run against the non-autumn run,
cell by cell, and the pre-registered SERVE clause re-read on the result.

WHY THIS TOOL EXISTS. Stage A scored 61 consecutive AUTUMN-SHOULDER origins
(2026-08-08..2026-10-07) and nothing else, so the pre-registered rule's cross-season sign
clause was unmet BY CONSTRUCTION and every recommendation was capped at SHADOW. The
critic's top follow-up was the 170 origins that complete the period
(2026-02-19..2026-08-07, zero overlap, union = the 231-origin full period PAR-827 and
PAR-830 use). This tool reads the two runs' committed paired-margin CSVs and answers three
questions with numbers rather than with prose:

  1. Does the SIGN agree across the two disjoint origin sets, cell by cell?
  2. Does each capped cell still pass every testable gate on NON-AUTUMN origins -- the
     bootstrap units, the CI excluding 0, the reference-side share and, separately, the
     BENCH-SPEC #8 candidate-side share, which is the one that can quietly collapse when a
     thin reference like `lvlh5_tft` is paired over origins production barely covered?
  3. What does the pre-registered rule therefore recommend now?

HOW THE CLAUSE IS READ, stated because it is a deviation. The rule says "the same sign in
the full-period pass as in the headline window", and "full period" means the 231 origins.
This compares two DISJOINT passes -- autumn (61) and non-autumn (170) -- instead of one
pooled pass over 231. That is a STRICTER test, not a looser one: a pooled pass can satisfy
a sign clause on the strength of whichever half contributes more rows, while agreement
across the two halves cannot. It is also the only form available here, because the
non-autumn run deliberately drops the ORACLE arm (it can never carry a recommendation and
cost ~25 % of per-origin time), so the two runs' parts carry different column sets and must
not be pooled into one report. A pooled 231-origin pass needs the ORACLE arm scored over
the 170 origins as well.

    python3 par829_xseason.py --autumn results/20261011-par829-stageA \\
                              --xseason results/20261011-par829-xseason [--pass window]
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import date
import sys
from pathlib import Path

MIN_UNITS, MIN_SHARE, MIN_ORIGIN_DAYS, MIN_COVERAGE = 10, 0.9, 30, 0.30
INCUMBENT = {"slot": ("lvlh5_tft", "h5"), "intraday": ("lvlh5_tft", "h5", "persistence")}
SERVABLE = ("nf_tide", "nf_nhits", "nf_tide_x_h5", "nf_nhits_x_h5")


def read(p: Path):
    with open(p, newline="") as f:
        return list(csv.DictReader(f))


def fl(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def s3(v, nd=3):
    return "—" if v is None else f"{v:+.{nd}f}"


def key(r, part):
    return (part, r["segment"], r["model"], str(r["lead"]), r["ref"])


def load(run: Path, lbl: str):
    out = {}
    for part in ("slot", "intraday"):
        f = run / f"paired_margin_{lbl}_{part}.csv"
        if not f.exists():
            continue
        for r in read(f):
            out[key(r, part)] = r
    return out


def cov_model(r):
    n_p, n_m = fl(r.get("n_paired")), fl(r.get("n_model"))
    return None if not n_m else n_p / n_m


def gates(r):
    """Every gate the pre-registered rule can test on a cell. Returns the list of
    FAILURES, so an empty list means the cell is testable and passed."""
    bad = []
    u = int(r.get("units_park") or 0)
    lo, hi = fl(r["lo_park"]), fl(r["hi_park"])
    share, cm = fl(r.get("paired_share_of_ref")), cov_model(r)
    nod = int(r.get("n_origin_days") or 0)
    if u < MIN_UNITS:
        bad.append(f"units {u}<{MIN_UNITS}")
    if lo is None or hi is None or not (hi - lo) > 0:
        bad.append("zero-width CI")
    if share is None or share < MIN_SHARE:
        bad.append(f"ref-share {share if share is None else round(share, 3)}<{MIN_SHARE}")
    if cm is None or cm < MIN_COVERAGE:
        bad.append(f"NOT TESTED: model-side coverage "
                   f"{cm if cm is None else round(cm, 3)}<{MIN_COVERAGE}")
    if nod < MIN_ORIGIN_DAYS:
        bad.append(f"LOW-N {nod}<{MIN_ORIGIN_DAYS}")
    return bad


def wins(r):
    return str(r.get("wins_park")) == "True"


def run_jsons(run: Path):
    """Every shard's run json, sorted. A sharded run writes one per shard
    (`run-0of2.json`, `run-1of2.json`); an unsharded one writes `run-0of1.json`."""
    # `run-0of1.PRE-RESUME.json` is a SNAPSHOT of the same shard taken before the resume
    # salvage, not a second shard, so counting it would invent one.
    files = [f for f in sorted(run.glob("run-*.json")) if "PRE-RESUME" not in f.name]
    return [json.loads(f.read_text()) for f in files]


PROV_KEYS = ("git_sha", "code_sha256", "image_id", "export", "models", "config", "reference")


def shard_consistency(js):
    """Which provenance fields are NOT identical across a run's shards. Must be empty:
    shards of one run differ only in `--shard` and their start time, and if they differ in
    anything else the run is not one measurement and must not be reported as one."""
    bad = []
    for k in PROV_KEYS:
        if len({json.dumps(j.get(k), sort_keys=True) for j in js}) > 1:
            bad.append(k)
    return bad


def section_provenance(a: Path, x: Path):
    print("\n## 1. Provenance — the two runs must differ ONLY in their origins and arms\n")
    rows, run_of = [], {}
    for lbl, run in (("autumn (Stage A)", a), ("non-autumn", x)):
        js = run_jsons(run)
        if not js:
            print(f"REFUSING TO EMIT: no run-*.json in {run}", file=sys.stderr)
            raise SystemExit(1)
        bad = shard_consistency(js)
        if bad:
            print(f"REFUSING TO EMIT: {run.name} shards disagree on {bad}", file=sys.stderr)
            raise SystemExit(1)
        rows.append((lbl, js[0]))
        run_of[lbl] = (run, js)
    keys = ("git_sha", "code_sha256", "image_id", "export", "models", "argv")
    print("| field | " + " | ".join(r[0] for r in rows) + " | identical |")
    print("|---|---|---|---|")
    for k in keys:
        vals = []
        for _, j in rows:
            v = j.get(k, j.get("config", {}).get(k) if isinstance(j.get("config"), dict) else None)
            if isinstance(v, list):
                v = " ".join(str(x) for x in v)
            v = str(v)
            vals.append(v if len(v) < 120 else v[:117] + "…")
        same = "**yes**" if vals[0] == vals[1] else "no"
        print(f"| `{k}` | " + " | ".join(f"`{v}`" for v in vals) + f" | {same} |")
    for lbl, _ in rows:
        run, js = run_of[lbl]
        # `origins` in run-*.json is the [first, last] pair per shard, not the enumeration;
        # the origin COUNT that matters is the number of _done sentinels, which is what the
        # report actually read.
        lo = min((j["origins"][0] for j in js if j.get("origins")), default="?")
        hi = max((j["origins"][-1] for j in js if j.get("origins")), default="?")
        # parts/ is gitignored, so in a checkout the sentinels are absent. Then fall back to
        # the calendar span the run json records -- both runs use stride 1, so the span IS
        # the origin count -- and SAY which of the two the number came from, because one is
        # a count of what was scored and the other is a count of what was asked for.
        done_dir = run / "parts" / "_done"
        if done_dir.is_dir():
            done = f"{len(list(done_dir.glob('*')))} scored origins (parts/_done sentinels)"
        elif lo != "?" and hi != "?":
            n = (date.fromisoformat(hi) - date.fromisoformat(lo)).days + 1
            done = (f"{n} origins by the recorded span at stride 1 "
                    "(parts/ is gitignored, so the sentinels are not in a checkout)")
        else:
            done = "? origins"
        secs = [j.get("seconds") for j in js if j.get("seconds") is not None]
        fin = [j.get("finished_utc") for j in js if j.get("finished_utc")]
        shard_note = "" if len(js) == 1 else (
            f", {len(js)} shards (provenance fields identical across shards; they differ "
            f"only in `--shard` and start time), wall "
            f"{max(secs):.1f} s longest shard" if secs else f", {len(js)} shards")
        print(f"\n- {lbl}: origins {lo} … {hi}, "
              f"{done}, "
              f"{('%.1f s' % sum(secs)) if secs else '? s'} CPU-side total"
              f"{shard_note}, finished {max(fin) if fin else '?'}")
    print("\n`git_sha`, `code_sha256`, `image_id` and `export` identical is the one-SHA "
          "property: the same harness, in the same image, over the same export, scored "
          "both origin sets. `models` and `argv` differ by design (the ORACLE arm and the "
          "date range).\n")


def section_coverage(x: Path, lbl: str):
    print("\n## 2. Coverage on non-autumn origins — read BEFORE any margin\n")
    print("`lvlh5_tft` exists only where production stored a daily TFT level, which skews "
          "LATE in the export. Stage A's 61 origins are exactly that late part and "
          "measured 0.97–0.98; PAR-827 measured 0.669–0.722 over 231 origins. So the "
          "candidate-side and reference-side shares on these 170 earlier origins are the "
          "first thing to look at, not the last.\n")
    rows = [r for r in read(x / f"paired_margin_{lbl}_slot.csv")
            if r["model"] == "nf_tide" and r["segment"] == "all"]
    print("| ref | lead | `cov_ref` | `cov_model` | units pk | origin days | gate |")
    print("|---|---|---|---|---|---|---|")
    for r in sorted(rows, key=lambda r: (r["ref"], float(r["lead"]))):
        cm = cov_model(r)
        g = gates(r)
        print(f"| `{r['ref']}` | {int(float(r['lead']))} | "
              f"{fl(r['paired_share_of_ref']):.4f} | "
              f"{'—' if cm is None else f'{cm:.4f}'} | {r['units_park']} | "
              f"{r['n_origin_days']} | {'; '.join(g) if g else 'all pass'} |")
    print()


def section_cells(a, x, part, refs, models, title, lead_filter=None):
    print(f"\n{title}\n")
    print("| segment | arm | lead | ref | autumn margin [park-cluster] | non-autumn margin "
          "[park-cluster] | sign agrees | non-autumn gates |")
    print("|---|---|---|---|---|---|---|---|")
    n_agree = n_tot = 0
    missing = []
    for k, ra in sorted(a.items()):
        p, seg, model, lead, ref = k
        if p != part or ref not in refs or model not in models:
            continue
        if lead_filter and lead not in lead_filter:
            continue
        rx = x.get(k)
        if rx is None:
            missing.append(f"{seg} {model} {lead} vs {ref}")
            continue
        da, dx = fl(ra["diff"]), fl(rx["diff"])
        g = gates(rx)
        agree = (da is not None and dx is not None
                 and ((da < 0 and dx < 0) or (da > 0 and dx > 0)))
        n_tot += 1
        n_agree += 1 if agree else 0
        print(f"| {seg} | `{model}` | {lead} | `{ref}` | "
              f"{s3(da)} [{s3(fl(ra['lo_park']))}, {s3(fl(ra['hi_park']))}] | "
              f"{s3(dx)} [{s3(fl(rx['lo_park']))}, {s3(fl(rx['hi_park']))}] | "
              f"{'**yes**' if agree else '**NO**'} | "
              f"{'; '.join(g) if g else 'all pass'} |")
    print(f"\n**Sign agrees at {n_agree} of {n_tot} cells.**")
    if missing:
        print(f"\nNot present in the non-autumn run ({len(missing)}): "
              + "; ".join(missing[:12]) + ("…" if len(missing) > 12 else ""))
    print()
    return n_agree, n_tot


def section_serve(a, x, part):
    """The pre-registered rule, now with the cross-season clause TESTABLE.

    A cell reaches SERVE-ELIGIBLE only if, in the autumn pass, it beats EVERY incumbent
    with the park-cluster CI excluding 0 and every gate passing -- and, in the non-autumn
    pass, the margin against every incumbent keeps its sign AND the gates there pass. A
    sign that flips, or a cell that becomes untestable on the other origin set, leaves the
    recommendation at SHADOW. That is the rule as logged at 2026-10-11T10:48:12Z, with
    SERVE_CLAUSE_TESTABLE now true for the arms the second run scored.
    """
    print(f"\n## 4. The pre-registered SERVE clause, re-read on the {part} part\n")
    inc = INCUMBENT[part]
    cells = {}
    for (p, seg, model, lead, ref), r in a.items():
        if p == part and ref in inc and model in SERVABLE:
            cells.setdefault((seg, model, lead), {})[ref] = r
    print("| segment | arm | lead | autumn verdict | cross-season | recommendation |")
    print("|---|---|---|---|---|---|")
    tally = {}
    for (seg, model, lead), refs in sorted(cells.items()):
        have = [i for i in inc if i in refs]
        if not have:
            continue
        a_win = all(wins(refs[i]) and not gates(refs[i]) for i in have)
        if not a_win:
            continue                      # only the capped cells are at issue here
        notes, ok = [], True
        for i in have:
            rx = x.get((part, seg, model, lead, i))
            if rx is None:
                notes.append(f"{i}: not scored")
                ok = False
                continue
            da, dx = fl(refs[i]["diff"]), fl(rx["diff"])
            g = gates(rx)
            if g:
                notes.append(f"{i}: {dx:+.3f} " + "; ".join(g))
                ok = False
            elif (da < 0) != (dx < 0):
                notes.append(f"{i}: {dx:+.3f} SIGN FLIPS")
                ok = False
            elif not wins(rx):
                notes.append(f"{i}: {dx:+.3f} same sign, not significant")
            else:
                notes.append(f"{i}: {dx:+.3f} same sign, significant")
        v = "**SERVE-ELIGIBLE**" if ok else "SHADOW (stays)"
        tally[v] = tally.get(v, 0) + 1
        print(f"| {seg} | `{model}` | {lead} | beats all of "
              f"{'/'.join('`' + i + '`' for i in have)} | " + "; ".join(notes)
              + f" | {v} |")
    print("\ntally: " + ", ".join(f"{k} = {v}" for k, v in sorted(tally.items()))
          + ("" if tally else "no capped cell on this part"))
    print()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--autumn", required=True)
    ap.add_argument("--xseason", required=True)
    ap.add_argument("--pass", dest="lbl", default="window",
                    choices=("window", "alltargets"))
    o = ap.parse_args(argv)
    A, X = Path(o.autumn), Path(o.xseason)
    a, x = load(A, o.lbl), load(X, o.lbl)
    if not a or not x:
        print(f"REFUSING TO EMIT: margins missing (autumn {len(a)} cells, "
              f"non-autumn {len(x)} cells) for pass '{o.lbl}'", file=sys.stderr)
        return 1
    print(f"# PAR-829 cross-season comparison — `{o.lbl}` pass")
    print(f"\nAutumn run `{A.name}` ({len(a)} margin cells) against non-autumn run "
          f"`{X.name}` ({len(x)} cells). Generated by `ml-bench/par829_xseason.py` from "
          "the two runs' committed CSVs; no figure is hand-transcribed. The two origin "
          "sets are DISJOINT and their union is the 231-origin full period.")
    section_provenance(A, X)
    section_coverage(X, o.lbl)
    print("\n## 3. Sign agreement, cell by cell\n")
    section_cells(a, x, "intraday", INCUMBENT["intraday"], ("nf_tide", "nf_nhits"),
                  "### 3a. The live / intraday surface — where every positive is")
    section_cells(a, x, "slot", ("lvlh5_tft", "h5"), SERVABLE,
                  "### 3b. The planner surface — the open question")
    section_serve(a, x, "intraday")
    section_serve(a, x, "slot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
