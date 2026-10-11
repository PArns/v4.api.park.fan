"""Apply PAR-829's PRE-REGISTERED decision rule to the paired-margin CSVs.

The rule was fixed and logged before the first scored cell existed, so running it as
code makes the verdict a derivation rather than a narrative.

Real incumbents (from PAR-827's committed tables): ``lvlh5_tft`` is strongest at d1-d3
on segment ``all`` and at EVERY lead on ``busy``; ``h5`` is strongest at d4-d7 on
``all``; ``persistence`` is additionally an incumbent on the intraday surface. Beating
only ``wt_med`` / ``clim`` / ``snaive7`` is NOT a serve case at any lead these models
produce.

THE SERVE CEILING. The committed rule requires, for SERVE, that "the sign agrees in the
full-period pass as well as the headline window". "Full period" means the 231
cross-season origins 2026-02-19..2026-10-07 that PAR-827 and PAR-830 use. This run has
61 consecutive AUTUMN-SHOULDER origins and no non-autumn origin at all, so that clause
is unmet BY CONSTRUCTION rather than by result, and the best recommendation this run can
support is SHADOW -- at every lead, for every arm, however large the margin. That cap is
applied here in code, and the reason is printed next to the cell, so
SHADOW-because-autumn-only is never confused with SHADOW-because-the-CI-spans-zero.

    python3 par829_verdict.py <margin.csv> [...]
"""
import collections
import csv
import sys

INCUMBENT = {"slot": ["lvlh5_tft", "h5"], "intraday": ["lvlh5_tft", "h5", "persistence"]}
MIN_UNITS, MIN_SHARE, MIN_ORIGIN_DAYS = 10, 0.9, 30
# The run cannot satisfy the cross-season sign clause, so no cell may print SERVE.
SERVE_CLAUSE_TESTABLE = False


def f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def main(paths):
    rows = []
    missing = []
    for path in paths:
        try:
            with open(path, newline="") as fh:
                for r in csv.DictReader(fh):
                    r["_src"] = path.rsplit("/", 1)[-1]
                    rows.append(r)
        except FileNotFoundError:
            missing.append(path)
    for m in missing:
        print(f"(MISSING INPUT: {m})")
    if not rows:
        print("no margin rows found -- refusing to print an empty verdict")
        return 1

    by = collections.defaultdict(dict)
    for r in rows:
        part = "intraday" if "intraday" in r["_src"] else "slot"
        by[(r["_src"], part, r["segment"], r["model"], str(r["lead"]))][r["ref"]] = r

    print("PAR-829 pre-registered decision rule, applied to "
          f"{len(rows)} margin cells from {len(paths) - len(missing)} file(s).")
    print("AUTUMN-SHOULDER ORIGINS 2026-08-08..2026-10-07 -- not comparable with any "
          "full-period figure.")
    print(f"serve clause testable on this run: {SERVE_CLAUSE_TESTABLE} "
          "(no non-autumn origin -> SHADOW is the ceiling)\n")
    print(f"{'source':38}{'seg':6}{'model':24}{'lead':>5}  verdict / why")
    print("-" * 124)
    tally = collections.Counter()
    for (src, part, seg, model, lead), refs in sorted(by.items()):
        inc = [i for i in INCUMBENT[part] if i in refs]
        if not inc:
            print(f"{src:38}{seg:6}{model:24}{lead:>5}  NOT TESTED (no incumbent paired here)")
            tally["not tested"] += 1
            continue
        states, notes = [], []
        for i in inc:
            r = refs[i]
            d, lo, hi = f(r["diff"]), f(r["lo_park"]), f(r["hi_park"])
            u = int(r["units_park"] or 0)
            share = f(r.get("paired_share_of_ref", "nan"))
            nod = int(r.get("n_origin_days") or 0)
            wins_pk = str(r["wins_park"]) == "True"
            wins_pd = str(r["wins_park_day"]) == "True"
            gate = []
            if u < MIN_UNITS:
                gate.append(f"units {u}<{MIN_UNITS}")
            if not (hi - lo) > 0:
                gate.append("zero-width CI")
            if share < MIN_SHARE:
                gate.append(f"share {share:.3f}<{MIN_SHARE}")
            if nod < MIN_ORIGIN_DAYS:
                gate.append(f"LOW-N {nod}<{MIN_ORIGIN_DAYS}")
            if gate:
                states.append("gated")
                notes.append(f"{i}: {d:+.3f} GATED [{'; '.join(gate)}]")
            elif wins_pk:
                states.append("win")
                notes.append(f"{i}: {d:+.3f} beats pk[{lo:+.3f},{hi:+.3f}]")
            elif wins_pd:
                states.append("pd-only")
                notes.append(f"{i}: {d:+.3f} park-day only (park-cluster CI spans 0)")
            elif lo > 0:
                # the CI lies ENTIRELY on the wrong side of zero: a real regression
                states.append("regression")
                notes.append(f"{i}: {d:+.3f} REGRESSION pk[{lo:+.3f},{hi:+.3f}]")
            elif d < 0:
                states.append("ns")
                notes.append(f"{i}: {d:+.3f} better, not significant")
            else:
                # positive point estimate but the interval spans 0 -- NOT a regression.
                # Calling this a regression would turn noise into a DROP recommendation.
                states.append("ns")
                notes.append(f"{i}: {d:+.3f} worse, not significant")
        if any(s == "regression" for s in states):
            v = "DROP on this cell (significant regression vs an incumbent)"
        elif all(s == "win" for s in states):
            v = ("SHADOW (beats every incumbent; SERVE withheld -- cross-season sign "
                 "clause untestable on autumn-only origins)"
                 if not SERVE_CLAUSE_TESTABLE else "SERVE-ELIGIBLE")
        elif any(s in ("win", "pd-only") for s in states):
            v = "SHADOW (beats some but not all incumbents)"
        elif any(s == "gated" for s in states):
            v = "no win (gated)"
        else:
            v = "no win"
        tally[v.split(" (")[0]] += 1
        print(f"{src:38}{seg:6}{model:24}{lead:>5}  {v}")
        for n in notes:
            print(f"{'':73}{n}")
    print("\ntally: " + ", ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
