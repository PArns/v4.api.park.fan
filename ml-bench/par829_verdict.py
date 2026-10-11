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
# BENCH-SPEC recorded change #8. `wins == True` is only the statistical verdict; the
# publication gate is report.tested(), which lives in another function and is applied ONLY
# when handover.csv / usable_horizon.csv are built. It rejects a cell whose paired rows
# fall below MIN_COVERAGE = 0.30 of EITHER arm's own covered rows (`env_min_share` is the
# min over candidates), or that is LOW-N on the metric's own unit.
#
# Translating that into the paired-margin world catches a hole the ref-side share alone
# cannot see: `paired_share_of_ref` = n_paired / n_ref, so a reference with THIN coverage
# has a small denominator and scores near 1.0 while the pairing still covers only a sliver
# of the model's own cells. `lvlh5_tft` is exactly such a reference at long leads --
# production stores long-lead TFT rows only where a fresh run wrote them. So the model-side
# share is gated too, at the harness's own threshold.
MIN_COVERAGE = 0.30
# The run cannot satisfy the cross-season sign clause, so no cell may print SERVE.
SERVE_CLAUSE_TESTABLE = False


def is_oracle(model):
    """`_owx` marks an arm fed the target day's weather ACTUALS (PAR-827's S8 fix freezes
    the suffix from `type(m)` at runner init, so the name is earned rather than asserted).
    Matches the harness's own suffix test, including the composed `_owx_x_h5` form."""
    return "_owx" in model


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
    print("ORACLE arms (the _owx suffix) are listed for their headroom value and are "
          "NEVER\ngiven a recommendation: the pre-registered rule forbids an _owx row from "
          "producing\nSERVE or SHADOW, because it consumes the target day's weather "
          "ACTUALS.\n")
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
            n_paired, n_model = f(r.get("n_paired", "nan")), f(r.get("n_model", "nan"))
            share_model = n_paired / n_model if n_model and n_model == n_model else float("nan")
            nod = int(r.get("n_origin_days") or 0)
            wins_pk = str(r["wins_park"]) == "True"
            wins_pd = str(r["wins_park_day"]) == "True"
            gate = []
            if u < MIN_UNITS:
                gate.append(f"units {u}<{MIN_UNITS}")
            if not (hi - lo) > 0:
                gate.append("zero-width CI")
            if share < MIN_SHARE:
                gate.append(f"ref-share {share:.3f}<{MIN_SHARE}")
            if not (share_model >= MIN_COVERAGE):
                # BENCH-SPEC #8: report this as NOT TESTED with its coverage, never as a
                # loss. "we did not look" and "we looked and it lost" are different
                # findings and this review has twice published the former as the latter.
                gate.append(f"NOT TESTED: model-side coverage {share_model:.3f}"
                            f"<{MIN_COVERAGE}")
            if nod < MIN_ORIGIN_DAYS:
                gate.append(f"LOW-N {nod}<{MIN_ORIGIN_DAYS}")
            if gate:
                states.append("gated")
                notes.append(f"{i}: {d:+.3f} GATED [{'; '.join(gate)}] "
                             f"(cov ref {share:.3f} / model {share_model:.3f})")
            elif wins_pk:
                states.append("win")
                notes.append(f"{i}: {d:+.3f} beats pk[{lo:+.3f},{hi:+.3f}] "
                             f"(cov ref {share:.3f} / model {share_model:.3f})")
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
        # The pre-registered rule: "An ORACLE (`_owx`) row can never produce a SERVE or
        # SHADOW recommendation; it bounds the headroom a real weather forecast could buy
        # and nothing else." That was stated in the docstring but not enforced in the
        # classification, so an _owx row could print SHADOW -- which is a recommendation
        # word. It is enforced here instead, AFTER the states are computed, so the
        # underlying comparison is still reported.
        if is_oracle(model):
            n_win = sum(1 for s in states if s == "win")
            v = (f"ORACLE -- no recommendation (headroom only; beats {n_win}/{len(inc)} "
                 "incumbents)")
        elif any(s == "regression" for s in states):
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
