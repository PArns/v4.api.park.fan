"""PAR-829 coverage census: the EXACT asymmetric gap, not a ratio of sums.

Two different questions get confused easily, so this tool answers them separately.

1. **Plug-in path parity (a GATE).** `example_level_h5` reproduces the built-in SQL
   column `lvlh5_naive` exactly, so if the plug-in arm covers fewer slots than the
   built-in arm the image is missing PAR-827's coverage-parity fixes and every paired
   diff is computed over a shifted intersection. That is how a sibling workstream lost
   a whole run. Must be exactly 0.

2. **Model coverage (a D9 NUMBER, never a gate).** `nf_*` legitimately covers fewer
   slots than a baseline, because `MIN_INSAMPLE_HOURS` suppresses rides with too little
   measured history in the input window. That is a reportable coverage figure.

The point of this script is that `sum(n__model) / sum(n__ref)` -- a ratio of sums -- can
sit at 1.00 while the two arms cover *different* slots, because a surplus in one group
offsets a deficit in another. The paired columns the runner writes give the exact answer:

    pn__<m>__<r> = |{slots where y, m and r are all non-null}| = |m AND r|
    n__<r>       = |r|
    => slots the reference covers that the model does NOT = n__<r> - pn__<m>__<r>

which is the quantity the review actually asked for. Requires the run to have been
scored with MLBENCH_EXTRA_PAIR_REFS covering <r> (or <r> in REF_CANDIDATES).

    python par829_coverage.py --run results/<id> [--model nf_tide ...] [--ref h5 ...]
"""

from __future__ import annotations

import argparse
import glob
import sys

import duckdb

GATE_PAIR = ("example_level_h5", "lvlh5_naive")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--model", action="append", default=[])
    ap.add_argument("--ref", action="append", default=[])
    ap.add_argument("--max-lead", type=int, default=7)
    ap.add_argument("--memory", default="1500MB")
    ap.add_argument("--threads", type=int, default=2)
    a = ap.parse_args()
    models = a.model or ["nf_tide", "nf_tide_x_h5", "nf_nhits", "nf_tsmixerx"]
    refs = a.ref or ["h5", "lvlh5_tft", "wt_med", "clim", "snaive7"]

    files = sorted(glob.glob(f"{a.run}/parts/slot/*.parquet"))
    if not files:
        print(f"FAIL -- no slot parts under {a.run}/parts/slot/")
        return 1
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.memory}'")
    con.execute(f"SET threads={a.threads}")
    cols = {c[0] for c in con.execute(
        "DESCRIBE SELECT * FROM read_parquet($1, union_by_name=true)", [files]).fetchall()}
    print(f"{len(files)} slot part files, {len(cols)} columns\n")

    # ---- 1. the GATE: plug-in path must reproduce the built-in arm exactly
    gm, gb = GATE_PAIR
    rc = 0
    if f"n__{gm}" in cols and f"n__{gb}" in cols:
        q = con.execute(f"""
            SELECT L, sum(n__{gb}) b, sum(n__{gm}) p,
                   sum(sae__{gb}) sb, sum(sae__{gm}) sp
            FROM read_parquet($1, union_by_name=true) GROUP BY L ORDER BY L""",
                        [files]).df()
        bad = 0
        for _, r in q.iterrows():
            ok = int(r.b) == int(r.p) and abs(float(r.sb) - float(r.sp)) < 1e-6
            bad += not ok
            print(f"GATE L={int(r.L):>3} builtin={int(r.b):>9} plugin={int(r.p):>9} "
                  f"sae_diff={float(r.sb) - float(r.sp):+.6g}  {'OK' if ok else 'BROKEN'}")
        print(f"\nPLUG-IN PATH PARITY: {'PASS' if bad == 0 else f'FAIL ({bad} leads)'}\n")
        rc = 1 if bad else 0
    else:
        print(f"GATE SKIPPED -- {gm}/{gb} not both in this run\n")

    # ---- 2. the D9 census: exact asymmetric gap per (model, ref, lead)
    print("EXACT ASYMMETRIC COVERAGE  (ref_only = slots the reference covers and the "
          "model does NOT = n__ref - pn__model__ref)")
    print(f"{'model':<16}{'ref':<12}{'L':>4}{'n_ref':>11}{'n_model':>11}"
          f"{'paired':>11}{'ref_only':>10}{'model_only':>11}{'share':>8}")
    rows = []
    for m in models:
        if f"n__{m}" not in cols:
            print(f"{m:<16}-- not scored in this run --")
            continue
        for r in refs:
            if f"pn__{m}__{r}" not in cols or f"n__{r}" not in cols:
                continue
            q = con.execute(f"""
                SELECT L, sum(n__{r}) nr, sum(n__{m}) nm, sum(pn__{m}__{r}) pn
                FROM read_parquet($1, union_by_name=true)
                WHERE L <= {a.max_lead} GROUP BY L ORDER BY L""", [files]).df()
            for _, x in q.iterrows():
                nr, nm, pn = int(x.nr), int(x.nm), int(x.pn)
                rows.append(dict(model=m, ref=r, L=int(x.L), n_ref=nr, n_model=nm,
                                 paired=pn, ref_only=nr - pn, model_only=nm - pn,
                                 paired_share=(pn / nr) if nr else float("nan")))
                print(f"{m:<16}{r:<12}{int(x.L):>4}{nr:>11}{nm:>11}{pn:>11}"
                      f"{nr - pn:>10}{nm - pn:>11}{(pn / nr) if nr else 0:>8.4f}")
    if not rows:
        print("  (no paired columns for these model/ref pairs -- was "
              "MLBENCH_EXTRA_PAIR_REFS set at scoring time?)")
    return rc


if __name__ == "__main__":
    sys.exit(main())
