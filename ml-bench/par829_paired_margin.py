"""PAR-829: paired margin of a candidate against each NAMED baseline, per lead.

Why this exists. ``baselines.REF_CANDIDATES`` is ``snaive7 / wt_med / clim``, so a run's
``cells.csv`` only carries a paired difference against the per-lead *reference* — and the
reference is chosen by an argmin over those three. The incumbents a PAR-829 candidate
actually has to beat are ``lvlh5_tft`` (TFT daily level x H5 profile) and ``h5``, neither
of which is a reference candidate. The baseline review's finding B1 is the same problem
from the other side: an unpaired argmin over naives can manufacture a "nothing beats it"
result, so "beats the reference" is not the claim to make. This tool reports the margin
against each baseline **by name**, paired on the slots where both arms produced a value.

It reads the run's own ``parts/slot/*.parquet`` and reuses the harness's bootstrapper, so
the numbers are computed exactly as ``report.evaluate`` computes them. The paired columns
have to have been written at scoring time (``MLBENCH_EXTRA_PAIR_REFS=h5,lvlh5_tft``); a
paired intersection cannot be reconstructed afterwards from per-model aggregates.

Both CIs are reported (review finding B5): the park-cluster bootstrap is the one a win
claim rests on, the park-day bootstrap is narrower, and where they disagree that
disagreement is the finding.

    python /app/par829_paired_margin.py --run /app/results/<run> \
        --model nf_tide --ref h5 --ref lvlh5_tft --ref wt_med --ref clim \
        --target-from 2026-08-15 --target-to 2026-10-07
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, "/app")
from mlbench.stats import Bootstrapper, significant  # noqa: E402

REPS, SEED = 1000, 827


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--model", action="append", required=True)
    ap.add_argument("--ref", action="append", required=True)
    ap.add_argument("--target-from", default=None)
    ap.add_argument("--target-to", default=None)
    ap.add_argument("--segment", action="append", default=None,
                    help="all | busy | headliners (default: all and busy)")
    ap.add_argument("--memory", default="2000MB")
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--out", default=None, help="write the rows to this CSV as well")
    a = ap.parse_args(argv)
    segments = a.segment or ["all", "busy"]

    run = Path(a.run)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.memory}'")
    con.execute(f"SET threads={a.threads}")
    tmp = run / "work" / "par829-margin"
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{tmp}'")

    flt = ["TRUE"]
    if a.target_from:
        flt.append(f"date >= DATE '{a.target_from}'")
    if a.target_to:
        flt.append(f"date <= DATE '{a.target_to}'")
    con.execute(f"""CREATE TABLE slot AS SELECT * FROM
        read_parquet('{run}/parts/slot/*.parquet', union_by_name=true)
        WHERE {' AND '.join(flt)}""")
    cols = {r[0] for r in con.execute("DESCRIBE slot").fetchall()}

    boot_pd = Bootstrapper(REPS, SEED)
    boot_park = Bootstrapper(REPS, SEED + 1)
    rows: list[dict] = []
    leads = [r[0] for r in con.execute("SELECT DISTINCT L FROM slot ORDER BY L").fetchall()]

    for seg in segments:
        seg_sql = {"all": "TRUE", "busy": "busy", "headliners": "hl"}.get(seg, seg)
        for m in a.model:
            if f"n__{m}" not in cols:
                print(f"SKIP: the run has no column n__{m} (model not scored)")
                continue
            for r in a.ref:
                missing = [c for c in (f"pn__{m}__{r}", f"psm__{m}__{r}", f"psr__{m}__{r}")
                           if c not in cols]
                if missing:
                    print(f"SKIP {m} vs {r}: no paired columns in this run "
                          f"(re-score with MLBENCH_EXTRA_PAIR_REFS={r})")
                    continue
                g = con.execute(f"""
                    SELECT L, park_id, date,
                           sum(pn__{m}__{r}) AS pn, sum(psm__{m}__{r}) AS psm,
                           sum(psr__{m}__{r}) AS psr,
                           sum(n__{m}) AS n_m, sum(sae__{m}) AS sae_m,
                           sum(n__{r}) AS n_r, sum(sae__{r}) AS sae_r
                    FROM slot WHERE {seg_sql} GROUP BY 1, 2, 3""").df()
                for L in leads:
                    d = g[g["L"] == L]
                    p = d[d["pn"] > 0]
                    if p.empty:
                        continue
                    keys_pd = p["park_id"].astype(str) + "|" + p["date"].astype(str)
                    nm, dm = p["psm"].to_numpy(), p["pn"].to_numpy()
                    nr, dr = p["psr"].to_numpy(), p["pn"].to_numpy()
                    diff, lo_pd, hi_pd = boot_pd.diff(keys_pd, nm, dm, nr, dr)
                    _, lo_pk, hi_pk = boot_park.diff(p["park_id"].astype(str), nm, dm, nr, dr)
                    n_m, n_r = d["n_m"].sum(), d["n_r"].sum()
                    rows.append(dict(
                        segment=seg, lead=int(L), model=m, ref=r,
                        model_mae_own=d["sae_m"].sum() / n_m if n_m else float("nan"),
                        ref_mae_own=d["sae_r"].sum() / n_r if n_r else float("nan"),
                        model_mae_paired=nm.sum() / dm.sum(),
                        ref_mae_paired=nr.sum() / dr.sum(),
                        diff=diff, lo_park_day=lo_pd, hi_park_day=hi_pd,
                        lo_park=lo_pk, hi_park=hi_pk,
                        wins_park=significant(lo_pk, hi_pk, True),
                        wins_park_day=significant(lo_pd, hi_pd, True),
                        n_model=int(n_m), n_ref=int(n_r), n_paired=int(dm.sum()),
                        # share of the BASELINE's covered slots that the pair retains:
                        # the candidate's coverage loss (MIN_INSAMPLE_HOURS) shows up here
                        paired_share_of_ref=float(dm.sum()) / float(n_r) if n_r else float("nan"),
                        coverage_vs_ref=float(n_m) / float(n_r) if n_r else float("nan"),
                        n_origin_days=int(d["date"].nunique()),
                        n_park_days=int((d["pn"] > 0).sum())))

    if not rows:
        print("no paired rows produced")
        return 1
    df = pd.DataFrame(rows)
    pd.set_option("display.width", 220)
    show = ["segment", "lead", "model", "ref", "model_mae_paired", "ref_mae_paired", "diff",
            "lo_park", "hi_park", "wins_park", "lo_park_day", "hi_park_day", "wins_park_day",
            "paired_share_of_ref", "coverage_vs_ref", "n_origin_days"]
    print(df[show].to_string(index=False, float_format=lambda v: f"{v:.4f}"))
    print("\ndiff < 0 means the CANDIDATE is better. A win requires the PARK-CLUSTER "
          "interval to exclude 0.\nStaleness asymmetry: the candidate's weights are up to "
          "31 days stale inside a month block,\nwhile h5 / wt_med / clim are recomputed at "
          "every origin. The sign of that bias is NOT\nestablished -- do not read these "
          "margins as a conservative lower bound.")
    if a.out:
        df.to_csv(a.out, index=False)
        print(f"\nwrote {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
