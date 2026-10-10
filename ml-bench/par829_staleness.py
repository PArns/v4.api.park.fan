"""PAR-829: measure the staleness slope instead of asserting its direction.

One fit per month block means an origin late in a block is served weights up to 31 days
old, while `h5` / `wt_med` / `clim` are recomputed at every origin. It is tempting to call
that a conservative handicap -- i.e. to claim any PAR-829 result is a lower bound on a
freshly retrained model -- but the direction is not guaranteed:

* it hurts us when a regime change inside the block is learnable but not covariate-encoded
  and the recomputed baselines track it;
* it can *help* us when the weeks immediately before an origin are anomalous, because a
  refit-at-origin would inherit the anomaly and the trailing-window baselines certainly
  do, while stale weights do not;
* and a refit is a fresh seed, so "fresher is at least as good" fails per origin anyway.

So the claim is withdrawn and replaced by this measurement. The slot parts are keyed by
(L, park_id, date, ...) where `date` is the TARGET date, so the origin is `date - L` and
the staleness of the weights serving it is `day_of_month(origin) - 1` (blocks are calendar
months, `nf_precompute.month_blocks`). What this prints, per model:

* MAE by staleness bucket, on the model's own covered slots, with a park-day CI; and
* the MAE of the same slots for a chosen recomputed baseline, so the *margin* can be read
  by bucket rather than only the level -- the margin is what the staleness argument is
  about.

A margin that worsens with staleness means the handicap is real and a pooled number is
pessimistic. A flat margin means staleness is immaterial on this data. A margin that
*improves* with staleness means staleness flatters us, which is the case the withdrawn
claim ruled out by assumption.

    python /app/par829_staleness.py --run /app/results/<run> --model nf_tide --ref h5
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, "/app")
from mlbench.stats import Bootstrapper  # noqa: E402

REPS, SEED = 1000, 827


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--model", action="append", required=True)
    ap.add_argument("--ref", action="append", default=None,
                    help="recomputed baseline(s) to read the MARGIN against (default h5)")
    ap.add_argument("--target-from", default=None)
    ap.add_argument("--target-to", default=None)
    ap.add_argument("--max-lead", type=int, default=7)
    ap.add_argument("--buckets", default="0-6,7-13,14-20,21-30",
                    help="staleness buckets in days since the block's first origin")
    ap.add_argument("--memory", default="2000MB")
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)
    refs = a.ref or ["h5"]

    run = Path(a.run)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.memory}'")
    con.execute("SET threads=2")
    tmp = run / "work" / "par829-staleness"
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{tmp}'")

    flt = [f"L <= {int(a.max_lead)}"]
    if a.target_from:
        flt.append(f"date >= DATE '{a.target_from}'")
    if a.target_to:
        flt.append(f"date <= DATE '{a.target_to}'")
    con.execute(f"""CREATE TABLE slot AS SELECT * FROM
        read_parquet('{run}/parts/slot/*.parquet', union_by_name=true)
        WHERE {' AND '.join(flt)}""")
    cols = {r[0] for r in con.execute("DESCRIBE slot").fetchall()}

    bounds = []
    for b in a.buckets.split(","):
        lo, _, hi = b.partition("-")
        bounds.append((int(lo), int(hi), b))
    case = " ".join(f"WHEN stale BETWEEN {lo} AND {hi} THEN '{lbl}'" for lo, hi, lbl in bounds)

    # True distinct ORIGIN count per bucket. It cannot come from the aggregation below:
    # that groups by (bucket, park_id, TARGET date), and one target date is reached from
    # several origins at different leads, so counting distinct `date` there would answer a
    # different question (and did, in an earlier version of this script).
    org = con.execute(f"""
        WITH s AS (SELECT CAST(date_part('day', date - CAST(L AS INTEGER)) AS INTEGER) - 1 AS stale,
                          date - CAST(L AS INTEGER) AS origin FROM slot)
        SELECT CASE {case} ELSE 'other' END AS bucket,
               count(DISTINCT origin) AS n_origin_days,
               min(origin) AS first_origin, max(origin) AS last_origin
        FROM s GROUP BY 1""").df().set_index("bucket")

    boot = Bootstrapper(REPS, SEED)
    rows = []
    for m in a.model:
        if f"n__{m}" not in cols:
            print(f"SKIP: no column n__{m}")
            continue
        for r in refs:
            pn, psm, psr = f"pn__{m}__{r}", f"psm__{m}__{r}", f"psr__{m}__{r}"
            paired = all(c in cols for c in (pn, psm, psr))
            if not paired:
                print(f"NOTE {m} vs {r}: no paired columns, reporting the model's own MAE only "
                      f"(re-score with MLBENCH_EXTRA_PAIR_REFS={r} for the margin)")
            sel = (f"sum({psm}) psm, sum({psr}) psr, sum({pn}) pn," if paired
                   else "0 psm, 0 psr, 0 pn,")
            g = con.execute(f"""
                WITH s AS (SELECT *, date - CAST(L AS INTEGER) AS origin FROM slot)
                SELECT CASE {case} ELSE 'other' END AS bucket, park_id, date,
                       {sel} sum(sae__{m}) sae_m, sum(n__{m}) n_m
                FROM (SELECT *, CAST(date_part('day', origin) AS INTEGER) - 1 AS stale FROM s)
                GROUP BY 1, 2, 3""").df()
            for _, lbl in enumerate(b for _, _, b in bounds):
                d = g[g["bucket"] == lbl]
                if d.empty or d["n_m"].sum() <= 0:
                    continue
                keys = d["park_id"].astype(str) + "|" + d["date"].astype(str)
                mae, lo, hi = boot.ratio(keys, d["sae_m"].to_numpy(), d["n_m"].to_numpy())
                row = dict(model=m, ref=r, stale_days=lbl, model_mae=mae, lo=lo, hi=hi,
                           n=int(d["n_m"].sum()), n_park_days=int((d["n_m"] > 0).sum()),
                           n_target_days=int(d["date"].nunique()),
                           n_origin_days=(int(org.loc[lbl, "n_origin_days"])
                                          if lbl in org.index else 0))
                if paired and d["pn"].sum() > 0:
                    dd, dlo, dhi, u = boot.diff(keys, d["psm"].to_numpy(), d["pn"].to_numpy(),
                                                d["psr"].to_numpy(), d["pn"].to_numpy())
                    row.update(margin_vs_ref=dd, margin_lo=dlo, margin_hi=dhi, units=u)
                rows.append(row)

    if not rows:
        print("no rows produced")
        return 1
    df = pd.DataFrame(rows)
    pd.set_option("display.width", 200)
    print(df.to_string(index=False, float_format=lambda v: f"{v:.4f}"))
    print("\nRead the MARGIN column, not the level: the level drifts with the season too.\n"
          "margin worsening with staleness -> the handicap is real, the pooled number is\n"
          "pessimistic. Flat -> staleness is immaterial here. Improving -> staleness\n"
          "FLATTERS us, which is the case the withdrawn 'lower bound' claim assumed away.")
    if a.out:
        df.to_csv(a.out, index=False)
        print(f"\nwrote {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
