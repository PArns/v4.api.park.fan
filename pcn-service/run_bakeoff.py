"""Intraday bake-off across parks — "find what fits us best" (design doc §11.7 step 2).

For each park: build the cross-ride tensor, run every registered candidate (GP-STGNN +
the LocalGRU ablation) through the leakage-free rolling-origin harness, and pool the
segmented MAE/bias across parks (n-weighted). Persistence + yesterday-same-slot are the
naive references every candidate must beat; LocalGRU is the ablation that tells us
whether the learned park-crowd graph (GP-STGNN) is worth its cost.

    python3 run_bakeoff.py --parks <UUID>,<UUID> --out report.json
    python3 run_bakeoff.py --tensor-dir models --limit 10        # from saved .npz
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os

import backbones
import metrics
from backtest import run_backtest

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("pcn.bakeoff")


def bakeoff_park(t, registry: dict, L: int, H: int, eval_days: int, base_hour: int) -> dict:
    """Run all candidates on one park's tensor. Returns {segment: {column: (mae,bias,n)}}
    with one column per candidate plus the shared persistence / yesterday baselines."""
    combined: dict[str, dict] = {}
    baseline_done = False
    for factory in registry.values():
        model = factory()
        res = run_backtest(t, model, L=L, H=H, eval_days=eval_days, base_hour=base_hour)
        for seg, by in res["scores"].items():
            row = combined.setdefault(seg, {})
            row[model.name] = by[model.name]
            if not baseline_done:
                row["persist"] = by["persist"]
                row["yest"] = by["yest"]
        baseline_done = True
    return combined


# n-weighted cross-park pooling lives in metrics.pool_scores (torch-free, so the
# aggregation is unit-testable without the model deps); re-exported for callers.
pool_scores = metrics.pool_scores


def _load_tensors(args):
    import tensor as tns

    if args.tensor_dir:
        paths = sorted(glob.glob(os.path.join(args.tensor_dir, "crt_*.npz")))
        if args.limit:
            paths = paths[: args.limit]
        for p in paths:
            yield tns.CrossRideTensor.from_npz(p)
        return

    # Build from the DB.
    import db
    from config import get_settings

    s = get_settings()
    park_ids = [p.strip() for p in args.parks.split(",") if p.strip()] if args.parks \
        else (s.park_ids or db.fetch_park_ids())
    if args.limit:
        park_ids = park_ids[: args.limit]
    meta = db.fetch_attraction_meta(park_ids)
    by_park = dict(tuple(meta.groupby("park_id"))) if not meta.empty else {}
    for pid in park_ids:
        rows = by_park.get(pid)
        if rows is None or rows.empty:
            continue
        tz = rows["timezone"].iloc[0]
        panel = db.fetch_cross_ride_panel(pid, tz)
        if panel.empty:
            continue
        yield tns.build(panel, park_id=pid, freq=s.slot_freq,
                        min_rides_open=s.PCN_MIN_RIDES_OPEN)


def main():
    ap = argparse.ArgumentParser(description="PCN intraday bake-off across parks")
    ap.add_argument("--parks", help="comma-separated park UUIDs (default: scope/all)")
    ap.add_argument("--tensor-dir", help="load crt_*.npz from here instead of the DB")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--input-size", type=int, default=480)
    ap.add_argument("--horizon", type=int, default=48)
    ap.add_argument("--eval-days", type=int, default=5)
    ap.add_argument("--base-hour", type=int, default=11)
    ap.add_argument("--max-steps", type=int, default=500)
    ap.add_argument("--loss", default="quantile", choices=["quantile", "tweedie"])
    # Graph WaveNet temporal depth. Receptive field = 1 + sum(2^i): 2 layers see only
    # 4 slots (1h) of the context; 8 layers see 256 (> L). THE bake-off knob for the
    # "does a full-context TCN beat the 1h-RF one on busy?" question (review §5a).
    ap.add_argument("--layers", type=int, default=2)
    ap.add_argument("--out", help="write the pooled report as JSON")
    args = ap.parse_args()

    registry = backbones.build_registry(
        loss=args.loss, max_steps=args.max_steps, layers=args.layers)
    per_park, parks_done = [], []
    for t in _load_tensors(args):
        try:
            sc = bakeoff_park(t, registry, args.input_size, args.horizon,
                              args.eval_days, args.base_hour)
            per_park.append(sc)
            parks_done.append(t.park_id)
            logger.info("bakeoff park %s: %d rides, %d slots done",
                        t.park_id, len(t.ride_ids), len(t.slots))
        except Exception as e:  # noqa: BLE001 — one bad park must not abort the sweep
            logger.warning("park %s failed: %s", t.park_id, e)

    if not per_park:
        logger.error("no parks produced scores")
        return
    pooled = pool_scores(per_park)
    print(f"\n=== POOLED BAKE-OFF ({len(parks_done)} parks, loss={args.loss}) ===")
    print(metrics.format_table(pooled))
    if args.out:
        with open(args.out, "w") as f:
            json.dump({"parks": parks_done, "loss": args.loss, "pooled": pooled}, f, indent=2)
        logger.info("wrote %s", args.out)


if __name__ == "__main__":
    main()
