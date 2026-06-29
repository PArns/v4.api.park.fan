"""Build + inspect the cross-ride tensor for one or more parks (Phase 0).

This is the runnable verification surface: point it at the live DB and it prints the
tensor shape, sensor density, open-slot fraction and the scalar crowd baseline per
park — the numbers that tell you the tensor is sane before any model touches it.

    docker exec <pcn> python3 /app/build_cross_ride_tensor.py            # all parks in scope
    docker exec <pcn> python3 /app/build_cross_ride_tensor.py --park <UUID>
    docker exec <pcn> python3 /app/build_cross_ride_tensor.py --park <UUID> --save /app/models
"""

from __future__ import annotations

import argparse
import logging
import os

import db
import tensor
from config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("pcn.build")
settings = get_settings()


def _parks(explicit: str | None) -> list[str]:
    if explicit:
        return [explicit]
    return settings.park_ids or db.fetch_park_ids()


def build_for_park(park_id: str, meta_by_park, save_dir: str | None):
    rows = meta_by_park.get(park_id)
    if rows is None or rows.empty:
        logger.warning("park %s: no attractions, skip", park_id)
        return None
    tz = rows["timezone"].iloc[0]
    panel = db.fetch_cross_ride_panel(park_id, tz)
    if panel.empty:
        logger.warning("park %s (tz=%s): no queue data in window, skip", park_id, tz)
        return None

    crt = tensor.build(
        panel, park_id=park_id, freq=settings.slot_freq,
        min_rides_open=settings.PCN_MIN_RIDES_OPEN,
    )
    s = crt.summary()
    logger.info(
        "park %s tz=%s: %d rides × %d slots × %d ch | obs_density=%.3f "
        "open_slots=%.3f loss_slots=%d occ_mean=%.1f | %s..%s",
        park_id, tz, s["rides"], s["slots"], s["channels"], s["obs_density"],
        s["open_slot_frac"], s["loss_slots"], s["park_occ_mean"],
        s["span"][0], s["span"][1],
    )
    if save_dir:
        os.makedirs(save_dir, exist_ok=True)
        path = os.path.join(save_dir, f"crt_{park_id}.npz")
        crt.to_npz(path)
        logger.info("  saved %s", path)
    return crt


def main():
    ap = argparse.ArgumentParser(description="Build the PCN cross-ride tensor")
    ap.add_argument("--park", help="single park UUID (default: PCN_PARK_IDS or all)")
    ap.add_argument("--save", metavar="DIR", help="write crt_<park>.npz per park")
    ap.add_argument("--limit", type=int, default=0, help="cap #parks (0 = no cap)")
    args = ap.parse_args()

    park_ids = _parks(args.park)
    if args.limit:
        park_ids = park_ids[: args.limit]
    logger.info("building cross-ride tensor for %d park(s)", len(park_ids))

    meta = db.fetch_attraction_meta(park_ids if args.park else settings.park_ids)
    meta_by_park = dict(tuple(meta.groupby("park_id"))) if not meta.empty else {}
    # When scope is "all parks", meta was fetched empty-scoped → fetch per park lazily.
    if not meta_by_park:
        meta_by_park = dict(tuple(db.fetch_attraction_meta(park_ids).groupby("park_id")))

    ok = 0
    for pid in park_ids:
        try:
            if build_for_park(pid, meta_by_park, args.save) is not None:
                ok += 1
        except Exception as e:  # noqa: BLE001 — one bad park must not abort the sweep
            logger.warning("park %s failed: %s", pid, e)
    logger.info("done: %d/%d parks produced a tensor", ok, len(park_ids))


if __name__ == "__main__":
    main()
