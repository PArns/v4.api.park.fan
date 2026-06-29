"""CLI: build shape profiles for parks and print a coverage summary (Phase 0 — no
persistence yet; the shape_profiles table + render API land in Phase 1).

    python3 build_profiles.py [PARK_UUID ...]            # build + coverage for scope
    python3 build_profiles.py --sample PARK_UUID         # also render a sample curve
"""

from __future__ import annotations

import argparse
import logging
import time

import numpy as np

import pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("shape.build")


def run(park_ids: list[str] | None = None) -> int:
    parks = pipeline.scope_park_ids(park_ids)
    t0 = time.time()
    built = 0
    for pid in parks:
        try:
            prof = pipeline.build_park_profiles(pid)
        except Exception as e:  # noqa: BLE001 — one bad park must not abort the run
            logger.warning("park %s failed: %s", pid, e)
            continue
        if prof is None:
            continue
        built += 1
        logger.info("park %s: %s", pid, prof.coverage())
    logger.info("built %d/%d parks in %.1fs", built, len(parks), time.time() - t0)
    return built


def _sample(park_id: str) -> None:
    prof = pipeline.build_park_profiles(park_id)
    if prof is None:
        logger.info("park %s: nothing built", park_id)
        return
    logger.info("coverage: %s", prof.coverage())
    ride = next(iter(prof.g_r))[0]
    for lvl in (10.0, 40.0, 80.0):
        for dt in ("reg", "wend", "school"):
            curve = prof.render_additive(ride, lvl, dt)  # the served additive model
            finite = curve[np.isfinite(curve)]
            logger.info(
                "ride %s level=%.0f crowd=%d daytype=%-6s → %d open slots, peak=%.1f mean=%.1f",
                ride, lvl, prof.level_to_crowd(ride, lvl), dt, finite.size,
                float(np.nanmax(curve)) if finite.size else float("nan"),
                float(np.nanmean(curve)) if finite.size else float("nan"),
            )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Shape profile builder (Phase 0)")
    ap.add_argument("parks", nargs="*")
    ap.add_argument("--sample", action="store_true",
                    help="render a sample curve for the first park in scope")
    args = ap.parse_args()
    if args.sample:
        scope = pipeline.scope_park_ids(args.parks or None)
        _sample(scope[0])
    else:
        run(args.parks or None)
