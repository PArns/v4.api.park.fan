"""Nightly /build — assemble + persist each park's shape profiles (the served additive+
smooth model), so /forecast can load them and render fast. Cheap (SQL + pandas, no GPU)."""

from __future__ import annotations

import logging
import os
import time

import pipeline
from config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("shape.build")
settings = get_settings()


def _path(park_id: str) -> str:
    return os.path.join(settings.MODEL_DIR, f"shape_{park_id}.pkl")


def build_all(park_ids: list[str] | None = None) -> dict:
    parks = pipeline.scope_park_ids(park_ids)
    os.makedirs(settings.MODEL_DIR, exist_ok=True)
    t0 = time.time()
    built = 0
    for pid in parks:
        try:
            prof = pipeline.build_park_profiles(pid)
        except Exception as e:  # noqa: BLE001 — one bad park must not abort the run
            logger.warning("park %s build failed: %s", pid, e)
            continue
        if prof is None:
            continue
        prof.save(_path(pid))
        built += 1
    logger.info("build done: %d/%d parks in %.1fs", built, len(parks), time.time() - t0)
    return {"built": built, "parks": len(parks)}


if __name__ == "__main__":
    import sys

    build_all(sys.argv[1:] or None)
