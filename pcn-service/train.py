"""Nightly training — one GP-STGNN per park, persisted for the shadow producer.

Per-park (not one global model) because the AGCRN node embeddings are park-sized and
the park-wide crowd state is a WITHIN-park signal — rides in different parks don't share
a crowd. Each model is tiny, so ~150 parks nightly is cheap on the GPU (the same
per-park-chunk philosophy nf-service uses). CUDA is selected automatically.

    python3 train.py [PARK_UUID ...]      # default: all parks in scope
"""

from __future__ import annotations

import logging
import os
import sys
import time

import backbones
import pipeline
import windowing
from config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("pcn.train")
settings = get_settings()


def model_path(park_id: str) -> str:
    return os.path.join(settings.MODEL_DIR, f"pcn_{park_id}.pt")


def _build_model():
    reg = backbones.build_registry(
        loss=settings.PCN_LOSS, hidden=settings.PCN_HIDDEN_SIZE,
        max_steps=settings.PCN_MAX_STEPS, layers=settings.PCN_GWN_LAYERS,
    )
    return reg[settings.PCN_ARCH]()


def train_park(park_id: str, version: str) -> bool:
    t = pipeline.build_park_tensor(park_id)
    if t is None:
        return False
    L, H = settings.PCN_INPUT_SIZE, settings.PCN_HORIZON
    bases = windowing.valid_bases(len(t.slots), L, H)
    if bases.size == 0:
        logger.warning("park %s: only %d slots — need >= L+H=%d, skip",
                       park_id, len(t.slots), L + H)
        return False
    model = _build_model()
    model.fit(t, bases, L, H)
    os.makedirs(settings.MODEL_DIR, exist_ok=True)
    model.save(model_path(park_id), ride_ids=t.ride_ids)
    logger.info("park %s: trained %s on %d windows (%d rides) → %s",
                park_id, model.name, bases.size, len(t.ride_ids), model_path(park_id))
    return True


def train_all(version: str, park_ids: list[str] | None = None) -> dict:
    parks = pipeline.scope_park_ids(park_ids)
    t0 = time.time()
    ok = 0
    for pid in parks:
        try:
            if train_park(pid, version):
                ok += 1
        except Exception as e:  # noqa: BLE001 — one bad park must not abort the run
            logger.warning("park %s training failed: %s", pid, e)
    logger.info("training done: %d/%d parks in %.1fs (version=%s)",
                ok, len(parks), time.time() - t0, version)
    return {"trained": ok, "parks": len(parks), "version": version}


if __name__ == "__main__":
    import datetime as _dt

    ver = f"pcn{_dt.datetime.now(_dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    train_all(ver, sys.argv[1:] or None)
