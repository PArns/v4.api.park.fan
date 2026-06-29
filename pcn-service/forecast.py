"""Shadow producer — re-infer every 15 min and durably store the forward forecast.

Loads each park's trained GP-STGNN, builds the context up to NOW from the live panel,
predicts the next H 15-min slots, and writes the served quantiles (q0.5 display, q0.8
crowd) to pcn_forecasts. This is the going-forward shadow (design doc §12): forecasts
are stored at inference time so they can be scored later against actuals — CatBoost's
own intraday preds are dedup-destroyed for past origins, so a fair head-to-head MUST
capture both forward.

PCN runs in the SHADOW — it only writes pcn_forecasts; CatBoost stays the served
champion until score.py shows a busy/headliner win (gate §8). Inference is cheap and the
GPU is ~99% idle, so re-inferring every 15 min with the current state is effectively
free and never touches CatBoost (CPU).

    python3 forecast.py [PARK_UUID ...]
"""

from __future__ import annotations

import logging
import os
import sys
import time

import numpy as np
import pandas as pd

import backbones
import db
import pipeline
import windowing
from config import get_settings
from train import model_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("pcn.forecast")
settings = get_settings()


def _load_model(park_id: str):
    path = model_path(park_id)
    if not os.path.exists(path):
        return None
    reg = backbones.build_registry(
        loss=settings.PCN_LOSS, hidden=settings.PCN_HIDDEN_SIZE,
        max_steps=settings.PCN_MAX_STEPS,
    )
    return reg[settings.PCN_ARCH]().load(path)


def forecast_park(park_id: str, version: str) -> int:
    t = pipeline.build_park_tensor(park_id)
    if t is None:
        return 0
    model = _load_model(park_id)
    if model is None:
        logger.warning("park %s: no trained model — run /train first", park_id)
        return 0
    # The net's node order is fixed at train time; only forecast if the ride set matches
    # (a changed roster needs a retrain, which the nightly /train provides).
    if getattr(model, "ride_ids", None) != t.ride_ids:
        logger.warning("park %s: ride set changed since training — skip until retrain",
                       park_id)
        return 0

    L, H = settings.PCN_INPUT_SIZE, settings.PCN_HORIZON
    if len(t.slots) <= L:
        logger.warning("park %s: only %d slots — need > L=%d, skip", park_id, len(t.slots), L)
        return 0

    base = len(t.slots) - 1                      # forecast from the latest slot
    qpreds = model.predict_quantiles(t, np.array([base]), L, H)
    serve = set(settings.serve_quantiles)
    origin = pd.Timestamp(t.slots[base])
    step = pd.Timedelta(minutes=settings.PCN_SLOT_MINUTES)

    rows = []
    for q, arr in qpreds.items():
        if round(float(q), 4) not in {round(x, 4) for x in serve}:
            continue
        mat = arr[0]                              # [R, H]
        for ri, uid in enumerate(t.ride_ids):
            for h in range(H):
                rows.append({
                    "aid": uid,
                    "ts": (origin + (h + 1) * step).to_pydatetime(),
                    "os": origin.to_pydatetime(),
                    "q": float(q),
                    "pw": float(max(mat[ri, h], 0.0)),
                })
    n = db.write_pcn_forecasts(rows, version)
    logger.info("park %s: wrote %d pcn_forecasts (origin=%s, %d rides × %dh × %d q)",
                park_id, n, origin, len(t.ride_ids), H, len(serve))
    return n


def forecast_all(version: str, park_ids: list[str] | None = None) -> dict:
    parks = pipeline.scope_park_ids(park_ids)
    t0 = time.time()
    total = 0
    for pid in parks:
        try:
            total += forecast_park(pid, version)
        except Exception as e:  # noqa: BLE001
            logger.warning("park %s forecast failed: %s", pid, e)
    logger.info("forecast done: %d rows across %d parks in %.1fs",
                total, len(parks), time.time() - t0)
    return {"rows": total, "parks": len(parks), "version": version}


if __name__ == "__main__":
    import datetime as _dt

    ver = f"pcn{_dt.datetime.now(_dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    forecast_all(ver, sys.argv[1:] or None)
