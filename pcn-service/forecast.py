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
import tensor as tns
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
        max_steps=settings.PCN_MAX_STEPS, layers=settings.PCN_GWN_LAYERS,
    )
    return reg[settings.PCN_ARCH]().load(path)


def forecast_park(park_id: str, version: str) -> int:
    # Staleness pre-check BEFORE the panel fetch: seasonally-closed / dead-data parks
    # used to pay the full history aggregation every tick just to be skipped afterwards.
    # One bounded EXISTS query replaces that.
    if not db.park_has_fresh_data(park_id, settings.PCN_MAX_ORIGIN_AGE_HOURS):
        logger.info("park %s: no STANDBY data in %dh — skip forecast (pre-check)",
                    park_id, settings.PCN_MAX_ORIGIN_AGE_HOURS)
        return 0
    # SHORT inference window: the model only consumes the L-slot context (2 days) +
    # ffill warm-up; the training-sized window (~1.5y) re-aggregated the park's whole
    # history every 15 minutes for nothing. The scale factor lives in the checkpoint.
    t = pipeline.build_park_tensor(
        park_id, window_days=settings.PCN_FORECAST_WINDOW_DAYS)
    if t is None:
        return 0
    # Defense-in-depth: the pre-check said "some fresh data exists", this checks the
    # assembled grid's freshest slot too (park-local naive vs park wall-clock now).
    tz = pipeline.park_timezone(park_id)
    if tz is not None:
        latest = pd.Timestamp(t.slots[-1])
        age_h = (pd.Timestamp.now(tz=tz).tz_localize(None) - latest).total_seconds() / 3600.0
        if age_h > settings.PCN_MAX_ORIGIN_AGE_HOURS:
            logger.info("park %s: freshest slot %s is %.0fh stale (> %dh) — skip forecast",
                        park_id, latest, age_h, settings.PCN_MAX_ORIGIN_AGE_HOURS)
            return 0
    model = _load_model(park_id)
    if model is None:
        logger.warning("park %s: no trained model — run /train first", park_id)
        return 0

    L, H = settings.PCN_INPUT_SIZE, settings.PCN_HORIZON
    if len(t.slots) <= L:
        # Thin short-window grid (e.g. a park that just reopened after days closed):
        # fall back to the full training window once — same cost as every tick paid
        # before the short-window optimization, but only for this rare case.
        t = pipeline.build_park_tensor(park_id)
        if t is None or len(t.slots) <= L:
            logger.warning("park %s: only %d slots — need > L=%d, skip",
                           park_id, 0 if t is None else len(t.slots), L)
            return 0

    # The net's node order is fixed at train time → align the tensor's ride axis to
    # it (tensor.align_ride_axis). Rides missing from the window become synthetic
    # quiet nodes (their outputs are NOT persisted); new rides are dropped and fall
    # back to CatBoost until the nightly retrain. A strict-equality skip here used to
    # disable PCN for a whole park over any single roster/window drift.
    trained = getattr(model, "ride_ids", None)
    present = None
    if trained and trained != t.ride_ids:
        t, present = tns.align_ride_axis(t, trained)
        if not present.any():
            logger.warning("park %s: no trained ride present in the window — skip",
                           park_id)
            return 0
        if not present.all():
            logger.info("park %s: %d/%d trained rides in window (absent kept as "
                        "quiet nodes, not persisted)",
                        park_id, int(present.sum()), len(trained))

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
            if present is not None and not present[ri]:
                continue                          # absent ride → junk output, skip
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
