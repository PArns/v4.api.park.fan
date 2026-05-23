"""NeuralForecast service — FastAPI app.

Parallel/experimental forecaster for the far-future daily-peak surface, using
holidays/calendar as known-future covariates (TFT/NHITS). Batch-oriented:
train nightly, predict the horizon, expose the cached forecast. Mirrors the
ml-service endpoint shape so the NestJS side can consume it the same way.
"""

from __future__ import annotations

import faulthandler
import json
import logging
import os
import threading
import warnings
from datetime import datetime, timezone

# Dump a (C-level) traceback of all threads on a fatal signal (SIGSEGV/SIGABRT/…),
# so a "silent" native crash in torch/dataloader leaves a trace instead of a bare exit.
faulthandler.enable()

# Silence benign, high-volume third-party warnings so the CI-mode logs stay clean.
# Set at import (before any torch/neuralforecast fork) so dataloader workers inherit
# the filters on Linux. These are deprecation/internal notices, not actionable here.
warnings.filterwarnings("ignore", message=r".*TypedStorage is deprecated.*")
warnings.filterwarnings("ignore", message=r".*removed from hparams because it cannot be pickled.*")

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nf.main")
settings = get_settings()

app = FastAPI(title="Park Fan NeuralForecast Service", version="0.1.0")

_STATUS_FILE = os.path.join(settings.MODEL_DIR, "nf_training_status.json")
_FORECAST_FILE = os.path.join(settings.MODEL_DIR, "nf_forecast.parquet")


def _write_status(status: dict) -> None:
    os.makedirs(settings.MODEL_DIR, exist_ok=True)
    with open(_STATUS_FILE, "w") as f:
        json.dump(status, f)


def _read_status() -> dict:
    try:
        with open(_STATUS_FILE) as f:
            return json.load(f)
    except Exception:
        return {"is_training": False, "status": "idle", "version": None, "error": None}


class TrainRequest(BaseModel):
    version: str | None = None


@app.on_event("startup")
def _reset_stale_training_lock():
    """A fresh process means no training is actually running, so an is_training=true
    left in the status file is stale (e.g. a redeploy killed a training mid-run) and
    would wrongly 409 the next /train. Clear it on startup."""
    st = _read_status()
    if st.get("is_training"):
        logger.warning(
            "Resetting stale is_training lock (version=%s) left by an interrupted run",
            st.get("version"),
        )
        _write_status({
            "is_training": False, "status": "idle", "version": st.get("version"),
            "error": "reset on startup (previous run interrupted)",
        })


@app.get("/health")
def health():
    model_exists = os.path.exists(os.path.join(settings.MODEL_DIR, "nf_daily"))
    return {
        "status": "healthy",
        "model_trained": model_exists,
        "park_scope": settings.park_ids or "all",
        "horizon": settings.NF_HORIZON,
    }


@app.get("/train/status")
def train_status():
    return _read_status()


def _run_training(version: str) -> None:
    """Module-level training entry, run in a SPAWNED SUBPROCESS (see /train).

    Why a subprocess and not a thread: the DataLoader's worker processes
    (NF_NUM_WORKERS) fork from whatever process calls fit(). Forking from the
    threaded uvicorn worker killed the container (clean exit 0 right at DataLoader
    start → restart loop). A spawned subprocess is a clean process, so the workers
    fork safely and the cores actually get used. Status is shared via the file.
    """
    import traceback

    faulthandler.enable()
    import forecast
    import db

    _write_status({
        "is_training": True, "status": "training", "version": version,
        "started_at": datetime.now(timezone.utc).isoformat(), "error": None,
    })
    try:
        # Train + forecast in one process (no save — see train_and_forecast),
        # then cache + persist the forward forecast for the scoreboard.
        y_hat = forecast.train_and_forecast(version)
        y_hat.to_parquet(_FORECAST_FILE)
        tcol = _tft_column(list(y_hat.columns))
        persisted = db.persist_forecast(y_hat, version, tcol) if tcol else 0
        info = {"rows": int(len(y_hat)), "persisted": int(persisted)}
        _write_status({
            "is_training": False, "status": "completed", "version": version,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "info": info, "error": None,
        })
        logger.info("Training + forecast complete: %s", info)
    except BaseException as e:  # noqa: BLE001 — catch everything incl. SystemExit/KeyboardInterrupt
        tb = traceback.format_exc()
        logger.error("Training failed: %s\n%s", e, tb)  # full stacktrace to the log
        traceback.print_exc()  # also to stderr (Coolify log)
        _write_status({
            "is_training": False, "status": "failed", "version": version,
            "error": f"{e}\n{tb}",
        })


@app.post("/train")
def train(req: TrainRequest):
    if _read_status().get("is_training"):
        raise HTTPException(status_code=409, detail="Training already in progress")
    version = req.version or f"nf{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    # In-thread training (NOT a subprocess): with NF_NUM_WORKERS=0 there are no
    # DataLoader child processes to isolate, and the spawned subprocess variant was
    # itself getting OOM-killed (-9) at fit start where in-thread training ran fine.
    threading.Thread(target=_run_training, args=(version,), daemon=True).start()
    return {"status": "training_started", "version": version}


def _tft_column(cols: list[str]) -> str | None:
    """Pick TFT's central forecast column (NeuralForecast emits 'TFT' or 'TFT-median'
    plus '-lo-/-hi-' quantile bands)."""
    for c in cols:
        if c in ("TFT", "TFT-median"):
            return c
    for c in cols:
        if c.startswith("TFT") and "-lo-" not in c and "-hi-" not in c:
            return c
    return None


@app.post("/forecast")
def run_forecast():
    """Re-persist the latest cached forecast to tft_forecasts (idempotent upsert).

    Training is done by /train, which trains + forecasts + persists in one process
    (the model is intentionally not saved — see forecast.train_and_forecast). This
    endpoint therefore serves the cached forecast rather than reloading a model;
    call /train to refresh it."""
    import db
    import pandas as pd

    if not os.path.exists(_FORECAST_FILE):
        raise HTTPException(status_code=404, detail="No forecast yet — run /train first")
    try:
        y_hat = pd.read_parquet(_FORECAST_FILE)
        tcol = _tft_column(list(y_hat.columns))
        version = _read_status().get("version") or "unknown"
        persisted = db.persist_forecast(y_hat, version, tcol) if tcol else 0
        return {"status": "ok", "rows": int(len(y_hat)), "persisted": int(persisted)}
    except Exception as e:  # noqa: BLE001
        import traceback
        logger.error("Forecast failed: %s", e)
        raise HTTPException(status_code=500, detail=f"{e}\n{traceback.format_exc()}")


@app.get("/forecast/latest")
def latest_forecast(unique_id: str | None = None):
    import pandas as pd

    if not os.path.exists(_FORECAST_FILE):
        raise HTTPException(status_code=404, detail="No cached forecast yet")
    df = pd.read_parquet(_FORECAST_FILE)
    if unique_id:
        df = df[df["unique_id"] == unique_id]
    return json.loads(df.to_json(orient="records", date_format="iso"))
