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
# Mute Lightning's INFO chatter (Seed set / GPU available / TPU / per-step lines) so
# the logs show only our own progress + phase lines. Our nf.* loggers stay at INFO.
logging.getLogger("pytorch_lightning").setLevel(logging.WARNING)
logging.getLogger("lightning_fabric").setLevel(logging.WARNING)
logging.getLogger("lightning.pytorch").setLevel(logging.WARNING)
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
    st = _read_status()
    # While training, merge the live progress (chunk/step/% + loss) the forecast
    # callback writes, so a UI / the admin system-health endpoint can show the %.
    if st.get("is_training"):
        try:
            with open(os.path.join(settings.MODEL_DIR, "nf_progress.json")) as f:
                st["progress"] = json.load(f)
        except Exception:
            pass
    return st


@app.post("/train")
def train(req: TrainRequest):
    if _read_status().get("is_training"):
        raise HTTPException(status_code=409, detail="Training already in progress")
    version = req.version or f"nf{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    import subprocess
    import sys

    runner = os.path.join(os.path.dirname(os.path.abspath(__file__)), "train_runner.py")

    def _launch():
        # Run training as its OWN OS process (not a uvicorn thread). Keeps uvicorn
        # responsive, lets DataLoader workers fork from a clean process, and means an
        # OOM kills only the runner — uvicorn survives. A daemon thread waits on it
        # and clears a stale lock if the runner is killed without a terminal status.
        proc = subprocess.Popen([sys.executable, runner, version])
        proc.wait()
        logger.info("train_runner %s exited with code %s", version, proc.returncode)
        st = _read_status()
        if st.get("is_training") and st.get("version") == version:
            _write_status({
                "is_training": False, "status": "failed", "version": version,
                "error": f"train_runner exited {proc.returncode} without completing (likely OOM-killed)",
            })

    threading.Thread(target=_launch, daemon=True).start()
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
