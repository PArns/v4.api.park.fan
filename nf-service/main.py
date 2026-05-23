"""NeuralForecast service — FastAPI app.

Parallel/experimental forecaster for the far-future daily-peak surface, using
holidays/calendar as known-future covariates (TFT/NHITS). Batch-oriented:
train nightly, predict the horizon, expose the cached forecast. Mirrors the
ml-service endpoint shape so the NestJS side can consume it the same way.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone

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


@app.post("/train")
def train(req: TrainRequest):
    if _read_status().get("is_training"):
        raise HTTPException(status_code=409, detail="Training already in progress")
    version = req.version or f"nf{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    def worker():
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
        except Exception as e:  # noqa: BLE001
            import traceback
            logger.error("Training failed: %s", e)
            _write_status({
                "is_training": False, "status": "failed", "version": version,
                "error": f"{e}\n{traceback.format_exc()}",
            })

    threading.Thread(target=worker, daemon=True).start()
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
