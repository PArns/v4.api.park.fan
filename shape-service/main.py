"""Shape service — FastAPI app (Level×Shape day-curve expander, SHADOW).

Mirrors the pcn-service endpoint shape so NestJS triggers it the same way: nightly /build
(assemble + persist per-park profiles), daily /forecast (render the next N days' curves →
durable shape_forecasts), and periodic /score (matured shape vs CatBoost vs actuals →
shape_comparisons → /v1/admin/ml-comparison). SHADOW only — it never serves the champion.
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
logger = logging.getLogger("shape.main")
settings = get_settings()

app = FastAPI(title="Park Fan Shape Service", version="0.1.0")


def _job_path(kind: str) -> str:
    # Per-kind lock so build / forecast / score don't block each other.
    return os.path.join(settings.MODEL_DIR, f"shape_job_{kind}.json")


def _read(path: str, default: dict) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def _write(path: str, status: dict) -> None:
    os.makedirs(settings.MODEL_DIR, exist_ok=True)
    with open(path, "w") as f:
        json.dump(status, f)


class JobRequest(BaseModel):
    park_ids: list[str] | None = None
    lookback_hours: int | None = None


@app.on_event("startup")
def _reset_stale_locks():
    for kind in ("build", "forecast", "score"):
        st = _read(_job_path(kind), {})
        if st.get("running"):
            _write(
                _job_path(kind),
                {"running": False, "kind": kind, "status": "idle", "error": "reset on startup"},
            )


@app.get("/health")
def health():
    import glob

    n = len(glob.glob(os.path.join(settings.MODEL_DIR, "shape_*.pkl")))
    return {
        "status": "healthy",
        "profiles_built": n,
        "park_scope": settings.park_ids or "all",
        "alpha": settings.SHAPE_ALPHA_CROWD,
        "beta": settings.SHAPE_BETA_DAYTYPE,
        "smooth": settings.SHAPE_SMOOTH_SLOTS,
        "horizon_days": settings.SHAPE_FORECAST_DAYS,
    }


@app.get("/status")
def status():
    return {k: _read(_job_path(k), {"status": "idle"}) for k in ("build", "forecast", "score")}


def _run_job(kind: str, fn, **kwargs):
    path = _job_path(kind)
    if _read(path, {}).get("running"):
        raise HTTPException(status_code=409, detail=f"{kind} already running")
    _write(path, {"running": True, "kind": kind, "status": "running"})

    def _go():
        try:
            res = fn(**kwargs)
            _write(path, {"running": False, "kind": kind, "status": "completed", "result": res})
        except Exception as e:  # noqa: BLE001
            logger.exception("%s job failed", kind)
            _write(path, {"running": False, "kind": kind, "status": "failed", "error": str(e)})

    threading.Thread(target=_go, daemon=True).start()
    return {"status": f"{kind}_started"}


@app.post("/build")
def run_build(req: JobRequest):
    import build

    return _run_job("build", build.build_all, park_ids=req.park_ids)


@app.post("/forecast")
def run_forecast(req: JobRequest):
    import forecast

    version = f"shape{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    return _run_job("forecast", forecast.forecast_all, version=version, park_ids=req.park_ids)


@app.post("/score")
def run_score(req: JobRequest):
    import score

    return _run_job(
        "score", score.score_all, lookback_hours=req.lookback_hours or 96, park_ids=req.park_ids
    )
