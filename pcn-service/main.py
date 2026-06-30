"""PCN service — FastAPI app (Park-Crowd Nowcaster, intraday shadow).

Mirrors the ml-service / nf-service endpoint shape so the NestJS side triggers it the
same way: nightly /train (one GP-STGNN per park), /forecast every 15 min (writes the
durable shadow snapshot pcn_forecasts), and periodic /score (writes the segmented
pcn_intraday_comparisons board → /v1/admin/system-health). Batch/precompute oriented:
heavy work runs off the request thread; CUDA is selected automatically by the model.
"""

from __future__ import annotations

import faulthandler
import json
import logging
import os
import threading
from datetime import datetime, timezone

faulthandler.enable()

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pcn.main")
settings = get_settings()

app = FastAPI(title="Park Fan PCN Service", version="0.1.0")

_TRAIN_STATUS = os.path.join(settings.MODEL_DIR, "pcn_training_status.json")


def _job_status_path(kind: str) -> str:
    # PER-KIND lock: forecast (*/15) and score (hourly) both fire at :00, so a single
    # shared lock would let forecast starve score every hour. Separate files = independent.
    return os.path.join(settings.MODEL_DIR, f"pcn_job_{kind}.json")


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


class TrainRequest(BaseModel):
    version: str | None = None
    park_ids: list[str] | None = None


class JobRequest(BaseModel):
    version: str | None = None
    park_ids: list[str] | None = None
    lookback_hours: int | None = None


@app.on_event("startup")
def _reset_stale_lock():
    st = _read(_TRAIN_STATUS, {})
    if st.get("is_training"):
        logger.warning("Resetting stale is_training lock (version=%s)", st.get("version"))
        _write(_TRAIN_STATUS, {"is_training": False, "status": "idle",
                               "version": st.get("version"),
                               "error": "reset on startup (previous run interrupted)"})
    # Per-kind forecast/score locks: a restart mid-job (deploy, OOM-kill) leaves
    # {"running": true} on disk → every subsequent /forecast (or /score) 409s forever
    # because nothing ever flips it back. A daemon worker thread can't survive the
    # restart either, so the lock can only be released here. Self-heal on boot.
    for kind in ("forecast", "score"):
        path = _job_status_path(kind)
        if _read(path, {}).get("running"):
            logger.warning("Resetting stale %s lock (previous run interrupted)", kind)
            _write(path, {"running": False, "kind": kind, "status": "idle",
                          "error": "reset on startup (previous run interrupted)"})


@app.get("/health")
def health():
    import glob
    n_models = len(glob.glob(os.path.join(settings.MODEL_DIR, "pcn_*.pt")))
    return {
        "status": "healthy",
        "models_trained": n_models,
        "park_scope": settings.park_ids or "all",
        "arch": settings.PCN_ARCH,
        "loss": settings.PCN_LOSS,
        "horizon_slots": settings.PCN_HORIZON,
    }


@app.get("/gpu")
def gpu_stats():
    """GPU telemetry via nvidia-smi (same as nf-service /gpu). {available: False} on CPU."""
    import shutil
    import subprocess

    if not shutil.which("nvidia-smi"):
        return {"available": False, "reason": "nvidia-smi not found (CPU-only host)"}
    fields = ["index", "name", "temperature.gpu", "utilization.gpu",
              "utilization.memory", "memory.used", "memory.total", "power.draw"]
    try:
        out = subprocess.run(
            ["nvidia-smi", f"--query-gpu={','.join(fields)}",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout
    except Exception:  # noqa: BLE001
        # Full traceback to the server log; don't leak the exception text to the HTTP
        # client (CodeQL: information exposure through an exception).
        logger.exception("nvidia-smi query failed")
        return {"available": False, "reason": "nvidia-smi query failed"}
    gpus = []
    for line in out.strip().splitlines():
        p = [c.strip() for c in line.split(",")]
        gpus.append({"index": p[0], "name": p[1], "tempC": p[2], "utilGpuPct": p[3],
                     "memUsedMB": p[5], "memTotalMB": p[6]})
    return {"available": bool(gpus), "count": len(gpus), "gpus": gpus}


@app.get("/train/status")
def train_status():
    return _read(_TRAIN_STATUS, {"is_training": False, "status": "idle"})


@app.get("/status")
def job_status():
    return {
        "train": _read(_TRAIN_STATUS, {"status": "idle"}),
        "forecast": _read(_job_status_path("forecast"), {"status": "idle"}),
        "score": _read(_job_status_path("score"), {"status": "idle"}),
    }


@app.post("/train")
def train(req: TrainRequest):
    if _read(_TRAIN_STATUS, {}).get("is_training"):
        raise HTTPException(status_code=409, detail="Training already in progress")
    version = req.version or f"pcn{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    import subprocess
    import sys

    runner = os.path.join(os.path.dirname(os.path.abspath(__file__)), "train_runner.py")

    def _launch():
        proc = subprocess.Popen([sys.executable, runner, version, *(req.park_ids or [])])
        proc.wait()
        st = _read(_TRAIN_STATUS, {})
        if st.get("is_training") and st.get("version") == version:
            _write(_TRAIN_STATUS, {"is_training": False, "status": "failed",
                                   "version": version,
                                   "error": f"train_runner exited {proc.returncode}"})

    threading.Thread(target=_launch, daemon=True).start()
    return {"status": "training_started", "version": version}


def _run_job(kind: str, fn, **kwargs):
    """Run a light job (forecast/score) off the request thread with a PER-KIND lock."""
    path = _job_status_path(kind)
    if _read(path, {}).get("running"):
        raise HTTPException(status_code=409, detail=f"{kind} already running")
    _write(path, {"running": True, "kind": kind, "status": "running"})

    def _go():
        try:
            res = fn(**kwargs)
            _write(path, {"running": False, "kind": kind, "status": "completed",
                          "result": res})
        except Exception as e:  # noqa: BLE001
            # Full traceback to the server log; the status file (surfaced by /status)
            # keeps only a short message — no stack-trace exposure to clients.
            logger.exception("%s job failed", kind)
            _write(path, {"running": False, "kind": kind, "status": "failed",
                          "error": str(e)})

    threading.Thread(target=_go, daemon=True).start()
    return {"status": f"{kind}_started"}


@app.post("/forecast")
def run_forecast(req: JobRequest):
    """Re-infer the next horizon for every park and durably write pcn_forecasts (shadow
    producer). Trigger every ~15 min."""
    import forecast
    version = req.version or f"pcn{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    return _run_job("forecast", forecast.forecast_all, version=version,
                    park_ids=req.park_ids)


@app.post("/score")
def run_score(req: JobRequest):
    """Score matured pcn_forecasts vs actuals + CatBoost → pcn_intraday_comparisons."""
    import score
    return _run_job("score", score.score_all,
                    lookback_hours=req.lookback_hours or 24, park_ids=req.park_ids)
