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
    model_exists = os.path.exists(os.path.join(settings.MODEL_DIR, "nf_forecast.parquet"))
    return {
        "status": "healthy",
        "model_trained": model_exists,
        "park_scope": settings.park_ids or "all",
        "horizon": settings.NF_HORIZON,
    }


@app.get("/gpu")
def gpu_stats():
    """GPU telemetry via NVML (nvidia-smi CLI isn't in the image, but the NVML
    library is, exposed through pynvml). Used by the admin system-health dashboard.
    Returns {available: False, ...} gracefully on CPU-only hosts."""
    try:
        import pynvml
    except Exception as e:  # pragma: no cover
        return {"available": False, "reason": f"pynvml not installed: {e}"}
    try:
        pynvml.nvmlInit()
    except Exception as e:
        return {"available": False, "reason": f"no GPU / NVML init failed: {e}"}
    try:
        count = pynvml.nvmlDeviceGetCount()
        gpus = []
        for i in range(count):
            h = pynvml.nvmlDeviceGetHandleByIndex(i)
            name = pynvml.nvmlDeviceGetName(h)
            if isinstance(name, bytes):
                name = name.decode()
            mem = pynvml.nvmlDeviceGetMemoryInfo(h)
            util = pynvml.nvmlDeviceGetUtilizationRates(h)
            try:
                temp = pynvml.nvmlDeviceGetTemperature(h, pynvml.NVML_TEMPERATURE_GPU)
            except Exception:
                temp = None
            try:
                power = round(pynvml.nvmlDeviceGetPowerUsage(h) / 1000.0, 1)
            except Exception:
                power = None
            try:
                power_limit = round(
                    pynvml.nvmlDeviceGetEnforcedPowerLimit(h) / 1000.0, 1
                )
            except Exception:
                power_limit = None
            mb = 1024 * 1024
            gpus.append({
                "index": i,
                "name": name,
                "temperatureC": temp,
                "utilizationGpuPct": util.gpu,
                "utilizationMemPct": util.memory,
                "memoryUsedMB": mem.used // mb,
                "memoryTotalMB": mem.total // mb,
                "memoryUsedPct": round(100.0 * mem.used / mem.total, 1),
                "powerW": power,
                "powerLimitW": power_limit,
            })
        return {"available": True, "count": count, "gpus": gpus}
    except Exception as e:
        return {"available": False, "reason": str(e)}
    finally:
        try:
            pynvml.nvmlShutdown()
        except Exception:
            pass


@app.get("/gpu")
def gpu_stats():
    """GPU telemetry via the `nvidia-smi` CLI (present in the CUDA base image; the
    pynvml lib is not). Used by the admin system-health dashboard. Returns
    {available: False, ...} gracefully on CPU-only hosts."""
    import shutil
    import subprocess

    if not shutil.which("nvidia-smi"):
        return {"available": False, "reason": "nvidia-smi not found (CPU-only host)"}
    fields = [
        "index", "name", "temperature.gpu", "utilization.gpu",
        "utilization.memory", "memory.used", "memory.total",
        "power.draw", "power.limit",
    ]
    try:
        out = subprocess.run(
            ["nvidia-smi", f"--query-gpu={','.join(fields)}",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout
    except Exception as e:  # noqa: BLE001
        return {"available": False, "reason": f"nvidia-smi failed: {e}"}

    def _num(v):
        v = v.strip()
        if v in ("", "[N/A]", "[Not Supported]"):
            return None
        try:
            return float(v) if "." in v else int(v)
        except ValueError:
            return None

    gpus = []
    for line in out.strip().splitlines():
        p = [c.strip() for c in line.split(",")]
        if len(p) < len(fields):
            continue
        used, total = _num(p[5]), _num(p[6])
        gpus.append({
            "index": _num(p[0]),
            "name": p[1],
            "temperatureC": _num(p[2]),
            "utilizationGpuPct": _num(p[3]),
            "utilizationMemPct": _num(p[4]),
            "memoryUsedMB": used,
            "memoryTotalMB": total,
            "memoryUsedPct": round(100.0 * used / total, 1)
            if used is not None and total else None,
            "powerW": _num(p[7]),
            "powerLimitW": _num(p[8]),
        })
    return {"available": bool(gpus), "count": len(gpus), "gpus": gpus}


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
