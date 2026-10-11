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
# pytorch_lightning internal: emits a DeprecationWarning about LeafSpec/TreeSpec on
# every dataloader setup. It's a lib-internal notice (not our code) — suppress until
# the pinned lightning version stops using the deprecated API.
warnings.filterwarnings("ignore", message=r".*isinstance\(treespec, LeafSpec\) is deprecated.*")

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
    except Exception:  # noqa: BLE001
        logger.exception("nvidia-smi query failed")
        return {"available": False, "reason": "nvidia-smi query failed"}

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
    # Backstop for the watchdog in /train: a run past its deadline is reported as
    # overdue, so a caller never mistakes a hung run for one still progressing.
    if st.get("is_training") and st.get("started_at"):
        try:
            age = datetime.now(timezone.utc) - datetime.fromisoformat(st["started_at"])
            st["overdue"] = age.total_seconds() > settings.NF_TRAIN_DEADLINE_MINUTES * 60
        except ValueError:
            pass
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
        deadline_min = settings.NF_TRAIN_DEADLINE_MINUTES
        try:
            proc.wait(timeout=deadline_min * 60)
            error = f"train_runner exited {proc.returncode} without completing (likely OOM-killed)"
        except subprocess.TimeoutExpired:
            # Hung (2026-10-09: blocked through a DB outage until a restart 4.5 h
            # later). Kill it so the lock clears and the next /train can run.
            logger.error("train_runner %s exceeded %d min — killing it", version, deadline_min)
            proc.kill()
            proc.wait()
            error = f"train_runner exceeded the {deadline_min}-min deadline and was killed"
        logger.info("train_runner %s exited with code %s", version, proc.returncode)
        st = _read_status()
        if st.get("is_training") and st.get("version") == version:
            _write_status({
                "is_training": False, "status": "failed", "version": version,
                "error": error,
            })

    # The training runner wants the GPU and the container's memory: the resident
    # Chronos-2 shadow (PAR-852) lets go of its weights first and refuses calls
    # until the run is over.
    _chronos_engine().unload()
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
    """Re-persist the cached forecast of the last COMPLETED run (idempotent upsert).

    Training is done by /train, whose runner trains + forecasts + persists in one
    process (the model is intentionally not saved — see forecast.train_and_forecast),
    so the nightly job no longer calls this. It stays as a manual repair path, and
    it re-writes exactly the rows that run wrote: same version, same forecast_date.

    It refuses (409) unless the status file says the last run completed and names
    the forecast_date it made the parquet on. Anything else — training, failed,
    "reset on startup", or a status from before forecast_date was recorded — means
    the parquet on disk is from an older run than the status describes, and
    persisting it would label an old forecast as a fresh one (PAR-814: on
    2026-10-09 all 231,480 rows were the 2026-10-08 forecast under the new date)."""
    from datetime import date

    import pandas as pd

    import db

    if not os.path.exists(_FORECAST_FILE):
        raise HTTPException(status_code=404, detail="No forecast yet — run /train first")
    st = _read_status()
    if st.get("status") != "completed" or not st.get("forecast_date"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Last run is not a completed one with a recorded forecast_date "
                f"(status={st.get('status')!r}, version={st.get('version')!r}) — "
                "refusing to re-persist a forecast this run did not produce"
            ),
        )
    try:
        forecast_date = date.fromisoformat(st["forecast_date"])
        y_hat = pd.read_parquet(_FORECAST_FILE)
        tcol = _tft_column(list(y_hat.columns))
        version = st.get("version") or "unknown"
        persisted = db.persist_forecast(y_hat, version, tcol, forecast_date) if tcol else 0
        return {
            "status": "ok", "rows": int(len(y_hat)), "persisted": int(persisted),
            "version": version, "forecast_date": forecast_date.isoformat(),
        }
    except Exception as e:  # noqa: BLE001
        import traceback
        # Full traceback to the server log; only a short message to the client
        # (CodeQL: information exposure through an exception).
        logger.error("Forecast failed: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/forecast/latest")
def latest_forecast(unique_id: str | None = None):
    import pandas as pd

    if not os.path.exists(_FORECAST_FILE):
        raise HTTPException(status_code=404, detail="No cached forecast yet")
    df = pd.read_parquet(_FORECAST_FILE)
    if unique_id:
        df = df[df["unique_id"] == unique_id]
    return json.loads(df.to_json(orient="records", date_format="iso"))


# --------------------------------------------------------------------------- Chronos-2 shadow (PAR-852)

_CHRONOS = None
_CHRONOS_INIT = threading.Lock()


def _chronos_engine():
    """The one resident engine; built on first use (the model itself loads on
    the first forecast, inside the engine's lock)."""
    global _CHRONOS
    with _CHRONOS_INIT:
        if _CHRONOS is None:
            import chronos_shadow

            _CHRONOS = chronos_shadow.ChronosEngine(
                settings.CHRONOS_MODEL_ID, settings.CHRONOS_REVISION,
                settings.CHRONOS_CACHE_DIR, settings.CHRONOS_VRAM_FRACTION,
            )
        return _CHRONOS


class ChronosRequest(BaseModel):
    park_id: str
    origin_at: datetime
    variant: str = "live"
    keep_minutes: int | None = None


@app.post("/chronos/forecast")
def chronos_forecast(req: ChronosRequest):
    """Chronos-2 SHADOW forecast for one park at one origin (PAR-852).

    Read only by the API's forward-archive capture; nothing serves it to users.
    ``variant``: ``live`` (bench ``chronos2``, today + tomorrow, compressed
    context) or ``grid`` (bench ``chronos2_grid``, d0 … d7, 15-min grid).
    Inputs are read as of ``origin_at``. 503 when switched off
    (``CHRONOS_ENABLED=false``) or while a TFT training run holds the GPU;
    ``status: "warming"`` while the park's context is still being backfilled.
    """
    import pandas as pd

    import chronos_data
    import chronos_shadow

    if not settings.CHRONOS_ENABLED:
        raise HTTPException(status_code=503, detail="Chronos shadow disabled (CHRONOS_ENABLED=false)")
    if _read_status().get("is_training"):
        raise HTTPException(status_code=503, detail="TFT training in progress — shadow skipped")
    if req.variant not in chronos_shadow.VARIANTS:
        raise HTTPException(status_code=400, detail="variant must be live or grid")
    origin = pd.Timestamp(req.origin_at)
    if origin.tzinfo is None:
        raise HTTPException(status_code=400, detail="origin_at needs a timezone (UTC instant)")
    try:
        return chronos_data.run(
            req.park_id, origin, req.variant, req.keep_minutes,
            _chronos_engine(), chronos_data.TruthCache(settings.CHRONOS_TRUTH_CACHE_DIR),
        )
    except chronos_data.Warming as w:
        return {"status": "warming", "parkId": req.park_id, "variant": req.variant,
                "reason": str(w), "curves": []}
    except Exception as e:  # noqa: BLE001
        import traceback

        logger.error("Chronos forecast failed for %s: %s\n%s", req.park_id, e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Chronos forecast failed")


@app.get("/chronos/status")
def chronos_status():
    eng = _CHRONOS
    return {
        "enabled": settings.CHRONOS_ENABLED,
        "model": settings.CHRONOS_MODEL_ID,
        "revision": settings.CHRONOS_REVISION,
        "loaded": bool(eng and eng.loaded),
        "device": eng.device if eng else None,
        "loadSeconds": eng.load_seconds if eng else None,
        "peakVramGb": round(eng.peak_vram_gb, 2) if eng else None,
    }
