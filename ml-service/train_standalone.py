"""
Isolated training subprocess entry point.

Launched by main.py via subprocess.Popen so an OOM kill during training
only tears down this process, not the uvicorn workers serving predictions.

Usage (internal — called by main.py):
    python train_standalone.py <version> <status_file>

This process only writes the model file, its metadata and the training status.
It never writes the active-version sentinel: what the workers serve changes only
when the NestJS training processor has registered the version in `ml_models`,
passed the champion/challenger gate and called `POST /model/reload` (PAR-815).
A rejected, failed or timed-out run therefore never touches what is served.
"""

import faulthandler
import sys
import os
import json
import logging
from datetime import datetime, timezone

# Dump a C-level traceback of all threads on a fatal signal (SIGSEGV/SIGABRT/…),
# so a native crash in CatBoost/numpy leaves a trace instead of a bare exit code.
faulthandler.enable()

# Ensure the ml-service package root is on the path regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _write_json(path: str, data: dict) -> None:
    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.warning(f"Could not write {path}: {e}")


def main() -> int:
    # A third argument (the sentinel path older main.py versions passed) is
    # accepted and ignored.
    if len(sys.argv) < 3:
        logger.error("Usage: train_standalone.py <version> <status_file>")
        return 1

    version = sys.argv[1]
    status_file = sys.argv[2]

    started_at = datetime.now(timezone.utc).isoformat()

    # Record our own PID so a recycling uvicorn worker's startup_event can tell a
    # genuinely-orphaned lock (dead PID) from a live training (this PID still alive)
    # and not clobber a run in progress. This subprocess shares the container PID
    # namespace with every gunicorn/uvicorn worker, so os.kill(pid, 0) is meaningful
    # across processes.
    _write_json(status_file, {
        "is_training": True,
        "current_version": version,
        "started_at": started_at,
        "status": "training",
        "error": None,
        "finished_at": None,
        "pid": os.getpid(),
    })

    try:
        from model import load_saved_metadata
        from train import train_model
        logger.info(f"Starting training for version {version}")
        budget = os.environ.get("TRAIN_TIME_BUDGET_SECONDS")
        metrics = train_model(
            version=version,
            time_budget_seconds=float(budget) if budget else None,
        )

        # train_model returns None when it stops early (no data, empty training
        # set) without saving anything. Reporting "completed" (and, before
        # PAR-815, writing the sentinel) announced a version that does not exist:
        # every worker retried loading it on every request.
        if metrics is None:
            raise RuntimeError(
                "train_model stopped early without saving a model "
                "(no training data or an empty training set — see log above)"
            )
        saved = load_saved_metadata(version)
        if saved is None:
            raise RuntimeError(
                f"train_model returned, but no model file and metadata exist for {version}"
            )

        _write_json(status_file, {
            "is_training": False,
            "current_version": version,
            "started_at": started_at,
            "status": "completed",
            "error": None,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "timings": saved.get("training_timings"),
            "refit_skipped": saved.get("refit_skipped"),
        })

        logger.info(f"Training completed for version {version}")
        return 0

    except Exception as e:
        import traceback
        error_tb = traceback.format_exc()
        logger.error(f"Training failed: {e}")
        logger.error(f"Traceback:\n{error_tb}")

        _write_json(status_file, {
            "is_training": False,
            "current_version": version,
            "started_at": started_at,
            "status": "failed",
            "error": f"{e}\n\nTraceback:\n{error_tb}",
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        return 1


if __name__ == "__main__":
    sys.exit(main())
