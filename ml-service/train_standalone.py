"""
Isolated training subprocess entry point.

Launched by main.py via subprocess.Popen so an OOM kill during training
only tears down this process, not the uvicorn workers serving predictions.

Usage (internal — called by main.py):
    python train_standalone.py <version> <status_file> <sentinel_file>
"""

import sys
import os
import json
import logging
from datetime import datetime, timezone

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
    if len(sys.argv) < 4:
        logger.error("Usage: train_standalone.py <version> <status_file> <sentinel_file>")
        return 1

    version = sys.argv[1]
    status_file = sys.argv[2]
    sentinel_file = sys.argv[3]

    started_at = datetime.now(timezone.utc).isoformat()

    _write_json(status_file, {
        "is_training": True,
        "current_version": version,
        "started_at": started_at,
        "status": "training",
        "error": None,
        "finished_at": None,
    })

    try:
        from train import train_model
        logger.info(f"Starting training for version {version}")
        train_model(version=version)

        _write_json(status_file, {
            "is_training": False,
            "current_version": version,
            "started_at": started_at,
            "status": "completed",
            "error": None,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })

        try:
            with open(sentinel_file, "w") as f:
                f.write(version)
            logger.info(f"Sentinel written for {version}")
        except Exception as e:
            logger.warning(f"Could not write sentinel: {e}")

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
