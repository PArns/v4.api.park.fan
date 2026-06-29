"""Subprocess entry for nightly training (launched by main.py /train).

Runs in its OWN OS process — like nf-service — so an OOM kills only the runner, uvicorn
survives, and torch/DataLoader fork from a clean process. Writes the training status
file so /train/status (and the admin dashboard) can read progress/outcome.

    python3 train_runner.py <version> [PARK_UUID ...]
"""

from __future__ import annotations

import json
import os
import sys

from config import get_settings

settings = get_settings()
_STATUS_FILE = os.path.join(settings.MODEL_DIR, "pcn_training_status.json")


def _write(status: dict) -> None:
    os.makedirs(settings.MODEL_DIR, exist_ok=True)
    with open(_STATUS_FILE, "w") as f:
        json.dump(status, f)


def main() -> int:
    version = sys.argv[1] if len(sys.argv) > 1 else "pcn-manual"
    parks = sys.argv[2:] or None
    _write({"is_training": True, "status": "running", "version": version, "error": None})
    try:
        import train
        res = train.train_all(version, parks)
        _write({"is_training": False, "status": "completed", "version": version,
                "error": None, "result": res})
        return 0
    except Exception as e:  # noqa: BLE001
        import traceback
        _write({"is_training": False, "status": "failed", "version": version,
                "error": f"{e}\n{traceback.format_exc()}"})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
