"""Standalone training runner — launched as its OWN process by /train (Popen).

Running training out-of-process keeps uvicorn responsive (no GIL starvation of
/health), lets the chunked DataLoader workers fork from a clean process, and means
an OOM kills only this process, not the API. Status is shared with the API via the
status file (main._write_status).

Usage: python3 train_runner.py <version>
"""

import sys
import traceback
from datetime import datetime, timezone

import main  # importing the FastAPI module is side-effect-free (no server start)
import forecast
import db


def run(version: str) -> None:
    main._write_status({
        "is_training": True, "status": "training", "version": version,
        "started_at": datetime.now(timezone.utc).isoformat(), "error": None,
    })
    try:
        y_hat = forecast.train_and_forecast(version)
        y_hat.to_parquet(main._FORECAST_FILE)
        tcol = main._tft_column(list(y_hat.columns))
        persisted = db.persist_forecast(y_hat, version, tcol) if tcol else 0
        info = {"rows": int(len(y_hat)), "persisted": int(persisted)}
        main._write_status({
            "is_training": False, "status": "completed", "version": version,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "info": info, "error": None,
        })
        main.logger.info("Training + forecast complete: %s", info)
    except BaseException as e:  # noqa: BLE001
        tb = traceback.format_exc()
        main.logger.error("Training failed: %s\n%s", e, tb)
        traceback.print_exc()
        main._write_status({
            "is_training": False, "status": "failed", "version": version,
            "error": f"{e}\n{tb}",
        })
        sys.exit(1)


if __name__ == "__main__":
    _v = sys.argv[1] if len(sys.argv) > 1 else (
        "nf" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    )
    run(_v)
