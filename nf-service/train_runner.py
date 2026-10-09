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
        chunks = dict(y_hat.attrs.get("chunks") or {})
        y_hat.to_parquet(main._FORECAST_FILE)
        tcol = main._tft_column(list(y_hat.columns))
        # The day this run made the forecast (UTC, as forecast_date always was).
        # Fixed here and written to the status file next to the version, so a
        # later /forecast re-persist of this parquet lands on the same key
        # instead of on whatever day it happens to be called (PAR-814).
        forecast_date = datetime.now(timezone.utc).date()
        persisted = (
            db.persist_forecast(y_hat, version, tcol, forecast_date) if tcol else 0
        )
        info = {"rows": int(len(y_hat)), "persisted": int(persisted), "chunks": chunks}
        if chunks.get("skipped"):
            main.logger.warning(
                "TFT run %s completed with %d of %d chunk(s) skipped — those parks "
                "keep their previous forecast_date", version, chunks["skipped"],
                chunks.get("total", 0),
            )
        main._write_status({
            "is_training": False, "status": "completed", "version": version,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "forecast_date": forecast_date.isoformat(),
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
