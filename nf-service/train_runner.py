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


def run_grid_shadow(version: str, forecast_date) -> dict | None:
    """PAR-1041: the calendar-grid TFT as a shadow, after the served run and in
    the same process, written to `tft_forecasts_shadow` under the served run's
    forecast_date. Never fails the night: the served forecast is already
    persisted, so an error here is reported in the status and nothing else."""
    if not main.settings.NF_GRID_SHADOW:
        return None
    if main.settings.NF_CALENDAR_GRID:
        # The served run already is the grid variant; a copy would compare it
        # with itself.
        return {"skipped": "NF_CALENDAR_GRID is on"}
    shadow_version = f"{version}-grid"
    try:
        with forecast.calendar_grid(True):
            y_hat = forecast.train_and_forecast(shadow_version)
        tcol = main._tft_column(list(y_hat.columns))
        persisted = (
            db.persist_forecast(
                y_hat, shadow_version, tcol, forecast_date, table=db.SHADOW_TABLE
            )
            if tcol else 0
        )
        return {
            "version": shadow_version, "rows": len(y_hat),
            "persisted": int(persisted),
            "chunks": dict(y_hat.attrs.get("chunks") or {}),
        }
    except Exception as e:  # noqa: BLE001
        main.logger.error("TFT grid shadow %s failed: %s\n%s", shadow_version, e,
                          traceback.format_exc())
        return {"version": shadow_version, "error": str(e)}


def run(version: str) -> None:
    started_at = datetime.now(timezone.utc)
    # The day this run makes the forecast (UTC, as forecast_date always was),
    # fixed at START: the panel is read from here on, so a run that crosses
    # midnight still describes the day it began. Written to the status file next
    # to the version, so a later /forecast re-persist of this parquet lands on
    # the same key instead of on whatever day it happens to be called (PAR-814).
    forecast_date = started_at.date()
    main._write_status({
        "is_training": True, "status": "training", "version": version,
        "started_at": started_at.isoformat(), "error": None,
    })
    try:
        y_hat = forecast.train_and_forecast(version)
        chunks = dict(y_hat.attrs.get("chunks") or {})
        y_hat.to_parquet(main._FORECAST_FILE)
        tcol = main._tft_column(list(y_hat.columns))
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
        del y_hat
        # The lock stays on through the shadow: no second /train and no Chronos
        # call on the GPU until both runs are done.
        main._write_status({
            "is_training": True, "status": "training", "version": version,
            "started_at": started_at.isoformat(), "phase": "grid_shadow",
            "error": None,
        })
        info["shadow"] = run_grid_shadow(version, forecast_date)
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
