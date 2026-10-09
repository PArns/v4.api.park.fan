"""PAR-814: a cached forecast is only ever persisted under the date of the run
that produced it.

On 2026-10-09 the nightly job fell through to `POST /forecast` after a hung
training, and that wrote the previous night's `nf_forecast.parquet` under
forecast_date=today — 231,480 rows identical to 2026-10-08, served as fresh.

The nf-service image ships no pytest, so this file doubles as a plain script:
`python3 tests/test_forecast_persist_guard.py` runs the same assertions. It
needs fastapi, pandas, pyarrow and pydantic-settings; `db` and `forecast` are
stubbed, so no database and no torch are touched.
"""

import json
import os
import sys
import tempfile
import types
from datetime import date, datetime, timezone

_MODEL_DIR = tempfile.mkdtemp(prefix="nf-test-")
os.environ["MODEL_DIR"] = _MODEL_DIR
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pandas as pd

# Stub the DB layer before anything imports it (the real one builds an engine).
_calls: list[dict] = []
_db = types.ModuleType("db")


def _persist_forecast(yhat, version, value_col, forecast_date):
    _calls.append({"version": version, "forecast_date": forecast_date, "rows": len(yhat)})
    return len(yhat)


_db.persist_forecast = _persist_forecast
sys.modules["db"] = _db

from fastapi import HTTPException

import main


def _write_parquet() -> pd.DataFrame:
    df = pd.DataFrame({
        "unique_id": ["a", "a"],
        "ds": pd.to_datetime(["2026-10-09", "2026-10-10"]),
        "TFT": [30.0, 40.0],
    })
    df.to_parquet(main._FORECAST_FILE)
    return df


def _status(**kw) -> None:
    with open(main._STATUS_FILE, "w") as f:
        json.dump(kw, f)


def _expect_409() -> None:
    try:
        main.run_forecast()
    except HTTPException as e:
        assert e.status_code == 409, e.status_code
    else:
        raise AssertionError("expected 409")
    assert _calls == [], _calls


def test_refuses_while_the_next_run_is_training():
    _calls.clear()
    _write_parquet()  # yesterday's parquet still on disk
    _status(is_training=True, status="training", version="nf20261009_030000")
    _expect_409()


def test_refuses_after_reset_on_startup():
    """The 2026-10-09 state: the run died, startup cleared the lock."""
    _calls.clear()
    _write_parquet()
    _status(is_training=False, status="idle", version="nf20261009_030000",
            error="reset on startup (previous run interrupted)")
    _expect_409()


def test_refuses_after_a_failed_run():
    _calls.clear()
    _write_parquet()
    _status(is_training=False, status="failed", version="nf20261009_030000", error="OOM")
    _expect_409()


def test_refuses_a_completed_status_without_forecast_date():
    """A status file written before forecast_date was recorded."""
    _calls.clear()
    _write_parquet()
    _status(is_training=False, status="completed", version="nf20261008_030000")
    _expect_409()


def test_repersists_under_the_runs_own_date_not_today():
    _calls.clear()
    _write_parquet()
    _status(is_training=False, status="completed", version="nf20261008_030000",
            forecast_date="2026-10-08")
    out = main.run_forecast()
    assert out["forecast_date"] == "2026-10-08", out
    assert _calls == [
        {"version": "nf20261008_030000", "forecast_date": date(2026, 10, 8), "rows": 2}
    ], _calls


def test_runner_records_forecast_date_and_chunk_counts():
    _calls.clear()
    fc = types.ModuleType("forecast")

    def _train_and_forecast(version):
        df = pd.DataFrame({
            "unique_id": ["a"], "ds": pd.to_datetime(["2026-10-10"]), "TFT": [30.0],
        })
        df.attrs["chunks"] = {"total": 10, "ok": 9, "skipped": 1}
        return df

    fc.train_and_forecast = _train_and_forecast
    sys.modules["forecast"] = fc
    import train_runner

    train_runner.forecast = fc
    train_runner.run("nf_test")

    with open(main._STATUS_FILE) as f:
        st = json.load(f)
    today = datetime.now(timezone.utc).date()
    assert st["status"] == "completed", st
    assert st["forecast_date"] == today.isoformat(), st
    assert st["info"]["chunks"] == {"total": 10, "ok": 9, "skipped": 1}, st
    assert _calls == [{"version": "nf_test", "forecast_date": today, "rows": 1}], _calls


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
