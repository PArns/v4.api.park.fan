"""PAR-1041: the calendar-grid TFT runs every night as a shadow beside the served
TFT, after it, and writes its own table.

    cd nf-service && python3 -m pytest tests/test_grid_shadow.py -q

No database and no torch: training and the DB write are replaced by fakes; the
SQL builder and the grid switch are the real ones.
"""

import importlib.util
import os
import sys
import tempfile
import types
from datetime import date, datetime, timezone

os.environ.setdefault("MODEL_DIR", tempfile.mkdtemp(prefix="nf-test-"))
NF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, NF_DIR)

import pandas as pd
import pytest

# Other test modules replace `db` in sys.modules with a stub; load the real file.
_spec = importlib.util.spec_from_file_location("nf_db_shadow", os.path.join(NF_DIR, "db.py"))
real_db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(real_db)

import forecast
import main
import train_runner

FD = date(2026, 10, 24)


def test_shadow_table_is_like_the_served_one_and_names_are_whitelisted():
    ddl, upsert = real_db.forecast_sql(real_db.SHADOW_TABLE)
    assert "CREATE TABLE IF NOT EXISTS tft_forecasts_shadow (LIKE tft_forecasts INCLUDING ALL)" in ddl
    assert "INSERT INTO tft_forecasts_shadow" in upsert
    ddl, upsert = real_db.forecast_sql("tft_forecasts")
    assert "CREATE TABLE IF NOT EXISTS tft_forecasts (" in ddl
    assert "INSERT INTO tft_forecasts\n" in upsert
    with pytest.raises(ValueError):
        real_db.forecast_sql("tft_forecasts; DROP TABLE x")


class _Fakes:
    def __init__(self, fail_shadow=False):
        self.calls = []
        self.fail_shadow = fail_shadow

    def train(self, version):
        grid = forecast.settings.NF_CALENDAR_GRID
        self.calls.append(("train", version, grid))
        if grid and self.fail_shadow:
            raise RuntimeError("CUDA out of memory")
        df = pd.DataFrame({"unique_id": ["a"], "ds": pd.to_datetime(["2026-10-24"]),
                           "TFT": [30.0]})
        df.attrs["chunks"] = {"total": 1, "ok": 1, "skipped": 0}
        return df

    def persist(self, yhat, version, col, fd, table="tft_forecasts"):
        self.calls.append(("persist", version, table, fd))
        return len(yhat)


@pytest.fixture
def fakes(monkeypatch, tmp_path):
    f = _Fakes()
    monkeypatch.setattr(train_runner, "forecast", types.SimpleNamespace(
        calendar_grid=forecast.calendar_grid, train_and_forecast=f.train))
    monkeypatch.setattr(train_runner, "db", types.SimpleNamespace(
        persist_forecast=f.persist, SHADOW_TABLE=real_db.SHADOW_TABLE))
    monkeypatch.setattr(main, "_STATUS_FILE", str(tmp_path / "status.json"))
    monkeypatch.setattr(main, "_FORECAST_FILE", str(tmp_path / "f.parquet"))
    monkeypatch.setattr(main.settings, "MODEL_DIR", str(tmp_path))
    monkeypatch.setattr(main.settings, "NF_CALENDAR_GRID", False)
    monkeypatch.setattr(main.settings, "NF_GRID_SHADOW", True)
    # The served run caches its parquet; no pyarrow needed for that here.
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path: None)
    return f


def test_shadow_trains_with_the_grid_forced_on_and_writes_its_own_table(fakes):
    out = train_runner.run_grid_shadow("nf1", FD)
    assert fakes.calls == [
        ("train", "nf1-grid", True),
        ("persist", "nf1-grid", "tft_forecasts_shadow", FD),
    ]
    assert out["persisted"] == 1
    assert main.settings.NF_CALENDAR_GRID is False  # restored


def test_flag_off_or_grid_already_served_runs_nothing(fakes, monkeypatch):
    monkeypatch.setattr(main.settings, "NF_GRID_SHADOW", False)
    assert train_runner.run_grid_shadow("nf1", FD) is None
    monkeypatch.setattr(main.settings, "NF_GRID_SHADOW", True)
    monkeypatch.setattr(main.settings, "NF_CALENDAR_GRID", True)
    assert "skipped" in train_runner.run_grid_shadow("nf1", FD)
    assert fakes.calls == []


def test_night_runs_served_first_then_shadow_and_a_shadow_failure_does_not_fail_it(fakes):
    fakes.fail_shadow = True
    train_runner.run("nf1")
    assert fakes.calls == [
        ("train", "nf1", False),
        ("persist", "nf1", "tft_forecasts", datetime.now(timezone.utc).date()),
        ("train", "nf1-grid", True),
    ]
    st = main._read_status()
    assert st["status"] == "completed" and st["is_training"] is False
    assert st["info"]["persisted"] == 1
    assert "CUDA out of memory" in st["info"]["shadow"]["error"]
    assert main.settings.NF_CALENDAR_GRID is False
