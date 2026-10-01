"""forecast_park skips an origin it already wrote with the same checkpoint.

    cd pcn-service && python3 -m pytest test_forecast_skip.py -q
"""

import types

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("torch")

import forecast  # noqa: E402


class _Model:
    ride_ids = None

    def __init__(self):
        self.calls = 0

    def predict_quantiles(self, t, bases, L, H):
        self.calls += 1
        return {0.5: np.ones((1, len(t.ride_ids), H)),
                0.8: np.ones((1, len(t.ride_ids), H)) * 2}


@pytest.fixture
def park(monkeypatch, tmp_path):
    forecast._written.clear()
    ckpt = tmp_path / "pcn_P.pt"
    ckpt.write_bytes(b"x")
    state = {"slots": pd.date_range("2026-10-01 08:00", periods=400, freq="15min"),
             "writes": []}
    model = _Model()

    monkeypatch.setattr(forecast, "model_path", lambda pid: str(ckpt))
    monkeypatch.setattr(forecast.db, "park_has_fresh_data", lambda pid, h: True)
    monkeypatch.setattr(forecast.pipeline, "park_timezone", lambda pid: None)
    monkeypatch.setattr(
        forecast.pipeline, "build_park_tensor",
        lambda pid, window_days=None: types.SimpleNamespace(
            slots=state["slots"], ride_ids=["r1", "r2"]))
    monkeypatch.setattr(forecast, "_load_model", lambda pid: model)
    monkeypatch.setattr(forecast.db, "write_pcn_forecasts",
                        lambda rows, v: state["writes"].append(len(rows)) or len(rows))
    return types.SimpleNamespace(state=state, model=model, ckpt=ckpt)


def test_unchanged_origin_is_not_rewritten(park):
    assert forecast.forecast_park("P", "v") > 0
    assert forecast.forecast_park("P", "v") == 0
    assert park.model.calls == 1
    assert len(park.state["writes"]) == 1


def test_new_origin_is_written(park):
    forecast.forecast_park("P", "v")
    park.state["slots"] = park.state["slots"].append(
        pd.DatetimeIndex([park.state["slots"][-1] + pd.Timedelta("15min")]))
    assert forecast.forecast_park("P", "v") > 0
    assert len(park.state["writes"]) == 2


def test_retrained_checkpoint_rewrites_same_origin(park):
    import os

    forecast.forecast_park("P", "v")
    st = os.stat(park.ckpt)
    os.utime(park.ckpt, (st.st_atime, st.st_mtime + 60))
    assert forecast.forecast_park("P", "v") > 0
    assert len(park.state["writes"]) == 2


def test_failed_write_is_retried(park, monkeypatch):
    def boom(rows, v):
        raise RuntimeError("db down")

    monkeypatch.setattr(forecast.db, "write_pcn_forecasts", boom)
    with pytest.raises(RuntimeError):
        forecast.forecast_park("P", "v")
    assert "P" not in forecast._written
