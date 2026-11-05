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
        lambda pid, window_days=None, in_force=None: state.setdefault(
            "in_force", []).append(in_force) or types.SimpleNamespace(
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


@pytest.mark.parametrize("ckpt_mode", ["median", "in_force"])
def test_inputs_follow_the_checkpoints_target_mode(park, ckpt_mode):
    # The env says median (default); the inputs must still be built the way the
    # loaded checkpoint was trained (PAR-1034), whichever way the mismatch goes.
    park.model.target_mode = ckpt_mode
    forecast.forecast_park("P", "v")
    assert park.state["in_force"] == [ckpt_mode == "in_force"]


# --- Skip reasons in the job result (PAR-1120) ---------------------------------

def _reason(pid="P"):
    skipped = {}
    assert forecast.forecast_park(pid, "v", skipped) == 0
    return skipped.get(pid)


def test_every_reason_is_in_exactly_one_group():
    assert not forecast.IDLE_REASONS & forecast.BLOCKED_REASONS


def test_no_fresh_data_is_idle(park, monkeypatch):
    monkeypatch.setattr(forecast.db, "park_has_fresh_data", lambda pid, h: False)
    assert _reason() == "no_fresh_data"


def test_no_model_is_blocked(park, monkeypatch):
    monkeypatch.setattr(forecast, "_load_model", lambda pid: None)
    assert _reason() == "no_model"


def test_no_panel_is_blocked(park, monkeypatch):
    monkeypatch.setattr(forecast.pipeline, "build_park_tensor",
                        lambda pid, window_days=None, in_force=None: None)
    assert _reason() == "no_panel"


def test_stale_origin_is_idle(park, monkeypatch):
    monkeypatch.setattr(forecast.pipeline, "park_timezone", lambda pid: "UTC")
    assert _reason() == "stale_origin"  # the fixture's slots end in October 2026


def test_too_few_slots_is_blocked(park):
    park.state["slots"] = park.state["slots"][:5]
    assert _reason() == "too_few_slots"


def test_no_trained_ride_is_blocked(park, monkeypatch):
    park.model.ride_ids = ["other1", "other2"]
    monkeypatch.setattr(forecast.tns, "align_ride_axis",
                        lambda t, ids: (t, np.zeros(len(ids), dtype=bool)))
    assert _reason() == "no_trained_ride"


def test_already_forecast_is_idle(park):
    forecast.forecast_park("P", "v")
    assert _reason() == "already_forecast"


def test_forecast_all_splits_idle_from_blocked(park, monkeypatch):
    monkeypatch.setattr(forecast.pipeline, "scope_park_ids",
                        lambda ids: ["A", "B", "C", "D"])
    monkeypatch.setattr(forecast.db, "park_has_fresh_data", lambda pid, h: pid != "A")
    monkeypatch.setattr(forecast, "_load_model",
                        lambda pid: None if pid == "B" else park.model)

    def build(pid, window_days=None, in_force=None):
        if pid == "D":
            raise RuntimeError("db down")
        return types.SimpleNamespace(slots=park.state["slots"], ride_ids=["r1", "r2"])

    monkeypatch.setattr(forecast.pipeline, "build_park_tensor", build)
    res = forecast.forecast_all("v")
    assert res["rows"] > 0  # C wrote
    assert res["skipped"] == {"idle": {"A": "no_fresh_data"},
                              "blocked": {"B": "no_model", "D": "failed"}}
