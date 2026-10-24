"""In-force (BENCH-SPEC) targets and the heartbeat filter (PAR-1034 / PAR-1039), no DB.

    cd pcn-service && python3 -m pytest test_in_force_truth.py -q
"""

import numpy as np
import pandas as pd

import db
import tensor

DAY = "2026-06-01"
SLOTS = pd.date_range(f"{DAY} 09:00", f"{DAY} 18:45", freq="15min")
WINDOWS = pd.DataFrame({
    "date": [pd.Timestamp(DAY).date()],
    "open": [pd.Timestamp(f"{DAY} 10:00")],
    "close": [pd.Timestamp(f"{DAY} 18:00")],
})


def _reading(uid, ts, status="OPERATING", wait=30.0):
    """One change-log row, binned the way db.MID_BIN bins it: into the slot whose
    (midpoint - 15 min, midpoint] holds the timestamp."""
    ts = pd.Timestamp(ts)
    ds = (ts - pd.Timedelta(minutes=7.5) - pd.Timedelta(microseconds=1)).floor("15min") \
        + pd.Timedelta(minutes=15)
    return {"unique_id": uid, "ds": ds, "status": status, "wait": wait, "ts": ts}


def _truth(rows, windows=WINDOWS, rides=("a",)):
    return tensor.in_force_truth(pd.DataFrame(rows), list(rides), SLOTS, windows)


def _col(hhmm):
    return SLOTS.get_loc(pd.Timestamp(f"{DAY} {hhmm}"))


def test_unchanged_ride_gets_every_slot_of_its_window():
    # Written at opening, then only the hourly "unchanged" re-write — the median panel
    # gave this ride ~1 slot in 4; the in-force truth gives it every one.
    rows = [_reading("a", f"{DAY} {h:02d}:00") for h in range(10, 18)]
    truth, in_window = _truth(rows)
    open_cols = [_col(f"{h:02d}:{m:02d}") for h in range(10, 18) for m in (0, 15, 30, 45)]
    assert np.isfinite(truth[0, open_cols]).all()
    assert (truth[0, open_cols] == 30.0).all()
    assert np.isfinite(truth[0]).sum() == len(open_cols)          # nothing outside
    assert in_window[_col("09:45")] == 0 and in_window[_col("17:45")] == 1


def test_midpoint_rule_and_supersession():
    # 10:20 change → in force at the 10:15 slot's midpoint (10:22:30), not before.
    # CLOSED at 12:00 supersedes it: no target until the ride reopens at 13:00.
    rows = [
        _reading("a", f"{DAY} 10:00", wait=20.0),
        _reading("a", f"{DAY} 10:20", wait=50.0),
        _reading("a", f"{DAY} 12:00", status="CLOSED", wait=0.0),
        _reading("a", f"{DAY} 13:00", wait=40.0),
    ]
    truth, _ = _truth(rows)
    assert truth[0, _col("10:00")] == 20.0
    assert truth[0, _col("10:15")] == 50.0
    assert truth[0, _col("11:45")] == 50.0
    assert np.isnan(truth[0, _col("12:00")]) and np.isnan(truth[0, _col("12:45")])
    assert truth[0, _col("13:00")] == 40.0


def test_closed_slot_and_low_wait_are_not_targets():
    rows = [
        _reading("a", f"{DAY} 10:00", status="DOWN", wait=0.0),
        _reading("a", f"{DAY} 11:00", wait=3.0),                 # < MIN_WAIT
        _reading("a", f"{DAY} 12:00", wait=25.0),
    ]
    truth, _ = _truth(rows)
    assert np.isnan(truth[0, _col("10:00"):_col("12:00")]).all()
    assert truth[0, _col("12:00")] == 25.0


def test_stale_reading_drops_after_three_hours():
    # One reading at 10:00, then the feed goes silent: in force through the slot whose
    # midpoint is 13:00 at the latest (age <= 3 h), dropped after.
    truth, _ = _truth([_reading("a", f"{DAY} 10:00")])
    assert truth[0, _col("12:45")] == 30.0                         # mid 12:52:30
    assert np.isnan(truth[0, _col("13:00")])                       # mid 13:07:30 > 3 h


def test_outside_published_window_is_not_a_target():
    # The feed keeps saying OPERATING after the park closed at 18:00 (last-known value).
    rows = [_reading("a", f"{DAY} 17:30"), _reading("a", f"{DAY} 18:20")]
    truth, in_window = _truth(rows)
    assert truth[0, _col("17:45")] == 30.0
    assert np.isnan(truth[0, _col("18:00"):]).all()
    assert in_window[_col("18:00"):].sum() == 0


def test_park_without_published_hours_falls_back_to_reporting_rides():
    rides = ("a", "b", "c")
    rows = [_reading(u, f"{DAY} 10:00") for u in rides] + [_reading("a", f"{DAY} 11:00")]
    truth, in_window = _truth(rows, windows=WINDOWS.iloc[0:0], rides=rides)
    # three rides report in the 10:00 slot → open there; elsewhere fewer than three
    assert in_window[_col("10:00")] == 1 and in_window[_col("11:00")] == 0
    assert np.isfinite(truth[:, _col("10:00")]).all()
    assert np.isnan(truth[:, _col("10:15")]).all()


def test_tensor_build_uses_truth_for_targets_and_mask():
    # Median panel: the ride appears only in the slots where it was written.
    panel = pd.DataFrame([
        {"unique_id": "a", "ds": pd.Timestamp(f"{DAY} {h:02d}:00"), "y": 30.0,
         "n_obs": 1, "down_count": 0}
        for h in range(9, 19)
    ])
    readings = pd.DataFrame([_reading("a", f"{DAY} {h:02d}:00") for h in range(10, 18)])
    legacy = tensor.build(panel, park_id="P", min_rides_open=1)
    crt = tensor.build(panel, park_id="P", min_rides_open=1,
                       readings=readings, windows=WINDOWS)
    assert legacy.loss_mask.sum() == 10                            # one slot per hour
    assert crt.loss_mask.sum() == 32                               # 10:00..17:45
    assert crt.park_open.sum() == 32
    assert crt.channel_names == tensor.CHANNELS                    # contract untouched


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        _SEEN.append(str(sql))

        class _R:
            def first(self):
                return None
        return _R()


class _Engine:
    def connect(self):
        return _Conn()


_SEEN: list[str] = []


def test_every_queue_data_read_drops_heartbeats(monkeypatch):
    _SEEN.clear()
    monkeypatch.setattr(db, "engine", lambda: _Engine())

    def fake_read_sql(sql, conn, params=None):
        _SEEN.append(str(sql))
        return pd.DataFrame()

    monkeypatch.setattr(db.pd, "read_sql", fake_read_sql)
    now = pd.Timestamp.now(tz="UTC")
    db.park_has_fresh_data("p", 36)
    db.fetch_cross_ride_panel("p", "Europe/Berlin", 7)
    db.fetch_actuals_local("p", "Europe/Berlin", now - pd.Timedelta(hours=2), now)
    db.fetch_in_force_readings("p", "Europe/Berlin", now - pd.Timedelta(hours=2), now)
    reads = [s for s in _SEEN if "queue_data" in s]
    assert len(reads) == 4
    for s in reads:
        assert "NOT COALESCE(qd.is_heartbeat, false)" in s


def test_in_force_actuals_carry_a_reading_from_before_the_window(monkeypatch):
    # Board actuals under PCN_TARGET_MODE=in_force: a reading written before `lo` is
    # still in force inside the window; slots whose midpoint has not passed are left out.
    monkeypatch.setattr(type(db.settings), "in_force_targets", property(lambda s: True))
    monkeypatch.setattr(db, "fetch_in_force_readings", lambda *a: pd.DataFrame(
        [_reading("a", f"{DAY} 10:00", wait=45.0)]))
    monkeypatch.setattr(db, "fetch_operating_windows", lambda *a: WINDOWS)
    tz = "Europe/Berlin"                                            # UTC+2 in June
    lo = pd.Timestamp(f"{DAY} 11:00", tz=tz).tz_convert("UTC")
    hi = pd.Timestamp(f"{DAY} 12:10", tz=tz).tz_convert("UTC")
    out = db.fetch_actuals_local("p", tz, lo.to_pydatetime(), hi.to_pydatetime())
    got = out.set_index("target_slot")["actual"]
    # 10:45 (mid 10:52:30) is before lo; 12:00 (mid 12:07:30) has passed by hi 12:10.
    assert list(got.index) == list(pd.date_range(f"{DAY} 11:00", f"{DAY} 12:00", freq="15min"))
    assert (got == 45.0).all()
