"""PAR-1033 / PAR-1039: the TFT panel and future frame on the park-local calendar.

    cd nf-service && python3 -m pytest tests/test_calendar_grid.py -q

Pure pandas: no database, no torch.
"""

import contextlib
import importlib.util
import os
import sys

import pandas as pd

NF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, NF_DIR)

# Other test modules replace `db` in sys.modules with a stub; load the real file.
_spec = importlib.util.spec_from_file_location("nf_db_grid", os.path.join(NF_DIR, "db.py"))
db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(db)

# 03:00 UTC, the nightly training time: already 12:00 in Tokyo, still the
# previous evening (20:00) in Los Angeles.
NOW = pd.Timestamp("2026-10-24 03:00", tz="UTC")


def _meta():
    return pd.DataFrame([
        {"unique_id": "tokyo", "park_id": "p1", "timezone": "Asia/Tokyo",
         "country": "JP", "region": None, "influencing": None, "latitude": 35.6},
        {"unique_id": "la", "park_id": "p2", "timezone": "America/Los_Angeles",
         "country": "US", "region": None, "influencing": None, "latitude": 34.1},
        {"unique_id": "gold_coast", "park_id": "p3", "timezone": "Australia/Brisbane",
         "country": "AU", "region": None, "influencing": None, "latitude": -27.9},
        {"unique_id": "stale", "park_id": "p1", "timezone": "Asia/Tokyo",
         "country": "JP", "region": None, "influencing": None, "latitude": 35.6},
    ])


def _panel():
    d = pd.to_datetime
    return pd.DataFrame({
        "unique_id": ["tokyo"] * 3 + ["la"] * 2 + ["stale"],
        # Tokyo: closed on 2026-10-21 (a rest day) — absent from the panel.
        "ds": d(["2026-10-20", "2026-10-22", "2026-10-23",
                 "2026-10-21", "2026-10-22", "2026-06-01"]),
        "y": [40.0, 50.0, 60.0, 30.0, 35.0, 20.0],
    })


def test_closed_day_is_a_masked_row_not_a_gap():
    g = db.to_calendar_grid(_panel(), _meta(), 90, now=NOW)
    tokyo = g[g.unique_id == "tokyo"].set_index("ds")
    assert list(tokyo.index) == list(pd.date_range("2026-10-20", "2026-10-23"))
    closed = tokyo.loc[pd.Timestamp("2026-10-21")]
    assert closed.available_mask == 0.0 and closed.y == 0.0
    assert tokyo.available_mask.sum() == 3


def test_every_series_ends_park_local_yesterday_and_stale_ones_drop():
    g = db.to_calendar_grid(_panel(), _meta(), 90, now=NOW)
    last = g.groupby("unique_id")["ds"].max()
    assert last["tokyo"] == pd.Timestamp("2026-10-23")
    # LA's "today" is still 2026-10-23: its last grid day is 10-22.
    assert last["la"] == pd.Timestamp("2026-10-22")
    assert "stale" not in last.index  # last seen 145 days ago > 90


def test_future_starts_park_local_today_for_every_series():
    g = db.to_calendar_grid(_panel(), _meta(), 90, now=NOW)
    fut = db.build_future_frame(g, _meta(), pd.DataFrame(), 60)
    first = fut.groupby("unique_id")["ds"].min()
    assert first["tokyo"] == pd.Timestamp("2026-10-24")
    assert first["la"] == pd.Timestamp("2026-10-23")
    assert (fut.groupby("unique_id").size() == 60).all()


def test_southern_hemisphere_peak_season_is_shifted():
    frame = pd.DataFrame({
        "unique_id": ["tokyo", "tokyo", "gold_coast", "gold_coast"],
        "ds": pd.to_datetime(["2026-07-15", "2026-01-15", "2026-07-15", "2026-01-15"]),
    })
    out = db.add_calendar_covariates(frame, _meta(), pd.DataFrame())
    assert out["is_peak_season"].tolist() == [1, 0, 0, 1]


def test_schedule_flags_leave_unknown_days_at_zero():
    frame = pd.DataFrame({
        "unique_id": ["tokyo"] * 3,
        "ds": pd.to_datetime(["2026-10-20", "2026-10-21", "2026-10-22"]),
    })
    schedule = pd.DataFrame({
        "park_id": ["p1", "p1"], "ds": pd.to_datetime(["2026-10-20", "2026-10-21"]),
        "sched_open": [1, 0], "sched_closed": [0, 1],
    })
    out = db.add_calendar_covariates(frame, _meta(), pd.DataFrame(), schedule=schedule)
    assert out["sched_open"].tolist() == [1, 0, 0]
    assert out["sched_closed"].tolist() == [0, 1, 0]


def test_school_holiday_distance():
    frame = pd.DataFrame({"unique_id": ["tokyo"], "ds": pd.to_datetime(["2026-07-15"])})
    hol = pd.DataFrame([{"date": pd.Timestamp("2026-07-20"), "country": "JP",
                         "region": None, "holiday_type": "school"}])
    out = db.add_calendar_covariates(frame, _meta(), hol)
    assert out["days_until_school_holiday"].tolist() == [5.0]
    assert out["days_until_holiday"].tolist() == [30.0]  # no public holiday


def test_grid_flag_switches_the_exog_list(monkeypatch):
    assert db.futr_exog() == db.FUTR_EXOG  # default: current inputs unchanged
    monkeypatch.setattr(db.settings, "NF_CALENDAR_GRID", True)
    assert db.futr_exog()[-4:] == ["sched_open", "sched_closed",
                                   "days_until_school_holiday", "days_since_school_holiday"]


class _Engine:
    @contextlib.contextmanager
    def connect(self):
        yield None


def test_panel_sql_excludes_partial_today_and_heartbeats(monkeypatch):
    calls = []

    def read_sql(sql, conn, params=None):
        calls.append(str(sql))
        return pd.DataFrame({"unique_id": [], "ds": [], "y": []})

    monkeypatch.setattr(db, "_engine", _Engine())
    monkeypatch.setattr(db.pd, "read_sql", read_sql)
    db.fetch_daily_peak_panel([])
    sql = calls[0]
    assert "DATE(qd.timestamp AT TIME ZONE p.timezone) < DATE(NOW() AT TIME ZONE p.timezone)" in sql
    assert "NOT COALESCE(qd.is_heartbeat, false)" in sql
