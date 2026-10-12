"""
PAR-816: predict.py fetched holidays with `date BETWEEN <first UTC timestamp>
AND <last UTC timestamp>`. A DATE compares as midnight, so an hourly forecast
starting at 10:00 never fetched today's holidays, and a park west of UTC lost
its local day entirely. A day d is fetched iff start <= d 00:00 <= end.
"""

from datetime import date, datetime

import pandas as pd
from holiday_features import holiday_fetch_window


def _midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day)


def _assert_covers(local_ts: pd.Series):
    start, end = holiday_fetch_window(local_ts)
    days = pd.to_datetime(local_ts).dt.date
    assert start <= _midnight(days.min()), (start, days.min())
    assert end >= _midnight(days.max()), (end, days.max())


def test_hourly_forecast_starting_mid_morning_includes_today():
    # Europe park, forecast from 10:00 local today for 24 hours.
    local = pd.Series(pd.date_range("2026-10-09 10:00", periods=24, freq="h"))
    _assert_covers(local)


def test_park_west_of_utc_includes_its_local_day():
    # US park at 22:00 local on 10-09 is already 10-10 in UTC; the local day
    # (what the features key on) must still be inside the window.
    local = pd.Series(pd.to_datetime(["2026-10-09 22:00", "2026-10-10 21:00"]))
    _assert_covers(local)


def test_daily_forecast_reaches_the_last_target_day():
    local = pd.Series(pd.date_range("2026-10-09 12:00", periods=365, freq="D"))
    _assert_covers(local)


def test_window_is_midnight_aligned():
    start, end = holiday_fetch_window(pd.Series(pd.to_datetime(["2026-10-09 10:37"])))
    assert start.time() == datetime.min.time()
    assert end.time() == datetime.min.time()
