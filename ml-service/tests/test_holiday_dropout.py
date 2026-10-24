"""PAR-1051: a dropped-out holiday row carries the values serving feeds it when
forecast from before the break, not proxies that already contain the break.

    cd ml-service && python3 -m pytest tests/test_holiday_dropout.py -q
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import train

PARK = "11111111-1111-1111-1111-111111111111"
RIDE = "22222222-2222-2222-2222-222222222222"

HISTORY = (
    "avg_wait_last_1h",
    "avg_wait_last_24h",
    "avg_wait_same_dow_4w",
    "rolling_avg_7d",
    "rolling_avg_weekday",
    "rolling_avg_weekend",
    "rolling_avg_28d",
    "rolling_avg_90d",
)


class _Cfg:
    CATBOOST_RANDOM_SEED = 42
    OCCUPANCY_DROPOUT_RATE = 0.0
    ROLLING_AVG_DROPOUT_RATE = 0.0
    ROLLING_1H_DROPOUT_RATE = 0.0
    ROLLING_7D_DROPOUT_RATE = 0.0
    HOLIDAY_DROPOUT_RATE = 1.0


class _Log:
    def info(self, *_a, **_k):
        pass


def _frame():
    """Hourly rows 10:00-17:00 over 2026-07-01..20; a school break from 07-11.
    Every history feature carries its own day's level: 20 before, 60 inside."""
    local = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-07-01") + pd.Timedelta(days=d, hours=h)
            for d in range(20)
            for h in range(10, 18)
        ]
    )
    hol = (local >= pd.Timestamp("2026-07-11")).astype(int)
    level = np.where(hol == 1, 60.0, 20.0)
    return pd.DataFrame(
        {
            "attractionId": RIDE,
            "parkId": PARK,
            "timestamp": local.tz_localize("UTC"),
            "local_timestamp": local,
            "hour": local.hour,
            "day_of_week": local.dayofweek,
            "is_weekend": (local.dayofweek >= 5).astype(int),
            "is_holiday_primary": 0,
            "is_school_holiday_primary": hol,
            "waitTime": level,
            "park_occupancy_pct": np.where(hol == 1, 150.0, 80.0),
            **{c: level for c in HISTORY},
            "wait_time_velocity": 2.0,
        }
    )


def _row(out, ts):
    return out[out["local_timestamp"] == pd.Timestamp(ts)].iloc[0]


def test_a_dropped_out_holiday_row_carries_pre_break_values():
    row = _row(train.apply_training_dropout(_frame(), _Cfg(), _Log()), "2026-07-15 14:00")
    for col in HISTORY:
        assert row[col] == pytest.approx(20.0), col
    assert row["wait_time_velocity"] == 0.0
    assert row["park_occupancy_pct"] == pytest.approx(80.0)
    assert row["hour_occupancy_interaction"] == pytest.approx(14 * 80.0 / 100)


def test_deep_inside_the_break_the_weekly_lags_are_unknown_too():
    """Nine days in, a week back is still the break: serving falls back to
    the origin's rolling means, and so does the dropout."""
    row = _row(train.apply_training_dropout(_frame(), _Cfg(), _Log()), "2026-07-20 14:00")
    assert row["avg_wait_same_dow_4w"] == pytest.approx(20.0)
    assert row["avg_wait_last_1h"] == pytest.approx(20.0)


def test_non_holiday_rows_are_untouched():
    df = _frame()
    out = train.apply_training_dropout(df.copy(), _Cfg(), _Log())
    before = df["is_school_holiday_primary"] == 0
    assert (out.loc[before, "rolling_avg_7d"] == 20.0).all()
    assert (out.loc[before, "wait_time_velocity"] == 2.0).all()
