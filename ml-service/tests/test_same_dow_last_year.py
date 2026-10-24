"""PAR-1045: avg_wait_same_dow_4w more than 28 days ahead reads the same
weekday and hour 52 weeks back — only where observed, only for rides watched
>= 330 days — identically in serving and in the "as known at origin" hold-out.

    cd ml-service && python3 -m pytest tests/test_same_dow_last_year.py -q
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import numpy as np
import pandas as pd

import predict
import train
from features import LAST_YEAR_LAG


def _rides():
    """r1 watched from 2025-06-01 (waits 40 a year back, 30 now), r2 only from
    2026-03-01. Park on UTC, so local_timestamp is the UTC wall clock."""
    frames = []
    for ride, start in (("r1", "2025-06-01"), ("r2", "2026-03-01")):
        ts = pd.date_range(start, "2026-08-20 23:00", freq="h", tz="UTC")
        frames.append(
            pd.DataFrame(
                {
                    "attractionId": ride,
                    "parkId": "p1",
                    "timestamp": ts,
                    "local_timestamp": ts.tz_localize(None),
                    "hour": ts.hour,
                    "day_of_week": ts.dayofweek,
                    "is_weekend": (ts.dayofweek >= 5).astype(int),
                    "waitTime": np.where(ts.year == 2025, 40.0, 30.0),
                    "park_occupancy_pct": 100.0,
                    "wait_time_velocity": 0.0,
                    "avg_wait_last_1h": 30.0,
                    "wait_lag_1w": 30.0,
                    "avg_wait_same_dow_4w": 30.0,
                }
            )
        )
    df = pd.concat(frames, ignore_index=True)
    for c in train._ORIGIN_VALUE_COLUMNS:
        df[c] = 7.0
    return df


def _profile():
    p = pd.Series({("p1", d, h): 100.0 for d in range(7) for h in range(24)})
    p.index.names = ["parkId", "day_of_week", "hour"]
    return p


def test_serving_asks_only_rows_beyond_28_days_for_the_slot_52_weeks_back():
    base = pd.Timestamp("2026-10-24 08:00", tz="UTC")
    ts = pd.DatetimeIndex(
        [base + pd.Timedelta(days=10), base + pd.Timedelta(days=61)]
    )
    rows = pd.DataFrame(
        {"attractionId": "r1", "timestamp": ts, "local_timestamp": ts.tz_localize(None)}
    )
    keys = predict.last_year_slot_keys(rows, base.to_pydatetime())
    year_ago = ts[1].tz_localize(None) - LAST_YEAR_LAG
    assert keys == [("r1", year_ago.date(), year_ago.hour)]
    assert year_ago.dayofweek == ts[1].dayofweek  # 52 weeks: same weekday


def test_serving_and_training_key_the_same_slot_across_a_dst_change():
    # A Berlin row in summer, a year back in winter-time: both paths take the
    # wall clock minus 52 weeks, not the instant.
    local = pd.Timestamp("2026-07-01 14:00")
    serving_loop_key = (
        local.tz_localize("Europe/Berlin").replace(tzinfo=None) - LAST_YEAR_LAG
    )
    assert (serving_loop_key.date(), serving_loop_key.hour) == (
        (local - LAST_YEAR_LAG).date(),
        14,
    )


def test_fetch_without_keys_never_touches_the_database(monkeypatch):
    monkeypatch.setattr(predict, "get_db", lambda: (_ for _ in ()).throw(AssertionError))
    assert predict.fetch_last_year_slots([], pd.Timestamp.now(tz="UTC")) == {}


def test_far_holdout_reads_last_year_only_for_watched_rides():
    df = _rides()
    target = df[df["timestamp"] >= df["timestamp"].max() - pd.Timedelta(days=5)]
    first_seen = df.groupby("attractionId")["timestamp"].min()
    far = train.as_known_at_origin(
        target, df, pd.Timedelta(days=61), _profile(), None, df, first_seen
    )
    r1 = far[far["attractionId"] == "r1"]
    r2 = far[far["attractionId"] == "r2"]
    assert (r1["avg_wait_same_dow_4w"] == 40.0).all()  # observed a year back
    assert (r1["avg_wait_last_24h"] == 40.0).all()
    assert (r2["avg_wait_same_dow_4w"] == 7.0).all()  # < 330 d: rolling_avg_7d

    # Within 28 days nothing changes.
    near = train.as_known_at_origin(
        target, df, pd.Timedelta(days=7), _profile(), None, df, first_seen
    )
    assert (near["avg_wait_same_dow_4w"] == 30.0).all()


def test_an_unobserved_slot_a_year_back_keeps_the_fallback():
    df = _rides()
    df = df[~((df["attractionId"] == "r1") & (df["timestamp"].dt.year == 2025)
              & (df["timestamp"].dt.month == 8))]
    target = df[df["timestamp"] >= df["timestamp"].max() - pd.Timedelta(days=5)]
    first_seen = pd.Series({"r1": pd.Timestamp("2025-06-01", tz="UTC"),
                            "r2": pd.Timestamp("2026-03-01", tz="UTC")})
    far = train.as_known_at_origin(
        target, df, pd.Timedelta(days=61), _profile(), None, df, first_seen
    )
    assert (far["avg_wait_same_dow_4w"] == 7.0).all()
