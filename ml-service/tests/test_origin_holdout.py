"""PAR-1040: reproducible validation weeks and the "as known at origin" hold-out."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import numpy as np
import pandas as pd

import train


def test_validation_weeks_are_fixed_and_survive_a_sliding_window():
    weeks = list(range(202501, 202553))
    a = train.select_validation_weeks(weeks, 42)
    assert a == train.select_validation_weeks(weeks, 42)
    assert 0 < len(a) < len(weeks)
    # Tomorrow's window drops the oldest week and adds a new one: every week
    # still in both windows keeps its side of the split.
    shifted = weeks[1:] + [202601]
    b = train.select_validation_weeks(shifted, 42)
    common = set(weeks) & set(shifted)
    assert a & common == b & common


def test_validation_weeks_never_empty():
    assert len(train.select_validation_weeks([202501, 202502, 202503, 202504], 7)) >= 1


def test_dropout_is_reproducible():
    class Cfg:
        CATBOOST_RANDOM_SEED = 42
        OCCUPANCY_DROPOUT_RATE = 0.5
        ROLLING_AVG_DROPOUT_RATE = 0.4
        ROLLING_1H_DROPOUT_RATE = 0.6
        ROLLING_7D_DROPOUT_RATE = 0.3
        HOLIDAY_DROPOUT_RATE = 0.0

    rng = np.random.default_rng(0)
    n = 200
    base = pd.DataFrame(
        {
            c: rng.random(n) * 50
            for c in (
                "park_occupancy_pct", "rolling_avg_7d", "rolling_avg_weekday",
                "rolling_avg_weekend", "avg_wait_last_24h", "avg_wait_same_dow_4w",
                "avg_wait_last_1h",
            )
        }
    )
    base["is_weekend"] = rng.integers(0, 2, n)
    a = train.apply_training_dropout(base.copy(), Cfg, train.logger)
    b = train.apply_training_dropout(base.copy(), Cfg, train.logger)
    pd.testing.assert_frame_equal(a, b)


def _frame(days=80):
    ts = pd.date_range("2026-06-01", periods=days * 24, freq="h", tz="UTC")
    n = len(ts)
    df = pd.DataFrame(
        {
            "attractionId": "r1",
            "parkId": "p1",
            "timestamp": ts,
            "hour": ts.hour,
            "day_of_week": ts.dayofweek,
            "is_weekend": (ts.dayofweek >= 5).astype(int),
            "is_holiday_primary": 0,
            "waitTime": 30.0,
            "park_occupancy_pct": 150.0,
            "wait_time_velocity": 3.0,
            "avg_wait_last_1h": 99.0,
            "wait_lag_1w": 11.0,
            "avg_wait_same_dow_4w": 22.0,
        }
    )
    # Origin-value columns carry their row's position, so a test can see which
    # row a rewritten target took them from.
    for c in train._ORIGIN_VALUE_COLUMNS:
        df[c] = np.arange(n, dtype=float) + 1
    return df


def test_origin_rewrite_reads_origin_row_and_far_row_profiles():
    df = _frame()
    target = df[df["timestamp"] >= df["timestamp"].max() - pd.Timedelta(days=5)]
    profile = pd.Series({("p1", d, h): 80.0 for d in range(7) for h in range(24)})
    profile.index.names = ["parkId", "day_of_week", "hour"]

    out = train.as_known_at_origin(target, df, pd.Timedelta(hours=24), profile)
    assert len(out) == len(target)
    pos = {t: i + 1 for i, t in enumerate(df["timestamp"])}
    expect = [pos[t - pd.Timedelta(hours=24)] for t in target["timestamp"]]
    assert out["rolling_avg_7d"].tolist() == expect
    assert out["avg_wait_last_24h"].tolist() == expect  # <= 24 h: origin's own
    assert (out["avg_wait_same_dow_4w"] == 22.0).all()  # all lags known at origin
    assert (out["avg_wait_last_1h"] == 11.0).all()  # target hour a week earlier
    assert (out["wait_time_velocity"] == 0.0).all()
    assert (out["park_occupancy_pct"] == 80.0).all()
    assert np.allclose(out["hour_occupancy_interaction"], out["hour"] * 0.8)

    far = train.as_known_at_origin(target, df, pd.Timedelta(days=61), profile)
    o7d = far["rolling_avg_7d"]
    assert (far["avg_wait_same_dow_4w"] == o7d).all()  # weekly lags after origin
    assert (far["avg_wait_last_24h"] == o7d).all()
    weekend = far["day_of_week"] >= 5
    assert (
        far["avg_wait_last_1h"]
        == far["rolling_avg_weekend"].where(weekend, far["rolling_avg_weekday"])
    ).all()


def test_origin_rewrite_drops_rows_without_an_origin():
    df = _frame(days=10)
    profile = pd.Series(dtype=float, index=pd.MultiIndex.from_tuples([], names=["parkId", "day_of_week", "hour"]))
    out = train.as_known_at_origin(df, df, pd.Timedelta(days=61), profile)
    assert out.empty



def _weathered(df):
    return df.assign(
        month=df["timestamp"].dt.month,
        temperature_avg=30.0,
        precipitation=5.0,
        weatherCode=61,
        is_raining=1,
        is_park_open=0,
        time_since_park_open_mins=120.0,
        has_extra_hours=1,
    )


def _profile():
    profile = pd.Series({("p1", d, h): 80.0 for d in range(7) for h in range(24)})
    profile.index.names = ["parkId", "day_of_week", "hour"]
    return profile


def test_far_origin_reads_climatology_and_no_schedule():
    df = _weathered(_frame())
    target = df[df["timestamp"] >= df["timestamp"].max() - pd.Timedelta(days=5)]
    clim = train.origin_weather_climatology(
        df.assign(temperature_avg=18.0, precipitation=0.0, weatherCode=1)
    )

    far = train.as_known_at_origin(target, df, pd.Timedelta(days=61), _profile(), clim)
    assert (far["temperature_avg"] == 18.0).all()
    assert (far["precipitation"] == 0.0).all()
    assert (far["is_raining"] == 0).all()
    assert (far["weatherCode"] == 1).all()
    assert (far["is_park_open"] == 1).all()
    assert (far["time_since_park_open_mins"] == 0.0).all()
    assert (far["has_extra_hours"] == 0).all()

    near = train.as_known_at_origin(target, df, pd.Timedelta(hours=24), _profile(), clim)
    assert (near["temperature_avg"] == 30.0).all()  # observed, within forecast
    assert (near["is_park_open"] == 0).all()  # final schedule, within horizon


def test_a_zero_week_lag_falls_back_to_the_profile_like_serving():
    df = _frame().assign(wait_lag_1w=0.0)
    target = df[df["timestamp"] >= df["timestamp"].max() - pd.Timedelta(days=5)]
    out = train.as_known_at_origin(target, df, pd.Timedelta(hours=24), _profile())
    weekend = out["day_of_week"] >= 5
    assert (
        out["avg_wait_last_1h"]
        == out["rolling_avg_weekend"].where(weekend, out["rolling_avg_weekday"])
    ).all()


def test_far_origin_without_climatology_or_reading_reads_weathercode_0():
    # 2026-10-24: a slot missing from the climatology whose own weatherCode is
    # NaN made `.astype(int)` raise and failed every CatBoost training.
    merged = pd.DataFrame({
        "parkId": ["p1", "p2"], "month": [10, 10], "hour": [12, 12],
        "weatherCode": [np.nan, 3.0], "precipitation": [0.0, 1.0],
    })
    clim = pd.DataFrame(
        {"weatherCode": [61], "precipitation": [2.0]},
        index=pd.MultiIndex.from_tuples([("p2", 10, 12)], names=["parkId", "month", "hour"]),
    )
    out = train._neutral_far_weather_and_schedule(merged, pd.Timedelta(days=61), clim)
    assert out["weatherCode"].tolist() == [0, 61]
