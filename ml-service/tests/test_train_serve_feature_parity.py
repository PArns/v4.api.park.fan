"""PAR-1030 / PAR-1032: features computed through the training path and the
serving path on the same fixture must agree.

    cd ml-service && python3 -m pytest tests/test_train_serve_feature_parity.py -q
"""

import os
import sys
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import db
import features
import predict

PARK = "11111111-1111-1111-1111-111111111111"
RIDE = "22222222-2222-2222-2222-222222222222"
TZ = "Europe/Berlin"

PARKS_METADATA = pd.DataFrame(
    [
        {
            "park_id": PARK,
            "name": "Fixture Park",
            "country": "DE",
            "region_code": "BW",
            "timezone": TZ,
            "influencingRegions": [],
            "influenceRadiusKm": None,
            "latitude": None,
            "longitude": None,
            "attraction_count": 1,
        }
    ]
)

# Thursday 2026-07-02 is a (fixture) public holiday, so Friday the 3rd is a
# bridge day.
HOLIDAYS = pd.DataFrame(
    [
        {
            "date": pd.Timestamp("2026-07-02"),
            "country": "DE",
            "region": None,
            "holiday_type": "public",
            "is_nationwide": True,
            "name": "Fixture Day",
        }
    ]
)

# 09:00-18:00 local (07:00-16:00 UTC in July) every day.
SCHEDULE_DAYS = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04"]


def _schedules_camel():
    return pd.DataFrame(
        [
            {
                "parkId": PARK,
                "attractionId": None,
                "date": pd.Timestamp(d),
                "scheduleType": "OPERATING",
                "openingTime": pd.Timestamp(f"{d} 07:00", tz="UTC"),
                "closingTime": pd.Timestamp(f"{d} 16:00", tz="UTC"),
            }
            for d in SCHEDULE_DAYS
        ]
    )


def _schedules_snake():
    return _schedules_camel().rename(
        columns={
            "parkId": "park_id",
            "attractionId": "attraction_id",
            "scheduleType": "schedule_type",
            "openingTime": "opening_time",
            "closingTime": "closing_time",
        }
    )


# --- time_since_park_open_mins / is_bridge_day ------------------------------

ROWS_UTC = [
    "2026-07-02 06:00",  # before the 2nd's opening
    "2026-07-02 10:00",  # 3 h after the 2nd's opening
    "2026-07-03 12:00",  # 5 h after the 3rd's opening (not the 2nd's)
    "2026-07-04 08:30",  # 1.5 h after the 4th's opening
]


def _frame():
    df = pd.DataFrame(
        {
            "parkId": PARK,
            "attractionId": RIDE,
            "timestamp": [pd.Timestamp(t, tz="UTC") for t in ROWS_UTC],
        }
    )
    return features.convert_to_local_time(df, PARKS_METADATA)


def test_time_since_park_open_is_per_row_and_identical_on_both_paths():
    train = features.add_park_schedule_features(
        _frame(),
        datetime(2026, 7, 1, tzinfo=timezone.utc),
        datetime(2026, 7, 5, tzinfo=timezone.utc),
        cached_schedules_df=_schedules_snake(),
    )
    serve = predict.apply_schedule_features(_frame(), _schedules_camel())

    expected = [0.0, 180.0, 300.0, 90.0]
    assert train["time_since_park_open_mins"].tolist() == pytest.approx(expected)
    assert serve["time_since_park_open_mins"].tolist() == pytest.approx(expected)


def test_is_park_open_outside_hours_on_an_operating_day_agrees():
    """PAR-1045: before opening and after close both paths read 0."""

    def frame():
        rows = ["2026-07-02 06:00", "2026-07-02 10:00", "2026-07-02 17:30"]
        df = pd.DataFrame(
            {
                "parkId": PARK,
                "attractionId": RIDE,
                "timestamp": [pd.Timestamp(t, tz="UTC") for t in rows],
            }
        )
        return features.convert_to_local_time(df, PARKS_METADATA)

    train = features.add_park_schedule_features(
        frame(),
        datetime(2026, 7, 1, tzinfo=timezone.utc),
        datetime(2026, 7, 5, tzinfo=timezone.utc),
        cached_schedules_df=_schedules_snake(),
    )
    serve = predict.apply_schedule_features(frame(), _schedules_camel())
    assert train["is_park_open"].tolist() == [0, 1, 0]
    assert serve["is_park_open"].tolist() == [0, 1, 0]


def test_is_bridge_day_is_per_date_and_ignores_todays_flag():
    df = _frame()
    df["date_local"] = df["local_timestamp"].dt.date
    out = features.add_bridge_day_feature(
        df,
        PARKS_METADATA,
        pd.Timestamp("2026-07-01"),
        pd.Timestamp("2026-07-05"),
        feature_context={"isBridgeDay": {PARK: True}},  # the old "today" flag
        cached_holidays_df=HOLIDAYS,
    )
    assert out["is_bridge_day"].tolist() == [0, 0, 1, 0]


# --- wait_time_velocity -----------------------------------------------------


def test_live_velocity_matches_training_definition():
    """Training: rolling(6) mean of hourly diffs, shifted by one row. Serving
    with the live wait as the latest hour must reproduce it."""
    waits = [10, 15, 30, 25, 40, 55, 50, 70, 65, 80]
    df = pd.DataFrame(
        {
            "attractionId": RIDE,
            "timestamp": pd.date_range("2026-07-02 07:00", periods=len(waits), freq="h", tz="UTC"),
            "waitTime": waits,
        }
    )
    trained = features.add_historical_features(df)["wait_time_velocity"].tolist()

    for i in range(2, len(waits)):
        served = predict.live_wait_velocity(waits[i - 1], waits[: i - 1])
        assert served == pytest.approx(trained[i]), i


def test_live_velocity_without_history_is_zero():
    assert predict.live_wait_velocity(40, []) == 0.0


# --- rolling weekday/weekend on the park-local weekday ---------------------


def test_rolling_weekday_split_uses_the_park_local_weekday():
    """Friday 23:30 UTC is Saturday 01:30 in Berlin: a weekend reading."""
    df = pd.DataFrame(
        {
            "parkId": PARK,
            "attractionId": RIDE,
            "timestamp": [
                pd.Timestamp("2026-07-03 23:30", tz="UTC"),
                pd.Timestamp("2026-07-04 10:00", tz="UTC"),
            ],
            "waitTime": [50.0, 10.0],
        }
    )
    df = features.convert_to_local_time(df, PARKS_METADATA)
    out = features.add_historical_features(df)
    # The second row's 7-day weekend window holds the first (local Saturday).
    assert out["rolling_avg_weekend"].tolist()[1] == pytest.approx(50.0)
    assert out["rolling_avg_weekday"].tolist()[1] == 0  # empty window -> 0


# --- park_occupancy_pct leave-one-out ---------------------------------------


def test_training_occupancy_leaves_the_target_out(monkeypatch):
    monkeypatch.setattr(features, "fetch_attraction_baselines", lambda: pd.DataFrame())
    ts = pd.Timestamp("2026-07-02 10:00", tz="UTC")
    df = pd.DataFrame(
        {
            "parkId": [PARK, PARK, PARK],
            "attractionId": ["a", "b", "c"],
            "timestamp": [ts, ts, ts],
            "waitTime": [10.0, 20.0, 90.0],
        }
    )
    out = features.add_park_occupancy_feature(df)
    baseline = df["waitTime"].mean()  # no stored P50s: each ride's median is its wait
    expected = [(20 + 90) / 2, (10 + 90) / 2, (10 + 20) / 2]
    assert out["park_occupancy_pct"].tolist() == pytest.approx(
        [min(200.0, e / baseline * 100) for e in expected]
    )


def test_training_occupancy_alone_at_its_timestamp_is_neutral(monkeypatch):
    monkeypatch.setattr(features, "fetch_attraction_baselines", lambda: pd.DataFrame())
    df = pd.DataFrame(
        {
            "parkId": [PARK],
            "attractionId": ["a"],
            "timestamp": [pd.Timestamp("2026-07-02 10:00", tz="UTC")],
            "waitTime": [80.0],
        }
    )
    assert features.add_park_occupancy_feature(df)["park_occupancy_pct"].tolist() == [100.0]


# --- interactions recomputed after dropout ---------------------------------


def test_interactions_follow_the_dropped_out_occupancy():
    import train

    class _Cfg:
        OCCUPANCY_DROPOUT_RATE = 1.0
        ROLLING_AVG_DROPOUT_RATE = 0.0
        ROLLING_1H_DROPOUT_RATE = 0.0
        ROLLING_7D_DROPOUT_RATE = 0.0
        HOLIDAY_DROPOUT_RATE = 0.0
        CATBOOST_RANDOM_SEED = 42  # read by apply_training_dropout after PAR-1040

    class _Log:
        def info(self, *_a, **_k):
            pass

    df = pd.DataFrame(
        {
            "hour": [14],
            "is_weekend": [0],
            "is_holiday_primary": [1],
            "park_occupancy_pct": [150.0],
            "rolling_avg_7d": [40.0],
            "rolling_avg_weekday": [20.0],
            "rolling_avg_weekend": [60.0],
        }
    )
    df = features.add_interaction_features(df)
    out = train.apply_training_dropout(df, _Cfg(), _Log())

    occ = out["park_occupancy_pct"].iloc[0]
    assert occ == pytest.approx(75.0)  # 20/40 * 150
    assert out["holiday_occupancy_interaction"].iloc[0] == pytest.approx(occ)
    assert out["hour_occupancy_interaction"].iloc[0] == pytest.approx(14 * occ / 100)


# --- create_prediction_features end to end ---------------------------------

BASE = datetime(2026, 7, 2, 8, 0, tzinfo=timezone.utc)  # Thu 10:00 CEST
LIVE_WAIT = 40


class _Result:
    def __init__(self, frame):
        self._frame = frame

    def fetchall(self):
        return list(self._frame.itertuples(index=False, name=None))

    def keys(self):
        return list(self._frame.columns)


class _Db:
    def execute(self, query, params=None):
        if "schedule_entries" in str(query):
            return _Result(_schedules_camel())
        return _Result(pd.DataFrame())

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _history():
    """Hourly local history for ten days before BASE: wait = 100 + local hour."""
    rows = []
    start = pd.Timestamp(BASE).tz_convert(TZ).floor("h") - pd.Timedelta(days=10)
    t = start
    end = pd.Timestamp(BASE).tz_convert(TZ).floor("h")
    while t < end:
        if 9 <= t.hour <= 18:
            rows.append(
                {
                    "attractionId": RIDE,
                    "attractionName": "Fixture Coaster",
                    "date": pd.Timestamp(t.date()),
                    "hour": t.hour,
                    "day_of_week": (t.dayofweek + 1) % 7,
                    "avg_wait": 100.0 + t.hour,
                    "data_points": 12,
                }
            )
        t += pd.Timedelta(hours=1)
    return pd.DataFrame(rows)


def _patch_serving(monkeypatch):
    monkeypatch.setattr(predict, "fetch_parks_metadata", lambda: PARKS_METADATA)
    monkeypatch.setattr(predict, "fetch_holidays", lambda *a, **k: HOLIDAYS.copy())
    monkeypatch.setattr(
        predict, "fetch_school_holiday_coverage", lambda *a, **k: HOLIDAYS.copy()
    )
    monkeypatch.setattr(predict, "get_db", lambda: _Db())
    monkeypatch.setattr(db, "fetch_historical_park_occupancy", lambda *a, **k: {})
    monkeypatch.setattr(predict, "fetch_recent_wait_times", lambda *a, **k: _history())
    monkeypatch.setattr(
        predict, "fetch_recent_rolling_stats", lambda *a, **k: pd.DataFrame()
    )
    import attraction_features

    monkeypatch.setattr(attraction_features, "get_db", lambda: _Db())


@pytest.fixture
def served(monkeypatch):
    _patch_serving(monkeypatch)
    timestamps = [BASE + timedelta(hours=h) for h in range(1, 49)]
    df = predict.create_prediction_features(
        [RIDE],
        [PARK],
        timestamps,
        BASE,
        current_wait_times={RIDE: LIVE_WAIT},
        feature_context={"parkOccupancy": {PARK: 100.0}, "baseTime": BASE},
    )
    df["lead_h"] = (
        pd.to_datetime(df["timestamp"], utc=True) - pd.Timestamp(BASE)
    ) / pd.Timedelta(hours=1)
    return df


def test_live_wait_only_reaches_rows_within_the_live_lead(served):
    near = served[served["lead_h"] <= 3]
    far = served[served["lead_h"] > 3]
    assert (near["avg_wait_last_1h"] == LIVE_WAIT).all()
    assert not (far["avg_wait_last_1h"] == LIVE_WAIT).any()
    # Beyond the lead: the same local hour a week earlier (fixture: 100 + hour)
    open_far = far[far["local_timestamp"].dt.hour.between(9, 18)]
    assert (
        open_far["avg_wait_last_1h"] == 100.0 + open_far["local_timestamp"].dt.hour
    ).all()


def test_served_velocity_is_training_definition_within_the_lead(served):
    hist = _history()
    earlier = hist["avg_wait"].tolist()  # all strictly before BASE's local hour
    expected = predict.live_wait_velocity(LIVE_WAIT, earlier)
    near = served[served["lead_h"] <= 3]
    far = served[served["lead_h"] > 3]
    assert near["wait_time_velocity"].tolist() == pytest.approx([expected] * len(near))
    assert (far["wait_time_velocity"] == 0).all()


def test_served_time_since_open_and_bridge_day_are_per_row(served):
    row = served[served["timestamp"] == pd.Timestamp("2026-07-03 12:00", tz="UTC")]
    assert row["time_since_park_open_mins"].iloc[0] == pytest.approx(300.0)
    by_date = served.groupby(served["local_timestamp"].dt.date)["is_bridge_day"].max()
    assert by_date.to_dict() == {
        date(2026, 7, 2): 0,
        date(2026, 7, 3): 1,
        date(2026, 7, 4): 0,
    }
    assert np.isfinite(served["time_since_park_open_mins"]).all()


def test_one_hour_dropout_also_zeroes_velocity_like_serving():
    import train

    class _Cfg:
        OCCUPANCY_DROPOUT_RATE = 0.0
        ROLLING_AVG_DROPOUT_RATE = 0.0
        ROLLING_1H_DROPOUT_RATE = 1.0
        ROLLING_7D_DROPOUT_RATE = 0.0
        HOLIDAY_DROPOUT_RATE = 0.0
        CATBOOST_RANDOM_SEED = 42

    class _Log:
        def info(self, *_a, **_k):
            pass

    df = pd.DataFrame(
        {
            "hour": [14],
            "is_weekend": [0],
            "avg_wait_last_1h": [55.0],
            "wait_time_velocity": [4.5],
            "rolling_avg_weekday": [20.0],
            "rolling_avg_weekend": [60.0],
        }
    )
    out = train.apply_training_dropout(df, _Cfg(), _Log())
    assert out["avg_wait_last_1h"].iloc[0] == 20.0
    assert out["wait_time_velocity"].iloc[0] == 0.0


def test_served_same_dow_beyond_28_days_reads_last_year_only_where_observed(
    monkeypatch,
):
    """PAR-1045: the observed slot 52 weeks back, else rolling_avg_7d."""
    _patch_serving(monkeypatch)
    far = [BASE + timedelta(days=40, hours=h) for h in (4, 5)]  # 14:00, 15:00 CEST
    year_ago = pd.Timestamp(far[0]).tz_convert(TZ).tz_localize(None) - features.LAST_YEAR_LAG
    asked = []

    def slots(keys, end_time):
        asked.extend(keys)
        return {(RIDE, year_ago.date(), 14): 77.0}

    monkeypatch.setattr(predict, "fetch_last_year_slots", slots)
    df = predict.create_prediction_features(
        [RIDE], [PARK], far, BASE, feature_context={"baseTime": BASE}
    )
    assert (RIDE, year_ago.date(), 14) in asked
    assert df["avg_wait_same_dow_4w"].iloc[0] == 77.0
    assert df["avg_wait_same_dow_4w"].iloc[1] == df["rolling_avg_7d"].iloc[1]
