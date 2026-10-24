"""PAR-1052: training (and serving) read OPERATING rows only, drop frozen
readings outside the published window, keep fresh ones, admit measured days,
and no longer set is_park_open from the label.

    cd ml-service && python3 -m pytest tests/test_training_rows.py -q
"""

import os
import sys
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import db
import features
import predict

PARK = "11111111-1111-1111-1111-111111111111"
RIDE = "22222222-2222-2222-2222-222222222222"


def _flat(sql: str) -> str:
    return " ".join(sql.split())


def test_training_reads_the_label_from_operating_rows_only():
    sql = _flat(db.TRAINING_DATA_SQL)
    assert 'OR qd."waitTime" > 0' not in sql
    assert "FILTER (WHERE qd.status = 'OPERATING') as \"waitTime\"" in sql
    assert 'WHERE hq."waitTime" IS NOT NULL' in sql


def test_frozen_rows_outside_the_window_are_dropped_on_both_paths():
    frozen = _flat(db.FROZEN_OUT_OF_WINDOW_SQL)
    assert "INTERVAL '60 minutes'" in frozen  # the window margin
    assert "qd.\"lastUpdated\" < qd.timestamp - INTERVAL '120 minutes'" in frozen
    assert 'qd."lastUpdated" IS NULL' in frozen
    assert frozen.startswith("(NOT (")
    assert frozen in _flat(db.TRAINING_DATA_SQL)
    assert frozen in _flat(predict._SERVING_QUEUE_FILTER)


def test_measured_operation_days_are_admitted_on_both_paths():
    sql = _flat(db.TRAINING_DATA_SQL)
    assert "LEFT JOIN park_day_operations pdo" in sql
    assert "OR pdo.measured_operation" in sql
    assert "pdo.measured_operation" in _flat(predict._SERVING_QUEUE_FILTER)


def test_downtime_survives_the_operating_only_label():
    """DOWN samples still reach the downtime features, from the earlier hours."""
    assert "AS downtime_count_before" in _flat(db.TRAINING_DATA_SQL)
    df = pd.DataFrame({"attractionId": [RIDE] * 2, "downtime_count_before": [0, 3]})
    out = features.add_downtime_features(df)
    assert out["downtime_minutes_today"].tolist() == [0.0, 15.0]
    assert out["had_downtime_today"].tolist() == [0, 1]


PARKS_METADATA = pd.DataFrame([{"park_id": PARK, "timezone": "UTC", "country": "DE"}])


def _frame(rows, **extra):
    df = pd.DataFrame(
        {
            "parkId": PARK,
            "attractionId": RIDE,
            "timestamp": [pd.Timestamp(t, tz="UTC") for t in rows],
            **extra,
        }
    )
    return features.convert_to_local_time(df, PARKS_METADATA)


def _schedules():
    return pd.DataFrame(
        {
            "park_id": [PARK],
            "attraction_id": [None],
            "date": [pd.Timestamp("2026-07-02")],
            "schedule_type": ["OPERATING"],
            "opening_time": [pd.Timestamp("2026-07-02 09:00", tz="UTC")],
            "closing_time": [pd.Timestamp("2026-07-02 17:00", tz="UTC")],
        }
    )


def _train(df):
    return features.add_park_schedule_features(
        df,
        datetime(2026, 7, 1, tzinfo=timezone.utc),
        datetime(2026, 7, 5, tzinfo=timezone.utc),
        cached_schedules_df=_schedules(),
    )


def test_is_park_open_is_not_read_from_the_label():
    """A 40-minute wait after close used to set is_park_open = 1; serving
    cannot see the label and reads 0 there."""
    out = _train(_frame(["2026-07-02 06:00", "2026-07-02 17:30"], waitTime=[40.0, 40.0]))
    assert out["is_park_open"].tolist() == [0, 0]


def test_a_fresh_after_hours_reading_marks_the_park_open():
    out = _train(
        _frame(
            ["2026-07-02 17:30", "2026-07-02 20:00"],
            waitTime=[40.0, 40.0],
            after_hours_reading=[0, 1],
        )
    )
    assert out["is_park_open"].tolist() == [0, 1]


def test_a_day_without_a_window_reads_open_like_serving():
    out = _train(_frame(["2026-07-03 12:00"], waitTime=[8.0]))
    assert out["is_park_open"].tolist() == [1]
