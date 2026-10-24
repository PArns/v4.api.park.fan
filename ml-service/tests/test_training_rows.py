"""PAR-1052: training (and serving) read OPERATING rows only, drop frozen
readings outside the published window, keep fresh ones, admit measured days,
and no longer set is_park_open from the label.

    cd ml-service && python3 -m pytest tests/test_training_rows.py -q
"""

import os
import sys
from datetime import datetime, timezone

import pandas as pd
import pytest

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
    assert "pdo.measured_operation" in sql
    assert "pdo.measured_operation" in _flat(predict._SERVING_QUEUE_FILTER)
    # ... but only through a CLOSED / UNKNOWN / missing schedule row, never the
    # EXTRA_HOURS copy the `se` join makes beside an OPERATING row.
    measured = _flat(db.MEASURED_DAY_SCHEDULE_SQL)
    assert measured in sql
    assert measured in _flat(predict._SERVING_QUEUE_FILTER)
    assert _flat(db.TRAINING_DAY_FILTER_SQL) in sql


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


# --- The day filter against a real Postgres (set ML_TEST_DATABASE_URL) ------
#
#     docker run -d --name pg -e POSTGRES_PASSWORD=t -p 5433:5432 postgres:16-alpine
#     ML_TEST_DATABASE_URL=postgresql://postgres:t@localhost:5433/postgres \
#         python3 -m pytest tests/test_training_rows.py -q

PG_URL = os.environ.get("ML_TEST_DATABASE_URL")

_SCHEMA = """
CREATE TEMP TABLE queue_data ("attractionId" uuid, timestamp timestamptz,
    "waitTime" int, status text, "queueType" text, is_heartbeat bool,
    "lastUpdated" timestamptz);
CREATE TEMP TABLE schedule_entries (id serial, "parkId" uuid, date date,
    "attractionId" uuid, "scheduleType" text, "openingTime" timestamptz,
    "closingTime" timestamptz);
CREATE TEMP TABLE park_day_operations (park_id uuid, day date,
    measured_operation bool);
CREATE TEMP TABLE odh ("parkId" uuid, day date);
"""

# 10-31: OPERATING 10-18 plus EXTRA_HOURS 18-22, measured to have run.
# 11-01: CLOSED, measured to have run. 11-02: CLOSED, not measured.
_ROWS = f"""
INSERT INTO schedule_entries ("parkId", date, "scheduleType", "openingTime", "closingTime") VALUES
  ('{PARK}', '2026-10-31', 'OPERATING',   '2026-10-31 10:00Z', '2026-10-31 18:00Z'),
  ('{PARK}', '2026-10-31', 'EXTRA_HOURS', '2026-10-31 18:00Z', '2026-10-31 22:00Z'),
  ('{PARK}', '2026-11-01', 'CLOSED', NULL, NULL),
  ('{PARK}', '2026-11-02', 'CLOSED', NULL, NULL);
INSERT INTO park_day_operations VALUES
  ('{PARK}', '2026-10-31', true), ('{PARK}', '2026-11-01', true),
  ('{PARK}', '2026-11-02', false);
INSERT INTO queue_data VALUES
  ('{RIDE}', '2026-10-31 12:00Z', 30, 'OPERATING', 'STANDBY', false, '2026-10-31 11:58Z'),
  ('{RIDE}', '2026-10-31 23:30Z', 30, 'OPERATING', 'STANDBY', false, '2026-10-31 18:00Z'),
  ('{RIDE}', '2026-11-01 12:00Z', 20, 'OPERATING', 'STANDBY', false, '2026-11-01 11:58Z'),
  ('{RIDE}', '2026-11-02 12:00Z', 20, 'OPERATING', 'STANDBY', false, '2026-11-02 11:58Z');
"""

_JOINS = f"""
FROM queue_data qd
LEFT JOIN schedule_entries se
    ON se."parkId" = '{PARK}' AND se.date = DATE(qd.timestamp AT TIME ZONE 'UTC')
    AND se."attractionId" IS NULL
LEFT JOIN odh ON odh."parkId" = '{PARK}' AND odh.day = DATE(qd.timestamp AT TIME ZONE 'UTC')
LEFT JOIN park_day_operations pdo
    ON pdo.park_id = '{PARK}' AND pdo.day = DATE(qd.timestamp AT TIME ZONE 'UTC')
"""

EXPECTED = ["2026-10-31 12:00", "2026-11-01 12:00"]


@pytest.mark.skipif(not PG_URL, reason="needs ML_TEST_DATABASE_URL")
def test_each_reading_once_on_an_operating_plus_extra_hours_day():
    """The EXTRA_HOURS copy must not re-admit the 12:00 reading (twice) or the
    stale 23:30 one (out of the OPERATING window), on either path."""
    from sqlalchemy import create_engine, text

    with create_engine(PG_URL).connect() as conn:
        for stmt in (_SCHEMA + _ROWS).split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
        fmt = "to_char(qd.timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI')"
        training = conn.execute(text(
            f"SELECT {fmt} {_JOINS} WHERE {db.FROZEN_OUT_OF_WINDOW_SQL}"
            f" AND {db.TRAINING_DAY_FILTER_SQL} ORDER BY 1"
        )).scalars().all()
        serving = conn.execute(
            text(f"SELECT {fmt} {_JOINS} WHERE {predict._SERVING_QUEUE_FILTER} ORDER BY 1"),
            {"attraction_ids": "{" + RIDE + "}"},
        ).scalars().all()
    assert training == EXPECTED
    assert serving == EXPECTED
