#!/usr/bin/env python3
"""
Tests for park-local time features (PAR-452).

Why this exists: `convert_to_local_time` used to write per-park tz-aware values
into ONE pandas column. A pandas datetime column carries a single timezone, so
pandas 2.2.3 converted them straight back to UTC — no warning, no error — and
every feature downstream (`hour`, `day_of_week`, `date_local`, `is_weekend`,
`season`, and the holiday/schedule joins hanging off `date_local`) was computed
on UTC while claiming to be local.

The fix stores the park's wall-clock reading tz-NAIVE. These tests pin both
halves of the resulting contract:

1. the wall-clock features are the park's, not UTC's — with two timezones in one
   frame, a moment just after UTC midnight, and a DST transition that the two
   regions do not share;
2. anything compared against a schedule from the DB uses `timestamp` (an
   instant), never the naive `local_timestamp` — while the schedule is joined on
   the park-LOCAL date.

Every assertion here is red on the pre-fix code except where the docstring of
the test says otherwise.
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from features import (
    add_park_schedule_features,
    add_time_features,
    add_time_since_park_open,
    convert_to_local_time,
)
from predict import apply_schedule_features

# --- Fixtures -------------------------------------------------------------

BERLIN = "park-berlin"
NEW_YORK = "park-new-york"

PARKS_METADATA = pd.DataFrame(
    [
        {"park_id": BERLIN, "timezone": "Europe/Berlin", "country": "DE"},
        {"park_id": NEW_YORK, "timezone": "America/New_York", "country": "US"},
    ]
)


def _frame(rows):
    """rows: list of (parkId, UTC timestamp string)."""
    return pd.DataFrame(
        [
            {"parkId": park, "timestamp": pd.Timestamp(ts, tz="UTC")}
            for park, ts in rows
        ]
    )


# --- 1. Wall-clock features ----------------------------------------------


def test_local_timestamp_is_naive_wall_clock_per_timezone():
    """One column, two timezones: each row keeps its OWN local reading.

    This is the bug itself. Pre-fix both rows read 01:30 because the column was
    tz-aware and pandas converted the New York value back to UTC.
    """
    df = convert_to_local_time(
        _frame([(BERLIN, "2026-07-04 01:30"), (NEW_YORK, "2026-07-04 01:30")]),
        PARKS_METADATA,
    )

    assert df["local_timestamp"].dt.tz is None, "local_timestamp must be naive"
    assert df.loc[0, "local_timestamp"] == pd.Timestamp("2026-07-04 03:30")
    assert df.loc[1, "local_timestamp"] == pd.Timestamp("2026-07-03 21:30")


def test_time_features_just_after_utc_midnight_cross_two_timezones():
    """01:30 UTC is Saturday morning in Berlin and Friday EVENING in New York.

    The New York row is the one that used to land on the wrong calendar day,
    which is what dragged the wrong holiday flag and the wrong schedule with it.
    """
    df = add_time_features(
        _frame([(BERLIN, "2026-07-04 01:30"), (NEW_YORK, "2026-07-04 01:30")]),
        PARKS_METADATA,
    )

    berlin, new_york = df.loc[0], df.loc[1]

    assert berlin["hour"] == 3
    assert berlin["day_of_week"] == 5  # Saturday
    assert berlin["date_local"] == datetime(2026, 7, 4).date()
    assert berlin["is_weekend"] == 1

    assert new_york["hour"] == 21
    assert new_york["day_of_week"] == 4  # Friday — a different day AND weekday
    assert new_york["date_local"] == datetime(2026, 7, 3).date()
    assert new_york["is_weekend"] == 0


def test_dst_transition_is_not_shared_between_the_two_regions():
    """2026-03-08: the US springs forward, the EU does not (it waits until 03-29).

    Pre-fix every `hour` here was the UTC hour, so the offset was the same on
    both sides of the transition and for both parks.
    """
    df = add_time_features(
        _frame(
            [
                (NEW_YORK, "2026-03-08 06:30"),  # 01:30 EST, before the jump
                (NEW_YORK, "2026-03-08 07:30"),  # 03:30 EDT, after the jump
                (BERLIN, "2026-03-08 07:30"),  # 08:30 CET, still winter time
            ]
        ),
        PARKS_METADATA,
    )

    assert df.loc[0, "hour"] == 1  # UTC-5
    assert df.loc[1, "hour"] == 3  # UTC-4 — 02:00 local never exists
    assert df.loc[2, "hour"] == 8  # UTC+1


def test_unknown_park_falls_back_to_utc():
    """A park with no timezone on record keeps the UTC reading, as before."""
    df = convert_to_local_time(
        _frame([("park-without-metadata", "2026-07-04 01:30")]), PARKS_METADATA
    )

    assert df.loc[0, "local_timestamp"] == pd.Timestamp("2026-07-04 01:30")


# --- 2. The schedule join ------------------------------------------------

# 2026-07-03 in New York: the park runs 10:00–23:00 EDT, which is
# 14:00 UTC on the 3rd until 03:00 UTC on the 4th.
US_EVENING_ROW = (NEW_YORK, "2026-07-04 01:30")  # 21:30 EDT, mid-evening
US_OPEN_UTC = pd.Timestamp("2026-07-03 14:00", tz="UTC")
US_CLOSE_UTC = pd.Timestamp("2026-07-04 03:00", tz="UTC")
EXPECTED_MINS_SINCE_OPEN = 11.5 * 60  # 10:00 → 21:30 local


def test_training_path_joins_the_us_evening_row_on_its_local_date():
    """Training: 21:30 EDT belongs to the 3rd's schedule, so the park is open.

    Pre-fix `date_local` read 2026-07-04, the join found no schedule for that
    date, and the row came out `is_park_open = 0` in the middle of the evening.
    """
    df = add_time_features(_frame([US_EVENING_ROW]), PARKS_METADATA)

    schedules = pd.DataFrame(
        [
            {
                "park_id": NEW_YORK,
                "attraction_id": None,
                "date": pd.Timestamp("2026-07-03"),
                "schedule_type": "OPERATING",
                "opening_time": US_OPEN_UTC,
                "closing_time": US_CLOSE_UTC,
            }
        ]
    )

    out = add_park_schedule_features(
        df,
        datetime(2026, 7, 1, tzinfo=timezone.utc),
        datetime(2026, 7, 5, tzinfo=timezone.utc),
        cached_schedules_df=schedules,
    )

    assert out.loc[0, "is_park_open"] == 1
    assert out.loc[0, "time_since_park_open_mins"] == pytest.approx(
        EXPECTED_MINS_SINCE_OPEN
    )


def test_inference_path_joins_the_us_evening_row_on_its_local_date():
    """Inference: same row, same verdict, through predict.py's own join.

    The park is CLOSED on the 4th and open until 23:00 on the 3rd. That second
    row is what makes this a test: inference defaults to `is_park_open = 1`
    when it finds no schedule, so joining on the wrong date would otherwise
    produce the right answer for the wrong reason. With the UTC date the row
    picks up the 4th's closure and reads CLOSED at 21:30 on a running evening.
    """
    df = add_time_features(_frame([US_EVENING_ROW]), PARKS_METADATA)

    schedules = pd.DataFrame(
        [
            {
                "parkId": NEW_YORK,
                "attractionId": None,
                "date": pd.Timestamp("2026-07-03"),
                "scheduleType": "OPERATING",
                "openingTime": US_OPEN_UTC,
                "closingTime": US_CLOSE_UTC,
            },
            {
                "parkId": NEW_YORK,
                "attractionId": None,
                "date": pd.Timestamp("2026-07-04"),
                "scheduleType": "CLOSED",
                "openingTime": None,
                "closingTime": None,
            },
        ]
    )

    out = apply_schedule_features(df, schedules)

    assert out.loc[0, "is_park_open"] == 1
    assert out.loc[0, "status"] == "OPERATING"


def test_schedule_join_still_closes_a_row_outside_opening_hours():
    """The counter-check: 04:00 UTC is 00:00 EDT, after the 23:00 close.

    Without it, a join that simply said "open" would pass the test above.
    """
    df = add_time_features(_frame([(NEW_YORK, "2026-07-04 04:00")]), PARKS_METADATA)

    schedules = pd.DataFrame(
        [
            {
                "park_id": NEW_YORK,
                "attraction_id": None,
                "date": pd.Timestamp("2026-07-03"),
                "schedule_type": "OPERATING",
                "opening_time": US_OPEN_UTC,
                "closing_time": US_CLOSE_UTC,
            },
            {
                "park_id": NEW_YORK,
                "attraction_id": None,
                "date": pd.Timestamp("2026-07-04"),
                "schedule_type": "OPERATING",
                "opening_time": pd.Timestamp("2026-07-04 14:00", tz="UTC"),
                "closing_time": pd.Timestamp("2026-07-05 03:00", tz="UTC"),
            },
        ]
    )

    out = add_park_schedule_features(
        df,
        datetime(2026, 7, 1, tzinfo=timezone.utc),
        datetime(2026, 7, 5, tzinfo=timezone.utc),
        cached_schedules_df=schedules,
    )

    assert out.loc[0, "is_park_open"] == 0


def test_time_since_park_open_compares_instants_not_wall_clocks():
    """`parkOpeningTimes` carries an ISO instant, so the difference is in UTC.

    This one is GREEN on the pre-fix code too — there `local_timestamp` happened
    to be UTC, so the subtraction worked by accident. It is here to keep the
    next change from reaching for `local_timestamp`, which is now naive and
    would silently add the park's offset to every reading.
    """
    df = add_time_features(_frame([US_EVENING_ROW]), PARKS_METADATA)

    out = add_time_since_park_open(
        df, {"parkOpeningTimes": {NEW_YORK: "2026-07-03T14:00:00.000Z"}}
    )

    assert out.loc[0, "time_since_park_open_mins"] == pytest.approx(
        EXPECTED_MINS_SINCE_OPEN
    )
