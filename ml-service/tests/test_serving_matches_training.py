"""PAR-943: serving reads the rows training learnt from, rates crowds only
against a real baseline, and keeps the slots of a day that runs past midnight.

    cd ml-service && python3 -m pytest tests/test_serving_matches_training.py -q
"""

import os
import sys
from contextlib import contextmanager
from datetime import date

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import predict  # noqa: E402
import schedule_filter  # noqa: E402


def test_serving_rows_include_unknown_schedule_days_like_training():
    """Training keeps no-schedule and UNKNOWN days (db.fetch_training_data);
    serving dropped UNKNOWN days, so the lags fed at inference came from a
    smaller population than the one the model learnt on."""
    sql = " ".join(predict._SERVING_QUEUE_FILTER.split())
    assert "se.id IS NULL OR se.\"scheduleType\" IN ('OPERATING', 'UNKNOWN')" in sql
    assert "qd.status = 'OPERATING'" in sql
    assert "OR qd.\"waitTime\" > 0" not in sql


def test_no_baseline_rates_unknown_not_moderate():
    assert predict.crowd_level_for(40, None, None) == ("unknown", 0.0)
    assert predict.crowd_level_for(40, 0, 0) == ("unknown", 0.0)


def test_a_real_baseline_still_rates():
    # typical-day-peak first, P50 only when it is absent
    assert predict.crowd_level_for(40, 40, 10) == ("moderate", 40)
    assert predict.crowd_level_for(40, None, 20) == ("very_high", 20)


# --- schedule_filter: a day that closes after midnight -----------------------

PARK = "11111111-1111-1111-1111-111111111111"


@contextmanager
def _db(rows):
    class _Result:
        def fetchall(self):
            return rows

    class _Session:
        def execute(self, query, params):
            _Session.params = params
            return _Result()

    yield _Session()


def test_hourly_filter_keeps_slots_after_midnight_inside_yesterdays_window(
    monkeypatch,
):
    """10:00 -> 01:00 (UTC park): the 00:00-00:45 slots carry the NEXT local
    date, whose own window opens at 10:00 - they used to be dropped."""
    monkeypatch.setattr(
        schedule_filter,
        "fetch_parks_metadata",
        lambda: pd.DataFrame([{"park_id": PARK, "timezone": "UTC"}]),
    )
    rows = [
        (
            date(2026, 7, 3),
            "OPERATING",
            pd.Timestamp("2026-07-03 10:00", tz="UTC"),
            pd.Timestamp("2026-07-04 01:00", tz="UTC"),
        ),
        (
            date(2026, 7, 4),
            "OPERATING",
            pd.Timestamp("2026-07-04 10:00", tz="UTC"),
            pd.Timestamp("2026-07-05 01:00", tz="UTC"),
        ),
    ]
    monkeypatch.setattr(schedule_filter, "get_db", lambda: _db(rows))

    times = ["2026-07-03T23:45:00Z", "2026-07-04T00:30:00Z", "2026-07-04T03:00:00Z"]
    preds = [{"parkId": PARK, "predictedTime": t} for t in times]

    kept = schedule_filter.filter_predictions_by_schedule(preds, [PARK], "hourly")

    # 03:00 is after the 01:00 close and before the 10:00 opening: still dropped.
    assert [p["predictedTime"] for p in kept] == times[:2]
