"""
Park-local time in the CatBoost inference path (PAR-818).

Three places read a park's wall clock off a UTC value:

1. The daily "peak hours" (DAILY_PEAK_HOURS = 12,14,16) were set on the UTC
   base_time, so Los Angeles was asked about 05/07/09 local and Tokyo about
   21/23/01 — and the per-day maximum was grouped by the UTC date.
2. The historical occupancy profile is keyed on the park-LOCAL (DOW, hour), but
   was looked up with the UTC weekday and hour.
3. The occupancy profile's cache key carried the request's base_time to the
   microsecond, so the 1-hour cache never hit.

Every test below is red on the pre-fix code.
"""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

import db
import predict
from features import _historical_occupancy_values, convert_to_local_time

LA = "park-la"
TOKYO = "park-tokyo"
BERLIN = "park-berlin"

PARKS_METADATA = pd.DataFrame(
    [
        {"park_id": LA, "timezone": "America/Los_Angeles", "country": "US"},
        {"park_id": TOKYO, "timezone": "Asia/Tokyo", "country": "JP"},
        {"park_id": BERLIN, "timezone": "Europe/Berlin", "country": "DE"},
    ]
)


def _local(ts_utc, tz):
    return pd.Timestamp(ts_utc).tz_convert(tz)


# --- 1. Daily peak hours on the park's clock -------------------------------


def test_daily_peak_hours_are_park_local_in_los_angeles_and_tokyo():
    base = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)
    for tz in ("America/Los_Angeles", "Asia/Tokyo", "Europe/Berlin"):
        ts = predict.generate_future_timestamps(base, "daily", tz)
        first_day = [_local(t, tz) for t in ts[:3]]
        assert [t.hour for t in first_day] == [12, 14, 16], tz
        # Day 1 is the park's TOMORROW, whatever the UTC date says.
        local_today = pd.Timestamp(base).tz_convert(tz).date()
        assert {t.date() for t in first_day} == {
            local_today + pd.Timedelta(days=1)
        }, tz


def test_daily_peak_hours_survive_a_dst_change():
    """Los Angeles leaves DST on 2026-11-01: 14:00 local is 21:00 UTC before, 22:00 after."""
    base = datetime(2026, 10, 30, 18, 0, tzinfo=timezone.utc)  # 11:00 PDT
    ts = predict.generate_future_timestamps(base, "daily", "America/Los_Angeles")
    by_day = {}
    for t in ts[:9]:
        local = _local(t, "America/Los_Angeles")
        by_day.setdefault(local.date().isoformat(), []).append(local.hour)
    assert by_day == {
        "2026-10-31": [12, 14, 16],
        "2026-11-01": [12, 14, 16],
        "2026-11-02": [12, 14, 16],
    }


def test_daily_anchor_is_local_noon_and_keeps_the_utc_date():
    # Los Angeles in winter: noon PST = 20:00 UTC, same date.
    assert predict.daily_anchor_time("2026-01-16", "America/Los_Angeles") == (
        "2026-01-16T20:00:00+00:00"
    )
    # Tokyo: noon JST = 03:00 UTC, same date.
    assert predict.daily_anchor_time("2026-01-16", "Asia/Tokyo") == (
        "2026-01-16T03:00:00+00:00"
    )


def test_daily_anchor_refuses_an_offset_of_twelve_hours_or_more():
    import pytest

    # Auckland in January is UTC+13: its noon is the previous UTC date.
    with pytest.raises(AssertionError):
        predict.daily_anchor_time("2026-01-16", "Pacific/Auckland")


class _FakeModel:
    version = "test"

    def predict_quantiles(self, X):
        # The prediction IS the row's local hour, so the max is always 16:00.
        return {0.5: X["hour"].to_numpy(dtype=float) * 3}


def test_daily_collapse_groups_by_the_park_local_day(monkeypatch):
    """LA in January: the 16:00 slot is 00:00 UTC of the NEXT day.

    Pre-fix the collapse keyed on predictedTime[:10] (UTC date), so that slot
    was filed under tomorrow. Each local day must collapse to exactly one row,
    published at local noon of THAT day.
    """
    monkeypatch.setattr(predict.settings, "DAILY_PREDICTIONS", 3)
    monkeypatch.setattr(predict, "fetch_parks_metadata", lambda: PARKS_METADATA)

    def fake_features(attraction_ids, park_ids, timestamps, base_time, *a, **kw):
        tbp = kw.get("timestamps_by_park") or {}
        rows = [
            {"attractionId": aid, "parkId": pid, "timestamp": ts}
            for aid, pid in zip(attraction_ids, park_ids)
            for ts in tbp.get(pid, timestamps)
        ]
        df = convert_to_local_time(pd.DataFrame(rows), PARKS_METADATA)
        df["hour"] = df["local_timestamp"].dt.hour
        df["date_local"] = df["local_timestamp"].dt.date
        df["status"] = "OPERATING"
        return df

    monkeypatch.setattr(predict, "create_prediction_features", fake_features)

    base = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)  # 02:00 PST
    results = predict.predict_wait_times(
        _FakeModel(), ["ride-la"], [LA], "daily", base_time=base
    )

    assert sorted(r["predictedTime"] for r in results) == [
        "2026-01-16T20:00:00+00:00",
        "2026-01-17T20:00:00+00:00",
        "2026-01-18T20:00:00+00:00",
    ]
    # The 16:00 slot won every day (3 * 16 = 48 → rounded to 50).
    assert {r["predictedWaitTime"] for r in results} == {50}


# --- 2. Occupancy profile looked up on the park's clock --------------------


def test_occupancy_profile_is_read_with_the_local_weekday_and_hour():
    # Tuesday 2026-07-07 03:00 UTC = Monday 20:00 in Los Angeles (PDT).
    df = convert_to_local_time(
        pd.DataFrame(
            [{"parkId": LA, "timestamp": pd.Timestamp("2026-07-07 03:00", tz="UTC")}]
        ),
        PARKS_METADATA,
    )
    profile = {
        (1, 20): 140.0,  # Postgres DOW 1 = Monday, 20:00 local
        (2, 3): 5.0,  # what the UTC reading asked for (Tuesday 03:00)
    }
    mask = df["parkId"] == LA
    ts_utc = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    assert _historical_occupancy_values(df, mask, profile, ts_utc) == [140.0]


def test_occupancy_profile_falls_back_to_utc_without_local_timestamp():
    df = pd.DataFrame(
        [{"parkId": LA, "timestamp": pd.Timestamp("2026-07-07 03:00", tz="UTC")}]
    )
    mask = df["parkId"] == LA
    ts_utc = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    assert _historical_occupancy_values(df, mask, {(2, 3): 5.0}, ts_utc) == [5.0]


# --- 3. The occupancy cache actually hits ----------------------------------


class _Result:
    def fetchall(self):
        return []


class _Conn:
    def __init__(self, calls):
        self.calls = calls

    def execute(self, query, params):
        self.calls.append(params)
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_occupancy_cache_hits_within_the_same_hour(monkeypatch):
    calls = []
    monkeypatch.setattr(db, "get_db", lambda: _Conn(calls))
    monkeypatch.setattr(db, "_historical_occupancy_cache", {})

    t1 = datetime(2026, 7, 7, 14, 3, 12, 345678, tzinfo=timezone.utc)
    t2 = datetime(2026, 7, 7, 14, 48, 1, 7, tzinfo=timezone.utc)
    db.fetch_historical_park_occupancy([LA], end_time=t1)
    db.fetch_historical_park_occupancy([LA], end_time=t2)

    assert len(calls) == 1, "second call within the hour must be a cache hit"
    assert calls[0]["end_time"] == datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)


def test_occupancy_cache_drops_expired_entries(monkeypatch):
    calls = []
    monkeypatch.setattr(db, "get_db", lambda: _Conn(calls))
    stale = {"hist_occ:old": ({}, 0.0)}
    monkeypatch.setattr(db, "_historical_occupancy_cache", stale)

    db.fetch_historical_park_occupancy(
        [LA], end_time=datetime(2026, 7, 7, 14, 3, tzinfo=timezone.utc)
    )
    assert "hist_occ:old" not in db._historical_occupancy_cache
    assert np.isclose(len(db._historical_occupancy_cache), 1)


# --- 4. Wiring: the real entry points use the helpers above ----------------


class _StopAfterRows(Exception):
    pass


def test_create_prediction_features_builds_rows_from_timestamps_by_park(monkeypatch):
    """The per-park daily timestamps must reach the feature frame.

    Stops `create_prediction_features` at the local-time conversion (the first
    step after the rows are built) and inspects the frame it was handed.
    """
    import features

    captured = {}

    def stop(df, parks_metadata):
        captured["df"] = df.copy()
        raise _StopAfterRows()

    monkeypatch.setattr(predict, "fetch_parks_metadata", lambda: PARKS_METADATA)
    monkeypatch.setattr(db, "fetch_historical_park_occupancy", lambda *a, **k: {})
    monkeypatch.setattr(features, "convert_to_local_time", stop)

    shared = [datetime(2026, 1, 16, 14, 0, tzinfo=timezone.utc)]
    la_ts = predict.generate_future_timestamps(
        datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc), "daily", "America/Los_Angeles"
    )[:3]
    try:
        predict.create_prediction_features(
            ["ride-la", "ride-tokyo"],
            [LA, TOKYO],
            shared,
            datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc),
            timestamps_by_park={LA: la_ts},
        )
    except _StopAfterRows:
        pass

    df = captured["df"]
    la_rows = df[df["parkId"] == LA]["timestamp"].tolist()
    tokyo_rows = df[df["parkId"] == TOKYO]["timestamp"].tolist()
    assert [pd.Timestamp(t) for t in la_rows] == [pd.Timestamp(t) for t in la_ts]
    # A park without its own list keeps the shared one.
    assert [pd.Timestamp(t) for t in tokyo_rows] == [pd.Timestamp(shared[0])]


def test_add_park_occupancy_feature_reads_the_profile_on_the_local_clock():
    """Both inference branches of add_park_occupancy_feature use the local profile."""
    from features import add_park_occupancy_feature

    # Monday 2026-07-06 20:00 PDT = Tuesday 03:00 UTC.
    profile = {LA: {(1, 20): 140.0, (2, 3): 5.0}}

    def frame():
        return convert_to_local_time(
            pd.DataFrame(
                [{"parkId": LA, "timestamp": pd.Timestamp("2026-07-07 03:00", tz="UTC")}]
            ),
            PARKS_METADATA,
        )

    # No real-time occupancy: historical lookup for every row.
    out = add_park_occupancy_feature(frame(), {"historicalOccupancy": profile})
    assert out["park_occupancy_pct"].tolist() == [140.0]

    # Real-time occupancy present, row > 2 h after base_time: historical lookup.
    out = add_park_occupancy_feature(
        frame(),
        {
            "parkOccupancy": {LA: 80.0},
            "historicalOccupancy": profile,
            "baseTime": datetime(2026, 7, 6, 12, 0, tzinfo=timezone.utc),
        },
    )
    assert out["park_occupancy_pct"].tolist() == [140.0]
