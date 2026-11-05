"""PAR-1149: the holiday, schedule and weather-climatology caches evict on write.

Their keys carry date ranges and park sets, so the key space grows every day;
before this, only gunicorn's worker recycling bounded their memory.
"""

import datetime
import os
import sys
from contextlib import contextmanager

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import db
import predict


class _Result:
    def fetchall(self):
        return []

    def keys(self):
        return ["date", "country", "region", "holiday_type", "is_nationwide"]


class _Db:
    def execute(self, *args, **kwargs):
        return _Result()


@contextmanager
def _fake_db():
    yield _Db()


def _expired():
    return ("old-frame", 0.0)  # epoch 0: older than any ttl


def test_fetch_holidays_drops_an_expired_key(monkeypatch):
    monkeypatch.setattr(db, "get_db", _fake_db)
    monkeypatch.setattr(db, "_holidays_cache", {"US:2020-01-01:2020-01-02": _expired()})

    db.fetch_holidays(["US"], datetime.datetime(2026, 11, 5), datetime.datetime(2026, 11, 6))

    assert list(db._holidays_cache) == ["US:2026-11-05:2026-11-06"]


def test_fetch_park_schedules_drops_an_expired_key(monkeypatch):
    monkeypatch.setattr(db, "get_db", _fake_db)
    monkeypatch.setattr(db, "_schedules_cache", {"2020-01-01:2020-01-02": _expired()})

    db.fetch_park_schedules(datetime.datetime(2026, 11, 5), datetime.datetime(2026, 11, 6))

    assert list(db._schedules_cache) == ["2026-11-05:2026-11-06"]


def test_weather_climatology_drops_an_expired_key(monkeypatch):
    monkeypatch.setattr(predict, "get_db", _fake_db)
    monkeypatch.setattr(predict, "_weather_historical_cache", {"old-park:1": _expired()})
    park = "00000000-0000-4000-8000-000000000001"

    predict.fetch_weather_climatology([park], [11])

    assert list(predict._weather_historical_cache) == [f"{park}:11"]


class _EvictedBetween(dict):
    """A cache whose key another thread evicts between `in` and `[]`."""

    def __contains__(self, key):
        return True

    def __getitem__(self, key):
        raise KeyError(key)


def test_reads_survive_a_key_evicted_by_another_thread(monkeypatch):
    monkeypatch.setattr(db, "get_db", _fake_db)
    monkeypatch.setattr(db, "_holidays_cache", _EvictedBetween())
    monkeypatch.setattr(db, "_schedules_cache", _EvictedBetween())
    day, next_day = datetime.datetime(2026, 11, 5), datetime.datetime(2026, 11, 6)

    # Both read as a miss and query, instead of a KeyError (HTTP 500).
    db.fetch_holidays(["US"], day, next_day)
    db.fetch_park_schedules(day, next_day)
