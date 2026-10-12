"""PAR-871: compare uuid to uuid, never cast an indexed uuid column to text.

`qd."attractionId"::text = ANY(:ids)` hid queue_data's index from the planner
and made the two serving reads ~80 % of all database time. The fix casts the
parameter instead, which raises on a value that is not a uuid, so the ids are
filtered first. These tests pin both halves: the SQL shape, and that the
filter keeps exactly the values the old text comparison could have matched.

    cd ml-service && python3 -m pytest tests/test_uuid_params.py -q
"""

import contextlib
import logging
import os
import re
import sys

ML_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, ML_DIR)

import uuid_params
from uuid_params import uuid_list, uuid_or_none

A = "0b5f4c1e-7a3d-4e2b-9c8f-1a2b3c4d5e6f"
B = "9e8d7c6b-5a4f-4e3d-8c2b-1a0f9e8d7c6b"


# --- the filter --------------------------------------------------------------


def test_keeps_canonical_ids_in_order():
    assert uuid_list([B, A], "t-order") == [B, A]


def test_drops_what_uuid_text_could_never_equal():
    # uuid::text prints lowercase with hyphens. Every other spelling never
    # matched the old `::text = ANY(...)`, so keeping it would ADD rows.
    upper = A.upper()
    braces = "{" + A + "}"
    bare = A.replace("-", "")
    kept = uuid_list(
        [A, upper, braces, bare, "qt-ride-8", "", None, 42, B], "t-drop"
    )
    assert kept == [A, B]


def test_accepts_uuid_objects_and_other_str_like_values():
    import uuid

    assert uuid_list([uuid.UUID(A)], "t-obj") == [A]


def test_empty_input_is_an_empty_list():
    assert uuid_list([], "t-empty") == []


def test_single_value_form():
    assert uuid_or_none(A, "t-one") == A
    assert uuid_or_none("nope", "t-one") is None
    assert uuid_or_none(None, "t-one") is None


def test_logs_a_bad_id_once_per_context(caplog):
    uuid_params._reported.clear()
    with caplog.at_level(logging.WARNING, logger="uuid_params"):
        uuid_list(["bad-1", A], "t-log")
        uuid_list(["bad-1", A], "t-log")
        uuid_list(["bad-1", "bad-2"], "t-log")
    messages = [r.getMessage() for r in caplog.records]
    assert len(messages) == 2
    assert "'bad-1'" in messages[0]
    assert "'bad-2'" in messages[1] and "'bad-1'" not in messages[1]


# --- the SQL shape -----------------------------------------------------------

# A column cast to text and then compared: `"x"::text = ...`, `x::text IN ...`,
# `x::text = ANY(...)`. Output casts (`... ::text AS name`) are fine.
COLUMN_CAST_COMPARED = re.compile(r"::text\s*(=|IN\b|<>)", re.IGNORECASE)

FILES = [
    "predict.py",
    "db.py",
    "features.py",
    "attraction_features.py",
    "schedule_filter.py",
]


def test_no_query_compares_a_column_cast_to_text():
    offenders = []
    for name in FILES:
        with open(os.path.join(ML_DIR, name), encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                if COLUMN_CAST_COMPARED.search(line):
                    offenders.append(f"{name}:{lineno}: {line.strip()}")
    assert offenders == [], "\n".join(offenders)


# --- the call sites bind the filtered list -----------------------------------


class _Result:
    def fetchall(self):
        return []

    def keys(self):
        return []

    def __iter__(self):
        return iter([])


class _Db:
    def __init__(self):
        self.calls = []

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), params))
        return _Result()


def _fake_get_db(db):
    @contextlib.contextmanager
    def get_db():
        yield db

    return get_db


def test_serving_reads_cast_the_parameter_and_bind_only_uuids(monkeypatch):
    import predict

    db = _Db()
    monkeypatch.setattr(predict, "get_db", _fake_get_db(db))
    predict._recent_wait_times_cache.clear()
    predict._recent_rolling_stats_cache.clear()

    predict.fetch_recent_wait_times([A, "not-a-uuid"])
    predict.fetch_recent_rolling_stats([A, "not-a-uuid"])

    assert len(db.calls) == 2
    for sql, params in db.calls:
        assert 'qd."attractionId" = ANY(CAST(:attraction_ids AS uuid[]))' in sql
        assert '"attractionId"::text = ANY' not in sql
        assert params["attraction_ids"] == [A]


def test_attraction_ids_for_park_skips_the_query_for_a_non_uuid(monkeypatch):
    import db as db_module

    fake = _Db()
    monkeypatch.setattr(db_module, "get_db", _fake_get_db(fake))

    assert db_module.fetch_attraction_ids_for_park("not-a-uuid") == []
    assert fake.calls == []

    db_module.fetch_attraction_ids_for_park(A)
    sql, params = fake.calls[0]
    assert '"parkId" = CAST(:park_id AS uuid)' in sql
    assert params == {"park_id": A}
