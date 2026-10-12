"""PAR-871: the park filter compares uuid to uuid, never `"parkId"::text`.

    cd pcn-service && python3 -m pytest test_uuid_params.py -q
"""

import contextlib
import os
import re

import pandas as pd

import db

HERE = os.path.dirname(os.path.abspath(__file__))
P = "0b5f4c1e-7a3d-4e2b-9c8f-1a2b3c4d5e6f"


def test_helper_is_the_same_file_as_ml_service():
    # Each service ships its own image, so the helper is copied, not shared.
    # Keep the copies identical.
    with open(os.path.join(HERE, "uuid_params.py"), encoding="utf-8") as a, open(
        os.path.join(HERE, "..", "ml-service", "uuid_params.py"), encoding="utf-8"
    ) as b:
        assert a.read() == b.read()


def test_no_query_compares_a_column_cast_to_text():
    pattern = re.compile(r"::text\s*(=|IN\b|<>)", re.IGNORECASE)
    with open(os.path.join(HERE, "db.py"), encoding="utf-8") as fh:
        offenders = [ln for ln in fh if pattern.search(ln)]
    assert offenders == []


class _Engine:
    @contextlib.contextmanager
    def connect(self):
        yield None


def _capture(monkeypatch):
    calls = []

    def read_sql(sql, conn, params=None):
        calls.append((str(sql), params))
        return pd.DataFrame()

    monkeypatch.setattr(db, "engine", lambda: _Engine())
    monkeypatch.setattr(db.pd, "read_sql", read_sql)
    return calls


def test_meta_casts_the_parameter_and_drops_non_uuids(monkeypatch):
    calls = _capture(monkeypatch)
    db.fetch_attraction_meta([P, "not-a-uuid"])
    sql, params = calls[0]
    assert 'a."parkId" = ANY(CAST(:pids AS uuid[]))' in sql
    assert params == {"pids": [P]}


def test_meta_with_only_bad_ids_still_filters_instead_of_reading_every_park(monkeypatch):
    calls = _capture(monkeypatch)
    db.fetch_attraction_meta(["not-a-uuid"])
    sql, params = calls[0]
    assert "WHERE" in sql
    assert params == {"pids": []}


def test_meta_without_ids_reads_every_park(monkeypatch):
    calls = _capture(monkeypatch)
    db.fetch_attraction_meta([])
    sql, params = calls[0]
    assert "WHERE" not in sql
    assert params == {}
