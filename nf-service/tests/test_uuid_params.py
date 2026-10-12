"""PAR-871: the park filters compare uuid to uuid, never `"parkId"::text`.

    cd nf-service && python3 -m pytest tests/test_uuid_params.py -q
"""

import contextlib
import os
import re
import sys

NF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, NF_DIR)

import importlib.util

import pandas as pd

# Other test modules in this directory replace `db` in sys.modules with a stub
# (the real one builds an engine), so load the real file under its own name.
_spec = importlib.util.spec_from_file_location("nf_db_real", os.path.join(NF_DIR, "db.py"))
db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(db)

P = "0b5f4c1e-7a3d-4e2b-9c8f-1a2b3c4d5e6f"


def test_helper_is_the_same_file_as_ml_service():
    # Each service ships its own image, so the helper is copied, not shared.
    with open(os.path.join(NF_DIR, "uuid_params.py"), encoding="utf-8") as a, open(
        os.path.join(NF_DIR, "..", "ml-service", "uuid_params.py"), encoding="utf-8"
    ) as b:
        assert a.read() == b.read()


def test_no_query_compares_a_column_cast_to_text():
    pattern = re.compile(r"::text\s*(=|IN\b|<>)", re.IGNORECASE)
    with open(os.path.join(NF_DIR, "db.py"), encoding="utf-8") as fh:
        offenders = [ln for ln in fh if pattern.search(ln)]
    assert offenders == []


class _Engine:
    @contextlib.contextmanager
    def connect(self):
        yield None


def _capture(monkeypatch, frame=None):
    calls = []

    def read_sql(sql, conn, params=None):
        calls.append((str(sql), params))
        return frame if frame is not None else pd.DataFrame()

    monkeypatch.setattr(db, "_engine", _Engine())
    monkeypatch.setattr(db.pd, "read_sql", read_sql)
    return calls


def test_meta_casts_the_parameter_and_drops_non_uuids(monkeypatch):
    calls = _capture(monkeypatch)
    db.fetch_attraction_meta([P, "not-a-uuid"])
    sql, params = calls[0]
    assert 'a."parkId" = ANY(CAST(:pids AS uuid[]))' in sql
    assert params == {"pids": [P]}


def test_meta_with_only_bad_ids_still_filters(monkeypatch):
    calls = _capture(monkeypatch)
    db.fetch_attraction_meta(["not-a-uuid"])
    sql, params = calls[0]
    assert "WHERE" in sql
    assert params == {"pids": []}


def test_panel_casts_the_parameter(monkeypatch):
    frame = pd.DataFrame({"unique_id": [], "ds": [], "y": []})
    calls = _capture(monkeypatch, frame)
    db.fetch_daily_peak_panel([P, "bad"])
    sql, params = calls[0]
    assert 'a."parkId" = ANY(CAST(:pids AS uuid[]))' in sql
    assert params["pids"] == [P]


def test_weather_casts_the_parameter(monkeypatch):
    calls = _capture(monkeypatch)
    db.fetch_weather([P, "bad"])
    sql, params = calls[0]
    assert '"parkId" = ANY(CAST(:pids AS uuid[]))' in sql
    assert params == {"pids": [P]}
