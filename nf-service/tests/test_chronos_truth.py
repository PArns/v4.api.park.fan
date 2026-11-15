"""PAR-1329: the Chronos-2 context is truth without heartbeat rows.

    cd nf-service && python3 -m pytest tests/test_chronos_truth.py -q
"""

import json
import os
import sys
import tempfile

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ.setdefault("MODEL_DIR", tempfile.mkdtemp(prefix="nf-chronos-test-"))

import chronos_data

PID = "8355a0b7-26e9-4a90-af47-246ec143e99a"


def test_readings_leave_heartbeat_rows_out(monkeypatch):
    # A heartbeat re-stamps a frozen reading every hour and so resets the 3 h
    # staleness clock; every other truth reader drops it.
    seen = []
    monkeypatch.setattr(
        chronos_data, "_query", lambda sql, params: seen.append(sql) or pd.DataFrame()
    )
    chronos_data.fetch_readings(
        [PID], pd.Timestamp("2026-11-01", tz="UTC"), pd.Timestamp("2026-11-02", tz="UTC")
    )
    assert "NOT COALESCE(is_heartbeat, false)" in seen[0]


def test_cache_from_another_truth_rule_reads_as_cold(tmp_path, monkeypatch):
    cache = chronos_data.TruthCache(str(tmp_path))
    slots = pd.DataFrame({
        "attraction_id": ["a"],
        "date": ["2026-11-01"],
        "slot_utc": ["2026-11-01T10:00Z"],
        "y": [45.0],
    })
    # The parquet engine is not what is under test (and not installed everywhere).
    monkeypatch.setattr(chronos_data.pd, "read_parquet", lambda path: slots.copy())
    span = {"from": "2026-11-01", "through": "2026-11-01"}
    meta = os.path.join(str(tmp_path), f"{PID}.json")

    with open(meta, "w") as f:
        json.dump({**span, "truth": chronos_data.TRUTH_VERSION}, f)
    assert len(cache.load(PID)[0]) == 1

    # What a cache written before the version existed looks like.
    with open(meta, "w") as f:
        json.dump(span, f)
    df, m = cache.load(PID)
    assert df.empty and m == {}
