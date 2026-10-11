"""PAR-852: the Chronos-2 shadow — series construction, covariates, the
information cut, post-processing and the endpoint's guards.

The model is never loaded here (a stub engine stands in); the one test that
loads the real weights is skipped unless ``CHRONOS_SMOKE=1``.
Run: ``cd nf-service && python -m pytest tests/test_chronos_shadow.py``.
"""

from __future__ import annotations

import datetime as dt
import filecmp
import os
import sys
import tempfile
import threading

import numpy as np
import pandas as pd
import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SERVICE = os.path.join(_HERE, "..")
os.environ.setdefault("MODEL_DIR", tempfile.mkdtemp(prefix="nf-chronos-test-"))
sys.path.insert(0, _SERVICE)

import chronos_data

PID = "8355a0b7-26e9-4a90-af47-246ec143e99a"
import chronos_shadow as cs

TZ = "Europe/Berlin"


def ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC")


def windows(*days: tuple[str, str, str]) -> pd.DataFrame:
    return pd.DataFrame({
        "date": [dt.date.fromisoformat(d) for d, _, _ in days],
        "open_utc": [ts(o) for _, o, _ in days],
        "close_utc": [ts(c) for _, _, c in days],
    })


# --------------------------------------------------------------------------- shared holiday module


@pytest.mark.parametrize("name", ["holiday_features.py", "holiday_utils.py"])
def test_holiday_module_is_the_ml_service_one(name):
    """The covariates must come from THE shared implementation: the nf-service
    copy is byte-identical to ml-service's (the image cannot import across
    build contexts, so it is vendored — and this keeps it from drifting)."""
    original = os.path.join(_SERVICE, "..", "ml-service", name)
    if not os.path.exists(original):
        pytest.skip("ml-service not checked out next to nf-service")
    assert filecmp.cmp(original, os.path.join(_SERVICE, name), shallow=False)


# --------------------------------------------------------------------------- covariates


def _holidays(rows):
    return pd.DataFrame(rows, columns=["country", "date", "region", "holiday_type", "is_nationwide"])


META = cs.ParkMeta(
    park_id=PID, timezone=TZ, country="DE", region="DE-NW",
    influencing=[{"countryCode": "NL", "regionCode": None}],
)


def test_covariate_vector_on_a_school_holiday_day():
    # Wed 2026-10-14: NRW autumn break and a Dutch (regional) school break.
    hol = _holidays([
        ("DE", "2026-10-14", "DE-NW", "school", False),
        ("NL", "2026-10-14", "NL-NH", "school", False),
    ])
    d = dt.date(2026, 10, 14)
    v = cs.day_covariates(META, [d], hol).loc[d]
    assert v.to_dict() == pytest.approx({
        "is_weekend": 0.0,
        "dow_sin": np.sin(2 * np.pi * 2 / 7),   # pandas/ISO weekday: Wed = 2 (0 = Mon)
        "dow_cos": np.cos(2 * np.pi * 2 / 7),
        "is_holiday_primary": 0.0,
        "is_school_holiday_primary": 1.0,
        "holiday_count_total": 0.0,
        "school_holiday_count_total": 2.0,      # own region + NL (any region of NL)
        "is_school_holiday_any": 1.0,
        "is_bridge_day": 0.0,
    })
    assert list(v.index) == cs.DAY_COVS


def test_covariate_vector_on_a_bridge_day():
    # Ascension, Thu 2026-05-14 (national) → Fri 2026-05-15 is a bridge day.
    hol = _holidays([("DE", "2026-05-14", None, "public", True)])
    thu, fri, sat = dt.date(2026, 5, 14), dt.date(2026, 5, 15), dt.date(2026, 5, 16)
    cov = cs.day_covariates(META, [thu, fri, sat], hol)
    assert cov.loc[thu, "is_holiday_primary"] == 1.0
    assert cov.loc[thu, "holiday_count_total"] == 1.0
    assert cov.loc[thu, "is_bridge_day"] == 0.0          # never on the holiday itself
    assert cov.loc[fri].to_dict() == pytest.approx({
        "is_weekend": 0.0,
        "dow_sin": np.sin(2 * np.pi * 4 / 7),
        "dow_cos": np.cos(2 * np.pi * 4 / 7),
        "is_holiday_primary": 0.0,
        "is_school_holiday_primary": 0.0,
        "holiday_count_total": 0.0,
        "school_holiday_count_total": 0.0,
        "is_school_holiday_any": 0.0,
        "is_bridge_day": 1.0,
    })
    assert cov.loc[sat, "is_weekend"] == 1.0


def test_bridge_day_sees_a_holiday_just_outside_the_asked_dates():
    # Asked only for the Monday; the Tuesday holiday is the next calendar day.
    hol = _holidays([("DE", "2026-12-29", None, "public", True)])
    mon = dt.date(2026, 12, 28)
    assert cs.day_covariates(META, [mon], hol).loc[mon, "is_bridge_day"] == 1.0


def test_holiday_coverage_flags_absent_data():
    assert cs.holiday_coverage(None, dt.date(2026, 10, 14)) == "none"
    assert cs.holiday_coverage(dt.date(2026, 10, 1), dt.date(2026, 10, 14)) == "short"
    assert cs.holiday_coverage(dt.date(2027, 12, 31), dt.date(2026, 10, 14)) == "full"


# --------------------------------------------------------------------------- windows & truth


def test_window_slots_ko_kc_and_midnight_overlap():
    w = windows(
        ("2026-10-09", "2026-10-09T20:00:00Z", "2026-10-09T23:00:00Z"),
        ("2026-10-10", "2026-10-09T22:30:00Z", "2026-10-10T00:00:00Z"),
    )
    s = cs.window_slots(w)
    first = s.iloc[0]
    assert first["ko_min"] == 0 and first["date"] == dt.date(2026, 10, 9)
    last_9 = s[s["date"] == dt.date(2026, 10, 9)].iloc[-1]
    # 22:15 is the 9th's last slot only if no later day claims it: 22:30 on is the 10th's.
    assert last_9["slot_utc"] == ts("2026-10-09T22:15:00Z")
    assert s[s["slot_utc"] == ts("2026-10-09T22:30:00Z")]["date"].item() == dt.date(2026, 10, 10)
    assert s.iloc[-1]["kc_min"] == 0          # the window's last slot


def test_truth_is_the_reading_in_force_at_the_midpoint():
    w = windows(("2026-10-09", "2026-10-09T08:00:00Z", "2026-10-09T09:00:00Z"))
    r = pd.DataFrame({
        "attraction_id": ["a"] * 4 + ["b"],
        "ts": [ts("2026-10-09T04:00:00Z"),   # 4 h before → stale at 08:07:30
               ts("2026-10-09T08:10:00Z"),   # in force from 08:15's midpoint on
               ts("2026-10-09T08:35:00Z"),   # DOWN → 08:45 slot dropped
               ts("2026-10-09T08:50:00Z"),
               ts("2026-10-09T07:00:00Z")],
        "status": ["OPERATING", "OPERATING", "DOWN", "OPERATING", "OPERATING"],
        "wait": [30, 20, 0, 4, 15],          # 4 < 5 → dropped
    })
    t = cs.truth_slots(r, w)
    a = t[t["attraction_id"] == "a"].set_index("slot_utc")["y"].to_dict()
    # 08:00: the 04:00 reading is 4 h old at the midpoint (stale); 08:30: DOWN;
    # 08:45: a wait of 4. Only 08:15 survives.
    assert a == {ts("2026-10-09T08:15:00Z"): 20.0}
    b = t[t["attraction_id"] == "b"]["y"].tolist()
    assert b == [15.0] * 4


# --------------------------------------------------------------------------- the task


def _history(days: int, origin: pd.Timestamp):
    """Two rides, open 09–17 Berlin (07–15 UTC) every day up to the origin's day."""
    rows, wins = [], []
    first = origin.date() - dt.timedelta(days=days)
    for i in range(days + 3):
        d = first + dt.timedelta(days=i)
        o, c = ts(f"{d}T07:00:00Z"), ts(f"{d}T15:00:00Z")
        wins.append((d, o, c))
        if d <= origin.date():
            for k in range(32):
                s = o + cs.SLOT * k
                rows.append(("a", d, s, 10.0 + k))
                rows.append(("b", d, s, 50.0))
    truth = pd.DataFrame(rows, columns=["attraction_id", "date", "slot_utc", "y"])
    w = pd.DataFrame(wins, columns=["date", "open_utc", "close_utc"])
    return truth, w


@pytest.mark.parametrize("mode", ["compressed", "grid"])
def test_the_context_never_sees_past_the_origin(mode):
    origin = ts("2026-10-09T10:05:00Z")                 # 12:05 Berlin
    truth, w = _history(20, origin)
    cut = cs.cut_truth(truth, origin)
    assert pd.to_datetime(cut["slot_utc"], utc=True).max() == ts("2026-10-09T09:45:00Z")
    dates = sorted(set(w["date"]))
    daycov = cs.day_covariates(META, dates, _holidays([]))
    future = [origin.date(), origin.date() + dt.timedelta(days=1)]
    task = cs.build_task(mode, origin, TZ, cut, w, future, daycov, ["a", "b"], 8192)
    assert task is not None
    # The last finite context value of ride "a" is the 09:45 slot (k = 11 → 21).
    last = task.target[0][np.isfinite(task.target[0])][-1]
    assert last == 21.0
    assert task.future["slot_utc"].min() >= origin
    assert task.past_cov.shape == (len(cs.cov_names(mode)), task.target.shape[1])
    assert task.future_cov.shape[0] == len(cs.cov_names(mode))
    if mode == "grid":
        # Grid steps are aligned to the quarter-hour after the origin.
        assert task.future["slot_utc"].iloc[0] == ts("2026-10-09T10:15:00Z")
        off = task.future["in_win"] == 0
        assert off.any()
        ko = task.future_cov[cs.cov_names(mode).index("ko_min")]
        assert np.isnan(ko[off.to_numpy()]).all()
    else:
        assert (task.future["in_win"] == 1).all()
        assert task.target.shape[1] == len(cut["slot_utc"].unique())


def test_compressed_context_keeps_the_last_8192_window_slots():
    origin = ts("2026-10-09T06:00:00Z")
    truth, w = _history(300, origin)                     # 300 × 32 slots > 8192
    cut = cs.cut_truth(truth, origin)
    daycov = cs.day_covariates(META, sorted(set(w["date"])), _holidays([]))
    task = cs.build_task("compressed", origin, TZ, cut, w, [origin.date()], daycov, ["a", "b"], 8192)
    assert task.target.shape == (2, 8192)


def test_postprocess_clips_orders_and_drops_closed_hours():
    fut = pd.DataFrame({
        "slot_utc": [ts("2026-10-09T10:00:00Z"), ts("2026-10-09T10:15:00Z"), ts("2026-10-09T16:00:00Z")],
        "date": [dt.date(2026, 10, 9)] * 3,
        "in_win": [1.0, 1.0, 0.0],
    })
    task = cs.Task(rides=["a"], target=np.zeros((1, 1)), past_cov=np.zeros((1, 1)),
                   future_cov=np.zeros((1, 3)), future=fut, names=["x"])
    q = np.array([[[-3.0, -5.0, -1.0], [20.0, 10.0, np.nan], [40.0, 50.0, 60.0]]])
    rows = cs.postprocess(task, q, keep_until=None)
    assert rows["q50"].tolist() == [0.0, 20.0]           # clipped; closed slot gone
    assert rows["q80"].tolist() == [0.0, 20.0]           # q80 ≥ q50
    assert rows["q95"].iloc[0] == 0.0 and np.isnan(rows["q95"].iloc[1])
    assert len(cs.postprocess(task, q, keep_until=ts("2026-10-09T10:10:00Z"))) == 1


def test_to_curves_keeps_gaps_and_reports_covariate_coverage():
    rows = pd.DataFrame({
        "attraction_id": ["a", "a"],
        "date": [dt.date(2026, 10, 9)] * 2,
        "slot_utc": [ts("2026-10-09T10:00:00Z"), ts("2026-10-09T10:30:00Z")],
        "q50": [10.0, 12.0], "q80": [11.0, 13.0], "q95": [15.0, 18.0],
    })
    curves = cs.to_curves(rows, lambda d: {"holidays": "short"})
    assert curves == [{
        "attractionId": "a", "date": "2026-10-09", "slotStart": "2026-10-09T10:00:00Z",
        "slotMinutes": 15, "covariates": {"holidays": "short"},
        "q50": [10.0, None, 12.0], "q80": [11.0, None, 13.0], "q95": [15.0, None, 18.0],
    }]


# --------------------------------------------------------------------------- run() with stubs


class StubEngine:
    """The bench's StubBackend: nan-mean of the last 64 context steps."""

    device = "cpu"
    peak_vram_gb = 0.0

    def __init__(self):
        self.calls = 0

    def forecast(self, task):
        self.calls += 1
        self.last_task = task
        m = np.nanmean(task.target[:, -64:], axis=1)
        h = task.future_cov.shape[1]
        return np.repeat(np.repeat(m[:, None, None], h, axis=1), 3, axis=2)


def test_run_reads_today_only_up_to_the_origin(monkeypatch, tmp_path):
    origin = ts("2026-10-09T10:05:00Z")
    truth, w = _history(100, origin)
    reads = []

    def fake_readings(ids, t0, t1):
        reads.append((t0, t1))
        out = []
        for _, r in truth.iterrows():
            out.append((r.attraction_id, r.slot_utc, "OPERATING", r.y))
        df = pd.DataFrame(out, columns=["attraction_id", "ts", "status", "wait"])
        return df[(df["ts"] >= t0) & (df["ts"] <= t1)]

    monkeypatch.setattr(chronos_data, "fetch_park", lambda pid: META)
    monkeypatch.setattr(chronos_data, "fetch_rides", lambda pid: ["a", "b"])
    monkeypatch.setattr(chronos_data, "fetch_windows", lambda pid, d0, d1: w[(w["date"] >= d0) & (w["date"] <= d1)])
    monkeypatch.setattr(chronos_data, "fetch_readings", fake_readings)
    monkeypatch.setattr(chronos_data, "fetch_holidays",
                        lambda c: (_holidays([]), {"DE": dt.date(2027, 12, 31)}))
    monkeypatch.setattr(chronos_data, "_start_worker", lambda: None)
    eng = StubEngine()
    cache = chronos_data.TruthCache(str(tmp_path))
    # Cold cache: the request does not wait for the backfill, it queues it.
    with pytest.raises(chronos_data.Warming):
        chronos_data.run(PID, origin, "live", 240, eng, cache)
    assert eng.calls == 0
    assert chronos_data.process_pending() == 1
    reads.clear()
    out = chronos_data.run(PID, origin, "live", 240, eng, cache)
    assert out["status"] == "ok" and eng.calls == 1
    assert out["benchName"] == "chronos2"
    assert out["covariateNames"] == cs.cov_names("compressed")
    assert max(t1 for _, t1 in reads) == origin          # nothing after the origin is read
    starts = {c["slotStart"] for c in out["curves"]}
    assert min(starts) >= "2026-10-09T10:15:00Z"
    assert all(c["covariates"] == {"holidays": "full", "schedule": "published"} for c in out["curves"])
    # keep 240 min → nothing at or after 14:05
    assert all(c["date"] == "2026-10-09" for c in out["curves"])

    assert len(reads) == 1                               # warm: today's rows only

    # The grid variant's off-window context steps reach back 85 days, before
    # any truth: their day covariates must still be there, never NaN.
    grid = chronos_data.run(PID, origin, "grid", None, eng, cache)
    assert grid["status"] == "ok" and grid["benchName"] == "chronos2_grid"
    names = eng.last_task.names
    day_rows = [names.index(c) for c in cs.DAY_COVS]
    assert np.isfinite(eng.last_task.past_cov[day_rows]).all()
    assert np.isfinite(eng.last_task.future_cov[day_rows]).all()
    assert {c["date"] for c in grid["curves"]} >= {"2026-10-09", "2026-10-11"}

    # Next day: one day of increment is read inline, plus today's rows.
    reads.clear()
    out = chronos_data.run(PID, origin + pd.Timedelta(days=1), "live", 240, eng, cache)
    assert out["status"] == "ok" and len(reads) == 2
    reads.clear()
    chronos_data.run(PID, origin + pd.Timedelta(hours=2), "live", 240, eng, cache)
    assert len(reads) == 1


def test_a_failed_backfill_is_dropped_from_the_queue(monkeypatch, tmp_path):
    origin = ts("2026-10-09T04:05:00Z")
    _truth, w = _history(30, origin)
    monkeypatch.setattr(chronos_data, "fetch_park", lambda pid: META)
    monkeypatch.setattr(chronos_data, "fetch_rides", lambda pid: ["a"])
    monkeypatch.setattr(chronos_data, "fetch_windows", lambda pid, d0, d1: w)

    def down(ids, t0, t1):
        raise RuntimeError("canceling statement due to statement timeout")

    monkeypatch.setattr(chronos_data, "fetch_readings", down)
    monkeypatch.setattr(chronos_data, "fetch_holidays", lambda c: (_holidays([]), {}))
    monkeypatch.setattr(chronos_data, "_start_worker", lambda: None)
    with pytest.raises(chronos_data.Warming):
        chronos_data.run(PID, origin, "grid", None, StubEngine(), chronos_data.TruthCache(str(tmp_path)))
    assert chronos_data.process_pending() == 0
    assert chronos_data._pending == {}


# --------------------------------------------------------------------------- endpoint & engine


@pytest.fixture()
def client(monkeypatch):
    from fastapi.testclient import TestClient

    import main

    return TestClient(main.app), main


def test_endpoint_kill_switch_and_training_guard(client, monkeypatch):
    tc, main = client
    body = {"park_id": PID, "origin_at": "2026-10-09T10:05:00Z", "variant": "live"}
    monkeypatch.setattr(main.settings, "CHRONOS_ENABLED", False)
    assert tc.post("/chronos/forecast", json=body).status_code == 503
    monkeypatch.setattr(main.settings, "CHRONOS_ENABLED", True)
    monkeypatch.setattr(main, "_read_status", lambda: {"is_training": True})
    assert tc.post("/chronos/forecast", json=body).status_code == 503
    monkeypatch.setattr(main, "_read_status", lambda: {"is_training": False})
    assert tc.post("/chronos/forecast", json={**body, "variant": "nope"}).status_code == 400
    assert tc.post("/chronos/forecast", json={**body, "origin_at": "2026-10-09T10:05:00"}).status_code == 400


def test_endpoint_answers_warming_and_hides_errors(client, monkeypatch):
    tc, main = client
    body = {"park_id": PID, "origin_at": "2026-10-09T10:05:00Z", "variant": "grid"}
    monkeypatch.setattr(main, "_read_status", lambda: {"is_training": False})

    def warming(*a, **k):
        raise chronos_data.Warming("backfilled 90 days")

    monkeypatch.setattr(chronos_data, "run", warming)
    r = tc.post("/chronos/forecast", json=body)
    assert r.status_code == 200 and r.json()["status"] == "warming"

    def boom(*a, **k):
        raise RuntimeError("secret detail")

    monkeypatch.setattr(chronos_data, "run", boom)
    r = tc.post("/chronos/forecast", json=body)
    assert r.status_code == 500 and "secret" not in r.text


def test_engine_loads_once_under_concurrent_calls(monkeypatch):
    eng = cs.ChronosEngine("m", "rev", tempfile.mkdtemp(), 0.25)
    loads = []

    class Pipe:
        model_context_length = 8192

        def predict_quantiles(self, inputs, prediction_length, quantile_levels, batch_size):
            import torch  # noqa: F401 — only reached with torch installed

    def fake_load():
        loads.append(1)
        eng.pipe = Pipe()

    monkeypatch.setattr(eng, "_load", fake_load)
    torch_stub = type(sys)("torch")
    torch_stub.inference_mode = lambda: _Null()
    torch_stub.cuda = type("c", (), {"is_available": staticmethod(lambda: False)})

    class _Q:
        def float(self):
            return self

        def cpu(self):
            return self

        def numpy(self):
            return np.zeros((1, 2, 3))

    class _Null:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    Pipe.predict_quantiles = lambda self, *a, **k: ([_Q()], None)
    monkeypatch.setitem(sys.modules, "torch", torch_stub)
    task = cs.Task(rides=["a"], target=np.zeros((1, 4), np.float32), past_cov=np.zeros((1, 4)),
                   future_cov=np.zeros((1, 2)), future=pd.DataFrame(), names=["x"])
    threads = [threading.Thread(target=eng.forecast, args=(task,)) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert loads == [1]
    eng.unload()
    assert not eng.loaded


@pytest.mark.skipif(os.environ.get("CHRONOS_SMOKE") != "1", reason="loads the real 478 MB model")
def test_smoke_real_model():
    s = chronos_data.settings
    eng = cs.ChronosEngine(s.CHRONOS_MODEL_ID, s.CHRONOS_REVISION,
                           os.environ.get("CHRONOS_SMOKE_CACHE", tempfile.mkdtemp()), 0.25)
    origin = ts("2026-10-09T10:05:00Z")
    truth, w = _history(40, origin)
    daycov = cs.day_covariates(META, sorted(set(w["date"])), _holidays([]))
    task = cs.build_task("compressed", origin, TZ, cs.cut_truth(truth, origin), w,
                         [origin.date()], daycov, ["a", "b"], 8192)
    q = eng.forecast(task)
    assert q.shape == (2, task.future_cov.shape[1], 3)
    assert np.isfinite(q).all()


def test_truth_cache_rejects_a_park_id_that_is_not_a_uuid(tmp_path):
    cache = chronos_data.TruthCache(str(tmp_path))
    for bad in ["../../etc/passwd", "p1", "", "a/b"]:
        with pytest.raises(ValueError):
            cache._paths(bad)
    data, meta = cache._paths(PID.upper())
    assert data.endswith(f"{PID}.parquet") and meta.endswith(f"{PID}.json")


def test_endpoint_rejects_a_park_id_that_is_not_a_uuid(client):
    tc, _ = client
    r = tc.post("/chronos/forecast", json={"park_id": "../x", "origin_at": "2026-10-09T10:05:00Z"})
    assert r.status_code == 422
