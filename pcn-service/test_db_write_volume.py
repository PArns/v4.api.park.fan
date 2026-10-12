"""pcn_forecasts write volume and retention (PAR-873).

Two parts. The first runs without a database: the retention covers the oldest row
the scorer reads, and the prune drains a backlog in bounded batches. The second
runs the real upsert and prune SQL against a Postgres named by
PCN_TEST_DATABASE_URL and is skipped without one:

    docker run -d --rm --name pcn-pg -e POSTGRES_PASSWORD=pw -p 55873:5432 postgres:18
    PCN_TEST_DATABASE_URL=postgresql://postgres:pw@localhost:55873/postgres \\
        python3 -m pytest test_db_write_volume.py -q
"""

import contextlib
import datetime as dt
import os

import pandas as pd
import pytest

import db
import score
from config import get_settings

settings = get_settings()


# --- retention vs readers ------------------------------------------------------


def test_retention_covers_the_oldest_row_the_scorer_reads():
    """The scorer reads targets from the first fully covered local day of its
    lookback, forecast up to PCN_HORIZON slots before that. Worst case is just
    before local midnight; add 14 h for the easternmost UTC offset against a
    created_at in UTC, which over-covers (both sides are instants) but costs
    nothing to assume."""
    now_local = pd.Timestamp("2026-10-12 23:59")
    lo, _ = score.full_day_window(
        now_local, settings.PCN_SCORE_LOOKBACK_HOURS, settings.slot_freq)
    horizon = pd.Timedelta(minutes=settings.PCN_SLOT_MINUTES * settings.PCN_HORIZON)
    oldest_needed = now_local - (lo - horizon)
    retention = pd.Timedelta(days=settings.PCN_FORECAST_RETENTION_DAYS)
    assert oldest_needed <= pd.Timedelta(hours=60)
    assert retention >= oldest_needed + pd.Timedelta(hours=14)


def test_retention_is_not_below_the_four_day_floor():
    assert settings.PCN_FORECAST_RETENTION_DAYS >= 4


class _Res:
    def __init__(self, n):
        self.rowcount = n


class _BacklogEngine:
    """Each DELETE removes min(limit, remaining) rows, one transaction each."""

    def __init__(self, backlog):
        self.backlog = backlog
        self.transactions = 0

    @contextlib.contextmanager
    def begin(self):
        self.transactions += 1
        eng = self

        class _C:
            def execute(self, stmt, params=None):
                if not str(stmt).strip().startswith("DELETE"):
                    return _Res(0)
                n = min(params["n"], eng.backlog)
                eng.backlog -= n
                return _Res(n)

        yield _C()


def test_prune_drains_a_backlog_in_bounded_batches(monkeypatch):
    eng = _BacklogEngine(backlog=1_050)
    monkeypatch.setattr(db, "engine", lambda: eng)
    monkeypatch.setattr(db, "_PCN_FORECASTS_SCHEMA_READY", True)
    assert db.prune_pcn_forecasts(4, batch_rows=100, max_batches=5) == 500
    assert eng.backlog == 550
    assert eng.transactions == 5
    # The next hourly run carries on, and stops at the first short batch.
    assert db.prune_pcn_forecasts(4, batch_rows=100, max_batches=50) == 550
    assert eng.backlog == 0


def test_prune_stops_after_one_short_batch(monkeypatch):
    eng = _BacklogEngine(backlog=0)
    monkeypatch.setattr(db, "engine", lambda: eng)
    monkeypatch.setattr(db, "_PCN_FORECASTS_SCHEMA_READY", True)
    assert db.prune_pcn_forecasts(4) == 0
    assert eng.transactions == 1


# --- the SQL itself, against Postgres -------------------------------------------

_URL = os.environ.get("PCN_TEST_DATABASE_URL")
needs_pg = pytest.mark.skipif(not _URL, reason="PCN_TEST_DATABASE_URL not set")

# The NestJS serving read (MLService.getPcnIntradayWaits), with the park join and
# timezone conversion dropped: the fixture has no parks table, and the staleness
# guard and the DISTINCT ON are what this test is about.
_SERVING_READ = """
    SELECT DISTINCT ON (f.attraction_id, f.target_slot, f.quantile)
        f.attraction_id::text AS aid, f.target_slot, f.quantile, f.predicted_wait
    FROM pcn_forecasts f
    WHERE f.attraction_id = ANY(CAST(:aids AS uuid[]))
      AND f.quantile IN (0.5, 0.8)
      AND f.created_at >= now() - interval '3 hours'
    ORDER BY f.attraction_id, f.target_slot, f.quantile, f.origin_slot DESC
"""

A1 = "00000000-0000-0000-0000-000000000001"
A2 = "00000000-0000-0000-0000-000000000002"
ORIGIN = dt.datetime(2026, 10, 12, 18, 0)


def _rows(origin, value_of):
    out = []
    for aid in (A1, A2):
        for h in range(48):
            for q in (0.5, 0.8):
                out.append({
                    "aid": aid,
                    "ts": origin + dt.timedelta(minutes=15 * (h + 1)),
                    "os": origin,
                    "q": q,
                    "pw": value_of(aid, h, q),
                })
    return out


def _base(aid, h, q):
    return 10.0 + h + (5 if q == 0.8 else 0) + (100 if aid == A2 else 0)


@pytest.fixture
def pg(monkeypatch):
    from sqlalchemy import create_engine, text

    eng = create_engine(_URL)
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS pcn_forecasts"))
    monkeypatch.setattr(db, "engine", lambda: eng)
    monkeypatch.setattr(db, "_PCN_FORECASTS_SCHEMA_READY", False)

    def q(sql, **params):
        with eng.begin() as c:
            res = c.execute(text(sql), params)
            return res.mappings().all() if res.returns_rows else []

    def versions():
        # Row identity -> the transaction that last wrote it. A rewrite gives the
        # row a new xmin; an untouched row keeps its own. (pg_stat counters are
        # flushed per backend with a delay, so they make a flaky assertion here.)
        return {(r["attraction_id"], r["target_slot"], r["origin_slot"], r["quantile"]):
                r["x"] for r in q("SELECT attraction_id, target_slot, origin_slot, "
                                  "quantile, xmin::text AS x FROM pcn_forecasts")}

    def stats():
        return versions()

    yield type("PG", (), {"q": staticmethod(q), "stats": staticmethod(stats)})
    eng.dispose()


def _delta(before, after):
    """(inserted, rewritten) between two xmin snapshots."""
    inserted = len(after.keys() - before.keys())
    rewritten = sum(1 for k in before.keys() & after.keys() if before[k] != after[k])
    return inserted, rewritten


def _serving(pg):
    return [dict(r) for r in pg.q(_SERVING_READ, aids=[A1, A2])]


@needs_pg
def test_an_unchanged_origin_is_not_rewritten(pg):
    db.write_pcn_forecasts(_rows(ORIGIN, _base), "pcn_tick1")
    before = pg.q("SELECT attraction_id, target_slot, quantile, created_at, model_version "
                  "FROM pcn_forecasts ORDER BY 1, 2, 3")
    served_before = _serving(pg)
    s0 = pg.stats()

    # A restart: the in-process memo is empty, the same origin is re-sent with the
    # same values under a new tick version.
    db.write_pcn_forecasts(_rows(ORIGIN, _base), "pcn_tick2")
    s1 = pg.stats()
    after = pg.q("SELECT attraction_id, target_slot, quantile, created_at, model_version "
                 "FROM pcn_forecasts ORDER BY 1, 2, 3")

    assert _delta(s0, s1) == (0, 0)
    assert after == before                       # created_at and version untouched
    assert _serving(pg) == served_before         # served output identical


@needs_pg
def test_a_changed_value_is_rewritten_and_restamped(pg):
    db.write_pcn_forecasts(_rows(ORIGIN, _base), "pcn_tick1")
    pg.q("UPDATE pcn_forecasts SET created_at = now() - interval '2 hours'")
    s0 = pg.stats()

    # A retrained checkpoint: A1's q0.5 values move, everything else is identical.
    def retrained(aid, h, q):
        return _base(aid, h, q) + (1.0 if aid == A1 and q == 0.5 else 0.0)

    db.write_pcn_forecasts(_rows(ORIGIN, retrained), "pcn_tick2")
    s1 = pg.stats()
    assert _delta(s0, s1) == (0, 48)

    fresh = pg.q("SELECT attraction_id::text AS aid, quantile, model_version FROM pcn_forecasts "
                 "WHERE created_at > now() - interval '1 hour'")
    assert {(r["aid"], r["quantile"], r["model_version"]) for r in fresh} == {
        (A1, 0.5, "pcn_tick2")}
    served = {(r["aid"], r["target_slot"], r["quantile"]): r["predicted_wait"]
              for r in _serving(pg)}
    assert served[(A1, ORIGIN + dt.timedelta(minutes=15), 0.5)] == 11.0
    assert served[(A2, ORIGIN + dt.timedelta(minutes=15), 0.5)] == 110.0


@needs_pg
def test_a_closed_park_still_ages_out_of_the_staleness_guard(pg):
    """The guard is meant to hand a park whose origin stopped moving back to
    CatBoost. A restart used to re-stamp its rows and undo that."""
    db.write_pcn_forecasts(_rows(ORIGIN, _base), "pcn_tick1")
    pg.q("UPDATE pcn_forecasts SET created_at = now() - interval '4 hours'")
    assert _serving(pg) == []
    db.write_pcn_forecasts(_rows(ORIGIN, _base), "pcn_after_restart")
    assert _serving(pg) == []


@needs_pg
def test_a_new_origin_is_inserted_and_served(pg):
    db.write_pcn_forecasts(_rows(ORIGIN, _base), "pcn_tick1")
    nxt = ORIGIN + dt.timedelta(minutes=15)
    s0 = pg.stats()
    db.write_pcn_forecasts(_rows(nxt, lambda a, h, q: _base(a, h, q) + 2), "pcn_tick2")
    s1 = pg.stats()
    assert _delta(s0, s1) == (2 * 48 * 2, 0)
    served = {(r["aid"], r["target_slot"], r["quantile"]): r["predicted_wait"]
              for r in _serving(pg)}
    # The newer origin wins for every slot both origins cover.
    assert served[(A1, nxt + dt.timedelta(minutes=15), 0.5)] == 12.0


@needs_pg
def test_prune_deletes_only_rows_past_retention_in_batches(pg):
    db.write_pcn_forecasts(_rows(ORIGIN, _base), "pcn_tick1")       # 192 rows
    pg.q("UPDATE pcn_forecasts SET created_at = now() - interval '5 days' "
         "WHERE attraction_id = CAST(:a AS uuid)", a=A1)           # 96 expired
    assert db.prune_pcn_forecasts(4, batch_rows=40, max_batches=2) == 80
    assert db.prune_pcn_forecasts(4, batch_rows=40, max_batches=2) == 16
    assert db.prune_pcn_forecasts(4, batch_rows=40, max_batches=2) == 0
    left = pg.q("SELECT attraction_id::text AS aid, count(*) AS n FROM pcn_forecasts GROUP BY 1")
    assert [(r["aid"], r["n"]) for r in left] == [(A2, 96)]
