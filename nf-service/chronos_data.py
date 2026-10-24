"""Inputs for the Chronos-2 shadow (PAR-852), read as of the origin.

Every read is bounded (``SET LOCAL statement_timeout`` / ``lock_timeout``) — the
API pool connects as the same role (db-health-runbook §0). Truth slots of past
days are cached per park as parquet on the persistent model volume, so after
the first fill a capture reads one new day of ``queue_data`` (plus today's rows
up to the origin) instead of the whole context. The cold fill after a deploy
runs in ONE background thread, park after park in 30-day reads, while the
captures answer ``warming`` at once.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict

import pandas as pd
from sqlalchemy import text

import chronos_shadow as cs
from config import get_settings
from holiday_features import parse_influencing_regions

settings = get_settings()
logger = logging.getLogger("nf.chronos.data")

#: Older cached days are dropped: the compressed context reaches ~343 days.
CACHE_MAX_DAYS = 400
_park_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)


class Warming(Exception):
    """The park's context is still being backfilled; try again next origin."""


def _engine():
    import db  # lazy: builds the SQLAlchemy engine

    return db._engine


def _query(sql: str, params: dict) -> pd.DataFrame:
    with _engine().begin() as c:
        c.execute(text(f"SET LOCAL statement_timeout = {int(settings.CHRONOS_STATEMENT_TIMEOUT_MS)}"))
        c.execute(text("SET LOCAL lock_timeout = 5000"))
        return pd.read_sql(text(sql), c, params=params)


def fetch_park(park_id: str) -> cs.ParkMeta | None:
    df = _query(
        'SELECT id::text AS id, timezone, "countryCode" AS country, "regionCode" AS region, '
        '"influencingRegions" AS influencing FROM parks WHERE id = CAST(:pid AS uuid)',
        {"pid": park_id},
    )
    if df.empty or not df.iloc[0]["timezone"]:
        return None
    r = df.iloc[0]
    return cs.ParkMeta(
        park_id=park_id, timezone=r["timezone"],
        country=r["country"] if isinstance(r["country"], str) else None,
        region=r["region"] if isinstance(r["region"], str) else None,
        influencing=parse_influencing_regions(r["influencing"]),
    )


def fetch_rides(park_id: str) -> list[str]:
    df = _query('SELECT id::text AS id FROM attractions WHERE "parkId" = CAST(:pid AS uuid)',
                {"pid": park_id})
    return sorted(df["id"].tolist())


def fetch_windows(park_id: str, d0: dt.date, d1: dt.date) -> pd.DataFrame:
    """Per service date: earliest published OPERATING opening, latest closing
    (park-wide rows) — the bench's ``windows`` table."""
    df = _query(
        """SELECT date, min("openingTime") AS open_utc, max("closingTime") AS close_utc
             FROM schedule_entries
            WHERE "parkId" = CAST(:pid AS uuid) AND date BETWEEN :d0 AND :d1
              AND "scheduleType" = 'OPERATING' AND "attractionId" IS NULL
              AND "openingTime" IS NOT NULL AND "closingTime" IS NOT NULL
            GROUP BY date ORDER BY date""",
        {"pid": park_id, "d0": d0, "d1": d1},
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["open_utc"] = pd.to_datetime(df["open_utc"], utc=True)
    df["close_utc"] = pd.to_datetime(df["close_utc"], utc=True)
    return df


def fetch_readings(ride_ids: list[str], t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
    """STANDBY change-log rows in [t0, t1]."""
    if not ride_ids:
        return pd.DataFrame(columns=["attraction_id", "ts", "status", "wait"])
    return _query(
        """SELECT "attractionId"::text AS attraction_id, timestamp AS ts,
                  status::text AS status, "waitTime" AS wait
             FROM queue_data
            WHERE "attractionId" = ANY(CAST(:ids AS uuid[]))
              AND "queueType" = 'STANDBY'
              AND timestamp >= :t0 AND timestamp <= :t1""",
        {"ids": ride_ids, "t0": t0.to_pydatetime(), "t1": t1.to_pydatetime()},
    )


def fetch_holidays(countries: list[str]) -> pd.DataFrame:
    """All holiday rows of the countries (a few thousand). The coverage the
    ``holidays`` flag on each curve is read from is ``cs.holidays_through`` —
    per type, never this frame's last date (PAR-1049)."""
    if not countries:
        return pd.DataFrame(columns=["country", "date", "region", "holiday_type", "is_nationwide"])
    df = _query(
        """SELECT country, date, region, "holidayType"::text AS holiday_type,
                  "isNationwide" AS is_nationwide
             FROM holidays WHERE country = ANY(:c)""",
        {"c": countries},
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


# --------------------------------------------------------------------------- truth cache


class TruthCache:
    """Per-park parquet of truth slots for COMPLETE past service days."""

    def __init__(self, root: str):
        self.root = root

    def _paths(self, park_id: str) -> tuple[str, str]:
        # The id names a file: accept only a canonical UUID, so no request value can
        # steer the path outside the cache directory.
        name = str(uuid.UUID(str(park_id)))
        root = os.path.realpath(self.root)
        data = os.path.realpath(os.path.join(root, f"{name}.parquet"))
        meta = os.path.realpath(os.path.join(root, f"{name}.json"))
        if os.path.dirname(data) != root or os.path.dirname(meta) != root:
            raise ValueError("park cache path escapes the cache directory")
        return data, meta

    def load(self, park_id: str) -> tuple[pd.DataFrame, dict]:
        data, meta = self._paths(park_id)
        try:
            with open(meta) as f:
                m = json.load(f)
            df = pd.read_parquet(data)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["slot_utc"] = pd.to_datetime(df["slot_utc"], utc=True)
            return df, m
        except Exception:  # noqa: BLE001 — a missing or broken cache is a cold one
            return pd.DataFrame(columns=["attraction_id", "date", "slot_utc", "y"]), {}

    def save(self, park_id: str, df: pd.DataFrame, meta: dict) -> None:
        os.makedirs(self.root, exist_ok=True)
        data, m = self._paths(park_id)
        tmp = data + ".tmp"
        df.to_parquet(tmp, index=False)
        os.replace(tmp, data)
        with open(m + ".tmp", "w") as f:
            json.dump(meta, f)
        os.replace(m + ".tmp", m)


def _truth_for_days(rides: list[str], windows: pd.DataFrame) -> pd.DataFrame:
    if windows.empty:
        return pd.DataFrame(columns=["attraction_id", "date", "slot_utc", "y"])
    t0 = windows["open_utc"].min() - cs.STALE
    t1 = windows["close_utc"].max()
    return cs.truth_slots(fetch_readings(rides, t0, t1), windows)


def _missing_chunks(meta: dict, need_from: dt.date, yesterday: dt.date) -> list[tuple[dt.date, dt.date]]:
    """The 30-day chunks the cache lacks, NEWEST first — so the cache stays one
    contiguous range however far a fill gets before it stops."""
    c_from = dt.date.fromisoformat(meta["from"]) if meta.get("from") else None
    c_through = dt.date.fromisoformat(meta["through"]) if meta.get("through") else None
    ranges: list[tuple[dt.date, dt.date]] = []
    if c_from is None:
        ranges.append((need_from, yesterday))
    else:
        if c_through < yesterday:
            ranges.append((c_through + dt.timedelta(days=1), yesterday))
        if need_from < c_from:
            ranges.append((need_from, c_from - dt.timedelta(days=1)))
    chunks: list[tuple[dt.date, dt.date]] = []
    for a, b in ranges:
        end = b
        while end >= a:
            start = max(a, end - dt.timedelta(days=29))
            chunks.append((start, end))
            end = start - dt.timedelta(days=1)
    return chunks


def _fill(park_id: str, rides: list[str], need_from: dt.date, yesterday: dt.date,
          cache: TruthCache, windows: pd.DataFrame) -> pd.DataFrame:
    """Reads every missing chunk, saving after each one (progress survives a
    restart). Caller holds the park lock."""
    df, meta = cache.load(park_id)
    for a, b in _missing_chunks(meta, need_from, yesterday):
        w = windows[(windows["date"] >= a) & (windows["date"] <= b)]
        part = _truth_for_days(rides, w)
        df = pd.concat([df, part], ignore_index=True) if len(df) else part
        df = df.drop_duplicates(["attraction_id", "slot_utc"], keep="last")
        floor = yesterday - dt.timedelta(days=CACHE_MAX_DAYS)
        df = df[df["date"] >= floor]
        c_from = dt.date.fromisoformat(meta["from"]) if meta.get("from") else a
        c_through = dt.date.fromisoformat(meta["through"]) if meta.get("through") else b
        meta = {"from": max(min(c_from, a), floor).isoformat(),
                "through": max(c_through, b).isoformat()}
        cache.save(park_id, df, meta)
    return df


# One background worker fills cold caches, one park at a time, so the cold
# start after a deploy is a steady trickle of 30-day reads and never a burst of
# parallel ones; a capture meanwhile answers "warming" at once instead of
# waiting (and timing out) behind it.
_pending: dict[str, tuple] = {}
_pending_lock = threading.Lock()
_worker: threading.Thread | None = None


def process_pending() -> int:
    """Drains the backfill queue in the calling thread; returns parks filled."""
    done = 0
    while True:
        with _pending_lock:
            if not _pending:
                return done
            park_id, args = next(iter(_pending.items()))
        rides, need_from, yesterday, cache = args
        try:
            windows = fetch_windows(park_id, need_from, yesterday)
            with _park_locks[park_id]:
                _fill(park_id, rides, need_from, yesterday, cache, windows)
            done += 1
        except Exception as e:  # noqa: BLE001 — the next capture re-queues it
            logger.warning("Chronos backfill for %s failed: %s", park_id, e)
        finally:
            with _pending_lock:
                _pending.pop(park_id, None)


def _start_worker() -> None:
    global _worker
    if _worker is None or not _worker.is_alive():
        _worker = threading.Thread(target=process_pending, name="chronos-backfill", daemon=True)
        _worker.start()


def schedule_backfill(park_id: str, rides: list[str], need_from: dt.date, yesterday: dt.date,
                      cache: TruthCache) -> None:
    with _pending_lock:
        _pending.setdefault(park_id, (rides, need_from, yesterday, cache))
    _start_worker()


def past_truth(park_id: str, rides: list[str], local_today: dt.date, need_days: int,
               cache: TruthCache, windows: pd.DataFrame) -> pd.DataFrame:
    """Truth of the service days in [today − need_days, today − 1], from the
    cache. A gap of a few days (the daily increment) is read inline; a larger
    one goes to the background worker and :class:`Warming` is raised."""
    need_from = local_today - dt.timedelta(days=need_days)
    yesterday = local_today - dt.timedelta(days=1)
    lock = _park_locks[park_id]
    # A backfill of this park may hold the lock for many chunks; a capture must
    # not sit behind it until NestJS times out — it answers "warming" instead.
    if not lock.acquire(timeout=5):
        raise Warming("park context is being backfilled")
    try:
        df, meta = cache.load(park_id)
        missing = sum((b - a).days + 1 for a, b in _missing_chunks(meta, need_from, yesterday))
        if missing > settings.CHRONOS_INLINE_FILL_DAYS:
            schedule_backfill(park_id, rides, need_from, yesterday, cache)
            raise Warming(f"{missing} days of context queued for backfill")
        if missing:
            df = _fill(park_id, rides, need_from, yesterday, cache, windows)
    finally:
        lock.release()
    return df[(df["date"] >= need_from) & (df["date"] <= yesterday)]


# --------------------------------------------------------------------------- orchestration


def run(park_id: str, origin: pd.Timestamp, variant: str, keep_minutes: int | None,
        engine: cs.ChronosEngine, cache: TruthCache) -> dict:
    spec = cs.VARIANTS[variant]
    mode = spec["mode"]
    t0 = time.monotonic()
    origin = pd.Timestamp(origin).tz_convert("UTC")
    meta = fetch_park(park_id)
    if meta is None:
        return {"status": "no_data", "reason": "unknown park or no timezone", "curves": []}
    local_today = origin.tz_convert(meta.timezone).date()
    need = cs.history_days_needed(mode, settings.CHRONOS_CONTEXT_SLOTS)
    future_dates = [local_today + dt.timedelta(days=i) for i in range(spec["lead_days"] + 1)]
    rides_all = fetch_rides(park_id)
    windows = fetch_windows(park_id, local_today - dt.timedelta(days=need + 1), future_dates[-1])

    past = past_truth(park_id, rides_all, local_today, need, cache, windows)
    # Today's own slots: read fresh, up to the origin, never cached.
    today_w = windows[windows["date"] == local_today]
    today = cs.truth_slots(
        fetch_readings(rides_all, today_w["open_utc"].min() - cs.STALE, origin)
        if len(today_w) else pd.DataFrame(columns=["attraction_id", "ts", "status", "wait"]),
        today_w,
    ) if len(today_w) else pd.DataFrame(columns=["attraction_id", "date", "slot_utc", "y"])
    truth = cs.cut_truth(pd.concat([past, today], ignore_index=True), origin)
    t_data = time.monotonic() - t0

    rides = cs.recent_rides(truth, origin, rides_all)
    countries = sorted({c for c in [meta.country, *[r.get("countryCode") for r in meta.influencing
                                                    if isinstance(r, dict)]] if c})
    holidays = fetch_holidays(countries)
    hol_through = cs.holidays_through(holidays, meta)
    # Day covariates for every date a context step can fall on: the grid's
    # off-window steps reach back 8192 × 15 min regardless of truth (the bench
    # had covariates for every calendar day).
    grid_start = (origin - cs.SLOT * settings.CHRONOS_CONTEXT_SLOTS).tz_convert(meta.timezone).date()
    first_ctx = min([grid_start, *([min(truth["date"])] if len(truth) else [])])
    span = [first_ctx + dt.timedelta(days=i) for i in range((future_dates[-1] - first_ctx).days + 1)]
    daycov = cs.day_covariates(meta, span, holidays)

    task = cs.build_task(mode, origin, meta.timezone, truth, windows, future_dates, daycov,
                         rides, settings.CHRONOS_CONTEXT_SLOTS)
    base = {
        "parkId": park_id, "variant": variant, "benchName": spec["bench"],
        "originAt": origin.isoformat().replace("+00:00", "Z"),
        "model": settings.CHRONOS_MODEL_ID, "revision": settings.CHRONOS_REVISION,
        "covariateNames": cs.cov_names(mode),
        "holidayDataThrough": hol_through.isoformat() if hol_through else None,
    }
    if task is None:
        return {**base, "status": "no_data", "reason": "no context or no published window ahead",
                "curves": [], "rides": 0}
    t1 = time.monotonic()
    q = engine.forecast(task)
    t_model = time.monotonic() - t1
    keep_until = origin + pd.Timedelta(minutes=keep_minutes) if keep_minutes else None
    rows = cs.postprocess(task, q, keep_until)

    def coverage(date: dt.date) -> dict:
        return {"holidays": cs.holiday_coverage(hol_through, date), "schedule": "published"}

    return {
        **base, "status": "ok", "rides": len(task.rides), "contextSteps": int(task.target.shape[1]),
        "horizonSteps": int(task.future_cov.shape[1]),
        "seconds": {"data": round(t_data, 2), "model": round(t_model, 2),
                    "total": round(time.monotonic() - t0, 2)},
        "device": engine.device, "peakVramGb": round(engine.peak_vram_gb, 2),
        "curves": cs.to_curves(rows, coverage),
    }
