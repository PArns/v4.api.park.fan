"""Data layer for the NeuralForecast service.

Builds the long-format daily-peak panel (unique_id=attraction, ds=park-local
day, y=daily P90 wait) and the holiday/calendar covariate frame used both for
the historical panel and for the future forecast window.

Timezone rule (repo-wide): every `ds` is a PARK-LOCAL day, never UTC.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from config import get_settings
from holiday_features import SchoolCoverage
from uuid_params import uuid_list

settings = get_settings()
logger = logging.getLogger("nf.db")


def get_engine():
    url = (
        f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        connect_args={
            # Name the session (all services share the `parkfan` role) and end a
            # transaction left idle for 10 min, which would otherwise hold its
            # locks indefinitely. See docs/troubleshooting/timescaledb-lock-queues.md.
            "application_name": "parkfan-nf-service",
            "options": "-c idle_in_transaction_session_timeout=600000",
        },
    )


_engine = get_engine()


def fetch_park_ids() -> list[str]:
    """The parks to train, in chunk order (see `order_parks`). A park is listed
    only if it has at least one row the panel could keep (OPERATING STANDBY wait
    >= NF_MIN_WAIT inside the window) — a superset of the panel's own filters, so
    no park with a series is dropped. Parks without any (status-only feeds) used
    to fill chunk slots and shift every later park into another chunk."""
    sql = text(
        f"""
        SELECT p.id::text AS pid, p."countryCode" AS country
        FROM parks p
        WHERE EXISTS (
            SELECT 1 FROM attractions a
            JOIN queue_data qd ON qd."attractionId" = a.id
            WHERE a."parkId" = p.id
              AND qd.timestamp >= NOW() - (:window || ' days')::interval
              AND qd.status = 'OPERATING'
              AND qd."queueType" = 'STANDBY'
              AND qd."waitTime" >= {settings.NF_MIN_WAIT}
        )
        """
    )
    with _engine.connect() as c:
        df = pd.read_sql(sql, c, params={"window": str(settings.NF_WINDOW_DAYS)})
    return order_parks(df.to_dict("records"))


def order_parks(rows: list[dict]) -> list[str]:
    """Park ids sorted by (country, park id), parks without a country last. Each
    chunk of NF_PARK_CHUNK_SIZE trains its own TFT, so membership is part of the
    model: sorting by country keeps a new park's shift inside its own country's
    chunks instead of moving every park after it in UUID order (PAR-1054)."""
    return [r["pid"] for r in sorted(rows, key=lambda r: (r.get("country") is None,
                                                         r.get("country") or "", r["pid"]))]


def fetch_attraction_meta(park_ids: list[str]) -> pd.DataFrame:
    """One row per attraction in scope: timezone, country/region, influencing
    regions (JSON). queue_data has no parkId, so attractions carry the link."""
    # An empty list means "every park"; a non-empty one that filters down to
    # nothing must still match nothing, so the WHERE follows the input.
    where = 'WHERE a."parkId" = ANY(CAST(:pids AS uuid[]))' if park_ids else ""
    sql = text(
        f"""
        SELECT a.id::text AS unique_id, a."parkId"::text AS park_id,
               p.timezone, p."countryCode" AS country, p."regionCode" AS region,
               p."influencingRegions" AS influencing, p.latitude::float AS latitude
        FROM attractions a
        JOIN parks p ON p.id = a."parkId"
        {where}
        """
    )
    with _engine.connect() as c:
        params = (
            {"pids": uuid_list(park_ids, "fetch_attraction_meta")} if park_ids else {}
        )
        df = pd.read_sql(sql, c, params=params)
    return df


def fetch_daily_peak_panel(park_ids: list[str]) -> pd.DataFrame:
    """Long panel: one (unique_id, ds) row per attraction per OPERATING local
    day, y = daily P90 of waitTime. Closed days are simply absent (we do NOT
    zero-fill — that would teach the model spurious zeros); with
    NF_CALENDAR_GRID, `to_calendar_grid` re-adds them as masked rows.

    Two filters hold regardless of that flag (PAR-1033 / PAR-1039):
    - park-local TODAY is excluded. Training runs 03:00 UTC, which is late
      morning in Tokyo/Shanghai: a partial day's P90 would become the last
      context value and a training target.
    - heartbeat rows are excluded: they copy the last status and wait with a
      fresh timestamp, so they are not observations (same rule as the forward
      archive's truth, `truthRowsSql`, PAR-1024). NULL = before the column."""
    pct = settings.NF_TARGET_PERCENTILE
    minw = settings.NF_MIN_WAIT
    window = settings.NF_WINDOW_DAYS
    where_park = 'AND a."parkId" = ANY(CAST(:pids AS uuid[]))' if park_ids else ""
    sql = text(
        f"""
        SELECT qd."attractionId"::text AS unique_id,
               DATE(qd.timestamp AT TIME ZONE p.timezone) AS ds,
               PERCENTILE_CONT({pct}) WITHIN GROUP (ORDER BY qd."waitTime") AS y,
               COUNT(*) AS n
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE qd.timestamp >= NOW() - (:window || ' days')::interval
          AND DATE(qd.timestamp AT TIME ZONE p.timezone) < DATE(NOW() AT TIME ZONE p.timezone)
          AND NOT COALESCE(qd.is_heartbeat, false)
          AND qd.status = 'OPERATING'
          AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" >= {minw}
          {where_park}
        GROUP BY 1, 2
        HAVING COUNT(*) >= 3
        """
    )
    params = {"window": str(window)}
    if park_ids:
        params["pids"] = uuid_list(park_ids, "fetch_daily_peak_panel")
    with _engine.connect() as c:
        df = pd.read_sql(sql, c, params=params)
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = df["y"].astype(float)
    return df[["unique_id", "ds", "y"]]


def fetch_holidays(countries: list[str]) -> pd.DataFrame:
    if not countries:
        return pd.DataFrame(columns=["date", "country", "region", "holiday_type"])
    sql = text(
        """
        SELECT date, country, region, "holidayType" AS holiday_type
        FROM holidays WHERE country = ANY(:countries)
        """
    )
    with _engine.connect() as c:
        df = pd.read_sql(sql, c, params={"countries": countries})
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def fetch_weather(park_ids: list[str]) -> pd.DataFrame:
    """Daily weather per (park, local day) for the weather futr_exog. Historical rows
    are retained (365d back) and the forecast reaches 16 days, so this covers both the
    training panel and the h=14 future frame. One row per (parkId, date) by PK."""
    # weather_data."parkId" is uuid in the DB; the panel's park key is text (meta
    # casts ::text). The SELECT casts the column for the join key; the filter casts
    # the PARAMETER, never the column, so the index stays usable (PAR-871).
    where = 'WHERE "parkId" = ANY(CAST(:pids AS uuid[]))' if park_ids else ""
    sql = text(
        f"""
        SELECT "parkId"::text AS park_id, date AS ds,
               "temperatureMax"::float AS temp_max,
               "precipitationSum"::float AS precip_mm,
               "windSpeedMax"::float AS wind_max
        FROM weather_data {where}
        """
    )
    with _engine.connect() as c:
        params = {"pids": uuid_list(park_ids, "fetch_weather")} if park_ids else {}
        df = pd.read_sql(sql, c, params=params)
    if df.empty:
        return df
    df["ds"] = pd.to_datetime(df["ds"]).dt.normalize()
    df["is_wet"] = (df["precip_mm"].fillna(0) >= 1.0).astype(int)
    return df.drop_duplicates(["park_id", "ds"])


def fetch_schedule(park_ids: list[str]) -> pd.DataFrame:
    """Published park-wide schedule per (park, local day), for NF_CALENDAR_GRID:
    sched_open = an OPERATING entry, sched_closed = a CLOSED entry and no
    OPERATING one. A day without an entry is neither — unknown, never closed.

    Also the OPERATING window as park-local NAIVE timestamps (open_local /
    close_local, datetime-handling §8) and `sched_asof` (PAR-1057): 1 when the
    window's last write (`updatedAt`) came before the day began — or, for today
    and later, before now — i.e. it was known at an origin before the day. The
    table keeps only the latest version of a day, so a re-sync that rewrote an
    unchanged row reads as unknown (conservative, as `add_asof` in ml-bench)."""
    where = 'AND se."parkId" = ANY(CAST(:pids AS uuid[]))' if park_ids else ""
    op = "se.\"scheduleType\" = 'OPERATING'"
    sql = text(
        f"""
        SELECT se."parkId"::text AS park_id, se.date AS ds,
               bool_or({op})::int AS sched_open,
               (bool_or(se."scheduleType" = 'CLOSED') AND NOT bool_or({op}))::int AS sched_closed,
               min(se."openingTime" AT TIME ZONE p.timezone) FILTER (WHERE {op}) AS open_local,
               max(se."closingTime" AT TIME ZONE p.timezone) FILTER (WHERE {op}) AS close_local,
               COALESCE(max(se."updatedAt") FILTER (WHERE {op})
                        < GREATEST(se.date::timestamp AT TIME ZONE p.timezone, now()),
                        false)::int AS sched_asof
        FROM schedule_entries se
        JOIN parks p ON p.id = se."parkId"
        WHERE se."attractionId" IS NULL
          AND se.date >= CURRENT_DATE - CAST(:window AS int)
          {where}
        GROUP BY 1, 2, p.timezone
        """
    )
    params = {"window": settings.NF_WINDOW_DAYS}
    if park_ids:
        params["pids"] = uuid_list(park_ids, "fetch_schedule")
    with _engine.connect() as c:
        df = pd.read_sql(sql, c, params=params)
    df["ds"] = pd.to_datetime(df["ds"]).dt.normalize()
    for col in ("open_local", "close_local"):
        df[col] = pd.to_datetime(df[col])
    return df


# PAR-1057: the grid shadow's extra inputs and target (the PAR-830 drivers). Set
# only by forecast.calendar_grid(..., drivers=True), i.e. inside the shadow run.
DRIVERS = False
CLOSED_DAYS_CAP = 14
_DRIVER_SCHEDULE_COLS = ["hours_ratio", "close_vs_ref", "closed_days_before", "sched_known"]
_DRIVER_NEUTRAL = {"hours_ratio": 1.0, "close_vs_ref": 0.0, "closed_days_before": 0, "sched_known": 0}


def schedule_drivers(schedule: pd.DataFrame | None) -> pd.DataFrame:
    """Per (park_id, ds) with a published window: the PAR-830 schedule drivers.

    - hours_ratio: the day's opening hours / their mean over the last four same
      weekdays (d-7 … d-28) that have a published window; 1.0 when unknown.
    - close_vs_ref: closing hour minus those days' mean closing hour, in hours
      (a past-midnight close counts past 24) — the late close; 0.0 when unknown.
    - closed_days_before: days without a published window since the previous
      one, capped at CLOSED_DAYS_CAP; > 0 is the first operating day after a
      closure. Only a gap BETWEEN two published windows counts; before the
      first one nothing is known (0).
    - sched_known: the day's window was published as of the origin
      (`sched_asof`). Where it is 0 every other column is neutral.
    """
    cols = ["park_id", "ds", *_DRIVER_SCHEDULE_COLS]
    if schedule is None or schedule.empty or "open_local" not in schedule.columns:
        return pd.DataFrame(columns=cols)
    w = schedule.dropna(subset=["open_local", "close_local"]).copy()
    w["hours"] = (w["close_local"] - w["open_local"]).dt.total_seconds() / 3600
    w["close_h"] = (w["close_local"] - w["ds"]).dt.total_seconds() / 3600
    w = w[w["hours"] > 0].drop_duplicates(["park_id", "ds"]).sort_values(["park_id", "ds"])
    w = w.reset_index(drop=True)
    if w.empty:
        return pd.DataFrame(columns=cols)
    idx = w.set_index(["park_id", "ds"])
    hsum = np.zeros(len(w))
    csum = np.zeros(len(w))
    n = np.zeros(len(w))
    for k in range(1, 5):
        key = pd.MultiIndex.from_arrays([w["park_id"], w["ds"] - pd.Timedelta(days=7 * k)])
        h = idx["hours"].reindex(key).to_numpy()
        c = idx["close_h"].reindex(key).to_numpy()
        ok = ~np.isnan(h)
        hsum += np.where(ok, h, 0.0)
        csum += np.where(ok, c, 0.0)
        n += ok
    known = w["sched_asof"].fillna(0).astype(int).to_numpy() == 1
    ref = known & (n > 0)
    nn = np.maximum(n, 1)
    w["hours_ratio"] = np.where(ref, w["hours"] / (hsum / nn), 1.0)
    w["close_vs_ref"] = np.where(ref, w["close_h"] - csum / nn, 0.0)
    gap = (w["ds"] - w.groupby("park_id")["ds"].shift()).dt.days - 1
    w["closed_days_before"] = np.where(known, gap.fillna(0).clip(0, CLOSED_DAYS_CAP), 0).astype(int)
    w["sched_known"] = known.astype(int)
    return w[cols]


def time_weighted_p90(
    readings: pd.DataFrame, windows: pd.DataFrame | None, pct: float, min_wait: float,
    max_stale_hours: float = 3, min_slots: int = 3,
) -> pd.DataFrame:
    """Daily P`pct` over a TIME-WEIGHTED 15-min grid, not over change-log rows.

    `queue_data` writes a row only when something changes, so a percentile over
    its rows weighs a jittery busy hour above a calm one. Here each quarter-hour
    of the day's operating window takes the last reading before its end — any
    status, so a DOWN/CLOSED row ends the carry — valid for at most `max_stale_hours`
    (the archive truth's 3 h rule); a slot whose value is OPERATING with a wait
    ≥ `min_wait` enters the percentile, as `rideQ90` in
    forecast-archive.service.ts does. The window is the park's published
    OPERATING window (open_local / close_local, park-local naive), cut at
    midnight; a park-day without one falls back to the span of the park's own
    OPERATING readings that day.

    `readings`: unique_id, park_id, ts (park-local naive), status, wait.
    Returns unique_id, ds, y for ride-days with ≥ `min_slots` counted slots."""
    out_cols = ["unique_id", "ds", "y"]
    if readings.empty:
        return pd.DataFrame(columns=out_cols)
    q = pd.Timedelta(minutes=15)
    day = pd.Timedelta(days=1)
    r = readings.sort_values("ts").reset_index(drop=True)
    r["ds"] = r["ts"].dt.normalize()
    rd = r[["unique_id", "park_id", "ds"]].drop_duplicates()
    # observed=True: ids/status may be categoricals (fetch_time_weighted_panel);
    # without it a groupby would build every id × day combination.
    span = (r[r["status"] == "OPERATING"].groupby(["park_id", "ds"], observed=True)["ts"]
            .agg(fb_open="min", fb_close="max").reset_index())
    span["fb_close"] += q
    rd = rd.merge(span, on=["park_id", "ds"], how="left")
    if windows is not None and not windows.empty and "open_local" in windows.columns:
        win = windows.dropna(subset=["open_local", "close_local"])
        rd = rd.merge(win[["park_id", "ds", "open_local", "close_local"]]
                      .drop_duplicates(["park_id", "ds"]), on=["park_id", "ds"], how="left")
    else:
        rd["open_local"] = rd["close_local"] = pd.NaT
    start = rd["open_local"].fillna(rd["fb_open"]).dt.floor("15min")
    end = rd["close_local"].fillna(rd["fb_close"]).clip(upper=rd["ds"] + day)
    n = np.ceil((end - start) / q).fillna(0).clip(lower=0).astype(int).to_numpy()
    rd = rd.assign(_start=start)[n > 0]
    n = n[n > 0]
    if rd.empty:
        return pd.DataFrame(columns=out_cols)
    g = rd.loc[rd.index.repeat(n), ["unique_id", "ds", "_start"]].reset_index(drop=True)
    offsets = np.arange(n.sum()) - np.repeat(np.cumsum(n) - n, n)
    g["t_end"] = g["_start"] + q * (offsets + 1)
    m = pd.merge_asof(
        g.sort_values("t_end"), r[["unique_id", "ts", "status", "wait"]],
        left_on="t_end", right_on="ts", by="unique_id", direction="backward",
        allow_exact_matches=False, tolerance=pd.Timedelta(hours=max_stale_hours),
    )
    m = m[(m["status"] == "OPERATING") & (m["wait"] >= min_wait)]
    grp = m.groupby(["unique_id", "ds"], observed=True)["wait"]
    # Linear interpolation, as PERCENTILE_CONT in fetch_daily_peak_panel.
    agg = pd.DataFrame({"y": grp.quantile(pct), "n": grp.size()}).reset_index()
    return agg.loc[agg["n"] >= min_slots, out_cols].reset_index(drop=True)


def fetch_time_weighted_panel(park_ids: list[str], schedule: pd.DataFrame | None) -> pd.DataFrame:
    """The grid shadow's target (PAR-1057): `time_weighted_p90` per ride-day over
    the rows `fetch_daily_peak_panel` reads (STANDBY, no heartbeat, before
    park-local today) — at every status, because a non-operating row is what
    ends the carry of the last wait."""
    where_park = 'AND a."parkId" = ANY(CAST(:pids AS uuid[]))' if park_ids else ""
    sql = text(
        f"""
        SELECT qd."attractionId"::text AS unique_id, a."parkId"::text AS park_id,
               qd.timestamp AT TIME ZONE p.timezone AS ts,
               qd.status::text AS status, qd."waitTime"::real AS wait
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE qd.timestamp >= NOW() - (:window || ' days')::interval
          AND DATE(qd.timestamp AT TIME ZONE p.timezone) < DATE(NOW() AT TIME ZONE p.timezone)
          AND NOT COALESCE(qd.is_heartbeat, false)
          AND qd."queueType" = 'STANDBY'
          {where_park}
        """
    )
    params = {"window": str(settings.NF_WINDOW_DAYS)}
    if park_ids:
        params["pids"] = uuid_list(park_ids, "fetch_time_weighted_panel")
    with _engine.connect() as c:
        r = pd.read_sql(sql, c, params=params)
    r["ts"] = pd.to_datetime(r["ts"])
    # Raw change-log rows (~3M per 10-park chunk) and their 15-min grid: as
    # categoricals the ids and status are int codes, not one Python str per row.
    r = r.astype({"unique_id": "category", "park_id": "category", "status": "category"})
    out = time_weighted_p90(r, schedule, settings.NF_TARGET_PERCENTILE, settings.NF_MIN_WAIT)
    out["unique_id"] = out["unique_id"].astype(str)
    out["ds"] = pd.to_datetime(out["ds"])
    out["y"] = out["y"].astype(float)
    import psutil
    logger.info("time-weighted panel: %d parks, %d readings -> %d ride-days, RSS %.2f GB",
                len(park_ids), len(r), len(out), psutil.Process().memory_info().rss / 1e9)
    return out


def park_local_today(meta: pd.DataFrame, now: pd.Timestamp | None = None) -> dict:
    """unique_id -> park-local today (naive midnight), from each park's timezone."""
    now = pd.Timestamp.now(tz="UTC") if now is None else now
    m = meta.drop_duplicates("unique_id").dropna(subset=["timezone"])
    by_tz = {tz: now.tz_convert(tz).normalize().tz_localize(None) for tz in m["timezone"].unique()}
    return {uid: by_tz[tz] for uid, tz in zip(m["unique_id"], m["timezone"])}


def to_calendar_grid(
    panel: pd.DataFrame, meta: pd.DataFrame, max_stale_days: int,
    now: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """NF_CALENDAR_GRID: every calendar day from a series' first observation to
    park-local YESTERDAY. Absent days get y=0 with available_mask=0 — NeuralForecast
    keeps them out of the scaler and the loss; y is 0, not NaN, because TFT never
    reads the mask in its forward pass and a NaN input would poison the batch.

    Every series then ends on the same park-local day, so build_future_frame's
    `last + 1` is park-local today for all of them. A series last observed more
    than `max_stale_days` ago is dropped (no forecast; CatBoost serves it)."""
    today = park_local_today(meta, now)
    one = pd.Timedelta(days=1)
    parts, stale = [], 0
    for uid, g in panel.groupby("unique_id"):
        t = today.get(uid)
        if t is None:
            continue
        if (t - g["ds"].max()).days > max_stale_days:
            stale += 1
            continue
        days = pd.date_range(g["ds"].min(), t - one, freq="D")
        y = g.set_index("ds")["y"].reindex(days)
        parts.append(pd.DataFrame({
            "unique_id": uid, "ds": days, "y": y.fillna(0.0).to_numpy(),
            "available_mask": y.notna().astype(float).to_numpy(),
            # The mask hides closed days from the scaler and loss, but TFT still
            # READS their y=0 as input; this hist_exog lets it tell closed from quiet.
            "is_observed": y.notna().astype(int).to_numpy(),
        }))
    logger.info("calendar grid: %d series kept, %d dropped as stale (>%dd)",
                len(parts), stale, max_stale_days)
    if not parts:
        return pd.DataFrame(columns=["unique_id", "ds", "y", "available_mask", "is_observed"])
    return pd.concat(parts, ignore_index=True)


# Non-ISO codes geocoding returns -> ISO 3166-2 suffix. Mirrors REGION_ALIASES in
# src/common/utils/region.util.ts and ml-service/holiday_utils.py.
_REGION_ALIASES = {"NRW": "NW", "NDS": "NI", "England": "ENG", "Scotland": "SCT", "Wales": "WLS"}


def _norm_region(r):
    """'DE-NW' / 'NW' / 'NRW' -> 'NW'; None/'' -> None."""
    if not r or not isinstance(r, str):
        return None
    short = r.split("-")[-1]
    return _REGION_ALIASES.get(short, short) or None


def add_calendar_covariates(
    df: pd.DataFrame, meta: pd.DataFrame, holidays: pd.DataFrame,
    weather: pd.DataFrame | None = None, schedule: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Add the futr_exog columns to a (unique_id, ds) frame (historical panel or
    future frame): calendar (incl. day-of-week one-hot), holiday flags + distance,
    and weather. All known-future within the h=14 horizon.

    Holiday flags are computed PER PARK (memoised by holiday signature) with
    vectorised isin — not per attraction with a row-wise apply over the whole
    holidays table. The old per-attraction path was O(attractions × holidays) and
    took >20 min on the ~2755-series all-parks panel; parks ≪ attractions and most
    share a signature, so this is orders of magnitude faster.
    """
    t0 = time.time()
    df = df.copy()
    ds = df["ds"]
    dow = ds.dt.dayofweek
    doy = ds.dt.dayofyear
    df["is_weekend"] = (dow >= 5).astype(int)
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    df["doy_sin"] = np.sin(2 * np.pi * doy / 365.0)
    df["doy_cos"] = np.cos(2 * np.pi * doy / 365.0)
    month = ds.dt.month
    df["season_code"] = (month % 12 // 3).astype(int)  # 0=winter..3=fall
    # Summer + December in the north; the same rule six months on in the south
    # (PAR-1033). Only south of 15°S: the tropics have no summer season to shift
    # (Jakarta −6°, Singapore +1°), so they keep the months they had. A park
    # without a latitude keeps them too.
    df["is_peak_season"] = month.isin([6, 7, 8, 12]).astype(int)
    # Day-of-week ONE-HOT: theme-park dow effect is step-like (Sa-peak), not smoothly
    # cyclical — explicit per-day columns help the model learn the jump directly
    # (sin/cos kept too; TFT's variable selection handles the redundancy).
    for k in range(7):
        df[f"dow_{k}"] = (dow == k).astype(int)

    for col in (
        "is_holiday_primary",
        "is_holiday_neighbor",
        "is_school_holiday",
        "is_bridge_day",
        "school_break_first_day",
        "school_break_last_day",
        "school_break_day_after",
    ):
        df[col] = 0
    # Holiday-distance defaults (capped at 30d). Filled per park inside the loop.
    df["days_until_holiday"] = 30.0
    df["days_since_holiday"] = 30.0
    df["days_until_school_holiday"] = 30.0
    df["days_since_school_holiday"] = 30.0
    # 1 where the park's own school-holiday data reaches the day (PAR-1049). NaN is
    # not allowed in a futr_exog, so an unpublished break keeps the placeholder
    # values (flag 0, distances 30) and this column says they are placeholders.
    df["school_holiday_known"] = 0

    # Per-series → park map (used by both the holiday loop and the weather merge), so
    # weather still attaches even when there are no holidays for the scope.
    df["_park"] = None
    meta_by_id = None
    if "park_id" in meta.columns:
        meta_by_id = meta.drop_duplicates("unique_id").set_index("unique_id")
        df["_park"] = df["unique_id"].map(meta_by_id["park_id"].to_dict())
        if "latitude" in meta_by_id.columns:
            south = df["unique_id"].map(meta_by_id["latitude"].to_dict()).lt(-15).to_numpy()
            df.loc[south, "is_peak_season"] = month[south].isin([12, 1, 2, 6]).astype(int)

    n_parks = 0
    if not holidays.empty and meta_by_id is not None:
        # Prepare holiday helpers ONCE (was recomputed for every attraction).
        h = holidays.copy()
        h["region_norm"] = h["region"].map(_norm_region)
        h["cr"] = list(zip(h["country"], h["region_norm"]))
        h_pub_or_school = h["holiday_type"].isin(["public", "school"])

        park_rep = meta.drop_duplicates("park_id").set_index("park_id")

        cache: dict = {}  # signature -> (local_public, neighbor, school, bridge, school_through)
        coverage = SchoolCoverage(h)

        def _sets_for_park(pid):
            row = park_rep.loc[pid]
            country, region = row["country"], row["region"]
            influencing = row.get("influencing") or []
            if isinstance(influencing, str):
                import json

                try:
                    influencing = json.loads(influencing)
                except Exception:
                    influencing = []
            # Normalise like the holiday side (h["cr"] holds 'NW', influencingRegions
            # holds 'DE-NW'): compared raw, no regional neighbour ever matched (PAR-816).
            neigh = {
                (d.get("countryCode"), _norm_region(d.get("regionCode")))
                for d in influencing
                if isinstance(d, dict)
            }
            countries_wide = {c for (c, r) in neigh if r is None}
            sig = (country, region, frozenset(neigh))
            if sig in cache:
                return cache[sig]
            rn = _norm_region(region)
            local = h[(h["country"] == country) & (h["region"].isna() | (h["region_norm"] == rn))]
            local_public = set(local[local["holiday_type"].isin(["public", "bank"])]["date"])
            school = set(local[local["holiday_type"] == "school"]["date"])
            bridge = set(local[local["holiday_type"] == "bridge"]["date"])
            # Neighbor: vectorised — (country,region_norm) in neigh OR country specified region-wide.
            # A regionally specified neighbour also has its country's NATIONAL rows
            # (NL-LI gets Koningsdag, stored as (NL, None)) — regional OR national,
            # as ml-service/holiday_features.py does (PAR-816).
            neigh_countries = {c for (c, _r) in neigh}
            nmask = (
                h["cr"].isin(neigh)
                | h["country"].isin(countries_wide)
                | (h["region_norm"].isna() & h["country"].isin(neigh_countries))
            )
            neighbor = set(h[nmask & h_pub_or_school]["date"])
            cache[sig] = (local_public, neighbor, school, bridge, coverage.through(country, rn))
            return cache[sig]

        groups = df.groupby("_park").groups
        total = len(groups)
        for pid, idx in groups.items():
            if pid not in park_rep.index:
                continue
            lp, nb, sc, br, sc_through = _sets_for_park(pid)
            d = df.loc[idx, "ds"].dt.normalize()
            df.loc[idx, "is_holiday_primary"] = d.isin(lp).astype(int).values
            df.loc[idx, "is_holiday_neighbor"] = d.isin(nb).astype(int).values
            df.loc[idx, "is_school_holiday"] = d.isin(sc).astype(int).values
            df.loc[idx, "is_bridge_day"] = d.isin(br).astype(int).values
            # Distance to the nearest public / school holiday, capped at 30d.
            dv = d.values.astype("datetime64[ns]")
            for dates, cu, cs in (
                (lp, "days_until_holiday", "days_since_holiday"),
                (sc, "days_until_school_holiday", "days_since_school_holiday"),
            ):
                if dates:
                    until, since = _holiday_distance(dv, dates)
                    df.loc[idx, cu] = until
                    df.loc[idx, cs] = since
            if sc_through is None:
                known = pd.Series(False, index=d.index)
            else:
                known = d <= pd.Timestamp(sc_through)
            df.loc[idx, "school_holiday_known"] = known.astype(int).values
            # PAR-1057: the CHANGE of school-break state, own region. A day it
            # reads past the coverage (d for all three, d+1 for the last day)
            # makes it 0 — school_holiday_known says so.
            one = pd.Timedelta(days=1)
            known_next = (d + one <= pd.Timestamp(sc_through)) if sc_through is not None else known
            on, on_prev, on_next = d.isin(sc), (d - one).isin(sc), (d + one).isin(sc)
            df.loc[idx, "school_break_first_day"] = (known & on & ~on_prev).astype(int).values
            df.loc[idx, "school_break_last_day"] = (known_next & on & ~on_next).astype(int).values
            df.loc[idx, "school_break_day_after"] = (known & ~on & on_prev).astype(int).values
            unknown_idx = known.index[~known.to_numpy()]
            df.loc[unknown_idx, ["days_until_school_holiday", "days_since_school_holiday"]] = 30.0
            n_parks += 1
            if n_parks % 50 == 0:
                logger.info(
                    "  calendar covariates: %d/%d parks (%d sigs, %.1fs)",
                    n_parks, total, len(cache), time.time() - t0,
                )

    df["holiday_count_total"] = (
        df["is_holiday_primary"]
        + df["is_holiday_neighbor"]
        + df["is_school_holiday"]
        + df["is_bridge_day"]
    )

    # --- Weather futr_exog (temp / precip / wind + derived is_wet) ---
    wcols = ["temp_max", "precip_mm", "wind_max", "is_wet"]
    for col in wcols:
        df[col] = np.nan
    if weather is not None and not weather.empty and df["_park"].notna().any():
        w = weather.drop_duplicates(["park_id", "ds"]).set_index(["park_id", "ds"])
        key = pd.MultiIndex.from_arrays([df["_park"], df["ds"].dt.normalize()])
        for col in wcols:
            df[col] = w[col].reindex(key).to_numpy()
    # Fill gaps per park first (don't bleed one park's climate into another), then a
    # global-median fallback, then sane constants for parks with no weather at all.
    for col, const in (("temp_max", 15.0), ("precip_mm", 0.0), ("wind_max", 10.0)):
        df[col] = df.groupby("_park")[col].transform(lambda s: s.fillna(s.median()))
        df[col] = df[col].fillna(df[col].median()).fillna(const)
    df["is_wet"] = df["is_wet"].fillna(0).astype(int)

    # --- Published schedule (NF_CALENDAR_GRID only; both 0 = unknown) ---
    if schedule is not None:
        for col in ("sched_open", "sched_closed"):
            df[col] = 0
        if not schedule.empty and df["_park"].notna().any():
            s = schedule.drop_duplicates(["park_id", "ds"]).set_index(["park_id", "ds"])
            key = pd.MultiIndex.from_arrays([df["_park"], df["ds"].dt.normalize()])
            for col in ("sched_open", "sched_closed"):
                df[col] = s[col].reindex(key).fillna(0).astype(int).to_numpy()
        # PAR-1057 drivers (read only by the shadow); neutral where unknown.
        drv = schedule_drivers(schedule).set_index(["park_id", "ds"])
        key = pd.MultiIndex.from_arrays([df["_park"], df["ds"].dt.normalize()])
        for col, neutral in _DRIVER_NEUTRAL.items():
            df[col] = drv[col].reindex(key).fillna(neutral).to_numpy() if len(drv) else neutral

    if "_park" in df.columns:
        df = df.drop(columns=["_park"])
    logger.info(
        "calendar covariates done: %d parks, %d rows in %.1fs",
        n_parks, len(df), time.time() - t0,
    )
    return df


def _holiday_distance(dv: np.ndarray, dates: set) -> tuple[np.ndarray, np.ndarray]:
    """Days until / since the nearest date in `dates`, clipped to [0, 30].
    searchsorted on the sorted holiday array — vectorised, ~O(n log h)."""
    hol_sorted = np.sort(np.array(list(dates), dtype="datetime64[ns]"))
    day = np.timedelta64(1, "D")
    until = np.full(len(dv), 30.0)
    pos = np.searchsorted(hol_sorted, dv, side="left")
    hasn = pos < len(hol_sorted)
    until[hasn] = (hol_sorted[pos[hasn]] - dv[hasn]) / day
    since = np.full(len(dv), 30.0)
    pos2 = np.searchsorted(hol_sorted, dv, side="right") - 1
    hasp = pos2 >= 0
    since[hasp] = (dv[hasp] - hol_sorted[pos2[hasp]]) / day
    return np.clip(until, 0, 30), np.clip(since, 0, 30)


FUTR_EXOG = [
    # holidays
    "is_holiday_primary",
    "is_holiday_neighbor",
    "is_school_holiday",
    "is_bridge_day",
    "holiday_count_total",
    "days_until_holiday",
    "days_since_holiday",
    # calendar
    "is_weekend",
    "dow_sin",
    "dow_cos",
    "doy_sin",
    "doy_cos",
    "season_code",
    "is_peak_season",
    # day-of-week one-hot
    "dow_0",
    "dow_1",
    "dow_2",
    "dow_3",
    "dow_4",
    "dow_5",
    "dow_6",
    # weather
    "temp_max",
    "precip_mm",
    "wind_max",
    "is_wet",
]

# NF_CALENDAR_GRID adds the published schedule and the school-holiday distance.
# Kept out of FUTR_EXOG so the default model's inputs stay exactly as they were.
FUTR_EXOG_GRID = FUTR_EXOG + [
    "sched_open",
    "sched_closed",
    "days_until_school_holiday",
    "days_since_school_holiday",
    "school_holiday_known",
]


# PAR-1057: the PAR-830 drivers, in the grid shadow only (`DRIVERS`), never in
# the served model. All NaN-free: unknown is the neutral value plus a flag
# (sched_known; school_holiday_known for the school-break changes).
DRIVER_EXOG = [
    *_DRIVER_SCHEDULE_COLS,
    "school_break_first_day",
    "school_break_last_day",
    "school_break_day_after",
]


def futr_exog() -> list[str]:
    if not settings.NF_CALENDAR_GRID:
        return FUTR_EXOG
    return FUTR_EXOG_GRID + DRIVER_EXOG if DRIVERS else FUTR_EXOG_GRID


def hist_exog() -> list[str]:
    """Past-only inputs: with the grid, whether the day was observed at all."""
    return ["is_observed"] if settings.NF_CALENDAR_GRID else []


# The served forecast and the two grid shadows (PAR-1041 grid only, PAR-1057
# grid + drivers). One table each because the key has no model_version: two
# variants of one night cannot share a row. The
# shadows are `LIKE` the served table so the schemas cannot drift apart; the
# literal statements are also what the E2E schema reads (test/helpers/
# ml-forecast-tables.ts), and the dict is the whitelist for the table name.
SHADOW_TABLE = "tft_forecasts_shadow"
SHADOW_DRIVERS_TABLE = "tft_forecasts_shadow_drivers"
_FORECAST_DDL = {
    "tft_forecasts": """
        CREATE TABLE IF NOT EXISTS tft_forecasts (
            attraction_id   uuid NOT NULL,
            target_date     date NOT NULL,
            forecast_date   date NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')::date,
            predicted_peak  double precision NOT NULL,
            model_version   text,
            created_at      timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (attraction_id, target_date, forecast_date)
        )
        """,
    SHADOW_TABLE: """
        CREATE TABLE IF NOT EXISTS tft_forecasts_shadow (LIKE tft_forecasts INCLUDING ALL)
        """,
    SHADOW_DRIVERS_TABLE: """
        CREATE TABLE IF NOT EXISTS tft_forecasts_shadow_drivers (LIKE tft_forecasts INCLUDING ALL)
        """,
}


def forecast_sql(table: str) -> tuple[str, str]:
    """(DDL, upsert) for one of the forecast tables; any other name raises."""
    if table not in _FORECAST_DDL:
        raise ValueError(f"not a TFT forecast table: {table!r}")
    upsert = f"""
        INSERT INTO {table}
            (attraction_id, target_date, forecast_date, predicted_peak, model_version)
        VALUES
            (:aid, :td, :fd, :pp, :ver)
        ON CONFLICT (attraction_id, target_date, forecast_date)
        DO UPDATE SET predicted_peak = EXCLUDED.predicted_peak,
                      model_version  = EXCLUDED.model_version,
                      created_at     = now()
        """
    return _FORECAST_DDL[table], upsert


def persist_forecast(
    yhat: pd.DataFrame, version: str, value_col: str, forecast_date: date,
    table: str = "tft_forecasts",
) -> int:
    """Store the forward daily-peak forecast so it can be scored against actuals
    once each target date passes (the forward-scoreboard vs CatBoost).

    One row per (attraction, target_date, forecast_date). `forecast_date` is the
    day the run that PRODUCED `yhat` made it, passed in by the caller — never the
    day this function happens to be called. Writing `now()` here is what let a
    re-persist of yesterday's cached parquet land under today's date (PAR-814):
    the reader and the scoreboard both take forecast_date as the forecast's age.
    Re-running for the same forecast_date overwrites that day's rows; past
    forecast_dates are immutable, so the genuine forward record is preserved.

    `value_col` is the MEDIAN/point forecast column. Because the model's target is
    the daily P90 (NF_TARGET_PERCENTILE), the persisted predicted_peak = E[daily-P90]
    (the expected/typical daily-peak), NOT a P90/upper quantile of the forecast
    distribution. It is scored against the realised daily P90 (apples-to-apples).
    """
    rows = yhat[["unique_id", "ds", value_col]].copy()
    rows = rows.rename(columns={"unique_id": "attraction_id", "ds": "target_date"})
    rows["target_date"] = pd.to_datetime(rows["target_date"]).dt.date
    rows["predicted_peak"] = rows[value_col].astype(float).clip(lower=0)

    ddl_sql, upsert_sql = forecast_sql(table)
    ddl, upsert = text(ddl_sql), text(upsert_sql)
    params = [
        {"aid": str(r.attraction_id), "td": r.target_date, "fd": forecast_date,
         "pp": float(r.predicted_peak), "ver": version}
        for r in rows.itertuples(index=False)
    ]
    with _engine.begin() as c:
        c.execute(ddl)
        # Bulk executemany in chunks (a forecast can be ~250k rows across all parks;
        # one execute() per row would be a quarter-million round-trips).
        for i in range(0, len(params), 5000):
            c.execute(upsert, params[i : i + 5000])
    return len(params)


def build_future_frame(
    panel: pd.DataFrame, meta: pd.DataFrame, holidays: pd.DataFrame, horizon: int,
    weather: pd.DataFrame | None = None, schedule: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Exactly `horizon` future daily rows per unique_id with all FUTR_EXOG
    columns filled (NeuralForecast requires futr_df = h rows/series).

    Starts at each series' `last ds + 1`, which NeuralForecast requires. On the
    default operating-days panel that is in the past for a ride last seen weeks
    ago (and its frame then reaches only `horizon - gap` days ahead); with
    NF_CALENDAR_GRID every series ends park-local yesterday (`to_calendar_grid`),
    so every frame starts park-local today (PAR-1033)."""
    rows = []
    for uid, g in panel.groupby("unique_id"):
        last = g["ds"].max()
        future_ds = pd.date_range(last + pd.Timedelta(days=1), periods=horizon, freq="D")
        rows.append(pd.DataFrame({"unique_id": uid, "ds": future_ds}))
    fut = pd.concat(rows, ignore_index=True)
    return add_calendar_covariates(fut, meta, holidays, weather, schedule)
