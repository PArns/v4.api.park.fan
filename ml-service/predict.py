"""
Prediction logic for hourly and daily forecasts
"""

import threading

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from sqlalchemy import text

from model import WaitTimeModel
from percentile_features import add_percentile_features
from db import (
    fetch_parks_metadata,
    get_db,
    fetch_holidays,
    fetch_school_holiday_coverage,
    convert_df_types,
    fetch_attraction_ids_for_park,
    FROZEN_OUT_OF_WINDOW_SQL,
    MEASURED_DAY_SCHEDULE_SQL,
)
from config import get_settings
from features import (
    LAST_YEAR_LAG,
    LAST_YEAR_MIN_LEAD,
    LAST_YEAR_MIN_OBSERVED_DAYS,
    LAST_YEAR_WATCH_LOOKBACK_DAYS,
    add_attraction_type_features,
)
from uuid_params import uuid_list
from holiday_features import (
    SchoolCoverage,
    assign_holiday_features,
    holiday_fetch_window,
    parse_influencing_regions,
)

settings = get_settings()

# Cache for weather historical data (1 hour TTL)
_weather_historical_cache = {}
_weather_historical_cache_ttl = 3600  # 1 hour


def round_to_nearest_5(value: float) -> int:
    """
    Round wait time to nearest 5 minutes for UX consistency

    Theme parks typically display wait times in 5-minute increments.
    This provides better user experience and consistency with actual queue displays.

    Args:
        value: Raw prediction value (any float)

    Returns:
        Rounded integer in 5-minute increments

    Examples:
        >>> round_to_nearest_5(7.2)
        5
        >>> round_to_nearest_5(8.9)
        10
        >>> round_to_nearest_5(12.4)
        10
        >>> round_to_nearest_5(34.7)
        35
    """
    if value < 2.5:
        return 0  # Very short wait → 0

    # Add 2.5 and floor divide by 5, then multiply by 5
    # This ensures consistent rounding: 2.5→5, 7.5→10, 12.5→15
    return int((value + 2.5) // 5) * 5


def quantile_minutes(
    quantiles: dict[float, Any], i: int
) -> tuple[int | None, int | None]:
    """Row ``i``'s q0.8 and q0.95 as whole minutes, for the API's accuracy rows.

    Only an alpha the model was actually trained for counts (within 0.01): the
    API stores the value as ``predicted_q80`` / ``predicted_q95`` and measures
    how often the actual lands below it, so a neighbouring quantile standing in
    would report the wrong coverage. ``None`` for a missing alpha, a model
    without a quantile head (``{}``) or a non-finite value.
    """

    def pick(alpha: float) -> int | None:
        for a, values in quantiles.items():
            if abs(float(a) - alpha) <= 0.01:
                v = float(values[i])
                return round(v) if np.isfinite(v) and v >= 0 else None
        return None

    return pick(0.8), pick(0.95)


# Caches for the two history reads behind the prediction features (short-lived).
# Together they were the single biggest steady-state DB load. History barely moves
# within minutes and the real-time signal arrives separately via current_wait_times,
# so both are cached for 15 min. The key buckets end_time to the same window — with
# base_time=now() carrying microsecond precision the key would never repeat.
_recent_wait_times_cache = {}
_recent_rolling_stats_cache = {}
_recent_wait_times_cache_ttl = 900  # 15 minutes
# Serializes eviction and insertion on the two caches above. Each gunicorn worker
# is its own PROCESS with its own copy of these dicts, so workers never race each
# other here; the race is between the THREADS of one worker, because the sync
# `def predict` handlers run in FastAPI's threadpool. Lookups stay lock-free.
_recent_cache_lock = threading.Lock()

# How far back the hour-by-hour lookups reach. Every lookup in
# create_prediction_features is keyed on a local date at most 30 days before
# base_time or before a prediction timestamp (yesterday, same hour last week,
# same hour last month = base - 30 d, same weekday 1-4 weeks back), so 32 days
# covers all of them with a day of timezone slack on each side.
RECENT_LOOKUP_DAYS = 32

# The longest rolling window the model was trained on (rolling_avg_90d).
ROLLING_STATS_DAYS = 90

# How far ahead of base_time a row may be fed the live wait as its
# `avg_wait_last_1h` and the live momentum as `wait_time_velocity` (PAR-1030).
# Training reads avg_wait_last_1h as the mean of [t - 1 h, t): the live wait IS
# that for a row up to an hour ahead, and stays its best estimate for a few
# hours more - the served persistence blend measured the current wait helping
# out to 3 h (applyPcnIntradayOverride). Past that the same-hour profile is the
# better proxy for [t - 1 h, t), so rows beyond get the profile >24 h rows get.
LIVE_LAG_MAX_LEAD = timedelta(hours=3)


def live_wait_velocity(current_wait: float, earlier_hourly_waits) -> float:
    """`wait_time_velocity` as training defines it, with the live wait as the
    latest hour.

    Training (features.add_historical_features) takes, on the hourly series,
    the mean of the last six first differences ending one row before the
    target: (x[i-1] - x[i-7]) / 6, fewer when the series is shorter. Here
    x[i-1] is the live wait and `earlier_hourly_waits` the hourly values
    before the current hour, oldest first. It used to be (now - 30 min ago) / 6,
    a twelfth of training's per-hour scale.
    """
    earlier = [w for w in earlier_hourly_waits if w is not None and not pd.isna(w)]
    if not earlier:
        return 0.0
    k = min(6, len(earlier))
    return (float(current_wait) - float(earlier[-k])) / k

# Rows the serving features are built from. Shared by both reads so the lookups
# and the rolling statistics see the same population.
#
# It mirrors db.fetch_training_data's row filter (PAR-943): training reads
# OPERATING rows with `waitTime >= 5`, minus frozen readings outside the
# published window (PAR-1052), on days with no schedule or an UNKNOWN
# one, so serving must too — the lags and rolling averages were learnt on that
# population. shortcut: training additionally requires its park-wide
# operating_days_heuristic on no-schedule/UNKNOWN days; replicating that CTE here
# would scan every ride of the park per request. A day on which this ride posted
# a >= 5 min wait passes that heuristic in practice; port the CTE if the two
# populations are ever measured apart.
_SERVING_QUEUE_FILTER = f"""
            qd."attractionId" = ANY(CAST(:attraction_ids AS uuid[]))
                AND qd."waitTime" IS NOT NULL
                AND qd."waitTime" >= 5
                AND qd.status = 'OPERATING'
                AND {FROZEN_OUT_OF_WINDOW_SQL}
                AND qd."queueType" = 'STANDBY'
                AND NOT COALESCE(qd.is_heartbeat, false)
                AND (
                    se.id IS NULL
                    OR se."scheduleType" IN ('OPERATING', 'UNKNOWN')
                    -- a day measured to have run, as training admits (PAR-1054)
                    OR ({MEASURED_DAY_SCHEDULE_SQL} AND EXISTS (
                        SELECT 1 FROM park_day_operations pdo
                        WHERE pdo.park_id = se."parkId" AND pdo.day = se.date
                            AND pdo.measured_operation
                    ))
                )
"""


def _evict_expired_entries(cache: dict, now_ts: float, ttl: float) -> None:
    """Drop cache buckets older than `ttl`, tolerating concurrent mutation.

    The cache is a module-level dict shared by the threadpool threads of one
    worker process (sync `def predict` handlers). Two races follow from that:

    - Iterating `cache.items()` while another thread inserts a bucket raised
      "dictionary changed size during iteration" and failed the whole request
      with an HTTP 500 (PAR-815). The iteration now runs over a snapshot taken
      in one C-level call, and `_cached_read` holds `_recent_cache_lock` around
      eviction and insertion.
    - A key collected as expired can already be gone by the time we act on it;
      `del` raised KeyError for the thread that lost that race (2026-08-31).
      `pop(..., None)` makes losing it a no-op — the entry we wanted gone is gone.
    """
    for key in [k for k, (_, ts) in list(cache.items()) if now_ts - ts >= ttl]:
        cache.pop(key, None)


def _bucket_end_time(end_time: Optional[datetime]) -> datetime:
    """end_time floored to the cache-TTL window: one SQL bound and one cache key
    per window. Defaults to now."""
    end_time = end_time or datetime.now(timezone.utc)
    bucket = int(end_time.timestamp()) // _recent_wait_times_cache_ttl
    return datetime.fromtimestamp(
        bucket * _recent_wait_times_cache_ttl,
        tz=end_time.tzinfo or timezone.utc,
    )


def _cached_read(cache: dict, cache_key: str, read) -> pd.DataFrame:
    import time

    hit = cache.get(cache_key)
    if hit is not None and time.time() - hit[1] < _recent_wait_times_cache_ttl:
        return hit[0].copy()
    df = read()
    now_ts = time.time()
    entry = (df.copy(), now_ts)
    with _recent_cache_lock:
        _evict_expired_entries(cache, now_ts, _recent_wait_times_cache_ttl)
        cache[cache_key] = entry
    return df


def fetch_recent_wait_times(
    attraction_ids: List[str],
    lookback_days: int = RECENT_LOOKUP_DAYS,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Hour-by-hour history for the date/hour lookups, in park-local time.

    One row per (attraction, local date, local hour) with the hour's average
    wait. The rolling statistics are NOT here — see fetch_recent_rolling_stats.

    This used to read 730 days and compute nine ROWS-based window functions
    over them, of which the caller then read `.iloc[-1]` — under
    `ORDER BY date DESC` the OLDEST row. Every served rolling_avg_* and
    volatility_* was therefore the value at the start of the two-year window
    (Taron, 2026-10-01: rolling_avg_7d served 54.75 from 2025-12-26 against
    28.8 now), and the query was ~2/3 of all Postgres execution time.
    """
    if not attraction_ids:
        return pd.DataFrame()

    query_end_time = _bucket_end_time(end_time)
    cache_key = f"{','.join(sorted(attraction_ids))}:{lookback_days}:{query_end_time}"

    def read() -> pd.DataFrame:
        query = text(
            f"""
            SELECT
                qd."attractionId"::text as "attractionId",
                a.name as "attractionName",
                DATE(qd.timestamp AT TIME ZONE p.timezone) as date,
                EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE p.timezone) as hour,
                EXTRACT(DOW FROM qd.timestamp AT TIME ZONE p.timezone) as day_of_week,
                AVG(qd."waitTime") as avg_wait,
                COUNT(*) as data_points
            FROM queue_data qd
            INNER JOIN attractions a ON a.id = qd."attractionId"
            INNER JOIN parks p ON p.id = a."parkId"
            LEFT JOIN schedule_entries se
                ON se."parkId" = a."parkId"
                AND se.date = DATE(qd.timestamp AT TIME ZONE p.timezone)
                AND se."attractionId" IS NULL
            WHERE {_SERVING_QUEUE_FILTER}
                AND qd.timestamp >= CAST(:end_time AS timestamptz) - :lookback_days * INTERVAL '1 day'
                AND qd.timestamp < :end_time
            GROUP BY qd."attractionId", a.name, DATE(qd.timestamp AT TIME ZONE p.timezone),
                     EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE p.timezone),
                     EXTRACT(DOW FROM qd.timestamp AT TIME ZONE p.timezone)
            ORDER BY "attractionId", date, hour
            """
        )
        with get_db() as db:
            result = db.execute(
                query,
                {
                    "attraction_ids": uuid_list(
                        attraction_ids, "fetch_recent_wait_times"
                    ),
                    "lookback_days": lookback_days,
                    "end_time": query_end_time,
                },
            )
            return convert_df_types(
                pd.DataFrame(result.fetchall(), columns=result.keys())
            )

    return _cached_read(_recent_wait_times_cache, cache_key, read)


def fetch_recent_rolling_stats(
    attraction_ids: List[str],
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    The rolling features at end_time, one row per attraction, indexed by id.

    Mirrors features.add_historical_features, which the model is trained on:
    the series is one MEDIAN wait per UTC hour, each window is time-based and
    left-closed ([t - N days, t)), weekday/weekend split on the park-LOCAL
    weekday (Mon-Fri vs Sat-Sun; it was the UTC one on both sides until PAR-1032) and the std is the sample std (pandas ddof=1, NULL
    below two values — training's min_periods=2). Columns: attractionName,
    rolling_avg_7d/_weekday/_weekend/_28d/_90d, rolling_std_7d/_weekday/_weekend.
    A window with no data yields NaN; the caller applies its fallbacks.
    """
    if not attraction_ids:
        return pd.DataFrame()

    query_end_time = _bucket_end_time(end_time)
    cache_key = f"{','.join(sorted(attraction_ids))}:{query_end_time}"

    def read() -> pd.DataFrame:
        query = text(
            f"""
            WITH hourly AS (
                SELECT
                    qd."attractionId"::text AS "attractionId",
                    a.name AS name,
                    p.timezone AS tz,
                    DATE_TRUNC('hour', qd.timestamp) AS ts,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") AS w
                FROM queue_data qd
                INNER JOIN attractions a ON a.id = qd."attractionId"
                INNER JOIN parks p ON p.id = a."parkId"
                LEFT JOIN schedule_entries se
                    ON se."parkId" = a."parkId"
                    AND se.date = DATE(qd.timestamp AT TIME ZONE p.timezone)
                    AND se."attractionId" IS NULL
                WHERE {_SERVING_QUEUE_FILTER}
                    AND qd.timestamp >= CAST(:end_time AS timestamptz) - :days * INTERVAL '1 day'
                    AND qd.timestamp < :end_time
                GROUP BY 1, 2, 3, 4
            ),
            tagged AS (
                SELECT
                    "attractionId",
                    name,
                    w,
                    ts >= CAST(:end_time AS timestamptz) - INTERVAL '7 days' AS in_7d,
                    ts >= CAST(:end_time AS timestamptz) - INTERVAL '28 days' AS in_28d,
                    EXTRACT(ISODOW FROM ts AT TIME ZONE tz) >= 6 AS is_weekend
                FROM hourly
            )
            SELECT
                "attractionId",
                MAX(name) AS "attractionName",
                AVG(w) FILTER (WHERE in_7d) AS rolling_avg_7d,
                AVG(w) FILTER (WHERE in_7d AND NOT is_weekend) AS rolling_avg_weekday,
                AVG(w) FILTER (WHERE in_7d AND is_weekend) AS rolling_avg_weekend,
                AVG(w) FILTER (WHERE in_28d) AS rolling_avg_28d,
                AVG(w) AS rolling_avg_90d,
                STDDEV_SAMP(w) FILTER (WHERE in_7d) AS rolling_std_7d,
                STDDEV_SAMP(w) FILTER (WHERE in_7d AND NOT is_weekend) AS rolling_std_weekday,
                STDDEV_SAMP(w) FILTER (WHERE in_7d AND is_weekend) AS rolling_std_weekend
            FROM tagged
            GROUP BY "attractionId"
            """
        )
        with get_db() as db:
            result = db.execute(
                query,
                {
                    "attraction_ids": uuid_list(
                        attraction_ids, "fetch_recent_rolling_stats"
                    ),
                    "days": ROLLING_STATS_DAYS,
                    "end_time": query_end_time,
                },
            )
            df = convert_df_types(
                pd.DataFrame(result.fetchall(), columns=result.keys())
            )
        if df.empty:
            return df
        df = df.set_index("attractionId")
        stat_cols = [c for c in df.columns if c != "attractionName"]
        df[stat_cols] = df[stat_cols].astype(float)
        return df

    return _cached_read(_recent_rolling_stats_cache, cache_key, read)


# avg_wait_same_dow_4w for a row more than 28 days ahead: all four weekly lags
# lie after base_time, so the 32-day lookup cannot hold any. Such a row reads
# the same weekday-and-hour 52 weeks before it instead (PAR-1045) — but only for
# a ride watched at least LAST_YEAR_MIN_OBSERVED_DAYS before base_time, and only
# where that slot holds a reading. Otherwise rolling_avg_7d, as before: an
# unobserved week is never made into a value.
# shortcut: queue_data starts 2025-12-24, so rides watched since then pass the
# gate from ~2026-11-19 and the whole 29-365 d daily horizon has a slot a year
# back from ~2026-11-24; parks added later stay on rolling_avg_7d until their
# own 330 days. Revisit the 28-day cut (rows 8-28 d use a partial mean) then.
# The constants live in features.py so train.as_known_at_origin reads the same.


def last_year_slot_keys(df: pd.DataFrame, base_time: datetime) -> list:
    """(attractionId, local date, local hour) 52 weeks before each row further
    than LAST_YEAR_MIN_LEAD ahead — the only slots fetch_last_year_slots reads."""
    base = pd.Timestamp(base_time)
    base = base.tz_localize("UTC") if base.tzinfo is None else base.tz_convert("UTC")
    ts = pd.to_datetime(df["timestamp"], utc=True)
    far = df[ts > base + LAST_YEAR_MIN_LEAD]
    if far.empty:
        return []
    local = pd.to_datetime(far["local_timestamp"]) - LAST_YEAR_LAG
    return sorted(
        set(zip(far["attractionId"].astype(str), local.dt.date, local.dt.hour.astype(int)))
    )


def fetch_last_year_slots(keys: list, end_time: datetime) -> Dict[tuple, float]:
    """Mean wait per (attractionId, local date, local hour) key, in the serving
    population, for rides with a reading at least LAST_YEAR_MIN_OBSERVED_DAYS
    before `end_time`. One query; it reads only the keys' date span (up to
    ~336 days for a 365-day daily batch, hourly requests never get here)."""
    valid = set(uuid_list(sorted({k[0] for k in keys}), "fetch_last_year_slots"))
    keys = [k for k in keys if k[0] in valid]
    if not keys:
        return {}
    aids, dates, hours = (list(x) for x in zip(*keys))
    start = datetime.combine(min(dates), datetime.min.time(), tzinfo=timezone.utc)
    stop = min(
        datetime.combine(max(dates), datetime.min.time(), tzinfo=timezone.utc)
        + timedelta(days=2),
        end_time if end_time.tzinfo else end_time.replace(tzinfo=timezone.utc),
    )
    local_ts = "qd.timestamp AT TIME ZONE p.timezone"
    query = text(
        f"""
        WITH keys AS (
            SELECT * FROM unnest(
                CAST(:key_ids AS uuid[]), CAST(:key_dates AS date[]), CAST(:key_hours AS int[])
            ) AS k(aid, d, h)
        ),
        watched AS (
            SELECT a.id FROM attractions a
            WHERE a.id = ANY(CAST(:attraction_ids AS uuid[]))
              AND EXISTS (
                SELECT 1 FROM queue_data w
                WHERE w."attractionId" = a.id
                  AND w.timestamp >= CAST(:end_time AS timestamptz) - :lookback * INTERVAL '1 day'
                  AND w.timestamp < CAST(:end_time AS timestamptz) - :min_days * INTERVAL '1 day'
                  AND w."waitTime" >= 5
                  AND w."queueType" = 'STANDBY'
                  AND NOT COALESCE(w.is_heartbeat, false)
              )
        )
        SELECT qd."attractionId"::text AS "attractionId",
               DATE({local_ts}) AS date,
               EXTRACT(HOUR FROM {local_ts})::int AS hour,
               AVG(qd."waitTime") AS avg_wait
        FROM queue_data qd
        INNER JOIN attractions a ON a.id = qd."attractionId"
        INNER JOIN parks p ON p.id = a."parkId"
        LEFT JOIN schedule_entries se
            ON se."parkId" = a."parkId"
            AND se.date = DATE({local_ts})
            AND se."attractionId" IS NULL
        INNER JOIN keys k
            ON k.aid = qd."attractionId"
            AND k.d = DATE({local_ts})
            AND k.h = EXTRACT(HOUR FROM {local_ts})
        WHERE {_SERVING_QUEUE_FILTER}
            AND qd."attractionId" IN (SELECT id FROM watched)
            AND qd.timestamp >= :start AND qd.timestamp < :stop
        GROUP BY 1, 2, 3
        """
    )
    with get_db() as db:
        result = db.execute(
            query,
            {
                "key_ids": aids,
                "key_dates": dates,
                "key_hours": hours,
                "attraction_ids": sorted(valid),
                "end_time": end_time,
                "lookback": LAST_YEAR_WATCH_LOOKBACK_DAYS,
                "min_days": LAST_YEAR_MIN_OBSERVED_DAYS,
                "start": start,
                "stop": stop,
            },
        )
        rows = result.fetchall()
    return {(str(r[0]), pd.Timestamp(r[1]).date(), int(r[2])): float(r[3]) for r in rows}


def _park_timezones(park_ids: List[str]) -> Dict[str, str]:
    """{parkId: IANA timezone} for the given parks, "UTC" where none is on record."""
    import logging

    logger = logging.getLogger(__name__)
    try:
        meta = fetch_parks_metadata()
        tz_map = meta.set_index(meta["park_id"].astype(str))["timezone"].to_dict()
    except Exception as e:
        logger.warning(f"Park timezones unavailable ({e}); daily rows fall back to UTC")
        tz_map = {}
    result = {}
    for p in {str(x) for x in park_ids}:
        tz_name = tz_map.get(p)
        if not tz_name:
            logger.warning(f"Park {p} has no timezone on record; daily rows use UTC")
            tz_name = "UTC"
        result[p] = tz_name
    return result


def daily_anchor_time(local_date: str, tz_name: str) -> str:
    """The instant a collapsed DAILY row is published at: park-local NOON of its day.

    A daily prediction is a statement about a park-local calendar day, not about
    the hour whose slot happened to be the maximum. Publishing the winning slot's
    instant put a Los Angeles 16:00 peak on the NEXT UTC date in winter (00:00
    UTC), and every consumer that reads the day off the ISO string — the calendar,
    /plan/day, the TFT merge — would have filed it under tomorrow. Local noon
    lands on the same date in UTC for every offset between -12 h and +12 h, and
    `DATE(predictedTime AT TIME ZONE p.timezone)` reads the right day for all of
    them (PAR-818).

    ASSUMES the park's offset lies strictly between -12 h and +12 h (true for
    every park on record). A park at +13 h (Auckland in summer) would have its
    noon on the previous UTC date; the assert makes that loud instead of silent.
    """
    noon = pd.Timestamp(f"{local_date} 12:00:00").tz_localize(
        tz_name or "UTC", nonexistent="shift_forward", ambiguous=False
    )
    offset_h = noon.utcoffset().total_seconds() / 3600
    assert -12 < offset_h < 12, f"daily anchor needs |offset| < 12 h, got {offset_h}"
    return noon.tz_convert("UTC").isoformat()


def generate_future_timestamps(
    base_time: datetime, prediction_type: str, tz_name: Optional[str] = None
) -> List[datetime]:
    """
    Generate future timestamps for predictions

    Args:
        base_time: Starting time
        prediction_type: 'hourly' or 'daily'
        tz_name: The park's IANA timezone. Only read for 'daily': the peak
            hours (DAILY_PEAK_HOURS) are hours on the PARK'S clock, and the
            days are the park's calendar days after today (PAR-818).

    Returns:
        List of future timestamps (UTC instants)
    """
    if prediction_type == "hourly":
        # Round base_time to the NEXT 15-minute slot
        # This ensures all predictions have timestamps like "2024-01-15T14:15:00"
        minutes = (base_time.minute // 15 + 1) * 15
        rounded_base = base_time.replace(minute=0, second=0, microsecond=0)
        rounded_base = rounded_base + timedelta(minutes=minutes)

        # HOURLY_PREDICTIONS hours in 15-minute slots (starting from next slot),
        # so 48 hours * 4 slots per hour = 192 slots.
        return [
            rounded_base + timedelta(minutes=15 * i)
            for i in range(settings.HOURLY_PREDICTIONS * 4)
        ]
    elif prediction_type == "daily":
        # Predict several peak-window hours per day; predict_wait_times collapses them
        # to the per-day MAX = a daily-PEAK proxy (≈ P90, matching the calendar's
        # typical-day-peak baseline). A single 14:00 value under-read the peak.
        #
        # The hours are PARK-LOCAL (PAR-818). They used to be set on the UTC
        # base_time, so "12,14,16" meant 05/07/09 in Los Angeles and 21/23/01 in
        # Tokyo — the "peak" was read off the morning or the night. Each day is
        # the park's own calendar day, the hour is put on that day's wall clock
        # and only then converted to the UTC instant the features expect.
        hours = [int(h) for h in str(settings.DAILY_PEAK_HOURS).split(",") if h.strip()]
        if not hours:
            hours = [14]
        tz = tz_name or "UTC"
        base = pd.Timestamp(base_time)
        if base.tzinfo is None:
            base = base.tz_localize("UTC")
        local_today = base.tz_convert(tz).normalize().tz_localize(None)
        return [
            (local_today + pd.Timedelta(days=i, hours=h))
            .tz_localize(tz, nonexistent="shift_forward", ambiguous=False)
            .tz_convert("UTC")
            .to_pydatetime()
            for i in range(1, settings.DAILY_PREDICTIONS + 1)
            for h in hours
        ]
    else:
        raise ValueError(f"Unknown prediction_type: {prediction_type}")


def _open_against_schedule(
    df: pd.DataFrame, operating_schedules: pd.DataFrame, schedule_date: pd.Series
) -> tuple:
    """(mask_valid, mask_open, mins_since_open) of each row of `df` against the
    OPERATING window of `schedule_date` - the park-local date whose window is
    being asked. `mins_since_open` is NaN where that date has no window.

    The JOIN key is a park-local date; the time comparison is between INSTANTS.
    openingTime/closingTime from the DB are timezone-aware (TIMESTAMPTZ), so
    they are compared against `timestamp` (UTC) - never against the naive
    wall-clock `local_timestamp`.
    """
    df_merged = pd.DataFrame({"parkId": df["parkId"], "_date": schedule_date}).merge(
        operating_schedules[["parkId", "date_only", "openingTime", "closingTime"]],
        left_on=["parkId", "_date"],
        right_on=["parkId", "date_only"],
        how="left",
    )
    df_merged.index = df.index

    ts_compare = pd.to_datetime(df["timestamp"], utc=True)
    opening = df_merged["openingTime"]
    closing = df_merged["closingTime"]

    # Set tz to UTC to compare if opening has tzinfo
    if getattr(opening.dt, "tz", None) is not None:
        opening = opening.dt.tz_convert("UTC")
        closing = closing.dt.tz_convert("UTC")
    else:
        # If opening time from DB is naive, make ts_compare naive (UTC) as well
        ts_compare = ts_compare.dt.tz_localize(None)

    mask_valid = opening.notna() & closing.notna()
    mask_open = mask_valid & (ts_compare >= opening) & (ts_compare <= closing)
    mins_since_open = (ts_compare - opening).dt.total_seconds() / 60.0
    return mask_valid, mask_open, mins_since_open


def apply_schedule_features(
    df: pd.DataFrame, schedules_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Derive `is_park_open`, `status`, `has_special_event` and `has_extra_hours`
    from the park's schedule rows — the inference-side counterpart of
    `features.add_park_schedule_features`.

    Extracted from `create_prediction_features` so the join can be tested
    without a database. Two rules it has to keep, and the reason both exist:

    * The JOIN key is the park-LOCAL date (`local_timestamp.dt.date`). A row at
      21:30 in New York belongs to that day's schedule, not to the UTC next
      day's.
    * The time comparison is between INSTANTS: `timestamp` (UTC) against
      `openingTime`/`closingTime` (TIMESTAMPTZ). `local_timestamp` is a naive
      wall-clock reading and is never one side of that comparison.

    Args:
        df: rows with `parkId`, `timestamp` and (usually) `local_timestamp`
        schedules_df: schedule_entries rows for the parks and dates in `df`

    Returns:
        `df` with the schedule features set. The index is reset when
        `schedules_df` is non-empty, as the merges require positional alignment.
    """
    # Initialize schedule features and status
    df["is_park_open"] = 1  # Assume open if no schedule found
    df["has_special_event"] = 0
    df["has_extra_hours"] = 0
    df["status"] = "OPERATING"
    df["time_since_park_open_mins"] = 0.0  # training's value without a window

    if not schedules_df.empty:
        schedules_df["openingTime"] = pd.to_datetime(schedules_df["openingTime"])
        schedules_df["closingTime"] = pd.to_datetime(schedules_df["closingTime"])
        schedules_df["date"] = pd.to_datetime(schedules_df["date"])

        # Ensure attractionId is treated as string (handle NaN/None)
        if "attractionId" in schedules_df.columns:
            schedules_df["attractionId"] = (
                schedules_df["attractionId"].fillna("nan").astype(str)
            )

        # Parks without schedule integration: no OPERATING rows at all → treat as "no schedule"
        park_level = schedules_df[
            (schedules_df["attractionId"].isin(["nan", "None"]))
            | (schedules_df["attractionId"].isna())
        ]

        # Determine park schedule integration (at least one OPERATING row)
        park_has_operating = (
            park_level.groupby(park_level["parkId"].astype(str))["scheduleType"]
            .apply(lambda x: (x == "OPERATING").any())
            .to_dict()
        )

        # Ensure index alignment and type matching for merges
        df = df.reset_index(drop=True)
        df["parkId"] = df["parkId"].astype(str)
        if "attractionId" in df.columns:
            df["attractionId"] = df["attractionId"].astype(str)

        # Ensure local_timestamp and date_local exist in df.
        # The fallback is the UTC wall clock, but NAIVE: convert_to_local_time
        # stores a reading rather than an instant, and the column is read again
        # after this function returns.
        if "local_timestamp" not in df.columns:
            df["local_timestamp"] = pd.to_datetime(
                df["timestamp"], utc=True
            ).dt.tz_localize(None)
        df["local_timestamp"] = pd.to_datetime(df["local_timestamp"])
        df["schedule_date"] = pd.to_datetime(df["local_timestamp"].dt.date)

        # Create lookup structure for park-level schedules
        park_schedules = schedules_df[
            (schedules_df["attractionId"].isin(["nan", "None"]))
            | (schedules_df["attractionId"].isna())
        ].copy()

        # Prepare operating schedules
        operating_schedules = park_schedules[
            park_schedules["scheduleType"] == "OPERATING"
        ].copy()

        if not operating_schedules.empty:
            operating_schedules["date_only"] = pd.to_datetime(
                operating_schedules["date"].dt.date
            )
            # Keep first operating schedule per park/date
            operating_schedules = (
                operating_schedules.groupby(["parkId", "date_only"])
                .first()
                .reset_index()
            )

            # A slot is open when it falls inside its own local date's window
            # OR inside the previous date's: a 10:00 -> 01:00 day owns the
            # 00:00-00:45 slots that carry the NEXT local date (PAR-943).
            mask_valid, mask_open_same, since_same = _open_against_schedule(
                df, operating_schedules, df["schedule_date"]
            )
            _, mask_open_prev, since_prev = _open_against_schedule(
                df, operating_schedules, df["schedule_date"] - pd.Timedelta(days=1)
            )
            mask_open = mask_open_same | mask_open_prev

            # Minutes since the opening of the window the row is in, per row and
            # from that row's own day - the definition training learns
            # (features.add_park_schedule_features). It used to come from
            # TODAY's opening for every row, 0 when none was known (PAR-1030).
            use_prev = mask_open_prev & ~mask_open_same
            since = since_same.where(~use_prev, since_prev)
            df["time_since_park_open_mins"] = (
                since.where(mask_valid | use_prev, 0.0).clip(lower=0)
            )

            df.loc[mask_open, "is_park_open"] = 1
            mask_closed_operating_day = mask_valid & ~mask_open
            # Before opening / after close on an operating day training reads 0
            # (features.add_park_schedule_features); serving kept the "assume
            # open" default of 1 there until PAR-1045.
            df.loc[mask_closed_operating_day, "is_park_open"] = 0
            df.loc[mask_closed_operating_day, "status"] = "CLOSED"
        else:
            mask_open = pd.Series(False, index=df.index)

        # Handle non-operating schedules (CLOSED / UNKNOWN) on dates with NO operating schedule
        # First, find the dates where operating schedules were missing
        if not operating_schedules.empty:
            has_operating = (
                df.merge(
                    operating_schedules[["parkId", "date_only"]],
                    left_on=["parkId", "schedule_date"],
                    right_on=["parkId", "date_only"],
                    how="left",
                    indicator=True,
                )["_merge"]
                == "both"
            )
            has_operating.index = df.index
        else:
            has_operating = pd.Series(False, index=df.index)

        # A slot still inside yesterday's window is not on a day without hours.
        mask_no_operating = ~has_operating & ~mask_open

        if mask_no_operating.any():
            # For these rows, we check if park_has_operating is True.
            # If so, set status to UNKNOWN or CLOSED depending on what exists, and is_park_open = 0

            # Map park_has_operating flag to df
            df_park_has_operating = df["parkId"].map(park_has_operating).fillna(False)
            df_park_has_operating.index = df.index

            # Look up if schedule has UNKNOWN for that park/date
            unknown_schedules = park_schedules[
                park_schedules["scheduleType"] == "UNKNOWN"
            ].copy()
            if not unknown_schedules.empty:
                unknown_schedules["date_only"] = pd.to_datetime(
                    unknown_schedules["date"].dt.date
                )
                unknown_schedules = (
                    unknown_schedules.groupby(["parkId", "date_only"])
                    .first()
                    .reset_index()
                )

                has_unknown = (
                    df.merge(
                        unknown_schedules[["parkId", "date_only"]],
                        left_on=["parkId", "schedule_date"],
                        right_on=["parkId", "date_only"],
                        how="left",
                        indicator=True,
                    )["_merge"]
                    == "both"
                )
                has_unknown.index = df.index
            else:
                has_unknown = pd.Series(False, index=df.index)

            # Look up if schedule has CLOSED for that park/date
            closed_schedules = park_schedules[
                park_schedules["scheduleType"] == "CLOSED"
            ].copy()
            if not closed_schedules.empty:
                closed_schedules["date_only"] = pd.to_datetime(
                    closed_schedules["date"].dt.date
                )
                closed_schedules = (
                    closed_schedules.groupby(["parkId", "date_only"])
                    .first()
                    .reset_index()
                )

                has_closed = (
                    df.merge(
                        closed_schedules[["parkId", "date_only"]],
                        left_on=["parkId", "schedule_date"],
                        right_on=["parkId", "date_only"],
                        how="left",
                        indicator=True,
                    )["_merge"]
                    == "both"
                )
                has_closed.index = df.index
            else:
                has_closed = pd.Series(False, index=df.index)

            mask_apply_non_op = (
                mask_no_operating & df_park_has_operating & (has_unknown | has_closed)
            )

            # Default to CLOSED if no operating, park_has_operating, and it has some non-op schedule
            df.loc[mask_apply_non_op, "is_park_open"] = 0
            df.loc[mask_apply_non_op, "status"] = "CLOSED"

            # Overwrite with UNKNOWN if it has an UNKNOWN entry
            mask_apply_unknown = mask_apply_non_op & has_unknown
            df.loc[mask_apply_unknown, "status"] = "UNKNOWN"

        # Check for special events
        event_schedules = park_schedules[
            park_schedules["scheduleType"].isin(["TICKETED_EVENT", "PRIVATE_EVENT"])
        ].copy()
        if not event_schedules.empty:
            event_schedules["date_only"] = pd.to_datetime(
                event_schedules["date"].dt.date
            )
            event_schedules = (
                event_schedules.groupby(["parkId", "date_only"]).first().reset_index()
            )
            has_event = (
                df.merge(
                    event_schedules[["parkId", "date_only"]],
                    left_on=["parkId", "schedule_date"],
                    right_on=["parkId", "date_only"],
                    how="left",
                    indicator=True,
                )["_merge"]
                == "both"
            )
            has_event.index = df.index
            df.loc[has_event, "has_special_event"] = 1

        # Check for extra hours
        extra_hours_schedules = park_schedules[
            park_schedules["scheduleType"] == "EXTRA_HOURS"
        ].copy()
        if not extra_hours_schedules.empty:
            extra_hours_schedules["date_only"] = pd.to_datetime(
                extra_hours_schedules["date"].dt.date
            )
            extra_hours_schedules = (
                extra_hours_schedules.groupby(["parkId", "date_only"])
                .first()
                .reset_index()
            )
            has_extra = (
                df.merge(
                    extra_hours_schedules[["parkId", "date_only"]],
                    left_on=["parkId", "schedule_date"],
                    right_on=["parkId", "date_only"],
                    how="left",
                    indicator=True,
                )["_merge"]
                == "both"
            )
            has_extra.index = df.index
            df.loc[has_extra, "has_extra_hours"] = 1

        # Check specific attraction status (Maintenance or Closed)
        if "attractionId" in df.columns:
            attr_schedules = schedules_df[
                schedules_df["attractionId"].notna()
                & ~schedules_df["attractionId"].isin(["nan", "None"])
            ].copy()
            if not attr_schedules.empty:
                attr_schedules["date_only"] = pd.to_datetime(
                    attr_schedules["date"].dt.date
                )
                attr_schedules_closed = attr_schedules[
                    attr_schedules["scheduleType"].isin(["MAINTENANCE", "CLOSED"])
                ]
                if not attr_schedules_closed.empty:
                    attr_schedules_closed = (
                        attr_schedules_closed.groupby(
                            ["parkId", "attractionId", "date_only"]
                        )
                        .first()
                        .reset_index()
                    )
                    has_attr_closed = (
                        df.merge(
                            attr_schedules_closed[
                                ["parkId", "attractionId", "date_only"]
                            ],
                            left_on=["parkId", "attractionId", "schedule_date"],
                            right_on=["parkId", "attractionId", "date_only"],
                            how="left",
                            indicator=True,
                        )["_merge"]
                        == "both"
                    )
                    has_attr_closed.index = df.index
                    df.loc[has_attr_closed, "status"] = "CLOSED"

        df = df.drop(columns=["schedule_date"], errors="ignore")

    return df


# A forecast day with fewer hours than this is not summed into a daily total:
# its daily fields come from climatology. Sums of a day with at least this many
# hours are scaled to 24 h (the GMT-aligned feed cuts the park's first and last
# local day short).
MIN_FORECAST_HOURS_PER_DAY = 12

CLIMATOLOGY_COLUMNS = [
    "parkId",
    "month",
    "temp_max",
    "temp_min",
    "temp_avg",
    "precip_avg",
    "wind_avg",
    "snow_avg",
    "weather_code_mode",
]


def fetch_weather_climatology(park_ids: list[str], months: list[int]) -> pd.DataFrame:
    """Per-(park, month) averages of the daily `weather_data` rows, last 3 years.

    One row per park and month asked for — the rows of a request can span
    several months, and each row reads its own (PAR-1031: the whole request
    used to read the month of its first row). Cached for an hour per
    (park set, month set).
    """
    if not park_ids or not months:
        return pd.DataFrame(columns=CLIMATOLOGY_COLUMNS)

    import time

    cache_key = f"{','.join(sorted(set(map(str, park_ids))))}:{','.join(map(str, months))}"
    cached = _weather_historical_cache.get(cache_key)
    if cached is not None and time.time() - cached[1] < _weather_historical_cache_ttl:
        return cached[0].copy()

    query = text("""
        SELECT
            "parkId"::text as "parkId",
            EXTRACT(MONTH FROM date)::int as month,
            AVG("temperatureMax") as temp_max,
            AVG("temperatureMin") as temp_min,
            AVG("temperatureMax" + "temperatureMin") / 2 as temp_avg,
            AVG("precipitationSum") as precip_avg,
            AVG("windSpeedMax") as wind_avg,
            AVG("snowfallSum") as snow_avg,
            MODE() WITHIN GROUP (ORDER BY "weatherCode") as weather_code_mode
        FROM weather_data
        WHERE "parkId" = ANY(CAST(:park_ids AS uuid[]))
            AND EXTRACT(MONTH FROM date) = ANY(CAST(:months AS int[]))
            AND date >= CURRENT_DATE - INTERVAL '3 years'
        GROUP BY "parkId", EXTRACT(MONTH FROM date)
    """)
    try:
        with get_db() as db:
            result = db.execute(
                query,
                {
                    "park_ids": uuid_list(set(park_ids), "weather historical averages"),
                    "months": [int(m) for m in months],
                },
            )
            clim = convert_df_types(pd.DataFrame(result.fetchall(), columns=result.keys()))
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning(
            f"Failed to fetch historical weather data: {e}. Using defaults."
        )
        return pd.DataFrame(columns=CLIMATOLOGY_COLUMNS)

    if clim.empty:
        clim = pd.DataFrame(columns=CLIMATOLOGY_COLUMNS)
    else:
        clim["month"] = clim["month"].astype(int)
    _weather_historical_cache[cache_key] = (clim.copy(), time.time())
    return clim


def _forecast_daily_equivalents(
    forecast: pd.DataFrame, park_timezones: dict[str, str]
) -> pd.DataFrame:
    """The hourly forecast as the DAILY values training sees, per park-local date.

    Training reads `weather_data`, one row per park-local day: precipitation and
    snowfall are day sums, wind the day's maximum, the weather code Open-Meteo's
    "most severe of the day". The hourly forecast is reduced the same way, so a
    rainy hour (2 mm) is not read by the model as a dry day (PAR-1032).
    """
    rows = []
    for park_id, tz in park_timezones.items():
        local_date = (
            forecast["time"].dt.tz_localize("UTC").dt.tz_convert(tz).dt.date
        )
        g = forecast.assign(_local_date=local_date).groupby("_local_date")
        daily = pd.DataFrame(
            {
                "n_hours": g.size(),
                "fc_precipitation": g["precipitation"].sum(min_count=1),
                "fc_snowfall": g["snowfall"].sum(min_count=1),
                "fc_wind": g["windSpeed"].max(),
                "fc_code": g["weatherCode"].max(),
            }
        )
        daily = daily[daily["n_hours"] >= MIN_FORECAST_HOURS_PER_DAY]
        scale = 24.0 / daily["n_hours"]
        daily["fc_precipitation"] *= scale
        daily["fc_snowfall"] *= scale
        daily = daily.drop(columns=["n_hours"]).reset_index()
        daily["parkId"] = park_id
        rows.append(daily)
    if not rows:
        return pd.DataFrame(
            columns=["parkId", "_local_date", "fc_precipitation", "fc_snowfall", "fc_wind", "fc_code"]
        )
    return pd.concat(rows, ignore_index=True)


def apply_weather_features(
    df: pd.DataFrame,
    forecast: pd.DataFrame | None,
    climatology: pd.DataFrame,
    park_timezones: dict[str, str],
) -> pd.DataFrame:
    """Weather columns for every prediction row, in training's units.

    * Each row reads the climatology of its OWN park-local month (PAR-1031).
    * Where the forecast reaches, temperature is the forecast hour's and the
      daily fields (precipitation, snowfall, wind, weather code) are that
      park-local day's totals/maximum from the forecast — training's
      `weather_data` is daily (PAR-1032).
    * Beyond the forecast every field comes from the month's climatology (the
      temperature on training's 04:00-min / 14:00-max sinusoid) instead of
      20 °C, 0 mm and weather code 0.
    * `precipitation_last_3h` is the day's precipitation / 8 (mm per 3 h), the
      definition training uses — it has no hourly observed weather to roll.
    * `temperature_deviation` is against the (park, month) climatology mean.

    Needs `parkId`, `timestamp`, `local_timestamp`, `hour`. Returns a frame
    with a fresh RangeIndex (the merges reset it).
    """
    clim = climatology if climatology is not None else pd.DataFrame()
    clim = clim.reindex(columns=CLIMATOLOGY_COLUMNS).rename(columns={"month": "_month"})
    clim["parkId"] = clim["parkId"].astype(str)
    clim["_month"] = pd.to_numeric(clim["_month"]).astype(float)

    df = df.copy()
    df["parkId"] = df["parkId"].astype(str)
    df["_month"] = df["local_timestamp"].dt.month.astype(float)
    df["_local_date"] = df["local_timestamp"].dt.date
    df = df.merge(clim, on=["parkId", "_month"], how="left")

    temp_min = df["temp_min"].astype(float).fillna(df["temp_max"].astype(float)).fillna(15.0)
    temp_max = df["temp_max"].astype(float).fillna(25.0)
    interpolation_factor = np.cos(((df["hour"] - 14) / 12) * np.pi) * 0.5 + 0.5
    clim_temp = temp_min + interpolation_factor * (temp_max - temp_min)

    df["temperature_avg"] = clim_temp
    df["precipitation"] = df["precip_avg"].astype(float).fillna(0.0)
    df["windSpeedMax"] = df["wind_avg"].astype(float).fillna(0.0)
    df["snowfallSum"] = df["snow_avg"].astype(float).fillna(0.0)
    df["weatherCode"] = df["weather_code_mode"].astype(float).fillna(0)

    if forecast is not None and not forecast.empty and "time" in forecast.columns:
        fc = forecast.copy()
        fc["time"] = pd.to_datetime(fc["time"])
        if fc["time"].dt.tz is not None:
            fc["time"] = fc["time"].dt.tz_convert("UTC").dt.tz_localize(None)
        # One row per hour (a DST night maps two wall-clock hours onto one instant — PAR-818).
        fc = fc.drop_duplicates(subset="time", keep="first")
        for col in ["temperature", "precipitation", "snowfall", "windSpeed", "weatherCode"]:
            if col not in fc.columns:
                fc[col] = np.nan
            fc[col] = pd.to_numeric(fc[col], errors="coerce")

        join_time = pd.to_datetime(df["timestamp"], utc=True).dt.round("h").dt.tz_localize(None)
        hourly_temp = join_time.map(fc.set_index("time")["temperature"])
        df["temperature_avg"] = hourly_temp.fillna(df["temperature_avg"])

        daily = _forecast_daily_equivalents(fc, park_timezones)
        df = df.merge(daily, on=["parkId", "_local_date"], how="left")
        df["precipitation"] = df["fc_precipitation"].fillna(df["precipitation"])
        df["snowfallSum"] = df["fc_snowfall"].fillna(df["snowfallSum"])
        df["windSpeedMax"] = df["fc_wind"].fillna(df["windSpeedMax"])
        df["weatherCode"] = df["fc_code"].fillna(df["weatherCode"])
        df = df.drop(
            columns=["fc_precipitation", "fc_snowfall", "fc_wind", "fc_code"]
        )

    df["weatherCode"] = df["weatherCode"].astype(int)
    df["precipitation_last_3h"] = df["precipitation"] * 0.125
    df["temperature_deviation"] = df["temperature_avg"] - df["temp_avg"].astype(
        float
    ).fillna(20.0)

    return df.drop(
        columns=["_month", "_local_date"] + CLIMATOLOGY_COLUMNS[2:], errors="ignore"
    )


def create_prediction_features(
    attraction_ids: List[str],
    park_ids: List[str],
    timestamps: List[datetime],
    base_time: datetime,
    weather_forecast: List[Any] = None,
    current_wait_times: Dict[str, int] = None,
    recent_wait_times: Dict[str, int] = None,
    feature_context: Optional[Dict[str, Any]] = None,
    timestamps_by_park: Optional[Dict[str, List[datetime]]] = None,
    curated_ride_type: bool = False,
) -> pd.DataFrame:
    """
    Create feature DataFrame for predictions with all features from DB

    Args:
        attraction_ids: List of attraction IDs
        park_ids: Corresponding park IDs
        timestamps: Future timestamps to predict for
        base_time: Current time for fetching historical data
        weather_forecast: Optional list of hourly weather forecast items
        current_wait_times: Optional dict of {attractionId: waitTime} for current state
        timestamps_by_park: Optional {parkId: timestamps} overriding `timestamps`
            for that park — daily rows sit on each park's own clock (PAR-818)

    Returns:
        DataFrame with features ready for prediction
    """
    # Create base DataFrame
    rows = []
    for attraction_id, park_id in zip(attraction_ids, park_ids):
        park_timestamps = (timestamps_by_park or {}).get(str(park_id), timestamps)
        for ts in park_timestamps:
            rows.append(
                {
                    "attractionId": attraction_id,
                    "parkId": park_id,
                    "timestamp": ts,
                }
            )

    df = pd.DataFrame(rows)

    # Convert parkId and attractionId to strings (for CatBoost)
    df["parkId"] = df["parkId"].astype(str)
    df["attractionId"] = df["attractionId"].astype(str)

    # Convert UTC timestamps to local park time using helper function
    # This matches the training pipeline in features.py::engineer_features()
    # Without this, hour/day_of_week/month features would be wrong (UTC instead of local)
    from features import convert_to_local_time

    # Fetch park metadata for timezone conversion (fetches all parks)
    # OPTIMIZATION: Cache this to avoid multiple DB queries
    parks_metadata = fetch_parks_metadata()

    # Fetch historical occupancy for future prediction rows (avoids using stale real-time value)
    from db import fetch_historical_park_occupancy

    unique_park_ids = list(set(park_ids))
    hist_occ = fetch_historical_park_occupancy(unique_park_ids, end_time=base_time)
    if feature_context is not None:
        feature_context = dict(feature_context)  # don't mutate caller's dict
        feature_context["historicalOccupancy"] = hist_occ
        feature_context["baseTime"] = base_time
    # When feature_context is None, hist_occ is used directly in the else branch
    # so only park_occupancy_pct benefits (bridge day / other features keep their defaults)

    df = convert_to_local_time(df, parks_metadata)

    # Add time features from LOCAL timestamp (not UTC!)
    df["hour"] = df["local_timestamp"].dt.hour
    df["day_of_week"] = df["local_timestamp"].dt.dayofweek
    df["month"] = df["local_timestamp"].dt.month
    df["date_local"] = df["local_timestamp"].dt.date

    # Cyclical time encoding
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    df["day_of_week_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["day_of_week_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    # NEW: Day of year (1-365/366) for finer seasonal trends
    df["day_of_year"] = df["local_timestamp"].dt.dayofyear
    df["day_of_year_sin"] = np.sin(2 * np.pi * df["day_of_year"] / 365.25)
    df["day_of_year_cos"] = np.cos(2 * np.pi * df["day_of_year"] / 365.25)

    # Season calculation - MUST use local_timestamp to match training pipeline
    # Using UTC timestamp would cause season to be wrong for parks in different timezones
    # Example: 23:00 UTC Feb 28 = 08:00 JST Mar 1 → Season jumps from Winter to Spring
    df["season"] = df["local_timestamp"].dt.month.apply(lambda m: (m % 12) // 3)

    # NEW: Peak season indicator (summer months + December holidays)
    df["is_peak_season"] = ((df["month"] >= 6) & (df["month"] <= 8)) | (
        df["month"] == 12
    )
    df["is_peak_season"] = df["is_peak_season"].astype(int)

    # Region-specific weekends
    # OPTIMIZATION: Use centralized function from features.py to avoid code duplication
    from features import add_weekend_feature

    df = add_weekend_feature(df, parks_metadata)

    # Weather features: the hourly forecast where it reaches, the park's
    # climatology for each row's own month everywhere else (PAR-1031).
    wanted_parks = set(map(str, park_ids))
    park_timezones_wx = {
        str(row["park_id"]): row["timezone"]
        for _, row in parks_metadata.iterrows()
        if str(row["park_id"]) in wanted_parks and row.get("timezone")
    }
    climatology_df = fetch_weather_climatology(
        park_ids, sorted(int(m) for m in df["local_timestamp"].dt.month.dropna().unique())
    )
    forecast_df = None
    if weather_forecast and len(weather_forecast) > 0:
        try:
            wf_data = [w.dict() if hasattr(w, "dict") else w for w in weather_forecast]
            forecast_df = pd.DataFrame(wf_data)
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(
                f"Failed to read weather forecast: {e}. Using climatology."
            )
    df = apply_weather_features(df, forecast_df, climatology_df, park_timezones_wx)

    df["is_raining"] = (df["precipitation"] > 0).astype(int)

    # Rain Trend Features (starting/stopping) - matches features.py logic
    # Sort by (parkId, timestamp) to correctly find state changes
    original_index = df.index
    df = df.sort_values(["parkId", "timestamp"])

    # In inference, we might only have a single row or a short window.
    # We use shift(1) per park to find changes.
    was_raining = df.groupby("parkId")["is_raining"].shift(1).fillna(0)
    df["is_rain_starting"] = ((df["is_raining"] == 1) & (was_raining == 0)).astype(int)
    df["is_rain_stopping"] = ((df["is_raining"] == 0) & (was_raining == 1)).astype(int)

    # Restore original order
    df = df.loc[original_index]

    # Holiday features — the same function training calls
    # (holiday_features.assign_holiday_features), so the two paths cannot drift.
    # Until PAR-816 this block was its own re-implementation: last-write-wins
    # on duplicate holiday rows, only 'public' counted as a holiday (training:
    # public/bank/bridge), no national fallback for the neighbour slots, and no
    # Easter Sunday fallback.
    all_countries = set()
    for _, park in parks_metadata.iterrows():
        all_countries.add(park["country"])
        for region in parse_influencing_regions(park.get("influencingRegions")):
            if isinstance(region, dict) and region.get("countryCode"):
                all_countries.add(region["countryCode"])

    # Fetch by the park-LOCAL date range (see holiday_fetch_window: the old
    # UTC-timestamp window dropped today's holidays from every hourly forecast).
    holidays_start, holidays_end = holiday_fetch_window(df["local_timestamp"])
    holidays_df = fetch_holidays(list(all_countries), holidays_start, holidays_end)

    df["local_date"] = df["local_timestamp"].dt.date
    # Past a location's last school-holiday row the school columns are NaN, as
    # in training (PAR-1049): an unpublished break is not "no break".
    df = assign_holiday_features(
        df,
        parks_metadata,
        holidays_df,
        park_col="parkId",
        date_col="local_date",
        coverage=SchoolCoverage(fetch_school_holiday_coverage(list(all_countries))),
    )
    df = df.drop(columns=["local_date"], errors="ignore")

    # Bridge days per row from the same holidays, the way training computes them
    # (features.add_bridge_day_feature) and BEFORE the distance features read
    # them. The API used to send one flag for today, applied to every row
    # (PAR-1030).
    from features import add_bridge_day_feature

    df = add_bridge_day_feature(
        df, parks_metadata, holidays_start, holidays_end,
        cached_holidays_df=holidays_df,
    ).drop(columns=["country"], errors="ignore")

    # Park schedule features (check if park is open at predicted time)
    schedule_query = text(
        """
        SELECT
            "parkId"::text as "parkId",
            "attractionId"::text as "attractionId",
            date,
            "scheduleType",
            "openingTime",
            "closingTime"
        FROM schedule_entries
        WHERE "parkId" = ANY(CAST(:park_ids AS uuid[]))
            AND date BETWEEN :start_date AND :end_date
            AND (
                ("openingTime" IS NOT NULL AND "closingTime" IS NOT NULL)
                OR "scheduleType" IN ('MAINTENANCE', 'CLOSED', 'INFO', 'TICKETED_EVENT', 'PRIVATE_EVENT', 'UNKNOWN')
            )
    """
    )

    with get_db() as db:
        # Determine date range for schedule query using park local timezone
        # Schedules are stored as DATE type (park's local calendar dates)
        # We must query using dates in the park's timezone, not UTC
        # df has 'local_timestamp' column added by convert_to_local_time() earlier
        if "local_timestamp" in df.columns:
            # Extract date range from LOCAL timestamps (already in park TZ)
            start_date_local = df["local_timestamp"].min().date()
            end_date_local = df["local_timestamp"].max().date()
        else:
            # Fallback to UTC dates (should not happen in production)
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                "No local_timestamp column - using UTC for schedule dates (may miss boundary dates)"
            )
            start_date_local = df["timestamp"].min().date()
            end_date_local = df["timestamp"].max().date()

        result = db.execute(
            schedule_query,
            {
                "park_ids": uuid_list(set(park_ids), "schedule features"),
                # One day earlier: the first slots after midnight may still sit
                # inside the previous day's window (apply_schedule_features).
                "start_date": start_date_local - timedelta(days=1),
                "end_date": end_date_local,
            },
        )
        schedules_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Schedule-derived features (park open, status, events). The join lives
    # in its own function so it can be tested without a database.
    df = apply_schedule_features(df, schedules_df)

    # Live park status override: if NestJS determined a park is OPERATING via ride data,
    # correct is_park_open for rows with UNKNOWN schedule (no data from wiki).
    # Only overrides UNKNOWN — never CLOSED, which reflects an explicit schedule entry.
    # This fixes future prediction timestamps (hourly: next 24h) where currentWaitTimes won't help.
    if feature_context and "parkLiveStatus" in feature_context:
        for park_id, live_status in feature_context["parkLiveStatus"].items():
            if live_status == "OPERATING":
                mask = (
                    (df["parkId"] == park_id)
                    & (df["status"] == "UNKNOWN")
                    & (df["is_park_open"] == 0)
                )
                if mask.any():
                    df.loc[mask, "is_park_open"] = 1
                    df.loc[mask, "status"] = "OPERATING"

    # 3. Holiday Distance Features (Arrival & Departure signals)
    # Strategy: Find the next AND previous public holiday/bridge day per country
    df["days_until_next_holiday"] = 7
    df["days_since_last_holiday"] = 7
    df["is_long_weekend"] = 0  # Part of a 3+ day block

    # We need country mapping for distance calculation
    df_temp = df.merge(
        parks_metadata[["park_id", "country"]],
        left_on="parkId",
        right_on="park_id",
        how="left",
    )

    # Filter all event days (Holidays + Bridge Days) from our current dataframe
    event_days = df_temp[
        (df_temp["is_holiday_primary"] == 1) | (df_temp.get("is_bridge_day", 0) == 1)
    ][["country", "date_local"]].drop_duplicates()

    if not event_days.empty:
        event_days["event_date"] = pd.to_datetime(event_days["date_local"])
        event_days = event_days.sort_values("event_date")

        # Prepare main df for merge_asof
        df["_temp_ts"] = pd.to_datetime(df["date_local"])
        original_order = df.index
        df["country"] = df_temp["country"]
        df = df.sort_values("_temp_ts")

        # A. Next Event (Arrival Signal)
        df = pd.merge_asof(
            df,
            event_days[["country", "event_date"]],
            left_on="_temp_ts",
            right_on="event_date",
            by="country",
            direction="forward",
        )
        df["days_until_next_holiday"] = (
            (df["event_date"] - df["_temp_ts"]).dt.days.fillna(7).clip(0, 7)
        )
        df = df.drop(columns=["event_date"])

        # B. Previous Event (Departure Signal)
        df = pd.merge_asof(
            df,
            event_days[["country", "event_date"]],
            left_on="_temp_ts",
            right_on="event_date",
            by="country",
            direction="backward",
        )
        df["days_since_last_holiday"] = (
            (df["_temp_ts"] - df["event_date"]).dt.days.fillna(7).clip(0, 7)
        )
        df = df.drop(columns=["event_date"])

        # C. Long Weekend Flag (Heuristic)
        is_event = (df["is_holiday_primary"] == 1) | (df["is_bridge_day"] == 1)
        df["is_long_weekend"] = (
            is_event
            | ((df["is_weekend"] == 1) & (df["days_until_next_holiday"] <= 1))
            | ((df["is_weekend"] == 1) & (df["days_since_last_holiday"] <= 1))
        ).astype(int)

        # Cleanup and restore original row order.
        # IMPORTANT: use .loc[original_order], NOT df.index = ... + sort_index().
        # After sort_values + merge_asof the rows are in _temp_ts order;
        # overwriting the index labels and sorting would assign wrong labels.
        df = df.drop(columns=["_temp_ts", "country"])
        df = df.loc[original_order]

    # Historical features (most important!)
    # Two reads: hour-by-hour rows for the date/hour lookups (last 32 days) and
    # the rolling statistics at base_time, computed the way training computes
    # them (see fetch_recent_rolling_stats).
    recent_data = fetch_recent_wait_times(attraction_ids, end_time=base_time)
    rolling_stats = fetch_recent_rolling_stats(attraction_ids, end_time=base_time)
    last_year_slots = fetch_last_year_slots(last_year_slot_keys(df, base_time), base_time)
    if recent_data.empty:
        recent_data = pd.DataFrame(
            columns=["attractionId", "attractionName", "date", "hour", "avg_wait"]
        )

    # Initialize with defaults (0.0 to match training fallback)
    df["avg_wait_last_24h"] = 0.0
    df["avg_wait_last_1h"] = 0.0
    df["avg_wait_same_hour_last_week"] = 0.0
    df["avg_wait_same_hour_last_month"] = 0.0
    df["rolling_avg_7d"] = 0.0
    df["trend_7d"] = 0.0
    df["volatility_7d"] = 0.0

    if not recent_data.empty or not rolling_stats.empty:
        # Map attraction names for type heuristic
        attr_names_map = (
            rolling_stats["attractionName"].dropna().to_dict()
            if not rolling_stats.empty
            else {}
        )
        attr_names_map.update(
            recent_data.groupby("attractionId")["attractionName"].first().to_dict()
        )
        df["attractionName"] = df["attractionId"].map(attr_names_map)
        df = add_attraction_type_features(df, curated=curated_ride_type)

        recent_data["date"] = pd.to_datetime(recent_data["date"])
        # Ensure timezone-naive for comparison with cutoff dates
        if recent_data["date"].dt.tz is not None:
            recent_data["date"] = recent_data["date"].dt.tz_localize(None)

        # Historical feature lookups use local park time.
        # fetch_recent_wait_times() now aggregates by local park hour/DOW (via p.timezone JOIN),
        # so lookups keyed by local hour/DOW are self-consistent with the stored data.

        # Convert base_time to pandas Timestamp (timezone-aware UTC)
        base_time_pd = pd.Timestamp(base_time)
        if base_time_pd.tzinfo is None:
            base_time_pd = base_time_pd.tz_localize("UTC")
        else:
            base_time_pd = base_time_pd.tz_convert("UTC")

        # Get park timezone for each attraction
        attraction_to_park = dict(zip(attraction_ids, park_ids))
        park_timezones = {}
        for park_id in set(park_ids):
            park_info = parks_metadata[parks_metadata["park_id"] == park_id]
            if not park_info.empty:
                park_timezones[park_id] = park_info.iloc[0]["timezone"]

        import pytz

        base_time_local_cache: dict = {}
        for tz_name in set(park_timezones.values()):
            try:
                base_time_local_cache[tz_name] = base_time_pd.tz_convert(
                    pytz.timezone(tz_name)
                )
            except Exception:
                base_time_local_cache[tz_name] = base_time_pd

        for attraction_id in attraction_ids:
            attraction_data = recent_data[recent_data["attractionId"] == attraction_id]
            park_id = attraction_to_park.get(attraction_id)
            park_tz = park_timezones.get(park_id) if park_id else None

            stats = (
                rolling_stats.loc[attraction_id]
                if attraction_id in rolling_stats.index
                else None
            )

            def _stat(col):
                """The rolling statistic, or None when its window had no data."""
                if stats is None or pd.isna(stats[col]):
                    return None
                return float(stats[col])

            if not attraction_data.empty or stats is not None:
                # Long-run level used where a shorter window is empty. It was the
                # mean of 730 days of hourly averages; the 90-day rolling mean is
                # the longest history the model is trained on.
                overall_avg = _stat("rolling_avg_90d")
                if overall_avg is None:
                    overall_avg = attraction_data["avg_wait"].mean()

                base_time_local = (
                    base_time_local_cache.get(park_tz, base_time_pd)
                    if park_tz
                    else base_time_pd
                )

                rolling_7d = _stat("rolling_avg_7d")
                if rolling_7d is None:
                    rolling_7d = overall_avg

                # Last 24h average (approximation: today + yesterday average) - use local date
                cutoff_24h_local = (base_time_local - timedelta(days=1)).date()
                last_24h = attraction_data[
                    pd.to_datetime(attraction_data["date"]).dt.date >= cutoff_24h_local
                ]
                avg_24h = (
                    last_24h["avg_wait"].mean() if len(last_24h) > 0 else rolling_7d
                )

                # Last 1h average (Lag 1) - use local hour
                # Note: Data is still UTC-aggregated, but we use local hour for lookup
                # This is an approximation - for perfect accuracy, data should be aggregated by local hour
                prev_hour_dt_local = base_time_local - timedelta(hours=1)
                prev_hour_local = prev_hour_dt_local.hour
                prev_hour_date_local = prev_hour_dt_local.date()

                # Try local hour first
                last_1h_data = attraction_data[
                    (
                        pd.to_datetime(attraction_data["date"]).dt.date
                        == prev_hour_date_local
                    )
                    & (attraction_data["hour"] == prev_hour_local)
                ]

                # Fallback: Try UTC hour if local hour doesn't match (timezone offset)
                if last_1h_data.empty:
                    prev_hour_utc = (base_time_pd - timedelta(hours=1)).hour
                    prev_hour_date_utc = (base_time_pd - timedelta(hours=1)).date()
                    last_1h_data = attraction_data[
                        (
                            pd.to_datetime(attraction_data["date"]).dt.date
                            == prev_hour_date_utc
                        )
                        & (attraction_data["hour"] == prev_hour_utc)
                    ]

                # Intermediate Fallback: Same time yesterday (better than daily average)
                yesterday_prev_hour_date = prev_hour_date_local - timedelta(days=1)
                yesterday_fallback = attraction_data[
                    (
                        pd.to_datetime(attraction_data["date"]).dt.date
                        == yesterday_prev_hour_date
                    )
                    & (attraction_data["hour"] == prev_hour_local)
                ]
                avg_yesterday_fallback = (
                    yesterday_fallback["avg_wait"].mean()
                    if not yesterday_fallback.empty
                    else avg_24h
                )

                avg_1h = (
                    last_1h_data["avg_wait"].mean()
                    if not last_1h_data.empty
                    else avg_yesterday_fallback
                )

                # Same hour last week (7 days ago, same hour in local time)
                last_week_date_local = (base_time_local - timedelta(days=7)).date()
                current_hour_local = base_time_local.hour

                same_hour_last_week = attraction_data[
                    (
                        pd.to_datetime(attraction_data["date"]).dt.date
                        == last_week_date_local
                    )
                    & (attraction_data["hour"] == current_hour_local)
                ]
                avg_same_hour = (
                    same_hour_last_week["avg_wait"].mean()
                    if len(same_hour_last_week) > 0
                    else rolling_7d
                )

                # Same hour last month (30 days ago, same hour in local time) - NEW
                last_month_date_local = (base_time_local - timedelta(days=30)).date()
                same_hour_last_month = attraction_data[
                    (
                        pd.to_datetime(attraction_data["date"]).dt.date
                        == last_month_date_local
                    )
                    & (attraction_data["hour"] == current_hour_local)
                ]
                avg_same_hour_month = (
                    same_hour_last_month["avg_wait"].mean()
                    if len(same_hour_last_month) > 0
                    else rolling_7d
                )

                # Calculate 7-day trend (momentum) - matches features.py logic
                trend_7d = avg_24h - rolling_7d

                # Volatility: 7-day std, log1p-dampened and capped to match training
                # (an empty or single-value window is 0, training's fillna(0)).
                cap_std = get_settings().VOLATILITY_CAP_STD_MINUTES

                def _dampened_vol_pred(raw_std_val):
                    if raw_std_val is None or pd.isna(raw_std_val) or raw_std_val <= 0:
                        return 0.0
                    return min(np.log1p(raw_std_val), np.log1p(cap_std))

                volatility_7d = _dampened_vol_pred(_stat("rolling_std_7d"))
                volatility_weekday = _dampened_vol_pred(_stat("rolling_std_weekday"))
                volatility_weekend = _dampened_vol_pred(_stat("rolling_std_weekend"))

                def _extract_col(col, fallback):
                    value = _stat(col)
                    return fallback if value is None else value

                rolling_avg_weekday = _extract_col("rolling_avg_weekday", rolling_7d)
                rolling_avg_weekend = _extract_col("rolling_avg_weekend", rolling_7d)
                rolling_avg_28d = _extract_col("rolling_avg_28d", rolling_7d)
                rolling_avg_90d = _extract_col("rolling_avg_90d", rolling_7d)

                # Apply uniform features to all rows for this attraction
                mask = df["attractionId"] == attraction_id
                df.loc[mask, "avg_wait_last_24h"] = avg_24h
                df.loc[mask, "avg_wait_last_1h"] = avg_1h
                df.loc[mask, "avg_wait_same_hour_last_week"] = avg_same_hour
                df.loc[mask, "avg_wait_same_hour_last_month"] = avg_same_hour_month
                df.loc[mask, "rolling_avg_7d"] = rolling_7d
                df.loc[mask, "rolling_avg_weekday"] = rolling_avg_weekday
                df.loc[mask, "rolling_avg_weekend"] = rolling_avg_weekend
                df.loc[mask, "rolling_avg_28d"] = rolling_avg_28d
                df.loc[mask, "rolling_avg_90d"] = rolling_avg_90d
                df.loc[mask, "trend_7d"] = trend_7d
                df.loc[mask, "volatility_7d"] = volatility_7d
                df.loc[mask, "volatility_weekday"] = volatility_weekday
                df.loc[mask, "volatility_weekend"] = volatility_weekend

                # avg_wait_same_dow_4w: mean of last 4 same-day-of-week lookups (1w/2w/3w/4w).
                # Must be computed per prediction timestamp because future timestamps span
                # multiple days (e.g. Sunday 19:00 → Monday 18:00), each needing its own
                # same-DOW historical anchor.
                #
                # PERFORMANCE: Pre-build a (date, hour) → avg_wait dict once per attraction
                # so each per-row lookup is O(1) instead of O(len(attraction_data)).
                # This avoids saturating all uvicorn workers with DataFrame scans.
                _attr_lookup: dict = (
                    attraction_data.groupby(
                        [
                            pd.to_datetime(attraction_data["date"]).dt.date,
                            attraction_data["hour"].astype(int),
                        ]
                    )["avg_wait"]
                    .mean()
                    .to_dict()
                )

                def _same_dow_avg_fast(ts_local, weeks: list):
                    vals = []
                    for w in weeks:
                        key = (
                            (ts_local - timedelta(days=7 * w)).date(),
                            int(ts_local.hour),
                        )
                        if key in _attr_lookup:
                            vals.append(_attr_lookup[key])
                    if vals:
                        return float(np.mean(vals))
                    # Same weekday and hour a year back, where observed (PAR-1045).
                    year_ago = ts_local.replace(tzinfo=None) - LAST_YEAR_LAG
                    return last_year_slots.get(
                        (attraction_id, year_ago.date(), int(ts_local.hour)), rolling_7d
                    )

                # For future prediction rows (> 24h ahead), avg_wait_last_24h and
                # avg_wait_last_1h lose meaning — they just reflect current/recent
                # conditions, not conditions at the predicted time. Replace them with
                # DOW/hour-based historical profiles so the model can differentiate
                # weekdays from weekends and low-season from peak-season future dates.
                if park_tz:
                    import pytz as _pytz_future

                    _tz_future = _pytz_future.timezone(park_tz)
                    future_24h_cutoff = base_time_pd + timedelta(hours=24)

                    # Ensure timezone aware timestamps locally
                    ts_utc = pd.to_datetime(df.loc[mask, "timestamp"])
                    if ts_utc.dt.tz is None:
                        ts_utc = ts_utc.dt.tz_localize("UTC")
                    ts_local = ts_utc.dt.tz_convert(_tz_future)

                    # Condition: only apply where time is > 24h ahead
                    future_mask = ts_utc > future_24h_cutoff

                    # Calculate same-DOW-4-week avg via vectorized apply over the local timestamps
                    # Since _same_dow_avg_fast requires complex dictionary lookup, we apply it.
                    same_dow_vals = ts_local.apply(
                        lambda x: _same_dow_avg_fast(x, [1, 2, 3, 4])
                    )

                    # Update 'avg_wait_same_dow_4w' for all mask rows
                    df.loc[mask, "avg_wait_same_dow_4w"] = same_dow_vals.values

                    # avg_wait_last_24h: replaced beyond 24 h (its own window).
                    future_indices = df.loc[mask].index[future_mask]
                    if len(future_indices) > 0:
                        df.loc[future_indices, "avg_wait_last_24h"] = same_dow_vals[
                            future_mask
                        ].values

                    # avg_wait_last_1h: replaced beyond the live lead - the
                    # base-time value says nothing about [t - 1 h, t) there.
                    beyond_live = ts_utc > base_time_pd + LIVE_LAG_MAX_LEAD
                    future_indices = df.loc[mask].index[beyond_live]
                    if len(future_indices) > 0:
                        ts_local_future = ts_local[beyond_live]
                        row_hours = ts_local_future.dt.hour.values
                        row_is_weekends = (ts_local_future.dt.dayofweek >= 5).values

                        # Start with default weekday/weekend rolling averages
                        hour_hists = np.where(
                            row_is_weekends, rolling_avg_weekend, rolling_avg_weekday
                        )

                        # Lookup from _attr_lookup if key exists
                        ts_minus_7d = ts_local_future - pd.Timedelta(days=7)
                        dates = ts_minus_7d.dt.date.values

                        for i, idx in enumerate(future_indices):
                            hour_key = (dates[i], row_hours[i])
                            if hour_key in _attr_lookup:
                                hour_hists[i] = _attr_lookup[hour_key]

                        df.loc[future_indices, "avg_wait_last_1h"] = hour_hists
                else:
                    df.loc[mask, "avg_wait_same_dow_4w"] = _same_dow_avg_fast(
                        base_time_local, [1, 2, 3, 4]
                    )

    # Calculate wait time velocity (momentum) BEFORE overriding lags
    # Initialize with default (no change)
    df["wait_time_velocity"] = 0.0

    # Initialize trend and volatility if not already set
    if "trend_7d" not in df.columns:
        df["trend_7d"] = 0.0
    if "volatility_7d" not in df.columns:
        df["volatility_7d"] = 0.0
    if "volatility_weekday" not in df.columns:
        df["volatility_weekday"] = 0.0
    if "volatility_weekend" not in df.columns:
        df["volatility_weekend"] = 0.0
    if "rolling_avg_weekday" not in df.columns:
        df["rolling_avg_weekday"] = df.get("rolling_avg_7d", 0.0)
    if "rolling_avg_weekend" not in df.columns:
        df["rolling_avg_weekend"] = df.get("rolling_avg_7d", 0.0)
    if "rolling_avg_28d" not in df.columns:
        df["rolling_avg_28d"] = df.get("rolling_avg_7d", 0.0)
    if "rolling_avg_90d" not in df.columns:
        df["rolling_avg_90d"] = df.get("rolling_avg_7d", 0.0)
    if "avg_wait_same_dow_4w" not in df.columns:
        df["avg_wait_same_dow_4w"] = df.get("avg_wait_same_hour_last_week", 0.0)

    # Override lags with current wait times if available (Autoregression) - only
    # on rows within LIVE_LAG_MAX_LEAD of base_time (PAR-1030). It used to set
    # every row of the ride, 12-48 h ahead too, over the profile set above.
    # `recent_wait_times` is no longer read: velocity comes from the hourly
    # history, the series training differences.
    if current_wait_times:
        _base = pd.Timestamp(base_time)
        _base = _base.tz_localize("UTC") if _base.tzinfo is None else _base.tz_convert("UTC")
        _ts = pd.to_datetime(df["timestamp"])
        _ts = _ts.dt.tz_localize("UTC") if _ts.dt.tz is None else _ts.dt.tz_convert("UTC")
        live_rows = _ts <= _base + LIVE_LAG_MAX_LEAD
        _local_hour = {}  # tz -> base_time's local hour start (naive)
        for attraction_id, wait_time in current_wait_times.items():
            if wait_time is None:
                continue
            mask = (df["attractionId"] == str(attraction_id)) & live_rows
            if not mask.any():
                continue
            history = recent_data[recent_data["attractionId"] == str(attraction_id)]
            earlier = []
            if not history.empty:
                tz = parks_metadata.loc[
                    parks_metadata["park_id"] == df.loc[mask, "parkId"].iloc[0],
                    "timezone",
                ]
                tz = tz.iloc[0] if len(tz) else "UTC"
                if tz not in _local_hour:
                    _local_hour[tz] = (
                        _base.tz_convert(tz).tz_localize(None).floor("h")
                    )
                slot = pd.to_datetime(history["date"]) + pd.to_timedelta(
                    history["hour"].astype(int), unit="h"
                )
                before_now = (slot < _local_hour[tz]).values
                earlier = (
                    history.assign(_slot=slot.values)[before_now]
                    .sort_values("_slot")["avg_wait"]
                    .tolist()
                )
            df.loc[mask, "wait_time_velocity"] = live_wait_velocity(
                wait_time, earlier
            )
            df.loc[mask, "avg_wait_last_1h"] = float(wait_time)

    # Add percentile features (Weather extremes)
    df = add_percentile_features(df)

    # Add attraction and park features (using available data only)
    from attraction_features import (
        add_park_attraction_count_feature,
    )

    df = add_park_attraction_count_feature(df, parks_metadata)

    # Phase 2: Add real-time context features
    if feature_context:
        from features import (
            add_park_occupancy_feature,
            add_downtime_features,
            add_virtual_queue_feature,
            add_park_has_schedule_feature,
            add_interaction_features,
        )

        # time_since_park_open_mins comes from apply_schedule_features and
        # is_bridge_day from the holiday block, both per row (PAR-1030).
        df = add_park_occupancy_feature(df, feature_context)
        df = add_downtime_features(df, feature_context)
        df = add_virtual_queue_feature(df, feature_context)
        df = add_park_has_schedule_feature(
            df, feature_context
        )  # NEW: Data quality indicator

        # Interaction features (must be after all base features are added)
        df = add_interaction_features(df)
    else:
        # Defaults if no feature_context provided — use historical occupancy profile instead of flat 100%
        from features import add_park_occupancy_feature

        occ_context = {"historicalOccupancy": hist_occ, "baseTime": base_time}
        df = add_park_occupancy_feature(df, occ_context)
        df["had_downtime_today"] = 0
        df["downtime_minutes_today"] = 0.0
        df["has_virtual_queue"] = 0
        df["park_has_schedule"] = (
            1  # Default to 1 (assume schedule exists for better quality)
        )

        # Interaction features with defaults
        from features import add_interaction_features

        df = add_interaction_features(df)

    return df


def crowd_level_for(
    crowd_wait: float,
    typical_day_peak_baseline: float | None,
    p50_baseline: float | None,
) -> tuple:
    """(crowd level, baseline) of a predicted wait against the park's real baseline.

    The TYPICAL-DAY-PEAK baseline comes first so the ML service's crowd levels
    match the calendar (100% = a typical day = moderate); P50 when it is absent.
    Without either there is no baseline to rate against, and the answer is
    ("unknown", 0.0) (docs/rules/crowd-levels.md) - never a placeholder
    "moderate", and never the ride's own rolling_avg_7d, which rates on a
    different scale than the park path does (PAR-943). The 0.0 baseline keeps
    the API's served re-tiering (PCN / Chronos, gated on baseline > 0) off it.

    Same thresholds as the TypeScript determineCrowdLevel(): 60, 89, 110, 150, 200.
    """
    if typical_day_peak_baseline is not None and typical_day_peak_baseline > 0:
        baseline = typical_day_peak_baseline
    elif p50_baseline is not None and p50_baseline > 0:
        baseline = p50_baseline
    else:
        return "unknown", 0.0

    occupancy_pct = crowd_wait / baseline * 100
    if occupancy_pct <= 60:
        level = "very_low"
    elif occupancy_pct <= 89:
        level = "low"
    elif occupancy_pct <= 110:  # 90-110%: +/-10% around a typical day = moderate
        level = "moderate"
    elif occupancy_pct <= 150:
        level = "high"
    elif occupancy_pct <= 200:
        level = "very_high"
    else:
        level = "extreme"
    return level, baseline


def predict_wait_times(
    model: WaitTimeModel,
    attraction_ids: List[str],
    park_ids: List[str],
    prediction_type: str = "hourly",
    base_time: Optional[datetime] = None,
    weather_forecast: Optional[List[Dict[str, Any]]] = None,
    current_wait_times: Optional[Dict[str, int]] = None,
    recent_wait_times: Optional[Dict[str, int]] = None,
    feature_context: Optional[Dict[str, Any]] = None,
    p50_baseline: Optional[float] = None,  # P50 baseline (crowd-level fallback)
    typical_day_peak_baseline: Optional[float] = None,  # primary crowd-level ref
) -> List[Dict[str, Any]]:
    """
    Predict wait times for attractions

    Args:
        model: Trained WaitTimeModel
        attraction_ids: List of attraction IDs to predict for
        park_ids: Corresponding park IDs
        prediction_type: 'hourly' or 'daily'
        base_time: Starting time (defaults to now)
        weather_forecast: Optional hourly weather forecast
        current_wait_times: Optional current wait times for autoregression

    Returns:
        List of predictions:
        [
            {
                'attractionId': str,
                'predictedTime': datetime,
                'predictedWaitTime': int,
                'predictionType': str,
                'confidence': float (0-100)
            }
        ]
    """
    if base_time is None:
        base_time = datetime.now(timezone.utc)

    # Generate future timestamps
    timestamps = generate_future_timestamps(base_time, prediction_type)

    # Daily rows sit on each park's OWN clock (PAR-818): one timestamp list per
    # park timezone, so "the peak hours of tomorrow" mean the park's afternoon.
    timestamps_by_park: Optional[Dict[str, List[datetime]]] = None
    park_tz: Dict[str, str] = {}
    if prediction_type == "daily":
        park_tz = _park_timezones(park_ids)
        by_tz: Dict[str, List[datetime]] = {}
        timestamps_by_park = {}
        for pid in {str(p) for p in park_ids}:
            tz_name = park_tz.get(pid, "UTC")
            if tz_name not in by_tz:
                by_tz[tz_name] = generate_future_timestamps(
                    base_time, prediction_type, tz_name
                )
            timestamps_by_park[pid] = by_tz[tz_name]

    # Create features with all DB-loaded data
    # Reduced logging - only log summary, not details

    # Features
    features_df = create_prediction_features(
        attraction_ids,
        park_ids,
        timestamps,
        base_time,
        weather_forecast,
        current_wait_times,
        recent_wait_times,
        feature_context,
        timestamps_by_park=timestamps_by_park,
        # The definition the active model was trained on (PAR-1045).
        curated_ride_type=bool(
            (getattr(model, "metadata", None) or {}).get("curated_ride_type")
        ),
    )

    # OPTIMIZATION: Skip ML inference for CLOSED days when schedule exists.
    # UNKNOWN days are kept because they should receive predictions (Calendar expects ML or fallback "moderate" for them).
    #
    # CRITICAL - Parks with "quasi keine" schedules still get predictions:
    # - No schedule in DB for prediction range → status stays OPERATING (default) → we keep all rows
    # - Only UNKNOWN/CLOSED in range (no OPERATING) → park_has_operating=False → we keep default
    df_inference = features_df[features_df["status"].isin(["OPERATING", "UNKNOWN"])]
    if df_inference.empty:
        return []  # All days explicitly closed; filter_predictions_by_schedule would return [] anyway

    # Predict (only for OPERATING rows). Two regimes:
    #  - MultiQuantile model → PER-PURPOSE serving: `predictions` is the MEDIAN
    #    (honest wait display), `crowd_predictions` is q0.8 (busy-calibrated crowd
    #    signal), uncertainty = (top quantile − median) spread.
    #  - any other model → the legacy single-point + VirtEnsembles path, with the
    #    crowd signal equal to the displayed prediction (unchanged behaviour).
    crowd_predictions = None
    quantiles = {}
    try:
        quantiles = model.predict_quantiles(df_inference)
    except Exception:
        quantiles = {}

    if quantiles:
        def _pick(target: float):
            a = min(quantiles, key=lambda x: abs(x - target))
            return quantiles[a]

        predictions = _pick(getattr(settings, "SERVING_WAIT_QUANTILE", 0.5))
        crowd_predictions = _pick(getattr(settings, "SERVING_CROWD_QUANTILE", 0.8))
        # The top trained quantile (alpha=0.95) is used ONLY here, as headroom for
        # the uncertainty band — NOT as a served crowd/display value (it over-shoots
        # busy days). quantiles are now monotonic per row (model.predict_quantiles),
        # so hi >= median and the width can no longer silently collapse from crossing.
        hi = quantiles[max(quantiles)]
        uncertainties = np.maximum(hi - predictions, 0.0)
        use_uncertainty = bool(np.nanmax(uncertainties) > 1e-6)
    else:
        try:
            uncertainty_results = model.predict_with_uncertainty(df_inference)
            predictions = uncertainty_results["predictions"]
            uncertainties = uncertainty_results["uncertainty"]
            # A zero-width band means the model has no real uncertainty (e.g. a
            # Quantile model, or a collapsed VirtEnsembles) → don't derive a
            # (falsely high) model-confidence from it; use time-based confidence.
            use_uncertainty = bool(np.nanmax(uncertainties) > 1e-6) if len(uncertainties) else False
        except Exception as e:
            # Fallback to regular predictions if uncertainty estimation fails
            # Reduced logging - only log if it's a real issue
            if "missing" not in str(e).lower():
                print(f"⚠️  Uncertainty estimation failed: {e}")
            predictions = model.predict(df_inference)
            uncertainties = np.zeros(len(predictions))
            use_uncertainty = False
    if crowd_predictions is None:
        crowd_predictions = predictions

    # Format results (OPERATING and UNKNOWN rows; CLOSED never reach the client)
    results = []
    result_local_dates: List[str] = []  # park-local date per result (daily collapse)
    for i, (idx, row) in enumerate(df_inference.iterrows()):
        pred_wait = round_to_nearest_5(predictions[i])
        # Crowd-level uses the (busy-calibrated) crowd quantile, which differs from
        # the displayed median only for a MultiQuantile model; otherwise identical.
        crowd_wait = round_to_nearest_5(crowd_predictions[i])

        # Enforce a minimum 5 min wait if the ride is considered operating by the model
        # or zero if the model thinks it's closed anyway. Mirrored exactly by NestJS
        # roundServedWait and pcn-service serve_round (10 until PAR-1027).
        if row["status"] in ["UNKNOWN", "OPERATING"] and pred_wait > 0:
            pred_wait = max(5, pred_wait)
        if row["status"] in ["UNKNOWN", "OPERATING"] and crowd_wait > 0:
            crowd_wait = max(5, crowd_wait)

        # Calculate combined confidence (60% time-based + 40% model-based)
        hours_ahead = (row["timestamp"] - base_time).total_seconds() / 3600

        # Time-based confidence (60% weight)
        if prediction_type == "hourly":
            # NOT an error estimate. This line is a distance heuristic and is
            # not fitted to measured error: over the first 24 h the measured
            # MAE rises about 4.9% (10.913 -> 11.453, 421 attractions paired
            # across eight 3 h bands, prediction_accuracy, 30 days) while this
            # term falls from 95 to 50. Beyond 24 h the error is not measurable
            # at all, because recordPredictions keeps only the latest
            # prediction per target time (PAR-658). The floor is reached at
            # 22.5 h, so 103 of the 192 slots (53.6%) share the same value.
            # Read it as "how far ahead", never as "how accurate" (PAR-449).
            time_confidence = max(
                50, 95 - (hours_ahead * 2)
            )  # 95% at t+1h, drops to 50%
        else:
            # Daily: 85% at t+1d, drops to 30% at t+365d
            days_ahead = hours_ahead / 24
            time_confidence = max(30, 85 - (days_ahead * 0.15))

        # Model-based confidence from uncertainty (40% weight)
        if use_uncertainty and pred_wait > 0:
            # Calculate relative uncertainty (uncertainty / prediction)
            relative_uncertainty = uncertainties[i] / max(pred_wait, 1)
            # Convert to confidence: low uncertainty = high confidence
            # Cap relative uncertainty at 100% (1.0) for confidence calculation
            model_confidence = max(30, 100 * (1 - min(relative_uncertainty, 1.0)))
        else:
            # Fallback: use time confidence if no uncertainty available
            model_confidence = time_confidence

        # Combined confidence (weighted average)
        confidence = 0.6 * time_confidence + 0.4 * model_confidence

        # Crowd level off the busy-calibrated crowd quantile (= pred_wait for
        # non-MultiQuantile models), so the median display can read honestly low
        # on quiet rides without dragging the crowd signal down.
        crowd_level, baseline = crowd_level_for(
            crowd_wait, typical_day_peak_baseline, p50_baseline
        )

        # The uncertainty band, in minutes. `uncertainties[i]` is (top quantile −
        # median) for a MultiQuantile model, i.e. the width alpha=0.95 is trained
        # for and which config.py:104 calls "the displayed uncertainty width" —
        # until now it was folded into `confidence` above and then dropped, so no
        # consumer could draw it. It is the RAW spread against the unrounded
        # median, deliberately not rounded to 5: a band is a difference, not a
        # posted wait time, and parks post waits in fives while spreads are not.
        # None (not 0) when the model has no real uncertainty — a zero-width band
        # and "no band at all" are different statements, and a 0 would draw as a
        # confident hairline.
        uncertainty_minutes = (
            int(round(float(uncertainties[i]))) if use_uncertainty else None
        )

        # The raw q0.8 and q0.95 (PAR-912), so the API can store them beside the
        # served median and measure their live coverage. q0.8 is what the crowd
        # level reads (before its round-to-5); q0.95 is the band's top. Whole
        # minutes, never rounded to 5: a coverage test against a posted-wait
        # rounding would measure the rounding. None without a quantile head.
        q80_minutes, q95_minutes = quantile_minutes(quantiles, i)

        results.append(
            {
                "attractionId": row["attractionId"],
                "parkId": row["parkId"],  # Include parkId for schedule filtering
                "predictedTime": row["timestamp"].isoformat(),
                "predictedWaitTime": pred_wait,
                "predictionType": prediction_type,
                "confidence": round(confidence, 1),
                "uncertaintyMinutes": uncertainty_minutes,
                "predictedQ80": q80_minutes,
                "predictedQ95": q95_minutes,
                "crowdLevel": crowd_level,
                "baseline": round(baseline, 1),
                "modelVersion": model.version,
                "status": row["status"],
                "trend": "stable",  # Default
            }
        )
        result_local_dates.append(str(row.get("date_local", row["timestamp"].date())))

        # Calculate Trend
        # Compare current prediction window to previous window or current actual
        # Here we only have the current prediction "row", so we need context.
        # But we can calculate trend if we look at the sequence of predictions for this attraction.
        # Since we are iterating row by row, this is hard.
        # A simpler way is to calc trend AFTER collecting all results for an attraction.
        # BUT, for single-point prediction (e.g. next 1h), we compare to current_wait_times!

        if current_wait_times and row["attractionId"] in current_wait_times:
            current_actual = current_wait_times[row["attractionId"]]
            diff = pred_wait - current_actual
            if diff > 5:
                results[-1]["trend"] = "increasing"
            elif diff < -5:
                results[-1]["trend"] = "decreasing"
            else:
                results[-1]["trend"] = "stable"
        elif len(results) > 1 and results[-2]["attractionId"] == row["attractionId"]:
            # If no current actual, compare to previous hour prediction
            prev_pred = results[-2]["predictedWaitTime"]
            diff = pred_wait - prev_pred
            if diff > 5:
                results[-1]["trend"] = "increasing"
            elif diff < -5:
                results[-1]["trend"] = "decreasing"
            else:
                results[-1]["trend"] = "stable"

    # Daily = PEAK: we predicted several peak-window hours/day → collapse to one row
    # per (attraction, day) = the MAX predicted wait (peak proxy ≈ P90), so the daily
    # value matches the typical-day-peak baseline the calendar uses (not the lower
    # 14:00 value, which under-read the peak by 13-45 min on busy headliner days).
    #
    # The day is the park-LOCAL date (PAR-818), never the first ten characters of
    # the UTC ISO string: with the peak hours on the park's clock, Los Angeles'
    # 16:00 slot is 00:00 UTC of the next day in winter and would otherwise be
    # collapsed into tomorrow. The surviving row is re-published at local noon of
    # its day (see daily_anchor_time) so every downstream date key agrees.
    if prediction_type == "daily" and results:
        best: Dict[Any, Dict[str, Any]] = {}
        for r, local_date in zip(results, result_local_dates):
            key = (r["attractionId"], local_date)  # (attraction, park-local YYYY-MM-DD)
            if key not in best or r["predictedWaitTime"] > best[key]["predictedWaitTime"]:
                best[key] = r
        for (_, local_date), r in best.items():
            r["predictedTime"] = daily_anchor_time(
                local_date, park_tz.get(str(r["parkId"]), "UTC")
            )
        results = list(best.values())

    # NOTE: CLOSED/UNKNOWN rows were excluded before inference (no ML call for closed days).
    # Schedule filtering will be applied after this function returns.
    # See filter_predictions_by_schedule() for operating hours filtering
    return results


def predict_for_park(
    model: WaitTimeModel, park_id: str, prediction_type: str = "hourly"
) -> List[Dict[str, Any]]:
    """
    Predict wait times for all attractions in a park

    Args:
        model: Trained model
        park_id: Park ID
        prediction_type: 'hourly' or 'daily'

    Returns:
        List of predictions
    """
    attraction_ids = fetch_attraction_ids_for_park(park_id)
    park_ids = [park_id] * len(attraction_ids)

    if len(attraction_ids) == 0:
        return []

    return predict_wait_times(model, attraction_ids, park_ids, prediction_type)
