"""Data layer for the Shape service.

Fetches the per-(ride, park-local day, slot-of-day) median-wait panel for a park, on the
same park-local 15-min grid the rest of the stack serves on. The profile assembly (the
normalised daily form) lives in `profiles.py` (pure pandas/numpy, no DB) so it is
unit-testable without a database.

Conventions mirrored from pcn-service/db.py:
  - park-local binning via date_bin('15 minutes', ts AT TIME ZONE tz, origin)
  - STANDBY queue, waitTime >= MIN_WAIT, status='OPERATING' for the real-wait median
  - queue_data has no parkId → always JOIN attractions to reach the park + timezone
"""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import create_engine, text

from config import get_settings

settings = get_settings()
logger = logging.getLogger("shape.db")

_SLOT_MIN = settings.SHAPE_SLOT_MINUTES
# Park-LOCAL bin: `ts AT TIME ZONE tz` → naive local wall-clock; date_bin floors to the slot
# grid anchored at a fixed origin so every park aligns to :00/:15/:30/:45. Same as PCN.
_BIN = (
    f"date_bin('{_SLOT_MIN} minutes', qd.timestamp AT TIME ZONE :tz, "
    "TIMESTAMP '2000-01-01 00:00:00')"
)
# slot-of-day index 0..(slots_per_day-1) from the park-local bin.
_SLOT_OF_DAY = (
    f"(EXTRACT(HOUR FROM b.bin)::int * {60 // _SLOT_MIN} "
    f"+ EXTRACT(MINUTE FROM b.bin)::int / {_SLOT_MIN})"
)

_engine = None


def get_engine():
    url = (
        f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


def engine():
    global _engine
    if _engine is None:
        _engine = get_engine()
    return _engine


def fetch_park_ids() -> list[str]:
    """All park UUIDs (as text) that have attractions."""
    sql = text(
        'SELECT DISTINCT "parkId"::text AS pid FROM attractions '
        'WHERE "parkId" IS NOT NULL ORDER BY 1'
    )
    with engine().connect() as c:
        return pd.read_sql(sql, c)["pid"].tolist()


def park_timezone(park_id: str) -> str | None:
    sql = text('SELECT timezone FROM parks WHERE id = :pid')
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"pid": park_id})
    return None if df.empty else df["timezone"].iloc[0]


def park_meta(park_id: str) -> dict | None:
    """timezone + country/region (for the holiday-driven daytype conditioner)."""
    sql = text('SELECT timezone, "countryCode" AS country, "regionCode" AS region '
               'FROM parks WHERE id = :pid')
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"pid": park_id})
    return None if df.empty else df.iloc[0].to_dict()


def fetch_holidays(country: str) -> pd.DataFrame:
    """Holiday dates for a country: date, region, holidayType (school | public | bridge |
    observance | bank). School holidays (ferien) are regional; the daytype builder filters
    to nationwide + the park's own region."""
    sql = text('SELECT date, region, "holidayType" AS holiday_type '
               'FROM holidays WHERE country = :c')
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"c": country})
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def fetch_shape_panel(park_id: str, tz: str) -> pd.DataFrame:
    """Long panel for ONE park: one row per (ride, park-local day, slot-of-day) within the
    history window.

    Columns:
      - unique_id : attraction id (text)
      - day       : park-local calendar date (the day the form belongs to)
      - slot      : slot-of-day index 0..slots_per_day-1
      - y         : MEDIAN STANDBY wait over OPERATING rows with waitTime >= MIN_WAIT

    Only real-observed operating slots survive (no zero-fill) — closed slots are simply
    absent, exactly as the daily form should treat them.
    """
    minw = settings.SHAPE_MIN_WAIT
    window = settings.SHAPE_WINDOW_DAYS
    sql = text(
        f"""
        WITH b AS (
            SELECT qd."attractionId"::text AS unique_id,
                   {_BIN} AS bin,
                   qd."waitTime" AS w
            FROM queue_data qd
            JOIN attractions a ON a.id = qd."attractionId"
            WHERE a."parkId" = :park
              AND qd.timestamp >= NOW() - (:win || ' days')::interval
              AND qd."queueType" = 'STANDBY'
              AND qd.status = 'OPERATING'
              AND qd."waitTime" >= {minw}
        )
        SELECT unique_id,
               b.bin::date                                          AS day,
               {_SLOT_OF_DAY}                                       AS slot,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.w)     AS y
        FROM b
        GROUP BY 1, 2, 3
        """
    )
    with engine().connect() as c:
        df = pd.read_sql(
            sql, c, params={"park": park_id, "tz": tz, "win": str(window)}
        )
    if df.empty:
        return df
    df["day"] = pd.to_datetime(df["day"])
    df["slot"] = df["slot"].astype(int)
    df["y"] = df["y"].astype(float)
    return df


# --------------------------------------------------------------------------
# Shadow producer + scorer (Phase 1, design §6/§8). Mirrors pcn-service: a durable
# going-forward snapshot (shape_forecasts) + a segmented board (shape_comparisons),
# at the DAILY-curve horizon. SHADOW only — never replaces the served champion.
# --------------------------------------------------------------------------

_DDL_SHAPE_FORECASTS = text(
    """
    CREATE TABLE IF NOT EXISTS shape_forecasts (
        attraction_id  uuid              NOT NULL,
        target_slot    timestamp         NOT NULL,  -- park-local 15-min slot
        origin_date    date              NOT NULL,  -- park-local date the forecast was made
        predicted_wait double precision  NOT NULL,
        model_version  text,
        created_at     timestamptz       NOT NULL DEFAULT now(),
        PRIMARY KEY (attraction_id, target_slot, origin_date)
    )
    """
)

_DDL_SHAPE_COMPARISONS = text(
    """
    CREATE TABLE IF NOT EXISTS shape_comparisons (
        target_date  date              NOT NULL,
        model        varchar(16)       NOT NULL,  -- 'shape' | 'catboost'
        segment      varchar(16)       NOT NULL,  -- 'all'|'quiet'|'mid'|'busy'
        lead_bucket  varchar(16)       NOT NULL,  -- 'all'|'<=7d'|'8-30d'|'>30d'
        n            int               NOT NULL,
        mae          double precision  NOT NULL,
        bias         double precision  NOT NULL,
        mean_actual  double precision  NOT NULL,
        mean_pred    double precision  NOT NULL,
        created_at   timestamptz       NOT NULL DEFAULT now(),
        PRIMARY KEY (target_date, model, segment, lead_bucket)
    )
    """
)


def fetch_daily_levels(park_id: str, tz: str, lo_day, hi_day) -> pd.DataFrame:
    """Freshest DAILY forecast per (attraction, park-local day) in [lo_day, hi_day] — the
    LEVEL the shape expands. predictionType='daily' is one row/(ride, day) at 12:00 UTC."""
    bin_day = f'(wp."predictedTime" AT TIME ZONE :tz)::date'
    sql = text(
        f"""
        SELECT DISTINCT ON (wp."attractionId", {bin_day})
            wp."attractionId"::text AS unique_id, {bin_day} AS day,
            wp."predictedWaitTime"::float AS level
        FROM wait_time_predictions wp JOIN attractions a ON a.id = wp."attractionId"
        WHERE a."parkId" = :park AND wp."predictionType" = 'daily'
          AND {bin_day} >= :lo AND {bin_day} <= :hi
        ORDER BY wp."attractionId", {bin_day}, wp."createdAt" DESC
        """
    )
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"park": park_id, "tz": tz, "lo": lo_day, "hi": hi_day})
    if not df.empty:
        df["day"] = pd.to_datetime(df["day"])
        df["level"] = df["level"].astype(float)
    return df


def write_shape_forecasts(rows: list[dict], version: str) -> int:
    """Durable, immutable-per-origin upsert of the rendered forward curves."""
    if not rows:
        return 0
    for r in rows:
        r["ver"] = version
    upsert = text(
        """
        INSERT INTO shape_forecasts
            (attraction_id, target_slot, origin_date, predicted_wait, model_version)
        VALUES (:aid, :ts, :od, :pw, :ver)
        ON CONFLICT (attraction_id, target_slot, origin_date)
        DO UPDATE SET predicted_wait = EXCLUDED.predicted_wait,
                      model_version  = EXCLUDED.model_version, created_at = now()
        """
    )
    with engine().begin() as c:
        c.execute(_DDL_SHAPE_FORECASTS)
        for i in range(0, len(rows), 5000):
            c.execute(upsert, rows[i:i + 5000])
    return len(rows)


def fetch_shape_forecasts_window(park_id: str, lo_local, hi_local) -> pd.DataFrame:
    """ALL genuine-forward shape forecasts (origin_date < target date) whose TARGET slot is
    in [lo, hi) (park-local). Kept per-origin (not deduped) so the board can break accuracy
    out by lead (days between origin and target)."""
    sql = text(
        """
        SELECT f.attraction_id::text AS unique_id, f.target_slot, f.origin_date,
               f.predicted_wait
        FROM shape_forecasts f JOIN attractions a ON a.id = f.attraction_id
        WHERE a."parkId" = :park AND f.target_slot >= :lo AND f.target_slot < :hi
          AND f.origin_date < f.target_slot::date
        """
    )
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"park": park_id, "lo": lo_local, "hi": hi_local})
    if not df.empty:
        df["target_slot"] = pd.to_datetime(df["target_slot"])
        df["origin_date"] = pd.to_datetime(df["origin_date"])
    return df


def fetch_actuals_slots(park_id: str, tz: str, lo_utc, hi_utc) -> pd.DataFrame:
    """Realised 15-min median wait per (attraction, park-local slot) — scoring truth."""
    sql = text(
        f"""
        SELECT qd."attractionId"::text AS unique_id,
               {_BIN} AS target_slot,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") AS actual
        FROM queue_data qd JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = :park AND qd.timestamp >= :lo AND qd.timestamp < :hi
          AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY' AND qd."waitTime" >= {settings.SHAPE_MIN_WAIT}
        GROUP BY 1, 2 HAVING COUNT(*) >= 1
        """
    )
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"park": park_id, "tz": tz, "lo": lo_utc, "hi": hi_utc})
    if not df.empty:
        df["target_slot"] = pd.to_datetime(df["target_slot"])
        df["actual"] = df["actual"].astype(float)
    return df


def fetch_catboost_slots(park_id: str, tz: str, lo_utc, hi_utc) -> pd.DataFrame:
    """CatBoost's freshest 15-min pred per (attraction, park-local slot) for the head-to-head
    (predictionType='hourly' is the 15-min misnomer)."""
    b = _BIN
    sql = text(
        f"""
        SELECT DISTINCT ON (wp."attractionId", {b.replace('qd.timestamp', 'wp."predictedTime"')})
            wp."attractionId"::text AS unique_id,
            {b.replace('qd.timestamp', 'wp."predictedTime"')} AS target_slot,
            wp."predictedWaitTime"::float AS cat_pred
        FROM wait_time_predictions wp JOIN attractions a ON a.id = wp."attractionId"
        WHERE a."parkId" = :park AND wp."predictionType" = 'hourly'
          AND wp."predictedTime" >= :lo AND wp."predictedTime" < :hi
        ORDER BY wp."attractionId", {b.replace('qd.timestamp', 'wp."predictedTime"')}, wp."createdAt" DESC
        """
    )
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"park": park_id, "tz": tz, "lo": lo_utc, "hi": hi_utc})
    if not df.empty:
        df["target_slot"] = pd.to_datetime(df["target_slot"])
        df["cat_pred"] = df["cat_pred"].astype(float)
    return df


def upsert_shape_comparisons(rows: list[dict]) -> int:
    if not rows:
        return 0
    upsert = text(
        """
        INSERT INTO shape_comparisons
            (target_date, model, segment, lead_bucket, n, mae, bias, mean_actual, mean_pred)
        VALUES (:target_date, :model, :segment, :lead_bucket, :n, :mae, :bias,
                :mean_actual, :mean_pred)
        ON CONFLICT (target_date, model, segment, lead_bucket)
        DO UPDATE SET n=EXCLUDED.n, mae=EXCLUDED.mae, bias=EXCLUDED.bias,
                      mean_actual=EXCLUDED.mean_actual, mean_pred=EXCLUDED.mean_pred,
                      created_at=now()
        """
    )
    with engine().begin() as c:
        c.execute(_DDL_SHAPE_COMPARISONS)
        c.execute(upsert, rows)
    return len(rows)
