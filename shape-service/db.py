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
