"""Data layer for the PCN service.

Fetches the raw per-(ride, 15-min slot) wait panel for a park, park-local, exactly
on the same grid CatBoost serves and nf-service backtests on. The cross-ride TENSOR
assembly (the park-wide crowd matrix) lives in `tensor.py` (pure pandas/numpy, no DB)
so it is unit-testable without a database.

Conventions mirrored from nf-service/db.py + nf-service/backtest_intraday_nowcast.py:
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
logger = logging.getLogger("pcn.db")

# Postgres bins to park-LOCAL 15-min slots. `{col} AT TIME ZONE :tz` yields a naive
# local-wall-clock timestamp; date_bin floors it to the slot grid anchored at a fixed
# origin so every park's slots align to :00/:15/:30/:45. Identical to nf-service.
_SLOT_MIN = get_settings().PCN_SLOT_MINUTES
BIN = (
    f"date_bin('{_SLOT_MIN} minutes', {{col}} AT TIME ZONE :tz, "
    "TIMESTAMP '2000-01-01 00:00:00')"
)


def get_engine():
    url = (
        f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


_engine = get_engine()


def fetch_park_ids() -> list[str]:
    """All park UUIDs (as text) that have attractions."""
    sql = text(
        'SELECT DISTINCT "parkId"::text AS pid FROM attractions '
        'WHERE "parkId" IS NOT NULL ORDER BY 1'
    )
    with _engine.connect() as c:
        df = pd.read_sql(sql, c)
    return df["pid"].tolist()


def fetch_attraction_meta(park_ids: list[str]) -> pd.DataFrame:
    """One row per attraction in scope: park, timezone, country/region, name.
    timezone is needed to bin to park-local slots; name/type help the static
    ride embeddings later."""
    where = 'WHERE a."parkId"::text = ANY(:pids)' if park_ids else ""
    sql = text(
        f"""
        SELECT a.id::text         AS unique_id,
               a."parkId"::text   AS park_id,
               a.name             AS name,
               a."attractionType" AS attraction_type,
               p.timezone         AS timezone,
               p."countryCode"    AS country,
               p."regionCode"     AS region
        FROM attractions a
        JOIN parks p ON p.id = a."parkId"
        {where}
        """
    )
    with _engine.connect() as c:
        df = pd.read_sql(sql, c, params={"pids": park_ids} if park_ids else {})
    return df


def fetch_cross_ride_panel(park_id: str, tz: str) -> pd.DataFrame:
    """Long panel for ONE park: one row per (ride, 15-min local slot) within the
    history window.

    Columns:
      - unique_id  : attraction id (text)
      - ds         : park-local 15-min slot (naive local wall time)
      - y          : MEDIAN STANDBY wait over OPERATING rows with waitTime >= MIN_WAIT
                     (NULL if the slot had only DOWN/closed rows — a real "down" slot)
      - n_obs      : # of operating real-wait rows in the slot (sensor density)
      - down_count : # of DOWN-status rows in the slot (ride reporting but down)

    A slot survives if it has EITHER a real observation OR a DOWN signal, so the
    park-open heuristic (tensor.py) can tell "park open but ride down" from "no data".
    We do NOT zero-fill — closed slots are simply absent and become available_mask=0
    downstream (the flaw that broke the 2026-05-23 hourly PoC was 0-filling).
    """
    minw = settings.PCN_MIN_WAIT
    window = settings.PCN_WINDOW_DAYS
    real = f"qd.status = 'OPERATING' AND qd.\"waitTime\" >= {minw}"
    sql = text(
        f"""
        SELECT qd."attractionId"::text AS unique_id,
               {BIN.format(col='qd.timestamp')} AS ds,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime")
                   FILTER (WHERE {real})                       AS y,
               COUNT(*) FILTER (WHERE {real})                  AS n_obs,
               COUNT(*) FILTER (WHERE qd.status = 'DOWN')      AS down_count
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = :park
          AND qd.timestamp >= NOW() - (:w || ' days')::interval
          AND qd."queueType" = 'STANDBY'
        GROUP BY 1, 2
        HAVING COUNT(*) FILTER (WHERE {real}) >= 1
            OR COUNT(*) FILTER (WHERE qd.status = 'DOWN') >= 1
        """
    )
    with _engine.connect() as c:
        df = pd.read_sql(
            sql, c, params={"park": park_id, "tz": tz, "w": str(window)}
        )
    if df.empty:
        return df
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = df["y"].astype(float)
    df["n_obs"] = df["n_obs"].astype(int)
    df["down_count"] = df["down_count"].astype(int)
    return df
