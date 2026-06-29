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


# Lazy engine: created on first use so importing this module (e.g. for the pure
# aggregation in score.py, or /health) does NOT require the psycopg2 driver / a DB.
_engine = None


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
    with engine().connect() as c:
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
    with engine().connect() as c:
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


# --------------------------------------------------------------------------
# Shadow producer + scorer (design doc §12). Mirrors nf-service's durable
# tft_forecasts snapshot + the score-comparison job, at 15-min intraday grain.
# --------------------------------------------------------------------------

_DDL_PCN_FORECASTS = text(
    """
    CREATE TABLE IF NOT EXISTS pcn_forecasts (
        attraction_id  uuid                NOT NULL,
        target_slot    timestamp           NOT NULL,  -- park-local 15-min slot
        origin_slot    timestamp           NOT NULL,  -- slot the forecast was made at
        quantile       real                NOT NULL,  -- 0.5 (display) / 0.8 (crowd)
        predicted_wait double precision    NOT NULL,
        model_version  text,
        created_at     timestamptz         NOT NULL DEFAULT now(),
        PRIMARY KEY (attraction_id, target_slot, origin_slot, quantile)
    )
    """
)

_DDL_PCN_COMPARISONS = text(
    """
    CREATE TABLE IF NOT EXISTS pcn_intraday_comparisons (
        target_date  date              NOT NULL,
        model        varchar(16)       NOT NULL,  -- 'pcn' | 'catboost' | 'persist'
        segment      varchar(16)       NOT NULL,  -- 'all'|'quiet'|'mid'|'busy'
        lead_bucket  varchar(16)       NOT NULL,  -- 'all'|'<=3h'|'3-6h'|'>6h'
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


def write_pcn_forecasts(rows: list[dict], version: str) -> int:
    """Durable, immutable-per-origin upsert of forward 15-min forecasts. Re-running the
    same origin overwrites only that origin's rows; past origins are preserved, so the
    genuine forward record (made before the target) survives for scoring — the same
    going-forward-shadow contract as tft_forecasts."""
    if not rows:
        return 0
    upsert = text(
        """
        INSERT INTO pcn_forecasts
            (attraction_id, target_slot, origin_slot, quantile, predicted_wait, model_version)
        VALUES (:aid, :ts, :os, :q, :pw, :ver)
        ON CONFLICT (attraction_id, target_slot, origin_slot, quantile)
        DO UPDATE SET predicted_wait = EXCLUDED.predicted_wait,
                      model_version  = EXCLUDED.model_version,
                      created_at     = now()
        """
    )
    for r in rows:
        r["ver"] = version
    with engine().begin() as c:
        c.execute(_DDL_PCN_FORECASTS)
        for i in range(0, len(rows), 5000):
            c.execute(upsert, rows[i : i + 5000])
    return len(rows)


def upsert_pcn_comparisons(rows: list[dict]) -> int:
    if not rows:
        return 0
    upsert = text(
        """
        INSERT INTO pcn_intraday_comparisons
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
        c.execute(_DDL_PCN_COMPARISONS)
        c.execute(upsert, rows)
    return len(rows)


def fetch_pcn_forecasts_window(park_id: str, lo_local, hi_local) -> pd.DataFrame:
    """PCN forward forecasts whose TARGET slot falls in [lo, hi) (park-local), for
    scoring against realised actuals. Keeps the freshest genuine-forward record per
    (attraction, target_slot, quantile) — the one made at the latest origin <= target."""
    sql = text(
        """
        SELECT DISTINCT ON (f.attraction_id, f.target_slot, f.quantile)
            f.attraction_id::text AS unique_id, f.target_slot, f.origin_slot,
            f.quantile, f.predicted_wait
        FROM pcn_forecasts f
        JOIN attractions a ON a.id = f.attraction_id
        WHERE a."parkId" = :park
          AND f.target_slot >= :lo AND f.target_slot < :hi
          AND f.origin_slot <= f.target_slot
        ORDER BY f.attraction_id, f.target_slot, f.quantile, f.origin_slot DESC
        """
    )
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"park": park_id, "lo": lo_local, "hi": hi_local})
    if not df.empty:
        df["target_slot"] = pd.to_datetime(df["target_slot"])
        df["origin_slot"] = pd.to_datetime(df["origin_slot"])
    return df


def fetch_actuals_local(park_id: str, tz: str, lo_utc, hi_utc) -> pd.DataFrame:
    """Realised 15-min median wait per (attraction, park-local slot) — the scoring
    ground truth (mirror of backtest_intraday_nowcast.fetch_actuals)."""
    sql = text(
        f"""
        SELECT qd."attractionId"::text AS unique_id,
               {BIN.format(col='qd.timestamp')} AS target_slot,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") AS actual
        FROM queue_data qd JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = :park AND qd.timestamp >= :lo AND qd.timestamp < :hi
          AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY' AND qd."waitTime" >= 5
        GROUP BY 1, 2 HAVING COUNT(*) >= 1
        """
    )
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"park": park_id, "tz": tz, "lo": lo_utc, "hi": hi_utc})
    if not df.empty:
        df["target_slot"] = pd.to_datetime(df["target_slot"])
        df["actual"] = df["actual"].astype(float)
    return df


def fetch_catboost_local(park_id: str, tz: str, lo_utc, hi_utc) -> pd.DataFrame:
    """CatBoost's live 15-min preds, freshest per (attraction, park-local target slot),
    for the matched head-to-head. From wait_time_predictions (predictionType='hourly' is
    the 15-min misnomer)."""
    sql = text(
        f"""
        SELECT DISTINCT ON (wp."attractionId", {BIN.format(col='wp."predictedTime"')})
            wp."attractionId"::text AS unique_id,
            {BIN.format(col='wp."predictedTime"')} AS target_slot,
            wp."predictedWaitTime"::float AS cat_pred
        FROM wait_time_predictions wp JOIN attractions a ON a.id = wp."attractionId"
        WHERE a."parkId" = :park AND wp."predictionType" = 'hourly'
          AND wp."predictedTime" >= :lo AND wp."predictedTime" < :hi
        ORDER BY wp."attractionId", {BIN.format(col='wp."predictedTime"')}, wp."createdAt" DESC
        """
    )
    with engine().connect() as c:
        df = pd.read_sql(sql, c, params={"park": park_id, "tz": tz, "lo": lo_utc, "hi": hi_utc})
    if not df.empty:
        df["target_slot"] = pd.to_datetime(df["target_slot"])
        df["cat_pred"] = df["cat_pred"].astype(float)
    return df
