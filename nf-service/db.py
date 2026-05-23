"""Data layer for the NeuralForecast service.

Builds the long-format daily-peak panel (unique_id=attraction, ds=park-local
day, y=daily P90 wait) and the holiday/calendar covariate frame used both for
the historical panel and for the future forecast window.

Timezone rule (repo-wide): every `ds` is a PARK-LOCAL day, never UTC.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from config import get_settings

settings = get_settings()


def get_engine():
    url = (
        f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


_engine = get_engine()


def fetch_attraction_meta(park_ids: list[str]) -> pd.DataFrame:
    """One row per attraction in scope: timezone, country/region, influencing
    regions (JSON). queue_data has no parkId, so attractions carry the link."""
    where = "WHERE a.\"parkId\"::text = ANY(:pids)" if park_ids else ""
    sql = text(
        f"""
        SELECT a.id AS unique_id, a."parkId" AS park_id,
               p.timezone, p."countryCode" AS country, p."regionCode" AS region,
               p."influencingRegions" AS influencing
        FROM attractions a
        JOIN parks p ON p.id = a."parkId"
        {where}
        """
    )
    with _engine.connect() as c:
        df = pd.read_sql(sql, c, params={"pids": park_ids} if park_ids else {})
    return df


def fetch_daily_peak_panel(park_ids: list[str]) -> pd.DataFrame:
    """Long panel: one (unique_id, ds) row per attraction per OPERATING local
    day, y = daily P90 of waitTime. Closed days are simply absent (we do NOT
    zero-fill — that would teach the model spurious zeros)."""
    pct = settings.NF_TARGET_PERCENTILE
    minw = settings.NF_MIN_WAIT
    window = settings.NF_WINDOW_DAYS
    where_park = "AND a.\"parkId\"::text = ANY(:pids)" if park_ids else ""
    sql = text(
        f"""
        SELECT qd."attractionId" AS unique_id,
               DATE(qd.timestamp AT TIME ZONE p.timezone) AS ds,
               PERCENTILE_CONT({pct}) WITHIN GROUP (ORDER BY qd."waitTime") AS y,
               COUNT(*) AS n
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE qd.timestamp >= NOW() - (:window || ' days')::interval
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
        params["pids"] = park_ids
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


def _holiday_sets(meta_row, holidays: pd.DataFrame):
    """Return (local_dates, neighbor_dates, school_dates, bridge_dates) sets for
    one attraction's park, using its country/region + influencing regions."""
    country, region = meta_row["country"], meta_row["region"]
    influencing = meta_row.get("influencing") or []
    if isinstance(influencing, str):
        import json

        try:
            influencing = json.loads(influencing)
        except Exception:
            influencing = []
    neigh = {(d.get("countryCode"), d.get("regionCode")) for d in influencing}

    def _norm(r):
        return (r or "").split("-")[-1] or None

    h = holidays
    local_mask = (h["country"] == country) & (
        h["region"].isna() | (h["region"].apply(_norm) == _norm(region))
    )
    local = h[local_mask]
    local_public = set(local[local["holiday_type"].isin(["public", "bank"])]["date"])
    school = set(local[local["holiday_type"] == "school"]["date"])
    bridge = set(local[local["holiday_type"] == "bridge"]["date"])

    neigh_mask = h.apply(
        lambda r: (r["country"], _norm(r["region"])) in neigh
        or (r["country"], None) in neigh,
        axis=1,
    )
    neighbor = set(h[neigh_mask & h["holiday_type"].isin(["public", "school"])]["date"])
    return local_public, neighbor, school, bridge


def add_calendar_covariates(
    df: pd.DataFrame, meta: pd.DataFrame, holidays: pd.DataFrame
) -> pd.DataFrame:
    """Add the futr_exog columns to a (unique_id, ds) frame — works for both the
    historical panel and the future forecast frame."""
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
    df["is_peak_season"] = month.isin([6, 7, 8, 12]).astype(int)

    # Holiday flags per attraction (vectorised per unique_id via its park sets).
    meta_by_id = meta.set_index("unique_id")
    for col in (
        "is_holiday_primary",
        "is_holiday_neighbor",
        "is_school_holiday",
        "is_bridge_day",
    ):
        df[col] = 0

    for uid, g in df.groupby("unique_id"):
        if uid not in meta_by_id.index:
            continue
        local_public, neighbor, school, bridge = _holiday_sets(
            meta_by_id.loc[uid], holidays
        )
        d = g["ds"].dt.normalize()
        df.loc[g.index, "is_holiday_primary"] = d.isin(local_public).astype(int).values
        df.loc[g.index, "is_holiday_neighbor"] = d.isin(neighbor).astype(int).values
        df.loc[g.index, "is_school_holiday"] = d.isin(school).astype(int).values
        df.loc[g.index, "is_bridge_day"] = d.isin(bridge).astype(int).values

    df["holiday_count_total"] = (
        df["is_holiday_primary"]
        + df["is_holiday_neighbor"]
        + df["is_school_holiday"]
        + df["is_bridge_day"]
    )
    return df


FUTR_EXOG = [
    "is_holiday_primary",
    "is_holiday_neighbor",
    "is_school_holiday",
    "is_bridge_day",
    "holiday_count_total",
    "is_weekend",
    "dow_sin",
    "dow_cos",
    "doy_sin",
    "doy_cos",
    "season_code",
    "is_peak_season",
]


def persist_forecast(yhat: pd.DataFrame, version: str, value_col: str) -> int:
    """Store the forward daily-peak forecast so it can be scored against actuals
    once each target date passes (the forward-scoreboard vs CatBoost).

    One row per (attraction, target_date, forecast_date=today). Re-running on the
    same day overwrites that day's forecast; past forecast_dates are immutable, so
    the genuine forward record (made before the target) is preserved for scoring.
    """
    rows = yhat[["unique_id", "ds", value_col]].copy()
    rows = rows.rename(columns={"unique_id": "attraction_id", "ds": "target_date"})
    rows["target_date"] = pd.to_datetime(rows["target_date"]).dt.date
    rows["predicted_peak"] = rows[value_col].astype(float).clip(lower=0)

    ddl = text(
        """
        CREATE TABLE IF NOT EXISTS tft_forecasts (
            attraction_id   uuid NOT NULL,
            target_date     date NOT NULL,
            forecast_date   date NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')::date,
            predicted_peak  double precision NOT NULL,
            model_version   text,
            created_at      timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (attraction_id, target_date, forecast_date)
        )
        """
    )
    upsert = text(
        """
        INSERT INTO tft_forecasts
            (attraction_id, target_date, forecast_date, predicted_peak, model_version)
        VALUES
            (:aid, :td, (now() AT TIME ZONE 'UTC')::date, :pp, :ver)
        ON CONFLICT (attraction_id, target_date, forecast_date)
        DO UPDATE SET predicted_peak = EXCLUDED.predicted_peak,
                      model_version  = EXCLUDED.model_version,
                      created_at     = now()
        """
    )
    params = [
        {"aid": str(r.attraction_id), "td": r.target_date,
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
    panel: pd.DataFrame, meta: pd.DataFrame, holidays: pd.DataFrame, horizon: int
) -> pd.DataFrame:
    """Exactly `horizon` future daily rows per unique_id with all FUTR_EXOG
    columns filled (NeuralForecast requires futr_df = h rows/series)."""
    rows = []
    for uid, g in panel.groupby("unique_id"):
        last = g["ds"].max()
        future_ds = pd.date_range(last + pd.Timedelta(days=1), periods=horizon, freq="D")
        rows.append(pd.DataFrame({"unique_id": uid, "ds": future_ds}))
    fut = pd.concat(rows, ignore_index=True)
    return add_calendar_covariates(fut, meta, holidays)
