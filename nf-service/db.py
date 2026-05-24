"""Data layer for the NeuralForecast service.

Builds the long-format daily-peak panel (unique_id=attraction, ds=park-local
day, y=daily P90 wait) and the holiday/calendar covariate frame used both for
the historical panel and for the future forecast window.

Timezone rule (repo-wide): every `ds` is a PARK-LOCAL day, never UTC.
"""

from __future__ import annotations

import logging
import time

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from config import get_settings

settings = get_settings()
logger = logging.getLogger("nf.db")


def get_engine():
    url = (
        f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


_engine = get_engine()


def fetch_park_ids() -> list[str]:
    """All park UUIDs (as text) that have attractions — used to chunk the panel for
    iterative training so each fit stays within memory."""
    sql = text('SELECT DISTINCT "parkId"::text AS pid FROM attractions WHERE "parkId" IS NOT NULL ORDER BY 1')
    with _engine.connect() as c:
        df = pd.read_sql(sql, c)
    return df["pid"].tolist()


def fetch_attraction_meta(park_ids: list[str]) -> pd.DataFrame:
    """One row per attraction in scope: timezone, country/region, influencing
    regions (JSON). queue_data has no parkId, so attractions carry the link."""
    where = "WHERE a.\"parkId\"::text = ANY(:pids)" if park_ids else ""
    sql = text(
        f"""
        SELECT a.id::text AS unique_id, a."parkId"::text AS park_id,
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
        SELECT qd."attractionId"::text AS unique_id,
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


def fetch_weather(park_ids: list[str]) -> pd.DataFrame:
    """Daily weather per (park, local day) for the weather futr_exog. Historical rows
    are retained (365d back) and the forecast reaches 16 days, so this covers both the
    training panel and the h=14 future frame. One row per (parkId, date) by PK."""
    # weather_data."parkId" is uuid in the DB; the panel's park key is text (meta
    # casts ::text). Cast both sides so the join keys line up and ANY() type-matches.
    where = 'WHERE "parkId"::text = ANY(:pids)' if park_ids else ""
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
        df = pd.read_sql(sql, c, params={"pids": park_ids} if park_ids else {})
    if df.empty:
        return df
    df["ds"] = pd.to_datetime(df["ds"]).dt.normalize()
    df["is_wet"] = (df["precip_mm"].fillna(0) >= 1.0).astype(int)
    return df.drop_duplicates(["park_id", "ds"])


def _norm_region(r):
    return (r or "").split("-")[-1] or None


def add_calendar_covariates(
    df: pd.DataFrame, meta: pd.DataFrame, holidays: pd.DataFrame,
    weather: pd.DataFrame | None = None,
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
    ):
        df[col] = 0
    # Holiday-distance defaults (capped at 30d). Filled per park inside the loop.
    df["days_until_holiday"] = 30.0
    df["days_since_holiday"] = 30.0

    # Per-series → park map (used by both the holiday loop and the weather merge), so
    # weather still attaches even when there are no holidays for the scope.
    df["_park"] = None
    meta_by_id = None
    if "park_id" in meta.columns:
        meta_by_id = meta.drop_duplicates("unique_id").set_index("unique_id")
        df["_park"] = df["unique_id"].map(meta_by_id["park_id"].to_dict())

    n_parks = 0
    if not holidays.empty and meta_by_id is not None:
        # Prepare holiday helpers ONCE (was recomputed for every attraction).
        h = holidays.copy()
        h["region_norm"] = h["region"].map(_norm_region)
        h["cr"] = list(zip(h["country"], h["region_norm"]))
        h_pub_or_school = h["holiday_type"].isin(["public", "school"])

        park_rep = meta.drop_duplicates("park_id").set_index("park_id")

        cache: dict = {}  # signature -> (local_public, neighbor, school, bridge)

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
            neigh = {(d.get("countryCode"), d.get("regionCode")) for d in influencing}
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
            nmask = h["cr"].isin(neigh) | h["country"].isin(countries_wide)
            neighbor = set(h[nmask & h_pub_or_school]["date"])
            cache[sig] = (local_public, neighbor, school, bridge)
            return cache[sig]

        groups = df.groupby("_park").groups
        total = len(groups)
        for pid, idx in groups.items():
            if pid not in park_rep.index:
                continue
            lp, nb, sc, br = _sets_for_park(pid)
            d = df.loc[idx, "ds"].dt.normalize()
            df.loc[idx, "is_holiday_primary"] = d.isin(lp).astype(int).values
            df.loc[idx, "is_holiday_neighbor"] = d.isin(nb).astype(int).values
            df.loc[idx, "is_school_holiday"] = d.isin(sc).astype(int).values
            df.loc[idx, "is_bridge_day"] = d.isin(br).astype(int).values
            # Distance to nearest PUBLIC holiday (forward/backward), capped at 30d.
            # searchsorted on the sorted holiday array — vectorised, ~O(n log h).
            if lp:
                hol_sorted = np.sort(np.array(list(lp), dtype="datetime64[ns]"))
                dv = d.values.astype("datetime64[ns]")
                day = np.timedelta64(1, "D")
                until = np.full(len(dv), 30.0)
                pos = np.searchsorted(hol_sorted, dv, side="left")
                hasn = pos < len(hol_sorted)
                until[hasn] = (hol_sorted[pos[hasn]] - dv[hasn]) / day
                since = np.full(len(dv), 30.0)
                pos2 = np.searchsorted(hol_sorted, dv, side="right") - 1
                hasp = pos2 >= 0
                since[hasp] = (dv[hasp] - hol_sorted[pos2[hasp]]) / day
                df.loc[idx, "days_until_holiday"] = np.clip(until, 0, 30)
                df.loc[idx, "days_since_holiday"] = np.clip(since, 0, 30)
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

    if "_park" in df.columns:
        df = df.drop(columns=["_park"])
    logger.info(
        "calendar covariates done: %d parks, %d rows in %.1fs",
        n_parks, len(df), time.time() - t0,
    )
    return df


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
    panel: pd.DataFrame, meta: pd.DataFrame, holidays: pd.DataFrame, horizon: int,
    weather: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Exactly `horizon` future daily rows per unique_id with all FUTR_EXOG
    columns filled (NeuralForecast requires futr_df = h rows/series)."""
    rows = []
    for uid, g in panel.groupby("unique_id"):
        last = g["ds"].max()
        future_ds = pd.date_range(last + pd.Timedelta(days=1), periods=horizon, freq="D")
        rows.append(pd.DataFrame({"unique_id": uid, "ds": future_ds}))
    fut = pd.concat(rows, ignore_index=True)
    return add_calendar_covariates(fut, meta, holidays, weather)
