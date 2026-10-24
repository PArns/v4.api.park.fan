"""
Model training script
"""

import argparse
from datetime import timedelta
from typing import Optional
import numpy as np
import pandas as pd
import psutil
import os
import logging
import sys
import gc

from config import get_settings
from db import (
    fetch_attraction_accuracy,
    fetch_curated_attraction_facts,
    fetch_training_data,
)
from features import (
    LAST_YEAR_LAG,
    LAST_YEAR_MIN_LEAD,
    LAST_YEAR_MIN_OBSERVED_DAYS,
    curated_attraction_coverage,
    engineer_features,
    get_feature_columns,
)
from model import WaitTimeModel
from data_validation import validate_training_data

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

settings = get_settings()


def get_memory_usage():
    """Get current memory usage in GB"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024**3)  # Convert to GB


def remove_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out likely downtime anomalies (sudden drops to near-zero)

    User Request: "filter downtime with unusual drops... even if not closed"
    """
    if df.empty:
        return df

    logger.info("   🧹 Filtering anomalies...")
    initial_count = len(df)

    # Ensure timestamp sort
    df = df.sort_values(["attractionId", "timestamp"])

    # Calculate rolling median (centered) to determine "context" for sudden drops
    # We use a centered window to see what's happening around the data point for this ride
    df["rolling_median"] = df.groupby("attractionId")["waitTime"].transform(
        lambda x: x.rolling(window=7, min_periods=1, center=True).median()
    )

    # Calculate park-wide median at each timestamp to detect technical heartbeats
    # (Fake 5 min waits before park or area opening)
    park_medians = (
        df.groupby(["parkId", "timestamp"])["waitTime"]
        .median()
        .reset_index()
        .rename(columns={"waitTime": "park_timestamp_median"})
    )
    df = df.merge(park_medians, on=["parkId", "timestamp"], how="left")

    # 1. Catch sudden drops (Downtime/Reset)
    # Condition: Wait time is very low (<= 5 min) BUT the surrounding context (median) is high (> 20 min)
    drop_mask = (df["waitTime"] <= 5) & (df["rolling_median"] > 20)

    # 2. Catch extreme high outliers (API errors / data glitches)
    # Values >= 400 mins (6.7 hours) are almost certainly erroneous.
    high_outlier_mask = df["waitTime"] >= 400

    # 3. Catch technical heartbeats (Fake 5 min)
    # Condition: Ride reports 5 min, BUT the entire park is also at <= 5 min AND it's early.
    # Logic: Only filter these if it's before 1 hour after park opening (local time).
    # After that, a 5-min park median is likely a genuine quiet day, not a technical heartbeat.
    heartbeat_mask = (
        (df["waitTime"] <= 5)
        & (df["park_timestamp_median"] <= 5)
        & (df["hour"] < (df["opening_hour"] + 1))
    )

    # Combine masks
    anomaly_mask = drop_mask | high_outlier_mask | heartbeat_mask

    df_clean = df[~anomaly_mask].copy()
    df_clean = df_clean.drop(
        columns=["rolling_median", "park_timestamp_median", "opening_hour"]
    )

    removed_drops = drop_mask.sum()
    removed_highs = high_outlier_mask.sum()
    removed_heartbeats = heartbeat_mask.sum()

    logger.info(
        f"   Detected {removed_drops} sensor drops, {removed_highs} extreme high outliers, and {removed_heartbeats} technical heartbeats"
    )

    removed = initial_count - len(df_clean)
    logger.info(
        f"   Removed {removed} rows ({(removed / initial_count) * 100:.2f}%) identified as anomalies"
    )

    return df_clean


def apply_training_dropout(df: pd.DataFrame, cfg, log) -> pd.DataFrame:
    """
    Simulate the inference scenario for future predictions by randomly replacing
    real-time features with historical proxies on a fraction of training rows.

    Without dropout the model sees perfect real-time signals on every row and
    learns to rely on them (avg_wait_last_24h dominates at ~30%, holiday features
    near 0%). With dropout it must also learn from calendar/holiday/seasonal signals.

    Three independent dropout passes:
    1. Occupancy dropout (cfg.OCCUPANCY_DROPOUT_RATE):
       park_occupancy_pct → park's own rolling_avg_weekday or rolling_avg_weekend
       converted to an approximate occupancy ratio.  Simulates future predictions
       where only a historical DOW×hour profile is available.

    2. Rolling-avg dropout (cfg.ROLLING_AVG_DROPOUT_RATE):
       avg_wait_last_24h → avg_wait_same_dow_4w  (same-DOW 4-week average)
       avg_wait_last_1h  → rolling_avg_weekday / rolling_avg_weekend
       Simulates next-week / next-month predictions where yesterday's wait is
       irrelevant.

    3. Rolling-7d dropout (cfg.ROLLING_7D_DROPOUT_RATE):
       rolling_avg_7d → rolling_avg_weekend / rolling_avg_weekday
       Simulates predictions beyond a week where the 7-day rolling average is
       not yet known.

    NOTE: This function is called ONLY during training (train.py).
    add_park_occupancy_feature() (features.py) runs during inference too, so
    dropout must NOT be placed there.
    """
    import numpy as np

    # Fixed seed (PAR-1040): a nightly run must be reproducible, or the val MAE
    # the promotion gate compares moves with the dice rather than the model.
    rng = np.random.default_rng(cfg.CATBOOST_RANDOM_SEED)

    n = len(df)
    log.info("🎲 Applying training dropout...")

    # Built from the rows as they come in, before the passes below touch them.
    h_rate = getattr(cfg, "HOLIDAY_DROPOUT_RATE", 0.0)
    holiday_rows = pd.Series(False, index=df.index)
    for flag in ("is_holiday_primary", "is_school_holiday_primary"):
        if flag in df.columns:
            holiday_rows |= df[flag].fillna(0).to_numpy() == 1
    pre_break = holiday_pre_break_values(df, holiday_rows) if h_rate > 0 else None

    # --- 1. Occupancy dropout ---
    occ_rate = cfg.OCCUPANCY_DROPOUT_RATE
    if occ_rate > 0 and "park_occupancy_pct" in df.columns:
        occ_mask = rng.random(n) < occ_rate
        occ_count = int(occ_mask.sum())

        if occ_count > 0 and "rolling_avg_7d" in df.columns:
            # Use weekday/weekend rolling avg as a proxy for "expected" occupancy.
            # Approximate the ratio: if today the park is at rolling_avg_7d level
            # then occupancy would be ~100 (normalised). Scale accordingly.
            is_weekend = df["is_weekend"].values == 1
            proxy = np.where(
                is_weekend,
                df["rolling_avg_weekend"].values,
                df["rolling_avg_weekday"].values,
            )
            # Avoid div-by-zero: use rolling_avg_7d as the denominator
            r7d = df["rolling_avg_7d"].values.clip(1)
            # historical_occ = (proxy / r7d) * current_occ  →  smoother version of
            # actual occ that strips out today's spike/dip
            hist_occ = (proxy / r7d) * df["park_occupancy_pct"].values
            hist_occ = hist_occ.clip(0, 400)  # cap at 400% to avoid outliers
            df.loc[occ_mask, "park_occupancy_pct"] = hist_occ[occ_mask]

            log.info(
                f"   Occupancy dropout: {occ_count:,} rows ({occ_rate * 100:.0f}%) → historical proxy"
            )

    # --- 2a. Rolling-avg dropout (avg_wait_last_24h) ---
    ravg_rate = cfg.ROLLING_AVG_DROPOUT_RATE
    if ravg_rate > 0:
        ravg_mask = rng.random(n) < ravg_rate
        ravg_count = int(ravg_mask.sum())

        if ravg_count > 0 and (
            "avg_wait_last_24h" in df.columns
            and "avg_wait_same_dow_4w" in df.columns
        ):
            df.loc[ravg_mask, "avg_wait_last_24h"] = df.loc[
                ravg_mask, "avg_wait_same_dow_4w"
            ]
            log.info(
                f"   Rolling-avg-24h dropout: {ravg_count:,} rows ({ravg_rate * 100:.0f}%) → DOW 4-week historical"
            )

    # --- 2b. Short-window dropout (avg_wait_last_1h) — separate rate ---
    r1h_rate = getattr(cfg, "ROLLING_1H_DROPOUT_RATE", ravg_rate)
    if r1h_rate > 0 and "avg_wait_last_1h" in df.columns:
        r1h_mask = rng.random(n) < r1h_rate
        r1h_count = int(r1h_mask.sum())

        if r1h_count > 0:
            is_weekend_mask = df["is_weekend"].values == 1
            fallback_1h = np.where(
                is_weekend_mask,
                df["rolling_avg_weekend"].values,
                df["rolling_avg_weekday"].values,
            )
            df.loc[r1h_mask, "avg_wait_last_1h"] = fallback_1h[r1h_mask]
            # Serving pairs the profile with velocity 0 beyond its live lead
            # (predict.LIVE_LAG_MAX_LEAD); teach the model that pairing.
            if "wait_time_velocity" in df.columns:
                df.loc[r1h_mask, "wait_time_velocity"] = 0.0
            log.info(
                f"   Rolling-avg-1h dropout: {r1h_count:,} rows ({r1h_rate * 100:.0f}%) → weekend/weekday 7d historical"
            )

    # --- 3. Rolling-7d dropout ---
    r7d_rate = cfg.ROLLING_7D_DROPOUT_RATE
    if r7d_rate > 0 and "rolling_avg_7d" in df.columns:
        r7d_mask = rng.random(n) < r7d_rate
        r7d_count = int(r7d_mask.sum())

        if r7d_count > 0:
            is_weekend_mask = df["is_weekend"].values == 1
            fallback_7d = np.where(
                is_weekend_mask,
                df["rolling_avg_weekend"].values,
                df["rolling_avg_weekday"].values,
            )
            df.loc[r7d_mask, "rolling_avg_7d"] = fallback_7d[r7d_mask]

            log.info(
                f"   Rolling-7d dropout: {r7d_count:,} rows ({r7d_rate * 100:.0f}%) → weekend/weekday historical"
            )

    # --- 4. Holiday dropout ---
    # On rows where is_holiday_primary=1 OR is_school_holiday_primary=1, replace
    # the recent-history features with what serving feeds a holiday row it
    # forecasts from BEFORE the break (holiday_pre_break_values). Without this
    # the model sees holiday rows always carrying elevated rolling averages and
    # occupancy and never needs the holiday flags (0% importance). The proxies
    # used to come from 7-day windows that inside a multi-day break already hold
    # the break, and left the 28d/90d means alone (PAR-1051).
    if h_rate > 0 and pre_break is not None and len(pre_break):
        holiday_count = int(holiday_rows.sum())
        h_mask = holiday_rows.to_numpy() & (rng.random(n) < h_rate)
        h_idx = df.index[h_mask].intersection(pre_break.index)
        if len(h_idx) > 0:
            cols = [c for c in pre_break.columns if c in df.columns]
            df.loc[h_idx, cols] = pre_break.loc[h_idx, cols].values
        log.info(
            f"   Holiday dropout: {len(h_idx):,}/{holiday_count:,} holiday rows "
            f"({h_rate * 100:.0f}%, {int(h_mask.sum()) - len(h_idx):,} without pre-break history) "
            f"→ as forecast from before the break"
        )

    # The interactions multiply park_occupancy_pct, which the passes above just
    # replaced: computed before them, a dropped-out row kept the real
    # occupancy inside its interactions (PAR-1032).
    from features import add_interaction_features

    return add_interaction_features(df)


def compute_sample_weights(df: pd.DataFrame, accuracy_stats) -> "Optional[np.ndarray]":
    """Per-row training weights, or None for uniform weights.

    Two independent factors, both applied to every set the model is fitted on
    (the train pool AND the full refit set), so the refit learns the same
    objective the early-stopped fit was tuned on:

    - Accuracy feedback (ENABLE_SAMPLE_WEIGHTS): 1 + (MAE / 20) * factor, MAE per
      attraction from `attraction_accuracy_stats` (10 min when unknown), clipped
      to 1.0-2.0.
    - Busyness (CATBOOST_BUSY_WEIGHT): clip(sqrt(wait / 20), 0.4, 2.5), so the
      ~72% quiet rows do not dominate the loss.

    Does not modify `df`.
    """
    import numpy as _np

    weights = None
    if (
        settings.ENABLE_SAMPLE_WEIGHTS
        and accuracy_stats is not None
        and not accuracy_stats.empty
    ):
        mae_by_attraction = accuracy_stats.drop_duplicates(
            subset=["attraction_id"]
        ).set_index("attraction_id")["mae"]
        mae = df["attractionId"].map(mae_by_attraction).astype(float).fillna(10.0)
        weights = (
            (1.0 + (mae / 20.0) * settings.SAMPLE_WEIGHT_FACTOR).clip(1.0, 2.0).values
        )
        logger.info(
            f"   Accuracy weights on {len(df):,} rows from {len(mae_by_attraction)} attraction stats: "
            f"{weights.min():.2f}-{weights.max():.2f} (avg {weights.mean():.2f})"
        )

    if getattr(settings, "CATBOOST_BUSY_WEIGHT", False):
        w = df["waitTime"].values.astype(float)
        busy_factor = _np.clip((w / 20.0) ** 0.5, 0.4, 2.5)
        weights = busy_factor if weights is None else weights * busy_factor
        logger.info(
            "   Busyness weighting ON: factor %.2f-%.2f (quiet↓ busy↑)"
            % (busy_factor.min(), busy_factor.max())
        )
    return weights


def holdout_summary(y_true, y_pred, busy_threshold: float = 60.0) -> dict:
    """Out-of-time metrics persisted in the model metadata: MAE overall and on
    busy rows (actual >= `busy_threshold` minutes), plus the sample counts."""
    import numpy as _np

    y_true = _np.asarray(y_true, dtype=float)
    y_pred = _np.asarray(y_pred, dtype=float)
    err = _np.abs(y_true - y_pred)
    busy = y_true >= busy_threshold
    return {
        "mae": float(err.mean()) if len(err) else None,
        "busy_mae": float(err[busy].mean()) if busy.any() else None,
        "busy_threshold": busy_threshold,
        "samples": int(len(err)),
        "busy_samples": int(busy.sum()),
    }


def select_validation_weeks(unique_weeks, seed: int, share: float = 0.2) -> set:
    """ISO weeks (YYYYWW ints) that go to validation.

    Each week's draw depends on (seed, week) only, so a week that was a
    validation week last night still is one tonight: the window sliding by a
    day does not reshuffle the whole split, and the val MAE the promotion gate
    compares is measured on (almost) the same weeks run after run (PAR-1040).
    The share is approximate; at least one week is always picked.
    """
    weeks = [int(w) for w in unique_weeks]
    if not weeks:
        return set()
    draws = {w: np.random.default_rng([seed, w]).random() for w in weeks}
    picked = {w for w, d in draws.items() if d < share}
    return picked or {min(draws, key=draws.get)}


# Served leads the "as known at origin" hold-out is scored at (PAR-1040):
# hourly next-day, the daily calendar's next day, a week out, and the far tier
# CatBoost serves alone (31-365 d; 61 d sits past TFT's 60-day window).
ORIGIN_LEADS = {
    "12h": pd.Timedelta(hours=12),
    "24h": pd.Timedelta(hours=24),
    "7d": pd.Timedelta(days=7),
    "61d": pd.Timedelta(days=61),
}

# Features inference reads at base_time (the origin) for every row it serves,
# whatever the row's own time: the rolling statistics, the same-hour lags of
# the base hour, and today's downtime.
_ORIGIN_VALUE_COLUMNS = [
    "rolling_avg_7d",
    "rolling_avg_weekday",
    "rolling_avg_weekend",
    "rolling_avg_28d",
    "rolling_avg_90d",
    "trend_7d",
    "volatility_7d",
    "volatility_weekday",
    "volatility_weekend",
    "avg_wait_same_hour_last_week",
    "avg_wait_same_hour_last_month",
    "avg_wait_last_24h",
    "had_downtime_today",
    "downtime_minutes_today",
]


def origin_occupancy_profile(df: pd.DataFrame) -> pd.Series:
    """Mean park_occupancy_pct per (parkId, local weekday, local hour) — the
    training-side stand-in for fetch_historical_park_occupancy, which inference
    uses for rows more than 2 h from base_time. Built from rows older than the
    hold-out, BEFORE dropout touches them."""
    return df.groupby(["parkId", "day_of_week", "hour"])["park_occupancy_pct"].mean()


# Beyond these leads serving has no forecast / no published schedule: weather
# comes from climatology (Open-Meteo forecasts 16 days) and a day without
# schedule rows reads open with no window. shortcut: the publishing horizon is
# per park and unrecorded; 30 d is a conservative single cut-off — store when
# each schedule row first appeared to make it per park.
ORIGIN_WEATHER_FORECAST_DAYS = 16
ORIGIN_SCHEDULE_KNOWN_DAYS = 30

_ORIGIN_WEATHER_MEAN_COLUMNS = [
    "temperature_avg",
    "precipitation",
    "precipitation_last_3h",
    "windSpeedMax",
    "snowfallSum",
]


def origin_weather_climatology(df: pd.DataFrame) -> pd.DataFrame:
    """Observed weather per (parkId, month, hour), from rows older than the
    hold-out: the stand-in for serving's monthly climatology beyond its forecast."""
    cols = [c for c in _ORIGIN_WEATHER_MEAN_COLUMNS if c in df.columns]
    keys = ["parkId", "month", "hour"]
    clim = df.groupby(keys)[cols].mean()
    if "weatherCode" in df.columns:
        clim["weatherCode"] = df.groupby(keys)["weatherCode"].agg(
            lambda x: x.mode().iloc[0] if len(x.mode()) else 0
        )
    return clim


def _neutral_far_weather_and_schedule(
    merged: pd.DataFrame, lead: pd.Timedelta, weather_clim: pd.DataFrame | None
) -> pd.DataFrame:
    if lead > pd.Timedelta(days=ORIGIN_WEATHER_FORECAST_DAYS) and weather_clim is not None:
        keys = pd.MultiIndex.from_frame(merged[["parkId", "month", "hour"]])
        clim = weather_clim.reindex(keys)
        for c in clim.columns:
            if c in merged.columns:
                merged[c] = clim[c].fillna(merged[c]).values
        if "weatherCode" in merged.columns:
            # A slot with no climatology and no reading stays NaN; serving's
            # climatology fills it with 0 (predict.py), so the stand-in does too.
            merged["weatherCode"] = merged["weatherCode"].fillna(0).astype(int)
        if "precipitation" in merged.columns:
            merged["is_raining"] = (merged["precipitation"] > 0).astype(int)
        for c in ("is_rain_starting", "is_rain_stopping", "is_temp_extreme",
                  "is_wind_extreme", "temperature_deviation"):
            if c in merged.columns:
                merged[c] = 0
    if lead > pd.Timedelta(days=ORIGIN_SCHEDULE_KNOWN_DAYS):
        neutral = {"is_park_open": 1, "time_since_park_open_mins": 0.0,
                   "has_extra_hours": 0, "has_special_event": 0}
        for c, v in neutral.items():
            if c in merged.columns:
                merged[c] = v
    return merged


def _last_year_same_slot(
    merged: pd.DataFrame, source: pd.DataFrame, first_seen: pd.Series
) -> pd.Series:
    """Mean wait in the same local date-hour 52 weeks before each row, NaN where
    unobserved or the ride was first seen < 330 d before the row's origin —
    predict.fetch_last_year_slots' rule on the training frame."""
    src_local = pd.to_datetime(source["local_timestamp"])
    slots = source["waitTime"].groupby(
        [source["attractionId"].values, src_local.dt.date.values, src_local.dt.hour.values]
    ).mean()
    year_ago = pd.to_datetime(merged["local_timestamp"]) - LAST_YEAR_LAG
    keys = pd.MultiIndex.from_arrays(
        [merged["attractionId"].values, year_ago.dt.date.values, year_ago.dt.hour.values]
    )
    vals = pd.Series(slots.reindex(keys).values, index=merged.index, dtype=float)
    watched = merged["attractionId"].map(first_seen) <= (
        merged["_origin_ts"] - pd.Timedelta(days=LAST_YEAR_MIN_OBSERVED_DAYS)
    )
    return vals.where(watched.fillna(False).astype(bool))


def holiday_pre_break_values(df: pd.DataFrame, holiday_rows: pd.Series) -> pd.DataFrame:
    """The features serving gives each holiday row when it forecasts that row
    from before the break (PAR-1051).

    Origin = local midnight after the park's last NON-holiday day before the
    row's date (days the park has no rows on do not end a break). From there the
    row is built the way predict.create_prediction_features builds a row beyond
    its live window, the same rules as_known_at_origin applies at a fixed lead:

    * the _ORIGIN_VALUE_COLUMNS (rolling 7d/weekday/weekend/28d/90d means,
      trend, volatility, the base hour's lags, downtime) are the ride's last row
      before the origin, within 7 d;
    * avg_wait_same_dow_4w is the mean of the 1-4 weeks-back same-hour slots
      that lie before the origin, else the origin's rolling_avg_7d. shortcut:
      serving first tries the slot 52 weeks back (PAR-1045); add it here once a
      year of history makes that path common;
    * avg_wait_last_24h is the origin's within 24 h, else that same-dow value;
    * avg_wait_last_1h is the slot one week back when it lies before the
      origin, else the origin's weekday/weekend mean; wait_time_velocity 0.
      shortcut: rows within 3 h of the origin (before 03:00) are live at
      serving; nothing that early is a training row in practice;
    * park_occupancy_pct is the park's mean in the same weekday and hour over
      the 8 weeks before the origin (fetch_historical_park_occupancy's window),
      100 where none.

    Returns the replacement columns for the holiday rows that have such an
    origin row; the others are absent.
    """
    need = {"attractionId", "parkId", "local_timestamp", "waitTime", "rolling_avg_7d"}
    if not holiday_rows.any() or not need.issubset(df.columns):
        return pd.DataFrame()

    local = pd.to_datetime(df["local_timestamp"])
    day = local.dt.normalize()
    hour = local.dt.hour

    park_days = (
        pd.DataFrame({"parkId": df["parkId"].values, "day": day.values,
                      "hol": holiday_rows.to_numpy()})
        .groupby(["parkId", "day"])["hol"].max()
        .reset_index()
        .sort_values(["parkId", "day"])
    )
    park_days["anchor"] = park_days["day"].where(~park_days["hol"])
    park_days["anchor"] = park_days.groupby("parkId")["anchor"].transform(
        lambda s: s.shift().ffill()
    )
    anchor_of = park_days.set_index(["parkId", "day"])["anchor"]

    tgt = pd.DataFrame(
        {"attractionId": df["attractionId"], "parkId": df["parkId"], "_day": day,
         "_hour": hour, "_local": local, "_weekend": local.dt.dayofweek >= 5}
    )[holiday_rows.to_numpy()]
    tgt["_anchor"] = anchor_of.reindex(
        pd.MultiIndex.from_arrays([tgt["parkId"], tgt["_day"]])
    ).values
    tgt = tgt[tgt["_anchor"].notna()]
    if tgt.empty:
        return pd.DataFrame()
    tgt["_origin"] = tgt["_anchor"] + pd.Timedelta(days=1)

    cols = [c for c in _ORIGIN_VALUE_COLUMNS if c in df.columns]
    src = df[["attractionId", *cols]].rename(columns={c: f"_o_{c}" for c in cols})
    src["_origin"] = local.values
    merged = pd.merge_asof(
        tgt.reset_index().sort_values("_origin"),
        src.sort_values("_origin"),
        on="_origin",
        by="attractionId",
        direction="backward",
        allow_exact_matches=False,
        tolerance=pd.Timedelta(days=7),
    ).set_index("index")
    merged = merged[merged["_o_rolling_avg_7d"].notna()]
    if merged.empty:
        return pd.DataFrame()

    out = pd.DataFrame({c: merged[f"_o_{c}"] for c in cols}, index=merged.index)
    r7d = merged["_o_rolling_avg_7d"]
    # Weeks back to the first same-weekday date at or before the anchor (>= 1).
    k0 = np.ceil((merged["_day"] - merged["_anchor"]).dt.days / 7).astype(int)

    def slot(table, key, weeks):
        """`table`'s (key, date, hour) value `weeks` weeks before each row."""
        idx = pd.MultiIndex.from_arrays(
            [merged[key], merged["_day"] - pd.to_timedelta(7 * weeks, unit="D"), merged["_hour"]]
        )
        return pd.Series(table.reindex(idx).values, index=merged.index, dtype=float)

    waits = df["waitTime"].groupby([df["attractionId"].values, day.values, hour.values]).mean()
    known = [slot(waits, "attractionId", k).where(k0 <= k) for k in (1, 2, 3, 4)]
    same_dow = pd.concat(known, axis=1).mean(axis=1)
    same_dow = same_dow.where(same_dow.notna() & (same_dow > 0), r7d)
    out["avg_wait_same_dow_4w"] = same_dow
    lead = merged["_local"] - merged["_origin"]
    if "_o_avg_wait_last_24h" in merged.columns:
        out["avg_wait_last_24h"] = merged["_o_avg_wait_last_24h"].where(
            lead <= pd.Timedelta(hours=24), same_dow
        )
    else:
        out["avg_wait_last_24h"] = same_dow

    profile_1h = merged["_o_rolling_avg_weekend"].where(
        merged["_weekend"], merged["_o_rolling_avg_weekday"]
    ) if "_o_rolling_avg_weekday" in merged.columns else r7d
    lag_1w = known[0]
    out["avg_wait_last_1h"] = lag_1w.where(lag_1w.notna() & (lag_1w > 0), profile_1h)
    out["wait_time_velocity"] = 0.0

    if "park_occupancy_pct" in df.columns:
        occ_slots = df["park_occupancy_pct"].groupby(
            [df["parkId"].values, day.values, hour.values]
        ).mean()
        occ = pd.concat(
            [slot(occ_slots, "parkId", k0 + j) for j in range(8)], axis=1
        ).mean(axis=1)
        out["park_occupancy_pct"] = occ.fillna(100.0)
    return out


def as_known_at_origin(
    df_target: pd.DataFrame,
    df_source: pd.DataFrame,
    lead: pd.Timedelta,
    occ_profile: pd.Series,
    weather_clim: pd.DataFrame | None = None,
    last_year_source: pd.DataFrame | None = None,
    first_seen: pd.Series | None = None,
) -> pd.DataFrame:
    """The hold-out rows with the features they would have had if forecast
    `lead` ahead, i.e. built at origin = target - lead the way
    predict.create_prediction_features builds a row that far out.

    `df_source` holds the unmodified feature rows (pool + hold-out) the origin
    is looked up in: the last row of the same ride at or before the origin is
    what inference would have read as "now". Rows with no such row within 7 d
    are dropped (the ride was not running; nothing would have been served).

    Mirrors serving for leads beyond its live window (> 3 h):
    * the _ORIGIN_VALUE_COLUMNS are the origin row's values;
    * avg_wait_same_dow_4w is the row's own value only when all four weekly
      lags lie at or before the origin (lead <= 7 d); otherwise the origin's
      rolling_avg_7d, serving's fallback when the history lookup misses.
      shortcut: a lead between 7 d and 28 d knows SOME of the lags and serving
      averages those; no served lead falls there today — add a partial mean if
      one ever does. Beyond 28 d it is the same weekday and hour 52 weeks
      before the row instead, where `last_year_source` holds that reading and
      the ride was first seen >= 330 d before the origin (serving's
      fetch_last_year_slots, PAR-1045);
    * avg_wait_last_24h is the origin's for lead <= 24 h, else the
      same-dow value above (serving's > 24 h replacement);
    * avg_wait_last_1h is the target hour one week earlier when that is known
      at the origin, else the origin's weekday/weekend rolling mean — serving's
      far-row profile. This is the PAR-1030 definition; until that fix lands,
      serving still writes the live wait onto these rows;
    * wait_time_velocity is 0: a change rate measured at the origin says
      nothing about a row hours later (PAR-1030/1032 serve 0 there too);
    * park_occupancy_pct is the (park, weekday, hour) profile, interactions
      recomputed from it.
    * weather beyond ORIGIN_WEATHER_FORECAST_DAYS is the (park, month, hour)
      climatology (`weather_clim`), and the schedule columns beyond
      ORIGIN_SCHEDULE_KNOWN_DAYS read as serving reads a day without schedule
      rows (open, no window, no event / extra hours).
    Calendar and holiday features are known ahead and stay.

    Still optimistic, on purpose of scope: within 16 d the row keeps the
    OBSERVED weather where serving had a forecast, and within 30 d the FINAL
    schedule where serving had whatever was published at the origin. The 12 h /
    24 h / 7 d figures are therefore a lower bound on the served error.
    """
    from features import add_interaction_features

    tgt = df_target.copy()
    tgt["_origin_ts"] = tgt["timestamp"] - lead
    tgt["_row"] = np.arange(len(tgt))
    cols = [c for c in _ORIGIN_VALUE_COLUMNS if c in df_source.columns]
    src = df_source[["attractionId", "timestamp", *cols]].rename(
        columns={"timestamp": "_origin_ts", **{c: f"_o_{c}" for c in cols}}
    )
    merged = pd.merge_asof(
        tgt.sort_values("_origin_ts"),
        src.sort_values("_origin_ts"),
        on="_origin_ts",
        by="attractionId",
        direction="backward",
        tolerance=pd.Timedelta(days=7),
    ).sort_values("_row")
    merged = merged[merged["_o_rolling_avg_7d"].notna()].reset_index(drop=True)

    r7d = merged["_o_rolling_avg_7d"]
    same_dow = merged["avg_wait_same_dow_4w"] if lead <= pd.Timedelta(days=7) else r7d
    same_dow = same_dow.where(same_dow > 0, r7d)
    if lead > LAST_YEAR_MIN_LEAD and last_year_source is not None and first_seen is not None:
        year_ago = _last_year_same_slot(merged, last_year_source, first_seen)
        same_dow = year_ago.where(year_ago.notna() & (year_ago > 0), same_dow)

    for c in cols:
        merged[c] = merged[f"_o_{c}"]
    merged["avg_wait_same_dow_4w"] = same_dow
    if lead > pd.Timedelta(hours=24):
        merged["avg_wait_last_24h"] = same_dow

    weekend = merged["day_of_week"] >= 5
    profile_1h = merged["_o_rolling_avg_weekend"].where(
        weekend, merged["_o_rolling_avg_weekday"]
    )
    if lead <= pd.Timedelta(days=7) and "wait_lag_1w" in merged.columns:
        # 0 is a filled miss, never a reading: serving's lookup only holds
        # waits >= 5, so it falls back to the profile there.
        lag_1w = merged["wait_lag_1w"]
        profile_1h = lag_1w.where(lag_1w.notna() & (lag_1w > 0), profile_1h)
    merged["avg_wait_last_1h"] = profile_1h
    merged["wait_time_velocity"] = 0.0

    keys = pd.MultiIndex.from_frame(merged[["parkId", "day_of_week", "hour"]])
    merged["park_occupancy_pct"] = occ_profile.reindex(keys).fillna(100.0).values
    merged = _neutral_far_weather_and_schedule(merged, lead, weather_clim)
    merged = add_interaction_features(merged)
    return merged.drop(
        columns=["_origin_ts", "_row", *[f"_o_{c}" for c in cols]]
    )


def cap_training_rows(
    df: pd.DataFrame, max_rows: int, full_resolution_days: int, seed: int
) -> pd.DataFrame:
    """Bound the training pool at `max_rows` without dropping any season.

    The newest `full_resolution_days` of `df` (by `timestamp`) are kept whole.
    train_model calls this on the pool AFTER the 30-day hold-out is split off, so
    with the default 90 that block is days 30-120 before the newest data. The older
    rows are thinned by a uniform random sample so that the total fits the budget.
    A uniform sample keeps every month's share of the older rows, which a rolling
    window would not. If the recent block alone exceeds the budget, the whole pool
    is sampled uniformly instead, so older months still stay represented.

    Thinning happens AFTER feature engineering on purpose: the lag and rolling
    features are computed from each ride's full series, so a row keeps the same
    feature values it would have had without the cap.
    """
    if max_rows <= 0 or len(df) <= max_rows:
        return df

    recent_cutoff = df["timestamp"].max() - pd.Timedelta(days=full_resolution_days)
    recent_mask = (df["timestamp"] >= recent_cutoff).to_numpy()
    n_recent = int(recent_mask.sum())
    budget_old = max_rows - n_recent

    import numpy as np

    rng = np.random.default_rng(seed)
    keep = recent_mask.copy()
    if budget_old > 0:
        old_positions = np.flatnonzero(~recent_mask)
        keep[rng.choice(old_positions, size=budget_old, replace=False)] = True
    else:
        keep[:] = False
        keep[rng.choice(len(df), size=max_rows, replace=False)] = True

    logger.info(
        f"   Row cap: {len(df):,} rows > TRAIN_MAX_ROWS {max_rows:,} — kept the last "
        f"{full_resolution_days} days whole ({n_recent:,} rows), thinned older rows "
        f"to {max(budget_old, 0):,}"
        + ("" if budget_old > 0 else " (recent block alone over budget: sampled all rows)")
    )
    return df[keep].reset_index(drop=True)


def estimate_refit(
    elapsed_seconds: float,
    fit_seconds: float,
    rows_all: int,
    rows_train: int,
    budget_seconds: Optional[float],
    margin_seconds: float,
) -> dict:
    """Would the final refit finish inside the caller's budget?

    The refit is estimated as the early-stopped fit's duration scaled by the row
    ratio (refit rows / fit training rows). The refit runs best_iteration + 1
    trees, which is at most the trees the fit grew, so the estimate is an upper
    bound; at the defaults the validation loss still improves at the cap and the
    two are equal (PAR-902). No budget means no limit.
    """
    estimate = fit_seconds * (rows_all / max(rows_train, 1))
    if budget_seconds is None:
        return {"fits": True, "estimated_seconds": round(estimate, 1)}
    remaining = budget_seconds - margin_seconds - elapsed_seconds
    return {
        "fits": estimate <= remaining,
        "estimated_seconds": round(estimate, 1),
        "remaining_seconds": round(remaining, 1),
        "elapsed_seconds": round(elapsed_seconds, 1),
        "budget_seconds": budget_seconds,
        "margin_seconds": margin_seconds,
    }


def train_model(
    version: str = None, time_budget_seconds: Optional[float] = None
) -> Optional[dict]:
    """
    Train a new model

    Args:
        version: Model version string (e.g., 'v1.0.0'). If None, uses config.MODEL_VERSION

        time_budget_seconds: The caller's timeout. The optional final refit is
            skipped when it would not finish inside it (see estimate_refit).

    Returns:
        The validation metrics once the model is saved, or None when training
        stopped early (no data, empty training set) and nothing was saved. Callers
        must treat None as a failure — `train_standalone.py` does.
    """
    if version is None:
        version = settings.MODEL_VERSION

    import time

    budget_start = time.perf_counter()  # the caller's clock started ~here

    logger.info(f"\n{'=' * 60}")
    logger.info("🚀 Training Wait Time Prediction Model")
    logger.info(f"   Version: {version}")
    logger.info(f"{'=' * 60}\n")

    # Memory monitoring - initial
    initial_memory = get_memory_usage()
    logger.info(f"💾 Initial Memory: {initial_memory:.2f} GB\n")

    # 1. Define training period - use actual data range instead of fixed lookback
    # This prevents querying years of empty data
    logger.info("📅 Determining training period from actual data...")

    from db import get_db
    from sqlalchemy import text

    with get_db() as db:
        # Get actual data range
        range_query = text(
            """
            SELECT 
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                COUNT(*) as total_rows
            FROM queue_data
            WHERE "queueType" = 'STANDBY'
                AND status = 'OPERATING'
                AND "waitTime" IS NOT NULL
        """
        )
        result = db.execute(range_query).fetchone()

        if result.total_rows == 0:
            logger.error("❌ No queue data found in database!")
            return

        # Use actual range, or limit to configured lookback (whichever is smaller)
        data_start = result.earliest
        data_end = result.latest

        # Apply configured limit (don't train on more than X years even if available)
        max_lookback = data_end - timedelta(days=settings.TRAIN_LOOKBACK_YEARS * 365)
        start_date = max(data_start, max_lookback)
        end_date = data_end + timedelta(days=1)  # +1 day buffer for today's data

        actual_days = (data_end - data_start).days
        training_days = (end_date - start_date).days

        logger.info(
            f"   Actual data range: {data_start.strftime('%Y-%m-%d')} to {data_end.strftime('%Y-%m-%d')} ({actual_days} days)"
        )
        logger.info(
            f"   Training period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({training_days} days)"
        )
        logger.info(f"   Total rows available: {result.total_rows:,}")
        logger.info("")

    # Per-phase durations and row counts (PAR-815). Saved in the model metadata
    # and the training status so a slow nightly run shows which phase grew.
    run_start = time.perf_counter()
    timings: dict = {}

    # 2. Fetch training data
    logger.info("📊 Fetching training data from PostgreSQL...")
    phase_start = time.perf_counter()
    df = fetch_training_data(start_date, end_date)
    timings["fetch"] = {
        "seconds": round(time.perf_counter() - phase_start, 1),
        "rows": len(df),
    }
    logger.info(f"   ⏱️  Fetch: {timings['fetch']['seconds']:.1f}s, {len(df):,} rows")
    after_fetch_memory = get_memory_usage()
    logger.info(f"   Rows fetched: {len(df):,}")
    logger.info(
        f"   Memory after fetch: {after_fetch_memory:.2f} GB (+{after_fetch_memory - initial_memory:.2f} GB)"
    )
    logger.info("")

    if len(df) == 0:
        logger.error("❌ No training data found!")
        return

    # 2.5 Validate data quality
    df, validation_report = validate_training_data(df)

    # 2.6 Remove anomalies
    df = remove_anomalies(df)
    logger.info("")

    # 3. Feature engineering
    logger.info("🔧 Engineering features...")
    before_features_memory = get_memory_usage()
    rows_before_features = len(df)
    feature_start = time.time()
    df = engineer_features(df, start_date, end_date)
    feature_time = time.time() - feature_start
    try:
        coverage = curated_attraction_coverage(
            fetch_curated_attraction_facts(df["attractionId"].astype(str).unique().tolist())
        )
        logger.info(
            f"   Curated ride type coverage (PAR-1045, flag "
            f"{'on' if settings.CATBOOST_CURATED_RIDE_TYPE else 'off'}): "
            f"attraction_kind {coverage['attraction_kind']}/{coverage['attractions']}, "
            f"indoor_outdoor {coverage['indoor_outdoor']}/{coverage['attractions']}"
        )
    except Exception as e:  # a report, never a reason to fail training
        logger.warning(f"   Curated ride type coverage unavailable: {e}")
    timings["features"] = {
        "seconds": round(feature_time, 1),
        "rows_in": rows_before_features,
        "rows_out": len(df),
    }
    after_features_memory = get_memory_usage()
    logger.info(f"   Features: {len(get_feature_columns())}")
    logger.info(
        f"   Feature engineering time: {feature_time:.2f}s ({feature_time / 60:.1f} minutes)"
    )
    logger.info(
        f"   Memory after features: {after_features_memory:.2f} GB (+{after_features_memory - before_features_memory:.2f} GB)"
    )
    logger.info("")

    # 4. Drop rows with missing target
    df = df.dropna(subset=["waitTime"])
    gc.collect()  # Free memory from dropped rows
    logger.info(f"   Rows after cleaning: {len(df):,}")
    logger.info("")

    # 4.2 Chronological hold-out — last 30 days of data never seen during training.
    # Gives an honest out-of-time evaluation (random weekly split cannot detect
    # temporal drift or concept shift in recent data).
    holdout_cutoff = df["timestamp"].max() - pd.Timedelta(days=30)
    holdout_mask = df["timestamp"] >= holdout_cutoff
    # Unmodified rows the "as known at origin" hold-out looks its origins up in
    # (the farthest lead reaches 61 d back into the pool), and the occupancy
    # profile it serves far rows from — both taken before cap and dropout.
    origin_from = holdout_cutoff - max(ORIGIN_LEADS.values()) - pd.Timedelta(days=7)
    df_origin_source = df.loc[
        df["timestamp"] >= origin_from,
        ["attractionId", "timestamp", *[c for c in _ORIGIN_VALUE_COLUMNS if c in df.columns]],
    ].copy()
    # The hold-out's slots 52 weeks back (a ~31-day slice) and each ride's first
    # row, for the far leads' same-weekday-last-year value (PAR-1045).
    year_ago_from = holdout_cutoff - LAST_YEAR_LAG - pd.Timedelta(days=1)
    df_last_year = df.loc[
        (df["timestamp"] >= year_ago_from)
        & (df["timestamp"] < year_ago_from + pd.Timedelta(days=33)),
        ["attractionId", "timestamp", "local_timestamp", "waitTime"],
    ].copy()
    first_seen = df.groupby("attractionId")["timestamp"].min()
    occ_profile = origin_occupancy_profile(df[~holdout_mask])
    weather_clim = origin_weather_climatology(df[~holdout_mask])
    df_holdout = df[holdout_mask].copy().reset_index(drop=True)
    df = df[~holdout_mask].copy().reset_index(drop=True)
    gc.collect()
    logger.info(
        f"   Chronological hold-out: {len(df_holdout):,} rows (last 30 days, >{holdout_cutoff.strftime('%Y-%m-%d')})"
    )
    logger.info(f"   Training pool (excl. hold-out): {len(df):,} rows")
    rows_before_cap = len(df)
    df = cap_training_rows(
        df,
        settings.TRAIN_MAX_ROWS,
        settings.TRAIN_FULL_RESOLUTION_DAYS,
        settings.CATBOOST_RANDOM_SEED,
    )
    timings["pool"] = {
        "holdout_rows": len(df_holdout),
        "rows_before_cap": rows_before_cap,
        "rows_after_cap": len(df),
        "max_rows": settings.TRAIN_MAX_ROWS,
    }
    gc.collect()
    logger.info("")

    # 4.5 Training Dropout — simulate the inference scenario for future predictions.
    #
    # Problem: during inference for tomorrow/next week, real-time signals
    # (park_occupancy_pct, avg_wait_last_24h, avg_wait_last_1h) are unavailable
    # or misleading. Without dropout these features dominate (combined ~50%) and
    # the model learns to ignore calendar/holiday signals.
    #
    # Fix: randomly replace real-time features with historical proxies on a fraction
    # of training rows, forcing the model to also learn from time/holiday features.
    df = apply_training_dropout(df, settings, logger)

    # Data sufficiency check
    if len(df) == 0:
        logger.error("❌ No data available for training after validation/cleaning.")
        return

    if len(df) < 10:
        logger.warning(
            "⚠️  WARNING: Very limited data (< 10 rows). Model will have poor accuracy."
        )
        logger.warning(
            "   Training anyway - model will improve as more data accumulates."
        )
        logger.info("")
    elif len(df) < 100:
        logger.warning(
            "⚠️  WARNING: Limited data (< 100 rows). Model accuracy will be limited."
        )
        logger.warning(
            "   Model will improve significantly as more data is collected over time."
        )
        logger.info("")
    elif len(df) < 1000:
        logger.info(
            "ℹ️  Notice: Moderate data available. Model will improve with more historical data."
        )
        logger.info("")

    # 5. Prepare features and target
    feature_columns = get_feature_columns()

    # 5.5. Sample weights (attraction-accuracy feedback loop + busyness). The
    # accuracy stats are fetched once and reused for the full refit set below.
    accuracy_stats = None
    if settings.ENABLE_SAMPLE_WEIGHTS:
        logger.info("📊 Calculating sample weights from attraction accuracy stats...")
        try:
            accuracy_stats = fetch_attraction_accuracy()
        except Exception as e:
            logger.warning(f"   ⚠️ Failed to fetch attraction accuracy stats: {e}")
            accuracy_stats = None
    else:
        logger.info("   Sample weights disabled (ENABLE_SAMPLE_WEIGHTS=False)")
    sample_weights = compute_sample_weights(df, accuracy_stats)

    # Extract features/target AFTER merge so indices are consistent
    df = df.reset_index(drop=True)
    X = df[feature_columns]
    y = df["waitTime"]

    # 6. Train/Validation Split - Randomized Weekly Block Split
    # Strategy:
    # Instead of taking the last N days (which fails during seasonal transitions),
    # we group data by week and randomly select 20% of the weeks for validation.
    # This ensures both training and validation sets contain data from all seasonal
    # phases (e.g., cold winter and busy spring start).

    # Create a unique week identifier (ISO Year + ISO Week).
    # Must use isocalendar().year, NOT dt.year — late-December dates fall into
    # ISO week 1 of the next year, so dt.year=2025 + week=1 → 202501 (wrong).
    iso = df["timestamp"].dt.isocalendar()
    df["_week_id"] = (iso.year * 100 + iso.week).astype(int)
    unique_weeks = df["_week_id"].unique()

    if len(unique_weeks) < 4:
        # Fallback for very sparse data: use simple percentage split
        validation_ratio = 0.2
        logger.info(
            f"📊 Sparse data ({len(unique_weeks)} weeks): Using simple 80/20 percentage split"
        )

        df = df.sort_values("timestamp")
        split_idx = int(len(df) * (1 - validation_ratio))

        X_train = X.iloc[:split_idx]
        y_train = y.iloc[:split_idx]
        X_val = X.iloc[split_idx:]
        y_val = y.iloc[split_idx:]

        if sample_weights is not None:
            train_weights = sample_weights[:split_idx]
        else:
            train_weights = None
    else:
        # Blocked Split: Select 20% of weeks for validation. (numpy is imported at
        # module level; a local `import numpy as np` here would make `np` local to
        # the whole function and unbound on the sparse-split path.)
        val_weeks = select_validation_weeks(unique_weeks, settings.CATBOOST_RANDOM_SEED)
        val_week_count = len(val_weeks)

        val_mask = df["_week_id"].isin(val_weeks)
        train_mask = ~val_mask

        X_train = X[train_mask]
        y_train = y[train_mask]
        X_val = X[val_mask]
        y_val = y[val_mask]

        if sample_weights is not None:
            train_weights = sample_weights[train_mask.values]
        else:
            train_weights = None

        logger.info(
            f"📊 Randomized Weekly Block Split: {len(unique_weeks) - val_week_count} weeks for training, "
            f"{val_week_count} weeks for validation"
        )
        logger.info(f"   Validation weeks: {sorted(list(val_weeks))}")

    # Cleanup temporary column
    df = df.drop(columns=["_week_id"], errors="ignore")
    gc.collect()

    logger.info("📈 Train/Validation Split:")
    logger.info(f"   Training samples: {len(X_train):,}")
    logger.info(f"   Validation samples: {len(X_val):,}")

    # Check for empty datasets
    if len(X_train) == 0:
        logger.error("❌ ERROR: Training set is empty after split!")
        logger.error(f"   Total rows: {len(df):,}")
        return

    if len(X_val) == 0:
        logger.warning("⚠️  WARNING: Validation set is empty after split!")
        logger.warning("   Using all data for training (no validation)")
        X_val = X_train
        y_val = y_train

    if len(y_train) == 0:
        logger.error("❌ ERROR: Training labels (y_train) are empty!")
        logger.error(f"   X_train rows: {len(X_train):,}")
        logger.error(f"   waitTime column exists: {'waitTime' in df.columns}")
        logger.error(
            f"   waitTime non-null count: {df['waitTime'].notna().sum() if 'waitTime' in df.columns else 'N/A'}"
        )
        return

    logger.info(
        f"   Split ratio: {len(X_train) / (len(X_train) + len(X_val)) * 100:.1f}% / {len(X_val) / (len(X_train) + len(X_val)) * 100:.1f}%"
    )
    logger.info("")

    # 7. Train model
    logger.info("🤖 Training CatBoost model...")
    logger.info(f"   Training samples: {len(X_train):,}")
    logger.info(f"   Validation samples: {len(X_val):,}")
    logger.info(f"   Features: {len(feature_columns)}")
    logger.info(f"   Iterations: {settings.CATBOOST_ITERATIONS}")
    logger.info(f"   Learning rate: {settings.CATBOOST_LEARNING_RATE}")
    logger.info(f"   Depth: {settings.CATBOOST_DEPTH}")
    logger.info(f"   Early stopping: {settings.CATBOOST_EARLY_STOPPING_ROUNDS} rounds")
    logger.info("")

    model = WaitTimeModel(version)

    fit_start = time.perf_counter()
    metrics = model.train(X_train, y_train, X_val, y_val, sample_weights=train_weights)
    timings["fit"] = {
        "seconds": round(time.perf_counter() - fit_start, 1),
        "train_rows": len(X_train),
        "val_rows": len(X_val),
        # Trees grown and whether early stopping ended the fit (PAR-902), so a
        # slow run shows in /train/status whether the cap was the reason.
        "trees": model.metadata["early_stopping"]["trees_built"],
        "stopped_early": model.metadata["early_stopping"]["stopped_early"],
    }
    logger.info(
        f"   ⏱️  Fit: {timings['fit']['seconds']:.1f}s "
        f"({len(X_train):,} train / {len(X_val):,} val rows)"
    )

    logger.info("\n" + "=" * 60)
    logger.info("✅ Training Complete!")
    logger.info(f"{'=' * 60}")
    logger.info("\n📊 Randomized Val-Set Metrics (shuffled weeks):")
    logger.info(f"   MAE:              {metrics['mae']:.2f} minutes")
    logger.info(f"   RMSE:             {metrics['rmse']:.2f} minutes")
    logger.info(f"   MAPE:             {metrics['mape']:.2f}%")
    logger.info(f"   MAPE (≥5 min):    {metrics.get('mape_meaningful', 0):.2f}%")
    logger.info(f"   R²:               {metrics['r2']:.4f}")
    logger.info("")

    # 7.5 Chronological hold-out evaluation (honest out-of-time test). Evaluated
    # with the early-stopped model, BEFORE the refit below has seen these rows.
    holdout_eval = None
    if not df_holdout.empty:
        holdout_available = [c for c in feature_columns if c in df_holdout.columns]
        if len(holdout_available) == len(feature_columns):
            X_holdout = df_holdout[feature_columns].copy()
            if "parkId" in X_holdout.columns:
                X_holdout["parkId"] = X_holdout["parkId"].astype(str)
            if "attractionId" in X_holdout.columns:
                X_holdout["attractionId"] = X_holdout["attractionId"].astype(str)
            y_holdout_arr = df_holdout["waitTime"].values.astype(float)
            # Use WaitTimeModel.predict (not the raw booster) so multi-column
            # outputs collapse to a point prediction: MultiQuantile → median,
            # RMSEWithUncertainty → mean. Raw predict() returns (n, n_quantiles)
            # for MultiQuantile and crashes _calculate_metrics (1 != 3).
            h_pred = model.predict(X_holdout)
            holdout_metrics = model._calculate_metrics(y_holdout_arr, h_pred)
            holdout_eval = holdout_summary(y_holdout_arr, h_pred)
            holdout_eval.update(
                {k: holdout_metrics[k] for k in ("rmse", "mape", "r2") if k in holdout_metrics}
            )
            del X_holdout
            logger.info("📊 Chronological Hold-out Metrics (last 30 days, unseen):")
            logger.info(f"   MAE:              {holdout_metrics['mae']:.2f} minutes")
            logger.info(f"   RMSE:             {holdout_metrics['rmse']:.2f} minutes")
            logger.info(f"   MAPE:             {holdout_metrics['mape']:.2f}%")
            logger.info(
                f"   MAPE (≥5 min):    {holdout_metrics.get('mape_meaningful', 0):.2f}%"
            )
            logger.info(f"   R²:               {holdout_metrics['r2']:.4f}")
            if holdout_eval["busy_mae"] is not None:
                logger.info(
                    f"   MAE (busy ≥60):   {holdout_eval['busy_mae']:.2f} minutes "
                    f"({holdout_eval['busy_samples']:,} rows)"
                )
            logger.info(f"   Samples:          {holdout_eval['samples']:,}")
            logger.info("")
        else:
            missing = set(feature_columns) - set(df_holdout.columns)
            logger.warning(
                f"⚠️  Hold-out eval skipped — missing features: {missing}"
            )

    # 7.5b The same hold-out as it would have been forecast at served leads.
    # The block above scores every row with its real lags — a 1 h nowcast — while
    # the model is served 12 h to a year ahead (PAR-1040). Logged and stored only;
    # the promotion gate still reads the validation MAE.
    if holdout_eval is not None:
        holdout_eval["origin_leads"] = {}
        logger.info("📊 Hold-out as known at origin (served leads):")
        for name, lead in ORIGIN_LEADS.items():
            df_lead = as_known_at_origin(
                df_holdout, df_origin_source, lead, occ_profile, weather_clim,
                df_last_year, first_seen,
            )
            if df_lead.empty:
                holdout_eval["origin_leads"][name] = None
                logger.info(f"   {name:>4}: no rows with a known origin")
                continue
            X_lead = df_lead[feature_columns].copy()
            X_lead["parkId"] = X_lead["parkId"].astype(str)
            X_lead["attractionId"] = X_lead["attractionId"].astype(str)
            lead_eval = holdout_summary(
                df_lead["waitTime"].values.astype(float), model.predict(X_lead)
            )
            holdout_eval["origin_leads"][name] = lead_eval
            busy = lead_eval["busy_mae"]
            logger.info(
                f"   {name:>4}: MAE {lead_eval['mae']:.2f} min"
                + (f", busy ≥60 {busy:.2f}" if busy is not None else "")
                + f" ({lead_eval['samples']:,} rows)"
            )
            del df_lead, X_lead
        logger.info("")
    del df_origin_source, df_last_year, first_seen, occ_profile
    gc.collect()

    model.metadata["holdout_metrics"] = holdout_eval

    # 7.6 Final refit on ALL rows (train pool incl. validation weeks + hold-out).
    # Without it the served model never learned the newest 30 days, so a season
    # change (e.g. the start of Halloween) reached it a month late. The tree count
    # is the early-stopped best iteration, unscaled: the refit set is only ~15-20%
    # larger, and not scaling is the conservative choice until an out-of-time
    # comparison says otherwise. Validation metrics (the gate's input) stay those
    # of the early-stopped fit; the hold-out metrics above are its honest
    # out-of-time score.
    refit_plan = None
    if settings.TRAIN_REFIT_ON_ALL_ROWS and not df_holdout.empty:
        refit_plan = estimate_refit(
            elapsed_seconds=time.perf_counter() - budget_start,
            fit_seconds=timings["fit"]["seconds"],
            rows_all=len(df) + len(df_holdout),
            rows_train=timings["fit"]["train_rows"],
            budget_seconds=time_budget_seconds,
            margin_seconds=settings.TRAIN_REFIT_SAFETY_MARGIN_SECONDS,
        )
        if not refit_plan["fits"]:
            # Serving the early-stopped model a day longer beats a timed-out
            # run that registers nothing at all.
            model.metadata["refit_skipped"] = {"reason": "time_budget", **refit_plan}
            logger.warning(
                f"⏭️  Final refit SKIPPED: estimated {refit_plan['estimated_seconds']:.0f}s "
                f"> {refit_plan['remaining_seconds']:.0f}s left in the budget "
                f"({refit_plan['budget_seconds']:.0f}s - {refit_plan['margin_seconds']:.0f}s margin "
                f"- {refit_plan['elapsed_seconds']:.0f}s elapsed). Serving the early-stopped model."
            )
    if refit_plan is not None and refit_plan["fits"]:
        refit_iterations = model.best_iteration_count()
        logger.info(
            f"🔁 Refitting on all rows ({len(df):,} pool + {len(df_holdout):,} hold-out) "
            f"with {refit_iterations} iterations (estimated {refit_plan['estimated_seconds']:.0f}s)..."
        )
        # Free everything the refit does not need before building its frames;
        # otherwise df, df_all and the feature matrix are alive together.
        del X_train, X_val, y_train, y_val, X, y
        gc.collect()
        df_holdout = apply_training_dropout(df_holdout, settings, logger)
        df_all = pd.concat([df, df_holdout], ignore_index=True)
        del df, df_holdout
        gc.collect()
        # Recomputed on the combined frame rather than concatenated: the sparse
        # split above re-sorts `df`, so the pool's weight array may no longer be
        # in df's row order.
        all_weights = compute_sample_weights(df_all, accuracy_stats)
        rows_all = len(df_all)
        X_all = df_all[feature_columns]
        y_all = df_all["waitTime"]
        del df_all
        gc.collect()
        refit_start = time.perf_counter()
        model.refit(
            X_all,
            y_all,
            iterations=refit_iterations,
            sample_weights=all_weights,
        )
        del X_all, y_all, all_weights
        timings["refit"] = {
            "seconds": round(time.perf_counter() - refit_start, 1),
            "rows": rows_all,
            "iterations": refit_iterations,
            "estimated_seconds": refit_plan["estimated_seconds"],
        }
        logger.info(
            f"   ⏱️  Refit: {timings['refit']['seconds']:.1f}s ({rows_all:,} rows)"
        )
        gc.collect()
    elif not settings.TRAIN_REFIT_ON_ALL_ROWS:
        logger.info("   Final refit disabled (TRAIN_REFIT_ON_ALL_ROWS=False)")

    # 8. Feature importance — of the SERVED model (the refit one when the refit
    # ran). metrics["feature_importances"] in the metadata keeps those of the
    # early-stopped fit, which the validation metrics describe.
    served = "refit" if "refit" in timings else "early-stopped"
    logger.info(f"🔍 Top 10 Feature Importances (served model: {served}):")
    importance = model.get_feature_importance().head(10)
    for idx, row in importance.iterrows():
        logger.info(f"   {row['feature']:30s} {row['importance']:>8.2f}")
    logger.info("")

    # 9. Save model
    timings["total_seconds"] = round(time.perf_counter() - run_start, 1)
    model.metadata["training_timings"] = timings
    # Serving builds is_indoor / is_coaster / is_water_ride the way this model
    # learnt them (PAR-1045).
    model.metadata["curated_ride_type"] = bool(settings.CATBOOST_CURATED_RIDE_TYPE)
    logger.info(
        "⏱️  Phases: fetch %.0fs · features %.0fs · fit %.0fs · refit %.0fs · total %.0fs (excl. range query and save)"
        % (
            timings["fetch"]["seconds"],
            timings["features"]["seconds"],
            timings["fit"]["seconds"],
            timings.get("refit", {}).get("seconds", 0),
            timings["total_seconds"],
        )
    )
    logger.info("💾 Saving model...")
    model.save()
    logger.info("")

    logger.info("=" * 60)
    logger.info(f"✅ Model {version} ready for deployment!")
    logger.info("=" * 60)
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train wait time prediction model")
    parser.add_argument(
        "--version", type=str, default=None, help="Model version (e.g., v1.0.0)"
    )

    args = parser.parse_args()
    try:
        train_model(version=args.version)
    except Exception as e:
        logger.error(f"\n❌ FATAL ERROR during training: {e}")
        import traceback

        logger.error(traceback.format_exc())
        exit(1)
