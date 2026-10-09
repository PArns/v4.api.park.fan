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
from db import fetch_training_data, fetch_attraction_accuracy
from features import engineer_features, get_feature_columns
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

    import time as _time
    rng = np.random.default_rng(int(_time.time()))

    n = len(df)
    log.info("🎲 Applying training dropout...")

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
    # rolling averages + occupancy with historical proxies at a high rate.
    # Without this, CatBoost sees that holiday rows always have elevated rolling
    # averages and high occupancy, so it never needs is_holiday_primary (0% importance).
    # With this dropout the model must use the holiday flag itself for those rows.
    h_rate = getattr(cfg, "HOLIDAY_DROPOUT_RATE", 0.0)
    if h_rate > 0:
        holiday_rows = (df.get("is_holiday_primary", 0) == 1) | (
            df.get("is_school_holiday_primary", 0) == 1
        )
        holiday_count = int(holiday_rows.sum())
        if holiday_count > 0:
            h_mask = holiday_rows & (rng.random(n) < h_rate)
            h_count = int(h_mask.sum())
            if h_count > 0:
                is_weekend_mask = df["is_weekend"].values == 1

                for col in ("avg_wait_last_24h", "avg_wait_last_1h"):
                    if col in df.columns:
                        fallback = np.where(
                            is_weekend_mask,
                            df["rolling_avg_weekend"].values,
                            df["rolling_avg_weekday"].values,
                        )
                        df.loc[h_mask, col] = fallback[h_mask]

                if "rolling_avg_7d" in df.columns:
                    fallback_7d = np.where(
                        is_weekend_mask,
                        df["rolling_avg_weekend"].values,
                        df["rolling_avg_weekday"].values,
                    )
                    # Snapshot original r7d BEFORE overwriting — needed for occupancy ratio below
                    orig_r7d = df["rolling_avg_7d"].values.copy()
                    df.loc[h_mask, "rolling_avg_7d"] = fallback_7d[h_mask]

                if "park_occupancy_pct" in df.columns and "rolling_avg_7d" in df.columns:
                    proxy = np.where(
                        is_weekend_mask,
                        df["rolling_avg_weekend"].values,
                        df["rolling_avg_weekday"].values,
                    )
                    # Use original r7d so proxy/r7d reflects true holiday vs normal ratio
                    r7d = orig_r7d.clip(1)
                    hist_occ = (proxy / r7d) * df["park_occupancy_pct"].values
                    df.loc[h_mask, "park_occupancy_pct"] = hist_occ[h_mask].clip(0, 400)

                log.info(
                    f"   Holiday dropout: {h_count:,}/{holiday_count:,} holiday rows ({h_rate * 100:.0f}%) → historical proxies"
                )

    return df


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
    ratio (refit rows / fit training rows). In production early stopping never
    fires (tree_count = CATBOOST_ITERATIONS = 2000), so the refit runs as many
    trees as the fit did, on ~1.25x the rows. No budget means no limit.
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
        # Use a time-based seed so each training run tests different validation weeks.
        # A fixed seed (CATBOOST_RANDOM_SEED) would always produce the same split,
        # preventing detection of weeks the model over-fits to.
        import time as _time
        split_rng = np.random.default_rng(int(_time.time()))

        # Shuffle weeks and pick 20%
        shuffled_weeks = unique_weeks.copy()
        split_rng.shuffle(shuffled_weeks)

        val_week_count = max(1, int(len(unique_weeks) * 0.2))
        val_weeks = set(shuffled_weeks[:val_week_count])

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
    logger.info("   Early stopping: 100 rounds")
    logger.info("")

    model = WaitTimeModel(version)

    fit_start = time.perf_counter()
    metrics = model.train(X_train, y_train, X_val, y_val, sample_weights=train_weights)
    timings["fit"] = {
        "seconds": round(time.perf_counter() - fit_start, 1),
        "train_rows": len(X_train),
        "val_rows": len(X_val),
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
