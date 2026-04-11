"""
Model training script
"""

import argparse
from datetime import timedelta
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

    # Calculate rolling median (centered) to determine "context"
    # We use a centered window to see what's happening around the data point
    df["rolling_median"] = df.groupby("attractionId")["waitTime"].transform(
        lambda x: x.rolling(window=7, min_periods=1, center=True).median()
    )

    # Condition: Wait time is very low (<= 5 min)
    # BUT the surrounding context (median) is high (> 30 min)
    # This suggests a sudden, unrepresentative drop (downtime/reset) vs natural low crowds
    anomaly_mask = (df["waitTime"] <= 5) & (df["rolling_median"] > 30)

    df_clean = df[~anomaly_mask].copy()
    df_clean = df_clean.drop(columns=["rolling_median"])

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

    Two independent dropout passes:
    1. Occupancy dropout (cfg.OCCUPANCY_DROPOUT_RATE):
       park_occupancy_pct → park's own rolling_avg_weekday or rolling_avg_weekend
       converted to an approximate occupancy ratio.  Simulates future predictions
       where only a historical DOW×hour profile is available.

    2. Rolling-avg dropout (cfg.ROLLING_AVG_DROPOUT_RATE):
       avg_wait_last_24h → avg_wait_same_dow_4w  (same-DOW 4-week average)
       avg_wait_last_1h  → rolling_avg_weekday / rolling_avg_weekend
       Simulates next-week / next-month predictions where yesterday's wait is
       irrelevant.
    """
    import numpy as np

    rng = np.random.default_rng(cfg.CATBOOST_RANDOM_SEED)

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

    # --- 2. Rolling-avg dropout ---
    ravg_rate = cfg.ROLLING_AVG_DROPOUT_RATE
    if ravg_rate > 0:
        ravg_mask = rng.random(n) < ravg_rate
        ravg_count = int(ravg_mask.sum())

        if ravg_count > 0:
            if (
                "avg_wait_last_24h" in df.columns
                and "avg_wait_same_dow_4w" in df.columns
            ):
                df.loc[ravg_mask, "avg_wait_last_24h"] = df.loc[
                    ravg_mask, "avg_wait_same_dow_4w"
                ]

            if "avg_wait_last_1h" in df.columns:
                is_weekend_mask = df["is_weekend"].values == 1
                fallback_1h = np.where(
                    is_weekend_mask,
                    df["rolling_avg_weekend"].values,
                    df["rolling_avg_weekday"].values,
                )
                df.loc[ravg_mask, "avg_wait_last_1h"] = fallback_1h[ravg_mask]

            log.info(
                f"   Rolling-avg dropout: {ravg_count:,} rows ({ravg_rate * 100:.0f}%) → DOW/weekend historical"
            )

    return df


def train_model(version: str = None) -> None:
    """
    Train a new model

    Args:
        version: Model version string (e.g., 'v1.0.0'). If None, uses config.MODEL_VERSION
    """
    if version is None:
        version = settings.MODEL_VERSION

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

    # 2. Fetch training data
    logger.info("📊 Fetching training data from PostgreSQL...")
    df = fetch_training_data(start_date, end_date)
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
    import time

    logger.info("🔧 Engineering features...")
    before_features_memory = get_memory_usage()
    feature_start = time.time()
    df = engineer_features(df, start_date, end_date)
    feature_time = time.time() - feature_start
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
    X = df[feature_columns]
    y = df["waitTime"]

    # 5.5. Calculate sample weights based on attraction-level accuracy (feedback loop)
    # Attractions with high MAE get higher weights to force the model to focus on them.
    sample_weights = None

    if settings.ENABLE_SAMPLE_WEIGHTS:
        logger.info("📊 Calculating sample weights from attraction accuracy stats...")
        try:
            # Fetch pre-calculated MAE per attraction
            accuracy_stats = fetch_attraction_accuracy()

            if not accuracy_stats.empty:
                # Merge accuracy stats with our training data
                # Default weight is 1.0
                df = df.merge(
                    accuracy_stats[["attraction_id", "mae"]],
                    left_on="attractionId",
                    right_on="attraction_id",
                    how="left",
                )

                # Formula: Weight = 1.0 + (MAE / 20) * factor
                # MAE of 20 mins adds 'factor' to the weight.
                # We cap the weight at 2.0 to avoid extreme overfitting.
                weight_factor = settings.SAMPLE_WEIGHT_FACTOR

                # Fill missing MAE with a baseline (e.g., 10 mins)
                df["mae"] = df["mae"].fillna(10.0)

                # Calculate weights
                weights = 1.0 + (df["mae"] / 20.0) * weight_factor
                sample_weights = weights.clip(1.0, 2.0).values

                logger.info(
                    f"   Applied weights to {len(df):,} samples based on {len(accuracy_stats)} attraction stats"
                )
                logger.info(
                    f"   Weight range: {sample_weights.min():.2f} - {sample_weights.max():.2f} (Avg: {sample_weights.mean():.2f})"
                )

                # Cleanup
                df = df.drop(columns=["attraction_id", "mae"], errors="ignore")
                gc.collect()  # Free memory after dropping temp merge columns
            else:
                logger.info(
                    "   No attraction accuracy stats found (using uniform weights)"
                )
        except Exception as e:
            logger.warning(f"   ⚠️ Failed to calculate sample weights: {e}")
            sample_weights = None
    else:
        logger.info("   Sample weights disabled (ENABLE_SAMPLE_WEIGHTS=False)")

    # 6. Train/Validation Split - ADAPTIVE
    # Strategy:
    # - Small datasets (< 14 days): Use percentage split (80/20)
    # - Growing datasets (14-60 days): Scale validation from 20% up to 30 days
    # - Large datasets (> 60 days): Use fixed 30-day validation window
    # This ensures proper train/val ratio as data accumulates

    # Calculate actual data span
    data_span_days = (df["timestamp"].max() - df["timestamp"].min()).days

    # Initialize train_mask to None — only set in the time-based split path.
    # Percentage-split paths use slice indexing instead (handled at line ~427).
    train_mask = None

    # Adaptive validation sizing
    if data_span_days < 14:
        # Very small dataset: use percentage split
        # Very small dataset: use percentage split
        validation_ratio = 0.2  # 20% for validation
        logger.info(
            f"📊 Using percentage-based split (80/20) - only {data_span_days} days of data"
        )

        df = df.sort_values("timestamp")  # Ensure time ordering
        split_idx = int(len(df) * (1 - validation_ratio))

        X_train = X.iloc[:split_idx]
        y_train = y.iloc[:split_idx]
        X_val = X.iloc[split_idx:]
        y_val = y.iloc[split_idx:]

    else:
        # Time-based split for larger datasets
        # Scale validation days from 20% of span (min) to 30 days (max)
        if data_span_days < 60:
            # Growing phase: use 20% of data span, capped at 30 days
            validation_days = min(int(data_span_days * 0.2), 30)
            logger.info(
                f"📊 Using adaptive time-based split - {validation_days} days validation ({data_span_days} days total)"
            )
        else:
            # Stable phase: fixed VALIDATION_DAYS, but never exceed 20% of data span.
            # Without this cap, 30 days on 75 days of data = 40% validation (too little training).
            # Target: ~4% validation (30d on 730d), capped at 20% for smaller datasets.
            max_validation_days = max(7, int(data_span_days * 0.20))
            validation_days = min(settings.VALIDATION_DAYS, max_validation_days)
            logger.info(
                f"📊 Using time-based split - {validation_days} days validation ({data_span_days} days total)"
            )

        validation_cutoff = end_date - timedelta(days=validation_days)

        # Safety check: ensure validation cutoff is after data start
        data_start = df["timestamp"].min()
        if validation_cutoff <= data_start:
            # Fallback to percentage split
            logger.warning(
                f"⚠️  Validation cutoff ({validation_cutoff}) is before data start ({data_start})"
            )
            logger.warning("   Falling back to percentage-based split (80/20)")
            df = df.sort_values("timestamp")
            split_idx = int(len(df) * 0.8)

            X_train = X.iloc[:split_idx]
            y_train = y.iloc[:split_idx]
            X_val = X.iloc[split_idx:]
            y_val = y.iloc[split_idx:]
        else:
            train_mask = df["timestamp"] < validation_cutoff
            val_mask = df["timestamp"] >= validation_cutoff

            X_train = X[train_mask]
            y_train = y[train_mask]
            X_val = X[val_mask]
            y_val = y[val_mask]

    logger.info("📈 Train/Validation Split:")
    logger.info(f"   Training samples: {len(X_train):,}")
    logger.info(f"   Validation samples: {len(X_val):,}")

    # Check for empty datasets
    if len(X_train) == 0:
        logger.error("❌ ERROR: Training set is empty after split!")
        logger.error(f"   Total rows: {len(df):,}")
        logger.error(f"   Data span: {data_span_days} days")
        logger.error(
            f"   Validation cutoff: {validation_cutoff if data_span_days >= 7 else 'N/A (percentage split)'}"
        )
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
    logger.info("   Early stopping: 50 rounds")
    logger.info("")

    model = WaitTimeModel(version)

    # Prepare sample weights for training set
    train_weights = None
    if sample_weights is not None:
        # Split weights same way as data
        if train_mask is not None:
            # Time-based split
            train_weights = sample_weights[train_mask]
        else:
            # Percentage-based split
            train_weights = sample_weights[: len(X_train)]

    metrics = model.train(X_train, y_train, X_val, y_val, sample_weights=train_weights)

    logger.info("\n" + "=" * 60)
    logger.info("✅ Training Complete!")
    logger.info(f"{'=' * 60}")
    logger.info("\n📊 Validation Metrics:")
    logger.info(f"   MAE:  {metrics['mae']:.2f} minutes")
    logger.info(f"   RMSE: {metrics['rmse']:.2f} minutes")
    logger.info(f"   MAPE: {metrics['mape']:.2f}%")
    logger.info(f"   R²:   {metrics['r2']:.4f}")
    logger.info("")

    # 8. Feature importance
    logger.info("🔍 Top 10 Feature Importances:")
    importance = model.get_feature_importance().head(10)
    for idx, row in importance.iterrows():
        logger.info(f"   {row['feature']:30s} {row['importance']:>8.2f}")
    logger.info("")

    # 9. Save model
    logger.info("💾 Saving model...")
    model.save()
    logger.info("")

    logger.info("=" * 60)
    logger.info(f"✅ Model {version} ready for deployment!")
    logger.info("=" * 60)


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
