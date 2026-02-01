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

from config import get_settings
from db import fetch_training_data
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

    # Condition: Wait time is very low (< 10 min)
    # BUT the surrounding context (median) is high (> 25 min)
    # This suggests a sudden, unrepresentative drop (downtime/reset) vs natural low crowds
    anomaly_mask = (df["waitTime"] < 10) & (df["rolling_median"] > 25)

    df_clean = df[~anomaly_mask].copy()
    df_clean = df_clean.drop(columns=["rolling_median"])

    removed = initial_count - len(df_clean)
    logger.info(
        f"   Removed {removed} rows ({(removed / initial_count) * 100:.2f}%) identified as anomalies"
    )

    return df_clean


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
    logger.info(f"   Rows after cleaning: {len(df):,}")
    logger.info("")

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

    # 5.5. Calculate sample weights based on prediction errors (feedback loop)
    # NOTE: Errors are now included directly in the training data via SQL JOIN
    # This avoids loading a second massive DataFrame and merging in Python
    # WARNING: Sample weights can improve performance on difficult cases, but:
    # - Too high weights (factor > 1.0) can cause overfitting on errors
    # - Only a small subset of data will have weights (only those with predictions)
    # - Should be used conservatively (factor 0.3-0.5 recommended)
    # - Requires sufficient data (>30 days) to avoid overfitting
    sample_weights = None

    # Check if we have enough data for sample weights
    data_span_days = (df["timestamp"].max() - df["timestamp"].min()).days
    can_use_weights = (
        settings.ENABLE_SAMPLE_WEIGHTS
        and data_span_days >= settings.MIN_DATA_DAYS_FOR_WEIGHTS
    )

    if can_use_weights:
        logger.info("📊 Calculating sample weights from prediction accuracy...")
        import numpy as np

        # Check if we have error data (from SQL JOIN)
        if "absolute_error" in df.columns:
            # Calculate weights: higher weight for higher errors
            # Weight formula: 1.0 + (error / max_error) * weight_factor
            # Conservative default: 0.5 = 50% boost (weights: 1.0 - 1.5)
            # Aggressive: 1.0 = 100% boost (weights: 1.0 - 2.0)

            error_mask = df["absolute_error"].notna()
            matched_count = error_mask.sum()

            if matched_count > 0:
                max_error = df.loc[error_mask, "absolute_error"].max()
                weight_factor = settings.SAMPLE_WEIGHT_FACTOR

                sample_weights = np.ones(len(df))
                matched_errors = df.loc[error_mask, "absolute_error"]
                weights = 1.0 + (matched_errors / max_error) * weight_factor
                sample_weights[error_mask] = weights

                matched_percentage = (matched_count / len(df)) * 100
                avg_weight = weights.mean()
                max_weight = weights.max()

                logger.info(
                    f"   Matched {matched_count:,} samples ({matched_percentage:.1f}%) with prediction errors"
                )
                logger.info(
                    f"   Average weight: {avg_weight:.2f} (range: {weights.min():.2f} - {max_weight:.2f})"
                )
                logger.info(
                    f"   Weight factor: {weight_factor} (configurable via SAMPLE_WEIGHT_FACTOR)"
                )

                # Warning if too many samples are weighted (might indicate systematic issues)
                if matched_percentage > 50:
                    logger.warning(
                        f"   ⚠️  WARNING: {matched_percentage:.1f}% of samples have weights - this might cause overfitting"
                    )
                    logger.warning(
                        f"      Consider lowering SAMPLE_WEIGHT_FACTOR (current: {weight_factor})"
                    )
                elif matched_percentage < 5:
                    logger.info(
                        f"   ℹ️  Only {matched_percentage:.1f}% of samples have weights - limited impact expected"
                    )
            else:
                logger.info(
                    "   No matching prediction errors found (using uniform weights)"
                )
        else:
            logger.info("   No prediction errors available (using uniform weights)")
    elif (
        settings.ENABLE_SAMPLE_WEIGHTS
        and data_span_days < settings.MIN_DATA_DAYS_FOR_WEIGHTS
    ):
        logger.info(
            f"   Sample weights disabled: Only {data_span_days} days of data (< {settings.MIN_DATA_DAYS_FOR_WEIGHTS} days required)"
        )
        logger.info(
            "      Enable weights when you have more historical data to avoid overfitting"
        )
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
            # Stable phase: use fixed 30 days
            validation_days = settings.VALIDATION_DAYS
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
