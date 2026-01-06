"""
Model training script
"""

import argparse
from datetime import datetime, timedelta, timezone
import pandas as pd

from config import get_settings
from db import fetch_training_data
from features import engineer_features, get_feature_columns
from model import WaitTimeModel
from data_validation import validate_training_data

settings = get_settings()


def remove_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out likely downtime anomalies (sudden drops to near-zero)

    User Request: "filter downtime with unusual drops... even if not closed"
    """
    if df.empty:
        return df

    print("   🧹 Filtering anomalies...")
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
    print(
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

    print(f"\n{'=' * 60}")
    print("🚀 Training Wait Time Prediction Model")
    print(f"   Version: {version}")
    print(f"{'=' * 60}\n")

    # 1. Define training period (last 2 years + 1 day buffer for today's data)
    end_date = datetime.now(timezone.utc) + timedelta(days=1)
    start_date = end_date - timedelta(days=settings.TRAIN_LOOKBACK_YEARS * 365)

    print("📅 Training Period:")
    print(f"   Start: {start_date.strftime('%Y-%m-%d')}")
    print(f"   End: {end_date.strftime('%Y-%m-%d')}")
    print()

    # 2. Fetch training data
    print("📊 Fetching training data from PostgreSQL...")
    df = fetch_training_data(start_date, end_date)
    print(f"   Rows fetched: {len(df):,}")
    print()

    if len(df) == 0:
        print("❌ No training data found!")
        return

    # 2.5 Validate data quality
    df, validation_report = validate_training_data(df)

    # 2.6 Remove anomalies
    df = remove_anomalies(df)
    print()
    
    # 3. Feature engineering
    import time
    print("🔧 Engineering features...")
    feature_start = time.time()
    df = engineer_features(df, start_date, end_date)
    feature_time = time.time() - feature_start
    print(f"   Features: {len(get_feature_columns())}")
    print(f"   Feature engineering time: {feature_time:.2f}s ({feature_time/60:.1f} minutes)")
    print()

    # 4. Drop rows with missing target
    df = df.dropna(subset=["waitTime"])
    print(f"   Rows after cleaning: {len(df):,}")
    print()

    # Data sufficiency check
    if len(df) == 0:
        print("❌ No data available for training after validation/cleaning.")
        return

    if len(df) < 10:
        print(
            "⚠️  WARNING: Very limited data (< 10 rows). Model will have poor accuracy."
        )
        print("   Training anyway - model will improve as more data accumulates.")
        print()
    elif len(df) < 100:
        print("⚠️  WARNING: Limited data (< 100 rows). Model accuracy will be limited.")
        print(
            "   Model will improve significantly as more data is collected over time."
        )
        print()
    elif len(df) < 1000:
        print(
            "ℹ️  Notice: Moderate data available. Model will improve with more historical data."
        )
        print()

    # 5. Prepare features and target
    feature_columns = get_feature_columns()
    X = df[feature_columns]
    y = df["waitTime"]
    
    # 5.5. Calculate sample weights based on prediction errors (feedback loop)
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
        print("📊 Calculating sample weights from prediction accuracy...")
        from db import fetch_prediction_errors_for_training
        import numpy as np
        
        error_df = fetch_prediction_errors_for_training(start_date, end_date)
        
        if not error_df.empty:
            # Merge errors with training data
            # Match on attractionId and timestamp (within 5 minutes tolerance)
            df_with_errors = df.merge(
                error_df[["attractionId", "timestamp", "absolute_error", "percentage_error"]],
                on=["attractionId"],
                how="left",
                suffixes=("", "_error")
            )
            
            # Match timestamps (within 5 minutes)
            if "timestamp_error" in df_with_errors.columns:
                time_diff = (df_with_errors["timestamp"] - df_with_errors["timestamp_error"]).abs()
                time_match = time_diff <= pd.Timedelta(minutes=5)
                
                # Calculate weights: higher weight for higher errors
                # Weight formula: 1.0 + (error / max_error) * weight_factor
                # Conservative default: 0.5 = 50% boost (weights: 1.0 - 1.5)
                # Aggressive: 1.0 = 100% boost (weights: 1.0 - 2.0)
                max_error = error_df["absolute_error"].max() if len(error_df) > 0 else 1.0
                weight_factor = settings.SAMPLE_WEIGHT_FACTOR
                
                sample_weights = np.ones(len(df))
                matched_mask = time_match & df_with_errors["absolute_error"].notna()
                
                if matched_mask.sum() > 0:
                    matched_errors = df_with_errors.loc[matched_mask, "absolute_error"]
                    weights = 1.0 + (matched_errors / max_error) * weight_factor
                    sample_weights[matched_mask] = weights
                    
                    matched_count = matched_mask.sum()
                    matched_percentage = (matched_count / len(df)) * 100
                    avg_weight = weights.mean()
                    max_weight = weights.max()
                    
                    print(f"   Matched {matched_count:,} samples ({matched_percentage:.1f}%) with prediction errors")
                    print(f"   Average weight: {avg_weight:.2f} (range: {weights.min():.2f} - {max_weight:.2f})")
                    print(f"   Weight factor: {weight_factor} (configurable via SAMPLE_WEIGHT_FACTOR)")
                    
                    # Warning if too many samples are weighted (might indicate systematic issues)
                    if matched_percentage > 50:
                        print(f"   ⚠️  WARNING: {matched_percentage:.1f}% of samples have weights - this might cause overfitting")
                        print(f"      Consider lowering SAMPLE_WEIGHT_FACTOR (current: {weight_factor})")
                    elif matched_percentage < 5:
                        print(f"   ℹ️  Only {matched_percentage:.1f}% of samples have weights - limited impact expected")
                else:
                    print("   No matching prediction errors found (using uniform weights)")
            else:
                print("   No prediction errors available (using uniform weights)")
        else:
            print("   No prediction accuracy data available (using uniform weights)")
    elif settings.ENABLE_SAMPLE_WEIGHTS and data_span_days < settings.MIN_DATA_DAYS_FOR_WEIGHTS:
        print(f"   Sample weights disabled: Only {data_span_days} days of data (< {settings.MIN_DATA_DAYS_FOR_WEIGHTS} days required)")
        print("      Enable weights when you have more historical data to avoid overfitting")
    else:
        print("   Sample weights disabled (ENABLE_SAMPLE_WEIGHTS=False)")

    # 6. Train/test split
    # For small datasets (< 100 rows or < 7 days), use percentage split
    # For larger datasets, use time-based split (last N days as validation)
    # Note: data_span_days already calculated above for sample weights check

    train_mask = None  # Initialize for sample weights split
    
    if len(df) < 100 or data_span_days < 7:
        # Percentage-based split for small datasets
        print("📊 Using percentage-based split (80/20) due to limited data")
        split_idx = int(len(df) * 0.8)
        df = df.sort_values("timestamp")  # Ensure time ordering

        X_train = X.iloc[:split_idx]
        y_train = y.iloc[:split_idx]
        X_val = X.iloc[split_idx:]
        y_val = y.iloc[split_idx:]
    else:
        # Time-based split for larger datasets
        validation_cutoff = end_date - timedelta(days=settings.VALIDATION_DAYS)
        
        # Check if validation_cutoff is before data start (fallback to percentage split)
        data_start = df["timestamp"].min()
        if validation_cutoff <= data_start:
            print(f"⚠️  Validation cutoff ({validation_cutoff}) is before data start ({data_start})")
            print("   Falling back to percentage-based split (80/20)")
            split_idx = int(len(df) * 0.8)
            df = df.sort_values("timestamp")
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

    print("📈 Train/Validation Split:")
    print(f"   Training samples: {len(X_train):,}")
    print(f"   Validation samples: {len(X_val):,}")
    
    # Check for empty datasets
    if len(X_train) == 0:
        print("❌ ERROR: Training set is empty after split!")
        print(f"   Total rows: {len(df):,}")
        print(f"   Data span: {data_span_days} days")
        print(f"   Validation cutoff: {validation_cutoff if data_span_days >= 7 else 'N/A (percentage split)'}")
        return
    
    if len(X_val) == 0:
        print("⚠️  WARNING: Validation set is empty after split!")
        print("   Using all data for training (no validation)")
        X_val = X_train
        y_val = y_train
    
    if len(y_train) == 0:
        print("❌ ERROR: Training labels (y_train) are empty!")
        print(f"   X_train rows: {len(X_train):,}")
        print(f"   waitTime column exists: {'waitTime' in df.columns}")
        print(f"   waitTime non-null count: {df['waitTime'].notna().sum() if 'waitTime' in df.columns else 'N/A'}")
        return
    
    print(
        f"   Split ratio: {len(X_train) / (len(X_train) + len(X_val)) * 100:.1f}% / {len(X_val) / (len(X_train) + len(X_val)) * 100:.1f}%"
    )
    print()

    # 7. Train model
    print("🤖 Training CatBoost model...")
    print(f"   Training samples: {len(X_train):,}")
    print(f"   Validation samples: {len(X_val):,}")
    print(f"   Features: {len(feature_columns)}")
    print(f"   Iterations: {settings.CATBOOST_ITERATIONS}")
    print(f"   Learning rate: {settings.CATBOOST_LEARNING_RATE}")
    print(f"   Depth: {settings.CATBOOST_DEPTH}")
    print("   Early stopping: 50 rounds")
    print()

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
            train_weights = sample_weights[:len(X_train)]
    
    metrics = model.train(X_train, y_train, X_val, y_val, sample_weights=train_weights)

    print("\n" + "=" * 60)
    print("✅ Training Complete!")
    print(f"{'=' * 60}")
    print("\n📊 Validation Metrics:")
    print(f"   MAE:  {metrics['mae']:.2f} minutes")
    print(f"   RMSE: {metrics['rmse']:.2f} minutes")
    print(f"   MAPE: {metrics['mape']:.2f}%")
    print(f"   R²:   {metrics['r2']:.4f}")
    print()

    # 8. Feature importance
    print("🔍 Top 10 Feature Importances:")
    importance = model.get_feature_importance().head(10)
    for idx, row in importance.iterrows():
        print(f"   {row['feature']:30s} {row['importance']:>8.2f}")
    print()

    # 9. Save model
    print("💾 Saving model...")
    model.save()
    print()

    print("=" * 60)
    print(f"✅ Model {version} ready for deployment!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train wait time prediction model")
    parser.add_argument(
        "--version", type=str, default=None, help="Model version (e.g., v1.0.0)"
    )

    args = parser.parse_args()
    try:
        train_model(version=args.version)
    except Exception as e:
        print(f"\n❌ FATAL ERROR during training: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
