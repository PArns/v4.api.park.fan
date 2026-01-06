#!/usr/bin/env python3
"""
Test different resampling strategies to find optimal balance
between performance and accuracy.
"""

import sys

sys.path.append("/app")

from db import fetch_training_data
from features import remove_anomalies
from model import WaitTimeModel
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

# Test parameters
RESAMPLING_INTERVALS = ["5min", "15min", "30min", "1h"]
END_DATE = datetime.utcnow()
START_DATE = END_DATE - timedelta(days=7)  # Last 7 days

print("=" * 60)
print("🔬 Testing Resampling Strategies")
print("=" * 60)
print(f"\nPeriod: {START_DATE.date()} to {END_DATE.date()}")
print()

results = []

for interval in RESAMPLING_INTERVALS:
    print(f"\n{'=' * 60}")
    print(f"Testing: {interval} resampling")
    print(f"{'=' * 60}")

    # Fetch data
    df = fetch_training_data(START_DATE, END_DATE)
    print(f"✓ Fetched {len(df):,} rows")

    # Remove anomalies
    df = remove_anomalies(df)
    print(f"✓ After anomaly removal: {len(df):,} rows")

    # Modify resampling in engineer_features would go here
    # For now, we'll manually resample
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    before_count = len(df)

    # Simple resampling
    resampled_parts = []
    for (attraction_id, park_id), group in df.groupby(["attractionId", "parkId"]):
        group = group.set_index("timestamp").sort_index()

        # Aggregate
        numeric_cols = group.select_dtypes(include=np.number).columns.tolist()
        if "waitTime" in numeric_cols:
            numeric_cols.remove("waitTime")

        agg_dict = {"waitTime": "mean"}
        for col in numeric_cols:
            agg_dict[col] = "first"

        resampled = group.resample(interval).agg(agg_dict).ffill(limit=2)
        resampled["attractionId"] = attraction_id
        resampled["parkId"] = park_id
        resampled_parts.append(resampled.reset_index())

    if resampled_parts:
        df_resampled = pd.concat(resampled_parts, ignore_index=True)
        df_resampled = df_resampled.dropna(subset=["waitTime"])
    else:
        df_resampled = pd.DataFrame()

    after_count = len(df_resampled)
    reduction = (
        ((before_count - after_count) / before_count * 100) if before_count > 0 else 0
    )

    print(f"✓ After {interval} resampling: {after_count:,} rows")
    print(f"  Reduction: {reduction:.1f}%")

    # Quick feature engineering (simplified)
    if len(df_resampled) > 100:
        # Add basic features
        df_resampled["hour"] = pd.to_datetime(df_resampled["timestamp"]).dt.hour
        df_resampled["day_of_week"] = pd.to_datetime(
            df_resampled["timestamp"]
        ).dt.dayofweek

        # Prepare for training
        feature_cols = ["hour", "day_of_week", "parkId", "attractionId"]
        available_features = [f for f in feature_cols if f in df_resampled.columns]

        X = df_resampled[available_features]
        y = df_resampled["waitTime"]

        if len(X) > 20:
            # Split
            X_train, X_val, y_train, y_val = train_test_split(
                X, y, test_size=0.2, random_state=42
            )

            # Quick model
            model = WaitTimeModel()
            metrics = model.train(
                X_train,
                y_train,
                X_val,
                y_val,
                iterations=200,  # Quick training
                verbose=False,
            )

            print("\n📊 Quick Model Performance:")
            print(f"  MAE:  {metrics['mae']:.2f} min")
            print(f"  RMSE: {metrics['rmse']:.2f} min")
            print(f"  R²:   {metrics['r2']:.4f}")

            results.append(
                {
                    "interval": interval,
                    "rows": after_count,
                    "reduction": reduction,
                    "mae": metrics["mae"],
                    "rmse": metrics["rmse"],
                    "r2": metrics["r2"],
                }
            )
        else:
            print(f"⚠️  Too few samples ({len(X)}) for training")
    else:
        print(f"⚠️  Too few rows ({len(df_resampled)}) after resampling")

# Summary
print(f"\n\n{'=' * 60}")
print("📊 SUMMARY - Resampling Strategy Comparison")
print(f"{'=' * 60}\n")

if results:
    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))

    print(f"\n{'=' * 60}")
    print("💡 RECOMMENDATIONS:")
    print(f"{'=' * 60}\n")

    best_accuracy = df_results.loc[df_results["mae"].idxmin()]
    best_performance = df_results.loc[df_results["reduction"].idxmax()]

    print(f"🎯 Best Accuracy: {best_accuracy['interval']}")
    print(f"   MAE: {best_accuracy['mae']:.2f} min, Rows: {best_accuracy['rows']:,}")

    print(f"\n⚡ Best Performance: {best_performance['interval']}")
    print(
        f"   Reduction: {best_performance['reduction']:.1f}%, MAE: {best_performance['mae']:.2f} min"
    )

    # Find sweet spot (good accuracy + good performance)
    df_results["score"] = (1 - df_results["mae"] / df_results["mae"].max()) * 0.6 + (
        df_results["reduction"] / 100
    ) * 0.4
    sweet_spot = df_results.loc[df_results["score"].idxmax()]

    print(f"\n🎯 Sweet Spot: {sweet_spot['interval']}")
    print(
        f"   MAE: {sweet_spot['mae']:.2f} min, Reduction: {sweet_spot['reduction']:.1f}%"
    )
else:
    print("No results available")
