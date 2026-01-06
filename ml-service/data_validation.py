"""
Data validation for ML training pipeline
Ensures data quality before training
"""

import pandas as pd
import numpy as np
from datetime import timedelta
from typing import Tuple, Dict, Any


def validate_training_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Validate and clean training data

    Checks:
    1. Remove extreme outliers (likely sensor errors)
    2. Remove duplicate timestamps per attraction
    3. Flag attractions with insufficient data
    4. Verify timestamp consistency
    5. Check for data distribution issues

    Returns:
        Tuple of (cleaned_df, validation_report)
    """
    if df.empty:
        return df, {"status": "empty", "issues": []}

    initial_count = len(df)
    issues = []

    print(f"\n{'=' * 60}")
    print("🔍 Data Validation Pipeline")
    print(f"{'=' * 60}\n")
    print(f"📊 Initial dataset: {initial_count:,} rows")

    # 1. Remove extreme outliers (likely sensor/API errors)
    print("\n1️⃣  Checking for extreme outliers...")

    # Step 1: Remove negative wait times (always invalid)
    negative_mask = df["waitTime"] < 0
    negative_count = negative_mask.sum()

    # Step 2: Remove extremely high wait times (likely sensor errors)
    # Increased from 300 to 600 minutes (10 hours) to account for very busy days
    # Some popular attractions can legitimately have 5-8 hour waits on peak days
    MAX_WAIT_TIME = 600  # 10 hours - longer is likely an error
    extreme_high_mask = df["waitTime"] > MAX_WAIT_TIME

    # Step 3: Statistical outlier detection per attraction (IQR method)
    # This catches values that are outliers relative to each attraction's normal range
    # e.g., if an attraction normally has 30-60 min waits, a sudden 400 min is suspicious
    statistical_outliers = pd.Series(False, index=df.index)

    if len(df) > 0:
        for attraction_id in df["attractionId"].unique():
            attraction_data = df[df["attractionId"] == attraction_id]["waitTime"]

            if len(attraction_data) >= 10:  # Need enough data for IQR
                Q1 = attraction_data.quantile(0.25)
                Q3 = attraction_data.quantile(0.75)
                IQR = Q3 - Q1

                # Only flag as outlier if it's significantly outside normal range
                # Use 3x IQR (more conservative than standard 1.5x) to avoid removing legitimate peaks
                upper_bound = Q3 + 3 * IQR

                # Only flag if value is both:
                # 1. Outside IQR bounds (statistical outlier)
                # 2. Above a reasonable threshold (e.g., 200 min) to avoid flagging low values
                attraction_mask = (df["attractionId"] == attraction_id) & (
                    (df["waitTime"] > upper_bound) & (df["waitTime"] > 200)
                )
                statistical_outliers |= attraction_mask

    # Combine all outlier masks
    outlier_mask = negative_mask | extreme_high_mask | statistical_outliers
    outliers_count = outlier_mask.sum()

    if outliers_count > 0:
        breakdown = {
            "negative": negative_count,
            "extreme_high": extreme_high_mask.sum(),
            "statistical": statistical_outliers.sum(),
        }

        issue_msg = f"Removed {outliers_count} extreme outliers"
        if negative_count > 0:
            issue_msg += f" ({negative_count} negative"
        if extreme_high_mask.sum() > 0:
            issue_msg += f", {extreme_high_mask.sum()} > {MAX_WAIT_TIME} min"
        if statistical_outliers.sum() > 0:
            issue_msg += f", {statistical_outliers.sum()} statistical outliers"
        if negative_count > 0:
            issue_msg += ")"

        issues.append(issue_msg)
        print(f"   ⚠️  Found {outliers_count} extreme outliers")
        print(f"      Breakdown: {breakdown}")

        # Show examples
        if outliers_count <= 10:
            extreme_values = df.loc[outlier_mask, ["attractionId", "waitTime"]].values
            print(f"   Examples: {extreme_values}")
        elif outliers_count <= 50:
            # Show summary statistics
            extreme_wait_times = df.loc[outlier_mask, "waitTime"]
            print(
                f"   Wait time range: {extreme_wait_times.min():.1f} - {extreme_wait_times.max():.1f} min"
            )
            print(
                f"   Mean: {extreme_wait_times.mean():.1f} min, Median: {extreme_wait_times.median():.1f} min"
            )

    df = df[~outlier_mask].copy()
    print(f"   ✓ Retained {len(df):,} rows after outlier removal")

    # 2. Remove duplicate timestamps per attraction
    print("\n2️⃣  Checking for duplicate timestamps...")
    duplicates_before = len(df)
    df = df.drop_duplicates(subset=["attractionId", "timestamp"], keep="first")
    duplicates_removed = duplicates_before - len(df)

    if duplicates_removed > 0:
        issues.append(f"Removed {duplicates_removed} duplicate timestamp entries")
        print(f"   ⚠️  Removed {duplicates_removed} duplicates")
    else:
        print("   ✓ No duplicates found")

    # 3. Check attractions with insufficient data
    print("\n3️⃣  Checking data sufficiency per attraction...")

    # Adaptive threshold based on data availability
    # For early-stage systems with limited data, use lower threshold
    total_attractions = df["attractionId"].nunique()
    attraction_counts = df.groupby("attractionId").size()

    # Calculate data span to determine appropriate threshold
    data_span_days = (
        (df["timestamp"].max() - df["timestamp"].min()).days if len(df) > 0 else 0
    )

    # Adaptive threshold: Lower for systems with limited historical data
    if data_span_days < 30:
        # Early stage: Accept attractions with at least 10 samples
        MIN_SAMPLES_PER_ATTRACTION = 10
        print(f"   📊 Data span: {data_span_days} days (early stage)")
        print(
            f"   Using lower threshold: {MIN_SAMPLES_PER_ATTRACTION} samples per attraction"
        )
    elif data_span_days < 90:
        # Growing system: Require at least 20 samples
        MIN_SAMPLES_PER_ATTRACTION = 20
        print(f"   📊 Data span: {data_span_days} days (growing)")
        print(
            f"   Using moderate threshold: {MIN_SAMPLES_PER_ATTRACTION} samples per attraction"
        )
    else:
        # Mature system: Require at least 50 samples for robust training
        MIN_SAMPLES_PER_ATTRACTION = 50
        print(f"   📊 Data span: {data_span_days} days (mature)")
        print(
            f"   Using standard threshold: {MIN_SAMPLES_PER_ATTRACTION} samples per attraction"
        )

    insufficient_attractions = attraction_counts[
        attraction_counts < MIN_SAMPLES_PER_ATTRACTION
    ]

    if len(insufficient_attractions) > 0:
        # Calculate statistics about removed attractions
        removed_counts = insufficient_attractions.values
        removed_rows = removed_counts.sum()

        # Show distribution of removed attractions
        print(
            f"   ⚠️  Found {len(insufficient_attractions)} attractions with < {MIN_SAMPLES_PER_ATTRACTION} samples"
        )
        print(f"   Total rows from removed attractions: {removed_rows:,}")

        if len(insufficient_attractions) <= 20:
            # Show details for small number of attractions
            print(f"   Removed attractions (samples): {dict(insufficient_attractions)}")
        else:
            # Show statistics for larger sets
            print("   Sample distribution of removed attractions:")
            print(f"      Min: {removed_counts.min()}, Max: {removed_counts.max()}")
            print(
                f"      Median: {np.median(removed_counts):.1f}, Mean: {removed_counts.mean():.1f}"
            )
            print(
                f"      Q25: {np.percentile(removed_counts, 25):.1f}, Q75: {np.percentile(removed_counts, 75):.1f}"
            )

        issues.append(
            f"Removed {len(insufficient_attractions)} attractions with < {MIN_SAMPLES_PER_ATTRACTION} samples ({removed_rows:,} rows)"
        )

        # Remove insufficient attractions
        valid_attractions = attraction_counts[
            attraction_counts >= MIN_SAMPLES_PER_ATTRACTION
        ].index
        df = df[df["attractionId"].isin(valid_attractions)].copy()
    else:
        print("   ✓ All attractions have sufficient data")

    remaining_attractions = df["attractionId"].nunique() if len(df) > 0 else 0
    removed_count = total_attractions - remaining_attractions

    if remaining_attractions > 0:
        valid_counts = attraction_counts[
            attraction_counts >= MIN_SAMPLES_PER_ATTRACTION
        ]
        print(
            f"   ✓ Training on {remaining_attractions} attractions (removed {removed_count})"
        )
        print("   Sample distribution of kept attractions:")
        print(f"      Min: {valid_counts.min()}, Max: {valid_counts.max()}")
        print(
            f"      Median: {np.median(valid_counts.values):.1f}, Mean: {valid_counts.mean():.1f}"
        )
        print(
            f"      Q25: {np.percentile(valid_counts.values, 25):.1f}, Q75: {np.percentile(valid_counts.values, 75):.1f}"
        )
    else:
        print("   ⚠️  WARNING: No attractions remain after filtering!")

    # 4. Verify timestamp consistency (check for time order issues)
    print("\n4️⃣  Checking timestamp consistency...")
    df = df.sort_values(["attractionId", "timestamp"])

    # Check for backwards time jumps
    time_diff = df.groupby("attractionId")["timestamp"].diff()
    negative_diffs = (time_diff < timedelta(0)).sum()

    if negative_diffs > 0:
        issues.append(
            f"Found {negative_diffs} backward time jumps (resolved by sorting)"
        )
        print(f"   ⚠️  Found {negative_diffs} backward time jumps (fixed by sorting)")
    else:
        print("   ✓ Timestamps are consistent")

    # 5. Check data distribution
    print("\n5️⃣  Analyzing data distribution...")

    # Wait time statistics
    wait_stats = df["waitTime"].describe()
    print("   Wait Time Distribution:")
    print(f"      Mean: {wait_stats['mean']:.1f} min")
    print(f"      Median: {wait_stats['50%']:.1f} min")
    print(f"      Std: {wait_stats['std']:.1f} min")
    print(f"      Q25-Q75: {wait_stats['25%']:.1f} - {wait_stats['75%']:.1f} min")

    # Check for suspicious patterns
    zero_wait_pct = (df["waitTime"] == 0).sum() / len(df) * 100
    if zero_wait_pct > 50:
        issues.append(f"High percentage of zero wait times: {zero_wait_pct:.1f}%")
        print(f"   ⚠️  High percentage of zero wait times: {zero_wait_pct:.1f}%")

    # 6. Temporal coverage check
    print("\n6️⃣  Checking temporal coverage...")
    date_range = (df["timestamp"].max() - df["timestamp"].min()).days
    print(f"   Time span: {date_range} days")

    if date_range < 30:
        issues.append(f"Limited temporal coverage: only {date_range} days")
        print(f"   ⚠️  Limited data span: {date_range} days (< 30 days)")
    else:
        print(f"   ✓ Good temporal coverage: {date_range} days")

    # Final report
    print(f"\n{'=' * 60}")
    print("📋 Validation Summary")
    print(f"{'=' * 60}")
    print(f"Initial rows:    {initial_count:,}")
    print(f"Final rows:      {len(df):,}")
    print(
        f"Rows removed:    {initial_count - len(df):,} ({(initial_count - len(df)) / initial_count * 100:.2f}%)"
    )
    print(f"Issues found:    {len(issues)}")

    if issues:
        print("\nIssues:")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
    else:
        print("\n✅ No issues found - data is clean!")

    print(f"{'=' * 60}\n")

    validation_report = {
        "status": "success",
        "initial_rows": initial_count,
        "final_rows": len(df),
        "rows_removed": initial_count - len(df),
        "removal_percentage": (initial_count - len(df)) / initial_count * 100,
        "issues": issues,
        "attractions_count": len(df.groupby("attractionId")),
        "temporal_span_days": date_range,
        "wait_time_stats": {
            "mean": float(wait_stats["mean"]),
            "median": float(wait_stats["50%"]),
            "std": float(wait_stats["std"]),
            "min": float(wait_stats["min"]),
            "max": float(wait_stats["max"]),
        },
    }

    return df, validation_report
