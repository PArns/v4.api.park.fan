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
    # OPTIMIZED: Use vectorized groupby operations instead of loops
    import time

    outlier_start = time.time()

    statistical_outliers = pd.Series(False, index=df.index)

    if len(df) > 0:
        # Vectorized approach: Calculate IQR bounds per attraction using groupby
        # This is much faster than looping (O(n) vs O(n*m))
        attraction_groups = df.groupby("attractionId")["waitTime"]

        # Get group sizes to filter attractions with at least 10 samples
        group_sizes = attraction_groups.size()
        valid_attraction_ids = group_sizes[group_sizes >= 10].index

        if len(valid_attraction_ids) > 0:
            # Calculate Q1, Q3, IQR for each attraction (only for valid ones)
            Q1 = attraction_groups.quantile(0.25)
            Q3 = attraction_groups.quantile(0.75)
            IQR = Q3 - Q1

            # Calculate upper bound (Q3 + 3*IQR) for each attraction
            upper_bounds = Q3 + 3 * IQR

            # Map upper bounds back to dataframe rows
            df["_upper_bound"] = df["attractionId"].map(upper_bounds)

            # Flag outliers: waitTime > upper_bound AND waitTime > 200
            # Only check attractions with enough data
            mask_valid = df["attractionId"].isin(valid_attraction_ids)
            statistical_outliers = (
                mask_valid
                & (df["waitTime"] > df["_upper_bound"])
                & (df["waitTime"] > 200)
            )

            # Clean up temporary column
            df.drop(columns=["_upper_bound"], inplace=True, errors="ignore")

    outlier_time = time.time() - outlier_start
    if outlier_time > 1.0:  # Only log if it takes more than 1 second
        print(f"   Outlier detection time: {outlier_time:.2f}s")

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
    # Also considers data distribution to avoid filtering out too many attractions
    # during offseason transitions (e.g., when going from 13 to 30+ days)
    
    # Base threshold based on data span
    if data_span_days < 30:
        base_threshold = 10
        stage = "early stage"
    elif data_span_days < 60:
        # Gradual transition: 30-60 days use 15 (not 20) to avoid sudden jumps
        base_threshold = 15
        stage = "growing"
    elif data_span_days < 90:
        base_threshold = 20
        stage = "growing"
    else:
        base_threshold = 50
        stage = "mature"
    
    # Adjust threshold based on actual data distribution
    # If median samples per attraction is low, don't be too aggressive
    if len(attraction_counts) > 0:
        median_samples = attraction_counts.median()
        q25_samples = attraction_counts.quantile(0.25)
        
        # If median is very low (e.g., offseason), use a more lenient threshold
        # This prevents filtering out too many attractions when transitioning to 30+ days
        if median_samples < base_threshold * 1.5:
            # Use the lower of: base_threshold or (q25 * 1.2)
            # This ensures we don't filter out the bottom 25% of attractions
            adjusted_threshold = min(base_threshold, max(10, int(q25_samples * 1.2)))
            
            if adjusted_threshold < base_threshold:
                MIN_SAMPLES_PER_ATTRACTION = adjusted_threshold
                print(f"   📊 Data span: {data_span_days} days ({stage})")
                print(f"   📊 Data distribution: median={median_samples:.1f}, Q25={q25_samples:.1f}")
                print(
                    f"   Using adjusted threshold: {MIN_SAMPLES_PER_ATTRACTION} samples per attraction"
                )
                print(
                    f"   (Adjusted from {base_threshold} due to limited data distribution - likely offseason)"
                )
            else:
                MIN_SAMPLES_PER_ATTRACTION = base_threshold
                print(f"   📊 Data span: {data_span_days} days ({stage})")
                print(
                    f"   Using threshold: {MIN_SAMPLES_PER_ATTRACTION} samples per attraction"
                )
        else:
            MIN_SAMPLES_PER_ATTRACTION = base_threshold
            print(f"   📊 Data span: {data_span_days} days ({stage})")
            print(
                f"   Using threshold: {MIN_SAMPLES_PER_ATTRACTION} samples per attraction"
            )
    else:
        MIN_SAMPLES_PER_ATTRACTION = base_threshold
        print(f"   📊 Data span: {data_span_days} days ({stage})")
        print(
            f"   Using threshold: {MIN_SAMPLES_PER_ATTRACTION} samples per attraction"
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
