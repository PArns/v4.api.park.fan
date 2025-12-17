"""
Data validation for ML training pipeline
Ensures data quality before training
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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
    
    print(f"\n{'='*60}")
    print(f"🔍 Data Validation Pipeline")
    print(f"{'='*60}\n")
    print(f"📊 Initial dataset: {initial_count:,} rows")
    
    # 1. Remove extreme outliers (likely sensor/API errors)
    print(f"\n1️⃣  Checking for extreme outliers...")
    MAX_WAIT_TIME = 300  # 5 hours - longer is likely an error
    MIN_WAIT_TIME = 0
    
    outlier_mask = (df['waitTime'] > MAX_WAIT_TIME) | (df['waitTime'] < MIN_WAIT_TIME)
    outliers_count = outlier_mask.sum()
    
    if outliers_count > 0:
        issues.append(f"Removed {outliers_count} extreme outliers (wait time > {MAX_WAIT_TIME} min or < 0)")
        print(f"   ⚠️  Found {outliers_count} extreme outliers")
        
        # Show examples
        if outliers_count <= 10:
            extreme_values = df.loc[outlier_mask, 'waitTime'].values
            print(f"   Examples: {extreme_values}")
    
    df = df[~outlier_mask].copy()
    print(f"   ✓ Retained {len(df):,} rows after outlier removal")
    
    # 2. Remove duplicate timestamps per attraction
    print(f"\n2️⃣  Checking for duplicate timestamps...")
    duplicates_before = len(df)
    df = df.drop_duplicates(subset=['attractionId', 'timestamp'], keep='first')
    duplicates_removed = duplicates_before - len(df)
    
    if duplicates_removed > 0:
        issues.append(f"Removed {duplicates_removed} duplicate timestamp entries")
        print(f"   ⚠️  Removed {duplicates_removed} duplicates")
    else:
        print(f"   ✓ No duplicates found")
    
    # 3. Check attractions with insufficient data
    print(f"\n3️⃣  Checking data sufficiency per attraction...")
    MIN_SAMPLES_PER_ATTRACTION = 50  # Minimum for meaningful training
    
    attraction_counts = df.groupby('attractionId').size()
    insufficient_attractions = attraction_counts[attraction_counts < MIN_SAMPLES_PER_ATTRACTION]
    
    if len(insufficient_attractions) > 0:
        issues.append(f"Removed {len(insufficient_attractions)} attractions with < {MIN_SAMPLES_PER_ATTRACTION} samples")
        print(f"   ⚠️  Found {len(insufficient_attractions)} attractions with insufficient data")
        print(f"   Attractions removed: {len(insufficient_attractions)}")
        
        # Remove insufficient attractions
        valid_attractions = attraction_counts[attraction_counts >= MIN_SAMPLES_PER_ATTRACTION].index
        df = df[df['attractionId'].isin(valid_attractions)].copy()
    else:
        print(f"   ✓ All attractions have sufficient data")
    
    print(f"   ✓ Training on {len(df.groupby('attractionId'))} attractions")
    
    # 4. Verify timestamp consistency (check for time order issues)
    print(f"\n4️⃣  Checking timestamp consistency...")
    df = df.sort_values(['attractionId', 'timestamp'])
    
    # Check for backwards time jumps
    time_diff = df.groupby('attractionId')['timestamp'].diff()
    negative_diffs = (time_diff < timedelta(0)).sum()
    
    if negative_diffs > 0:
        issues.append(f"Found {negative_diffs} backward time jumps (resolved by sorting)")
        print(f"   ⚠️  Found {negative_diffs} backward time jumps (fixed by sorting)")
    else:
        print(f"   ✓ Timestamps are consistent")
    
    # 5. Check data distribution
    print(f"\n5️⃣  Analyzing data distribution...")
    
    # Wait time statistics
    wait_stats = df['waitTime'].describe()
    print(f"   Wait Time Distribution:")
    print(f"      Mean: {wait_stats['mean']:.1f} min")
    print(f"      Median: {wait_stats['50%']:.1f} min")
    print(f"      Std: {wait_stats['std']:.1f} min")
    print(f"      Q25-Q75: {wait_stats['25%']:.1f} - {wait_stats['75%']:.1f} min")
    
    # Check for suspicious patterns
    zero_wait_pct = (df['waitTime'] == 0).sum() / len(df) * 100
    if zero_wait_pct > 50:
        issues.append(f"High percentage of zero wait times: {zero_wait_pct:.1f}%")
        print(f"   ⚠️  High percentage of zero wait times: {zero_wait_pct:.1f}%")
    
    # 6. Temporal coverage check
    print(f"\n6️⃣  Checking temporal coverage...")
    date_range = (df['timestamp'].max() - df['timestamp'].min()).days
    print(f"   Time span: {date_range} days")
    
    if date_range < 30:
        issues.append(f"Limited temporal coverage: only {date_range} days")
        print(f"   ⚠️  Limited data span: {date_range} days (< 30 days)")
    else:
        print(f"   ✓ Good temporal coverage: {date_range} days")
    
    # Final report
    print(f"\n{'='*60}")
    print(f"📋 Validation Summary")
    print(f"{'='*60}")
    print(f"Initial rows:    {initial_count:,}")
    print(f"Final rows:      {len(df):,}")
    print(f"Rows removed:    {initial_count - len(df):,} ({(initial_count - len(df))/initial_count*100:.2f}%)")
    print(f"Issues found:    {len(issues)}")
    
    if issues:
        print(f"\nIssues:")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
    else:
        print(f"\n✅ No issues found - data is clean!")
    
    print(f"{'='*60}\n")
    
    validation_report = {
        "status": "success",
        "initial_rows": initial_count,
        "final_rows": len(df),
        "rows_removed": initial_count - len(df),
        "removal_percentage": (initial_count - len(df)) / initial_count * 100,
        "issues": issues,
        "attractions_count": len(df.groupby('attractionId')),
        "temporal_span_days": date_range,
        "wait_time_stats": {
            "mean": float(wait_stats['mean']),
            "median": float(wait_stats['50%']),
            "std": float(wait_stats['std']),
            "min": float(wait_stats['min']),
            "max": float(wait_stats['max']),
        }
    }
    
    return df, validation_report
