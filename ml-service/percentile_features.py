"""
Percentile-based ML Features (Phase 3)

This module adds temporal percentile lookups and weather extreme detection.
Uses pre-computed queue_data_aggregates for efficiency.

Key Principle: NO "averaging of averages" - only direct temporal lookups!
"""

import pandas as pd


def add_percentile_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add percentile-based features using pre-computed queue_data_aggregates.

    This is a SIMPLIFIED implementation that focuses on the most impactful features
    without the row-by-row iteration overhead.

    New Features:
    1. Weather Extreme Features (threshold-based):
       - is_temp_extreme: Temperature > P90 (last 14 days rolling)
       - is_wind_extreme: Wind > P90 (last 14 days rolling)

    Note: Temporal percentile lookups (yesterday, last week, etc.) are intentionally
    NOT implemented in this version due to performance concerns. The existing
    historical features (avg_wait_last_24h, avg_wait_same_hour_last_week) provide
    similar temporal context with better performance.

    Future: If needed, temporal percentile lookups can be added as batch queries
    rather than row-by-row lookups.
    """
    df = df.copy()

    # Initialize weather extreme features
    df["is_temp_extreme"] = 0
    df["is_wind_extreme"] = 0

    # Weather extreme features (threshold: P90 over last 14 days)
    if "temperature_avg" in df.columns and "parkId" in df.columns:
        # Adding temperature extreme features (logging removed)
        for park_id in df["parkId"].unique():
            park_mask = df["parkId"] == park_id
            park_df = df[park_mask].copy()

            # Sort by timestamp for rolling window
            park_df = park_df.sort_values("timestamp")

            # Calculate rolling P90 (14-day window)
            park_df["temp_p90_rolling"] = (
                park_df["temperature_avg"]
                .rolling(
                    window=14 * 24,  # 14 days * 24 hours
                    min_periods=7 * 24,  # At least 7 days
                )
                .quantile(0.90)
            )

            # Check if current temp exceeds P90
            park_df["is_temp_extreme"] = (
                park_df["temperature_avg"] > park_df["temp_p90_rolling"]
            ).astype(int)

            # Update main dataframe
            df.loc[park_mask, "is_temp_extreme"] = park_df["is_temp_extreme"].fillna(0)

    # Wind extreme features (important for ride closures!)
    if "windSpeedMax" in df.columns and "parkId" in df.columns:
        # Adding wind extreme features (logging removed)
        for park_id in df["parkId"].unique():
            park_mask = df["parkId"] == park_id
            park_df = df[park_mask].copy()
            park_df = park_df.sort_values("timestamp")

            # Calculate rolling P90
            park_df["wind_p90_rolling"] = (
                park_df["windSpeedMax"]
                .rolling(window=14 * 24, min_periods=7 * 24)
                .quantile(0.90)
            )

            park_df["is_wind_extreme"] = (
                park_df["windSpeedMax"] > park_df["wind_p90_rolling"]
            ).astype(int)

            df.loc[park_mask, "is_wind_extreme"] = park_df["is_wind_extreme"].fillna(0)

    # Percentile features added (logging removed)
    return df
