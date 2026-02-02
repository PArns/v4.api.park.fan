"""
Feature engineering for ML model
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from typing import Dict, List
from db import fetch_holidays, fetch_parks_metadata, fetch_park_schedules
from holiday_utils import normalize_region_code, calculate_holiday_info
from config import get_settings
from percentile_features import add_percentile_features
from attraction_features import (
    add_attraction_type_feature,
    add_park_attraction_count_feature,
)


def convert_to_local_time(
    df: pd.DataFrame, parks_metadata: pd.DataFrame
) -> pd.DataFrame:
    """
    Convert UTC timestamps to park-local time.
    Critical for correct hour/day features and date-based lookups (weather/holidays).
    """
    try:
        import pytz  # noqa: F401
    except ImportError:
        print("⚠️  pytz not installed, using UTC. Install with: pip install pytz")
        return df

    # No need for copy - we modify via loc which creates copies internally

    # Ensure timestamp is datetime and timezone-aware (as UTC)
    if df["timestamp"].dt.tz is None:
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
    else:
        df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

    # Create map {parkId: timezone_str}
    tz_map = parks_metadata.set_index("park_id")["timezone"].to_dict()

    # We need a 'local_timestamp' column for features
    # Vectorized approach per park (much faster than apply)
    df["local_timestamp"] = df["timestamp"]  # Default to UTC

    for park_id in df["parkId"].unique():
        tz_name = tz_map.get(park_id)
        if not tz_name:
            continue

        try:
            mask = df["parkId"] == park_id
            # Convert to local time
            df.loc[mask, "local_timestamp"] = df.loc[mask, "timestamp"].dt.tz_convert(
                tz_name
            )
        except Exception as e:
            print(f"⚠️  Timezone conversion failed for park {park_id} ({tz_name}): {e}")

    return df


def add_weekend_feature(df: pd.DataFrame, parks_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Add weekend feature based on region-specific weekend days.

    Centralized function to avoid code duplication between training and inference.

    Weekend definitions:
    - Middle East (SA, AE, BH, KW, OM, QA, IL): Friday (4) + Saturday (5)
    - Western countries: Saturday (5) + Sunday (6)

    Args:
        df: DataFrame with 'parkId' and 'day_of_week' columns
        parks_metadata: DataFrame with park metadata including 'park_id' and 'country'

    Returns:
        DataFrame with 'is_weekend' column added
    """
    # No need for copy - adding simple column

    # Middle East countries use Friday+Saturday as weekend
    middle_east_countries = ["SA", "AE", "BH", "KW", "OM", "QA", "IL"]

    # Initialize is_weekend column
    df["is_weekend"] = 0

    # For each park, determine weekend based on country
    for park_id in df["parkId"].unique():
        park_info = parks_metadata[parks_metadata["park_id"] == park_id]
        park_mask = df["parkId"] == park_id

        if not park_info.empty:
            country = park_info.iloc[0]["country"]

            if country in middle_east_countries:
                # Middle East: Friday (4) + Saturday (5)
                # dayofweek: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday, 5=Saturday, 6=Sunday
                df.loc[park_mask, "is_weekend"] = (
                    df.loc[park_mask, "day_of_week"].isin([4, 5]).astype(int)
                )
            else:
                # Western: Saturday (5) + Sunday (6)
                df.loc[park_mask, "is_weekend"] = (
                    df.loc[park_mask, "day_of_week"].isin([5, 6]).astype(int)
                )
        else:
            # Default: Western weekend (Saturday + Sunday)
            df.loc[park_mask, "is_weekend"] = (
                df.loc[park_mask, "day_of_week"].isin([5, 6]).astype(int)
            )

    return df


def add_time_features(df: pd.DataFrame, parks_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Add time-based features using LOCAL time.
    """
    # 1. Convert to local time first
    df = convert_to_local_time(df, parks_metadata)

    # 2. Extract features from LOCAL time
    df["hour"] = df["local_timestamp"].dt.hour
    df["month"] = df["local_timestamp"].dt.month
    df["day_of_week"] = df["local_timestamp"].dt.dayofweek  # 0=Monday, 6=Sunday

    # Cyclic encoding (essential for tree models to understand continuity)
    # e.g., hour=23 and hour=0 are close, not 23 units apart
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    df["day_of_week_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["day_of_week_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    # Day of year (1-365/366) for finer seasonal trends
    df["day_of_year"] = df["local_timestamp"].dt.dayofyear
    df["day_of_year_sin"] = np.sin(2 * np.pi * df["day_of_year"] / 365.25)
    df["day_of_year_cos"] = np.cos(2 * np.pi * df["day_of_year"] / 365.25)

    # Season (derived from month)
    def get_season(month):
        if month in [12, 1, 2]:
            return 0  # Winter
        if month in [3, 4, 5]:
            return 1  # Spring
        if month in [6, 7, 8]:
            return 2  # Summer
        return 3  # Fall

    df["season"] = df["month"].apply(get_season)

    # Peak season indicator (summer months + December holidays)
    # Peak seasons: June-August (summer), December (holidays)
    df["is_peak_season"] = ((df["month"] >= 6) & (df["month"] <= 8)) | (
        df["month"] == 12
    )
    df["is_peak_season"] = df["is_peak_season"].astype(int)

    # 3. Use LOCAL date for further lookups (holidays, weather)
    # This prevents using yesterday's weather for today's morning (due to UTC lag)
    df["date_local"] = df["local_timestamp"].dt.date

    # Region-specific weekend detection
    # Use centralized function to avoid code duplication
    df = add_weekend_feature(df, parks_metadata)

    return df


def add_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """Process weather features"""
    # No need for copy - pandas operations create copies automatically

    # Fill missing weather - numeric features with mean, categorical with mode
    numeric_weather_cols = [
        "temperatureMax",
        "temperatureMin",
        "precipitation",
        "windSpeedMax",
        "snowfallSum",
    ]

    for col in numeric_weather_cols:
        if col in df.columns:
            # Fill with park-specific mean, then global mean
            df[col] = (
                df.groupby("parkId")[col]
                .transform(lambda x: x.fillna(x.mean()))
                .fillna(df[col].mean())
            )

    # Fill weatherCode (categorical) with mode, then convert to int
    if "weatherCode" in df.columns:
        # Fill with park-specific mode (most common value), then global mode
        df["weatherCode"] = df.groupby("parkId")["weatherCode"].transform(
            lambda x: x.fillna(x.mode()[0] if len(x.mode()) > 0 else 0)
        )
        # Fill any remaining NaN with global mode
        if df["weatherCode"].isna().any():
            global_mode = df["weatherCode"].mode()
            df["weatherCode"] = df["weatherCode"].fillna(
                global_mode[0] if len(global_mode) > 0 else 0
            )
        # Convert to integer (CatBoost requires int/string for categorical features)
        df["weatherCode"] = df["weatherCode"].astype(int)

    # Temperature average
    if "temperatureMax" in df.columns and "temperatureMin" in df.columns:
        df["temperature_avg"] = (df["temperatureMax"] + df["temperatureMin"]) / 2

    # Binary rain indicator (explicit signal, valuable for ML)
    if "precipitation" in df.columns:
        df["is_raining"] = (df["precipitation"] > 0).astype(int)

        # Precipitation last 3 hours (cumulative effect)
        # For training: use rolling window on historical data
        # For inference: will be provided via feature_context
        if "timestamp" in df.columns:
            df_sorted = df.sort_values("timestamp").set_index("timestamp")
            df["precipitation_last_3h"] = (
                df_sorted.groupby("parkId")["precipitation"]
                .rolling("3h", closed="left", min_periods=1)
                .sum()
                .reset_index(level=0, drop=True)
                .values
            )
            df["precipitation_last_3h"] = df["precipitation_last_3h"].fillna(0)
        else:
            df["precipitation_last_3h"] = 0

    # Temperature deviation (current vs. monthly average)
    # Helps model understand if weather is unusually hot/cold
    if "temperature_avg" in df.columns and "month" in df.columns:
        # Calculate monthly average temperature per park
        monthly_avg = df.groupby(["parkId", "month"])["temperature_avg"].transform(
            "mean"
        )
        df["temperature_deviation"] = df["temperature_avg"] - monthly_avg
        df["temperature_deviation"] = df["temperature_deviation"].fillna(0)
    else:
        df["temperature_deviation"] = 0

    return df


def add_holiday_features(
    df: pd.DataFrame,
    parks_metadata: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    cached_holidays_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Add multi-country and regional holiday features

    For each park, checks holidays in:
    - Primary country/region (park's location)
    - Influencing regions (from influencingRegions JSON)
    """
    # No need for copy - merge operations create new DataFrames

    # Initialize holiday columns
    df["is_holiday_primary"] = 0
    df["is_school_holiday_primary"] = 0
    df["is_holiday_neighbor_1"] = 0
    df["is_holiday_neighbor_2"] = 0
    df["is_holiday_neighbor_3"] = 0
    df["holiday_count_total"] = 0
    df["school_holiday_count_total"] = 0
    df["is_school_holiday_any"] = (
        0  # Consolidated signal (matches Node.js inference feature)
    )

    # Merge park metadata
    df = df.merge(
        parks_metadata[["park_id", "country", "region_code", "influencingRegions"]],
        left_on="parkId",
        right_on="park_id",
        how="left",
    )

    # Use cached holidays if provided, otherwise fetch
    if cached_holidays_df is not None:
        holidays_df = cached_holidays_df.copy()
    else:
        # Get unique countries to fetch holidays for
        all_countries = set()
        for _, row in parks_metadata.iterrows():
            all_countries.add(row["country"])

            # Add countries from influencingRegions JSON
            if isinstance(row["influencingRegions"], list):
                for region in row["influencingRegions"]:
                    if isinstance(region, dict) and "countryCode" in region:
                        all_countries.add(region["countryCode"])

        # Fetch all holidays
        holidays_df = fetch_holidays(list(all_countries), start_date, end_date)

    # Convert date column to date type (handle both datetime and date types)
    if not holidays_df.empty:
        # Convert to datetime first (handles both date and datetime), then extract date
        holidays_df["date"] = pd.to_datetime(holidays_df["date"]).dt.date

        # Filter to date range (cached may include extra days for bridge day calculations)
        if cached_holidays_df is not None:
            holidays_df = holidays_df[
                (holidays_df["date"] >= start_date.date())
                & (holidays_df["date"] <= end_date.date())
            ]

    # Weekend extensions are now handled by the TypeScript API (enrichScheduleWithHolidays)
    # The API correctly extends ONLY school holidays to weekends, not public holidays
    # This data is already in the database, so we don't need to calculate it here
    # This ensures training and prediction use the same holiday logic

    # Create holiday lookup DataFrames for vectorized merge
    # Regional holidays: (country, region, date) -> holiday_type
    # National holidays: (country, date) -> holiday_type
    regional_holidays = holidays_df[holidays_df["region"].notna()].copy()
    national_holidays = holidays_df[holidays_df["is_nationwide"]].copy()

    # Ensure date_local exists in df
    if "date_local" not in df.columns:
        if "local_timestamp" in df.columns:
            df["date_local"] = pd.to_datetime(df["local_timestamp"]).dt.date
        else:
            df["date_local"] = pd.to_datetime(df["timestamp"]).dt.date

    # Import region normalization utility

    # 1. Primary Location Holiday Check (vectorized)
    if not regional_holidays.empty:
        # Normalize region codes in both DataFrames for consistent matching
        regional_holidays = regional_holidays.copy()
        regional_holidays["region_normalized"] = regional_holidays["region"].apply(
            normalize_region_code
        )
        df["region_code_normalized"] = df["region_code"].apply(normalize_region_code)

        # Merge regional holidays using normalized region codes
        regional_holidays["date_only"] = regional_holidays["date"]
        df_regional = df.merge(
            regional_holidays[
                ["country", "region_normalized", "date_only", "holiday_type"]
            ],
            left_on=["country", "region_code_normalized", "date_local"],
            right_on=["country", "region_normalized", "date_only"],
            how="left",
            suffixes=("", "_regional"),
        )
        df["primary_holiday_type_regional"] = df_regional["holiday_type"]
        # Clean up temporary column
        df = df.drop(columns=["region_code_normalized"], errors="ignore")
    else:
        df["primary_holiday_type_regional"] = None

    if not national_holidays.empty:
        # Merge national holidays
        national_holidays["date_only"] = national_holidays["date"]
        df_national = df.merge(
            national_holidays[["country", "date_only", "holiday_type"]],
            left_on=["country", "date_local"],
            right_on=["country", "date_only"],
            how="left",
            suffixes=("", "_national"),
        )
        df["primary_holiday_type_national"] = df_national["holiday_type"]
    else:
        df["primary_holiday_type_national"] = None

    # Combine regional and national (prefer regional, fallback to national)
    df["primary_holiday_type"] = df["primary_holiday_type_regional"].fillna(
        df["primary_holiday_type_national"]
    )

    # Assign primary features (vectorized)
    # Weekend extensions are already included in holidays_df from above
    df["is_holiday_primary"] = (df["primary_holiday_type"] == "public").astype(int)
    df["is_school_holiday_primary"] = (df["primary_holiday_type"] == "school").astype(
        int
    )

    # 2. Influencing Regions Check (still needs some iteration due to JSON structure)
    # Create lookup maps for faster access
    # Import region normalization utility

    holiday_map_regional = {}
    holiday_map_national = {}

    if not holidays_df.empty:
        for _, row in holidays_df.iterrows():
            h_date = row["date"]
            h_country = row["country"]
            h_type = row["holiday_type"]
            h_region = row["region"]
            is_nationwide = row["is_nationwide"]

            if is_nationwide:
                holiday_map_national[(h_country, h_date)] = h_type
            if h_region:
                # Normalize region code for consistent matching (handles both "DE-NW" and "NW")
                normalized_region = normalize_region_code(h_region)
                holiday_map_regional[(h_country, normalized_region, h_date)] = h_type

    def check_neighbor_holidays(row):
        """Check holidays for influencing regions"""
        neighbor_flags = [0, 0, 0]
        neighbor_school_flags = [0, 0, 0]

        date = row["date_local"]
        if pd.isna(date):
            return neighbor_flags, neighbor_school_flags

        raw_regions = row["influencingRegions"]
        if not isinstance(raw_regions, list) or len(raw_regions) == 0:
            return neighbor_flags, neighbor_school_flags

        for i, region_def in enumerate(raw_regions[:3]):
            try:
                if not isinstance(region_def, dict):
                    continue

                n_country = region_def.get("countryCode")
                n_region = region_def.get("regionCode")

                n_type = None
                # Check regional first (normalize region code for consistent matching)
                if n_region:
                    normalized_region = normalize_region_code(n_region)
                    n_type = holiday_map_regional.get(
                        (n_country, normalized_region, date)
                    )

                # Check national if no regional match
                if not n_type:
                    n_type = holiday_map_national.get((n_country, date))

                neighbor_flags[i] = int(n_type == "public")
                neighbor_school_flags[i] = int(n_type == "school")
            except Exception:
                pass

        return neighbor_flags, neighbor_school_flags

    # Apply neighbor check (only processes influencing regions, not full row)
    neighbor_results = df.apply(check_neighbor_holidays, axis=1)
    df["neighbor_flags"] = neighbor_results.apply(lambda x: x[0])
    df["neighbor_school_flags"] = neighbor_results.apply(lambda x: x[1])

    # Assign neighbor features (vectorized)
    df["is_holiday_neighbor_1"] = df["neighbor_flags"].apply(
        lambda x: x[0] if len(x) > 0 else 0
    )
    df["is_holiday_neighbor_2"] = df["neighbor_flags"].apply(
        lambda x: x[1] if len(x) > 1 else 0
    )
    df["is_holiday_neighbor_3"] = df["neighbor_flags"].apply(
        lambda x: x[2] if len(x) > 2 else 0
    )

    # Totals (vectorized)
    df["holiday_count_total"] = (
        df["is_holiday_primary"]
        + df["is_holiday_neighbor_1"]
        + df["is_holiday_neighbor_2"]
        + df["is_holiday_neighbor_3"]
    )

    df["school_holiday_count_total"] = df["is_school_holiday_primary"] + df[
        "neighbor_school_flags"
    ].apply(lambda x: sum(x) if isinstance(x, list) else 0)

    df["is_school_holiday_any"] = (
        (df["is_school_holiday_primary"] == 1)
        | (df["school_holiday_count_total"] > df["is_school_holiday_primary"])
    ).astype(int)

    # Clean up temporary columns
    df = df.drop(
        columns=[
            "neighbor_flags",
            "neighbor_school_flags",
            "primary_holiday_type_regional",
            "primary_holiday_type_national",
            "primary_holiday_type",
        ],
        errors="ignore",
    )

    # Drop temporary merge columns
    drop_cols = ["park_id", "country", "region_code", "influencingRegions"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    return df


def add_historical_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add historical wait time features

    Features:
    - avg_wait_last_24h: Average wait time in last 24 hours (per attraction)
    - avg_wait_last_1h: Average wait time in last 1 hour (Time-based, strictly past)
    - wait_lag_24h: Wait time at same time yesterday (Time-based)
    - wait_lag_1w: Wait time at same time last week (Time-based)
    - rolling_avg_7d: 7-day rolling average
    """
    # No need for copy - rolling/transform operations handle this
    if "timestamp" not in df.columns:
        return df

    # distinct sort for merge_asof
    df = df.sort_values("timestamp")

    # 1. Time-based Rolling Features
    # Must use index for time-based rolling
    df_indexed = df.set_index("timestamp").sort_index()

    # avg_wait_last_1h: [t-1h, t)
    # closed='left' excludes current timestamp, preventing data leakage
    df["avg_wait_last_1h"] = (
        df_indexed.groupby("attractionId")["waitTime"]
        .rolling("1h", closed="left", min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .values
    )

    # avg_wait_last_24h: [t-24h, t)
    df["avg_wait_last_24h"] = (
        df_indexed.groupby("attractionId")["waitTime"]
        .rolling("24h", closed="left", min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .values
    )

    # rolling_avg_7d: [t-7d, t)
    df["rolling_avg_7d"] = (
        df_indexed.groupby("attractionId")["waitTime"]
        .rolling("7d", closed="left", min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .values
    )

    # 2. Lag Features (Exact time lookups: T-24h, T-1w)
    # Use merge_asof to find the value closest to (timestamp - lag)
    # Group-wise merge_asof is not direct, so we loop or use exact match on shifted time?
    # Approximate match is better for robustness.

    # Helper to merge lagged values
    def merge_lag(source_df, lag_delta, col_name):
        target_time = source_df["timestamp"] - lag_delta
        temp = source_df.copy()
        temp["target_ts"] = target_time
        temp = temp.sort_values("target_ts")

        lookup = source_df[["attractionId", "timestamp", "waitTime"]].sort_values(
            "timestamp"
        )

        return pd.merge_asof(
            temp,
            lookup,
            left_on="target_ts",
            right_on="timestamp",
            by="attractionId",
            tolerance=pd.Timedelta("15min"),  # Allow 15 min slop
            direction="nearest",
            suffixes=("", "_lag"),
        )["waitTime_lag"]

    # Lag 24h
    df["wait_lag_24h"] = merge_lag(df, pd.Timedelta(hours=24), "wait_lag_24h")

    # Lag 1 week
    df["wait_lag_1w"] = merge_lag(df, pd.Timedelta(days=7), "wait_lag_1w")

    # Lag 1 month (30 days)
    df["wait_lag_1m"] = merge_lag(df, pd.Timedelta(days=30), "wait_lag_1m")

    # Map features to legacy names if needed or use new ones
    # We'll keep legacy column names where appropriate to minimize model drift if not retraining everything immediately,
    # but 'avg_wait_same_hour_last_week' essentially maps to 'wait_lag_1w'
    df["avg_wait_same_hour_last_week"] = df["wait_lag_1w"]
    df["avg_wait_same_hour_last_month"] = df["wait_lag_1m"]

    # 3. Fallback Logic (Impute missing short-term history with long-term patterns)
    # If avg_wait_last_1h is NaN (e.g. morning), fill with wait_lag_24h (Yesterday Same Hour)
    df["avg_wait_last_1h"] = df["avg_wait_last_1h"].fillna(df["wait_lag_24h"])

    # If still NaN, fill with Last Week
    df["avg_wait_last_1h"] = df["avg_wait_last_1h"].fillna(df["wait_lag_1w"])

    # Final Fills with Global Means
    hist_cols = [
        "avg_wait_last_24h",
        "avg_wait_last_1h",
        "avg_wait_same_hour_last_week",
        "avg_wait_same_hour_last_month",
        "rolling_avg_7d",
        "trend_7d",
        "volatility_7d",
    ]
    for col in hist_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # Wait time velocity (Momentum)
    # Logic: Change over last 30 mins
    # (Current - Avg 30 mins ago) ?
    # For simplicity, we keep the Diff-based logic but ensure it is robust
    df["wait_time_velocity"] = (
        df.groupby("attractionId")["waitTime"]
        .transform(lambda x: x.diff().rolling(window=6, min_periods=1).mean().shift(1))
        .fillna(0)
    )

    # Trend features (7-day trend slope)
    # Calculate slope of wait times over last 7 days using linear regression
    # Positive = increasing trend, negative = decreasing trend
    df["trend_7d"] = 0.0
    df["volatility_7d"] = 0.0

    # Use groupby().apply() for better performance
    # This is more efficient than iterating over unique attraction IDs
    def calculate_trend_volatility(group):
        """Calculate trend and volatility for a single attraction's data"""
        group = group.sort_values("timestamp")

        if len(group) < 2:
            return pd.DataFrame(
                {"trend_7d": [0.0] * len(group), "volatility_7d": [0.0] * len(group)},
                index=group.index,
            )

        # Get last 7 days of data (168 hours = 7 days * 24 hours)
        if len(group) > 168:
            recent_data = group.tail(168)
        else:
            recent_data = group

        if len(recent_data) < 2:
            return pd.DataFrame(
                {"trend_7d": [0.0] * len(group), "volatility_7d": [0.0] * len(group)},
                index=group.index,
            )

        # Calculate linear trend (slope) using vectorized operations
        x = np.arange(len(recent_data))
        y = recent_data["waitTime"].values

        # Remove NaN values
        mask = ~np.isnan(y)
        if mask.sum() < 2:
            return pd.DataFrame(
                {"trend_7d": [0.0] * len(group), "volatility_7d": [0.0] * len(group)},
                index=group.index,
            )

        x_clean = x[mask]
        y_clean = y[mask]

        # Calculate slope (trend) - simple linear regression
        n = len(x_clean)
        if n > 1:
            slope = (
                n * np.sum(x_clean * y_clean) - np.sum(x_clean) * np.sum(y_clean)
            ) / (n * np.sum(x_clean * x_clean) - np.sum(x_clean) ** 2)
            volatility = np.std(y_clean)
        else:
            slope = 0.0
            volatility = 0.0

        # Dampen with log(1+x) and cap so extreme volatility doesn't dominate importance
        cap_std = get_settings().VOLATILITY_CAP_STD_MINUTES
        volatility_dampened = min(np.log1p(max(0.0, volatility)), np.log1p(cap_std))

        # Return DataFrame with same index as group
        return pd.DataFrame(
            {
                "trend_7d": [slope] * len(group),
                "volatility_7d": [volatility_dampened] * len(group),
            },
            index=group.index,
        )

    # Apply to each attraction group and combine results
    trend_volatility_results = df.groupby("attractionId", group_keys=False).apply(
        calculate_trend_volatility
    )

    # Assign results back to df
    if (
        not trend_volatility_results.empty
        and "trend_7d" in trend_volatility_results.columns
    ):
        df["trend_7d"] = trend_volatility_results["trend_7d"]
        df["volatility_7d"] = trend_volatility_results["volatility_7d"]

    # Clean up temp columns if any
    return df


def add_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add interaction features (combinations of existing features)

    Only adds interactions that are computationally cheap and use existing features.
    No additional database queries required.

    Args:
        df: DataFrame with time and other features

    Returns:
        DataFrame with interaction features added
    """
    # No need for copy - simple column additions

    # hour * is_weekend: Peak times on weekends are different
    # Weekend mornings (9-11) and afternoons (14-16) are typically busier
    if "hour" in df.columns and "is_weekend" in df.columns:
        df["hour_weekend_interaction"] = df["hour"] * df["is_weekend"]

    # temperature * precipitation: Rain + heat = indoor attractions more popular
    if "temperature_avg" in df.columns and "precipitation" in df.columns:
        df["temp_precip_interaction"] = df["temperature_avg"] * df["precipitation"]

    # is_holiday * park_occupancy_pct: Holidays + high occupancy = extreme wait times
    if "is_holiday_primary" in df.columns and "park_occupancy_pct" in df.columns:
        df["holiday_occupancy_interaction"] = (
            df["is_holiday_primary"] * df["park_occupancy_pct"]
        )

    # hour * park_occupancy_pct: Time of day + occupancy = different patterns
    if "hour" in df.columns and "park_occupancy_pct" in df.columns:
        df["hour_occupancy_interaction"] = (
            df["hour"] * df["park_occupancy_pct"] / 100.0
        )  # Normalize

    # Simple hour * is_weekend interaction (complementary to hour_weekend_interaction)
    # This is a simpler binary interaction that may be easier for the model to learn
    if "hour" in df.columns and "is_weekend" in df.columns:
        df["hour_is_weekend"] = df["hour"] * df["is_weekend"]

    return df


def add_park_occupancy_feature(
    df: pd.DataFrame, feature_context: Dict = None
) -> pd.DataFrame:
    """
    Add park-wide occupancy percentage feature

    Occupancy % = (current avg wait / baseline) * 100
    - Inference: API passes occupancy from TypeScript (P50 baseline, headliner).
    - Training: Reconstruct using P50 (median) so train/inference scale matches.

    Args:
        df: DataFrame with parkId column
        feature_context: Optional dict with parkOccupancy data from API

    Returns:
        DataFrame with park_occupancy_pct feature
    """
    # No need for copy - adding simple column

    # Initialize with default (100% = typical)
    df["park_occupancy_pct"] = 100.0

    if feature_context and "parkOccupancy" in feature_context:
        # Inference Mode: Use provided real-time context (P50-based from API)
        park_occupancy_map = feature_context["parkOccupancy"]

        # Map occupancy to each row based on parkId
        for park_id, occupancy_pct in park_occupancy_map.items():
            if occupancy_pct is None:
                continue
            mask = df["parkId"] == park_id
            df.loc[mask, "park_occupancy_pct"] = float(occupancy_pct)

    else:
        # Training Mode: Reconstruct historical occupancy to match inference scale
        # Use P50 (median) so park_occupancy_pct aligns with API (P50 baseline)
        park_baselines = df.groupby("parkId")["waitTime"].quantile(0.50)

        # 2. Calculate Instantaneous Park Average (per timestamp)
        # Group by Park + Timestamp to get the average wait at that moment
        # Transform ensures we get a value aligned with the original index
        current_park_avg = df.groupby(["parkId", "timestamp"])["waitTime"].transform(
            "mean"
        )

        # 3. Calculate Percentage
        # We process per park to divide by the correct baseline
        for park_id in df["parkId"].unique():
            if park_id not in park_baselines:
                continue

            baseline = park_baselines[park_id]
            if baseline == 0:
                baseline = 1  # Avoid div by zero

            mask = df["parkId"] == park_id
            # Occupancy = (Current Avg / Baseline) * 100
            # Clip to reasonable limits (0-150%)
            occupancy = (current_park_avg.loc[mask] / baseline) * 100
            df.loc[mask, "park_occupancy_pct"] = occupancy.clip(0, 150)

    return df


def add_time_since_park_open(
    df: pd.DataFrame, feature_context: Dict = None
) -> pd.DataFrame:
    """
    Add time since park opening feature

    Helps capture morning rush vs evening patterns

    Args:
        df: DataFrame with timestamp and parkId
        feature_context: Optional dict with parkOpeningTimes data

    Returns:
        DataFrame with time_since_park_open_mins feature
    """
    # No need for copy - adding simple column

    # Initialize with 0 (unknown)
    df["time_since_park_open_mins"] = 0.0

    if feature_context and "parkOpeningTimes" in feature_context:
        opening_times_map = feature_context["parkOpeningTimes"]

        for park_id, opening_time_str in opening_times_map.items():
            if not opening_time_str:
                continue

            try:
                # Parse opening time
                opening_time = pd.to_datetime(opening_time_str)

                # Calculate minutes since opening for this park
                mask = df["parkId"] == park_id
                if mask.any():
                    # Use local_timestamp if available, otherwise timestamp
                    time_col = (
                        "local_timestamp"
                        if "local_timestamp" in df.columns
                        else "timestamp"
                    )

                    df.loc[mask, "time_since_park_open_mins"] = (
                        (df.loc[mask, time_col] - opening_time).dt.total_seconds() / 60
                    ).clip(lower=0)  # Negative = park not yet open, clip to 0

            except Exception as e:
                print(f"⚠️  Failed to parse opening time for park {park_id}: {e}")

    return df


def add_downtime_features(
    df: pd.DataFrame, feature_context: Dict = None
) -> pd.DataFrame:
    """
    Add attraction downtime features

    Tracks if attraction was DOWN today and for how long
    Helps capture pent-up demand after reopening

    Args:
        df: DataFrame with attractionId column
        feature_context: Optional dict with downtimeCache data

    Returns:
        DataFrame with had_downtime_today and downtime_minutes_today features
    """
    # No need for copy - adding simple columns

    # Initialize with defaults (no downtime)
    df["had_downtime_today"] = 0
    df["downtime_minutes_today"] = 0.0

    if feature_context and "downtimeCache" in feature_context:
        downtime_map = feature_context["downtimeCache"]

        for attraction_id, downtime_mins in downtime_map.items():
            mask = df["attractionId"] == str(attraction_id)
            if mask.any() and downtime_mins > 0:
                df.loc[mask, "had_downtime_today"] = 1
                df.loc[mask, "downtime_minutes_today"] = float(downtime_mins)

    return df


def add_virtual_queue_feature(
    df: pd.DataFrame, feature_context: Dict = None
) -> pd.DataFrame:
    """
    Add virtual queue (boarding group) feature

    Attractions with virtual queues typically have lower standby waits

    Args:
        df: DataFrame with attractionId column
        feature_context: Optional dict with queueData

    Returns:
        DataFrame with has_virtual_queue feature
    """
    # No need for copy - adding simple column

    # Initialize with default (no virtual queue)
    df["has_virtual_queue"] = 0

    if feature_context and "queueData" in feature_context:
        queue_data_map = feature_context["queueData"]

        for attraction_id, queue_info in queue_data_map.items():
            # Check if this attraction has BOARDING_GROUP queue type
            if queue_info and queue_info.get("queueType") == "BOARDING_GROUP":
                mask = df["attractionId"] == str(attraction_id)
                df.loc[mask, "has_virtual_queue"] = 1

    return df


def add_park_schedule_features(
    df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    cached_schedules_df: pd.DataFrame = None,
    feature_context: Dict = None,
) -> pd.DataFrame:
    """
    Add features related to park operating schedule

    Args:
        df: DataFrame with timestamps and parkId
        start_date: Query start date
        end_date: Query end date
        feature_context: Optional context for live data override
    """
    # No need for copy - merge operations create new DataFrames

    # Use cached schedules if provided, otherwise fetch
    if cached_schedules_df is not None:
        schedules_df = cached_schedules_df.copy()
    else:
        # Determine date range for schedule query using park local timezone
        # Schedules are stored as DATE type (park's local calendar dates)
        # We must query using dates in the park's timezone, not UTC
        # df has 'local_timestamp' column added by convert_to_local_time() earlier
        if "local_timestamp" in df.columns and not df["local_timestamp"].isna().all():
            # Extract date range from LOCAL timestamps (already in park TZ)
            start_date_local = df["local_timestamp"].min().date()
            end_date_local = df["local_timestamp"].max().date()
        else:
            # Fallback to provided dates (should be in park local timezone already)
            # If not, this may cause boundary date issues
            start_date_local = start_date.date()
            end_date_local = end_date.date()

        # Fetch park schedules (using DB helper with local dates)
        schedules_df = fetch_park_schedules(
            datetime.combine(start_date_local, time.min),
            datetime.combine(end_date_local, time.max),
        )

    # Initialize features
    df["is_park_open"] = 0
    df["has_special_event"] = 0  # TICKETED_EVENT or PRIVATE_EVENT
    df["has_extra_hours"] = 0  # EXTRA_HOURS (typically busier)
    df["time_since_park_open_mins"] = 0.0

    # Pre-process schedules for efficient lookup
    if not schedules_df.empty:
        # Pre-process schedule DF for faster lookup
        schedules_df["date"] = pd.to_datetime(schedules_df["date"])
        schedules_df["opening_time"] = pd.to_datetime(schedules_df["opening_time"])
        schedules_df["closing_time"] = pd.to_datetime(schedules_df["closing_time"])

        # Ensure local_timestamp and date_local exist in df
        if "local_timestamp" not in df.columns:
            # Fallback: use timestamp if local_timestamp missing
            df["local_timestamp"] = pd.to_datetime(df["timestamp"])
        if "date_local" not in df.columns:
            df["date_local"] = df["local_timestamp"].dt.date

        # Convert local_timestamp to datetime if needed
        df["local_timestamp"] = pd.to_datetime(df["local_timestamp"])

        # Create lookup structure: (park_id, date) -> schedule info
        # Prefer park-level schedules (attraction_id is null)
        park_schedules = schedules_df[schedules_df["attraction_id"].isnull()].copy()
        if park_schedules.empty:
            park_schedules = schedules_df.copy()

        # Create operating schedules lookup
        operating_schedules = park_schedules[
            park_schedules["schedule_type"] == "OPERATING"
        ].copy()

        if not operating_schedules.empty:
            # Group by park_id and date, take first operating schedule per day
            operating_schedules["date_only"] = operating_schedules["date"].dt.date
            operating_schedules = (
                operating_schedules.groupby(["park_id", "date_only"])
                .first()
                .reset_index()
            )

            # Merge with df to get schedule info
            df["schedule_date"] = df["date_local"]
            df_merged = df.merge(
                operating_schedules[
                    ["park_id", "date_only", "opening_time", "closing_time"]
                ],
                left_on=["parkId", "schedule_date"],
                right_on=["park_id", "date_only"],
                how="left",
                suffixes=("", "_schedule"),
            )

            # Vectorized time comparisons
            mask_valid = (
                df_merged["opening_time"].notna() & df_merged["closing_time"].notna()
            )
            mask_open = (
                mask_valid
                & (df_merged["local_timestamp"] >= df_merged["opening_time"])
                & (df_merged["local_timestamp"] <= df_merged["closing_time"])
            )

            df.loc[mask_open.index, "is_park_open"] = 1

            # Calculate time since open (vectorized)
            time_since_open = (
                df_merged["local_timestamp"] - df_merged["opening_time"]
            ).dt.total_seconds() / 60.0
            df.loc[mask_valid.index, "time_since_park_open_mins"] = (
                time_since_open.clip(lower=0)
            )

        # Check for special events (fully vectorized)
        event_schedules = park_schedules[
            park_schedules["schedule_type"].isin(["TICKETED_EVENT", "PRIVATE_EVENT"])
        ].copy()
        if not event_schedules.empty:
            event_schedules["date_only"] = event_schedules["date"].dt.date
            # Create lookup set for fast membership testing
            event_lookup = set(
                zip(event_schedules["park_id"], event_schedules["date_only"])
            )
            # Vectorized check using merge
            event_df = pd.DataFrame(
                list(event_lookup), columns=["park_id", "date_only"]
            )
            event_df["has_event"] = 1
            # Ensure schedule_date exists for merge
            if "schedule_date" not in df.columns:
                df["schedule_date"] = df["date_local"]
            df_events = df.merge(
                event_df,
                left_on=["parkId", "schedule_date"],
                right_on=["park_id", "date_only"],
                how="left",
            )
            df["has_special_event"] = df_events["has_event"].fillna(0).astype(int)

        # Check for extra hours (fully vectorized)
        extra_hours_schedules = park_schedules[
            park_schedules["schedule_type"] == "EXTRA_HOURS"
        ].copy()
        if not extra_hours_schedules.empty:
            extra_hours_schedules["date_only"] = extra_hours_schedules["date"].dt.date
            # Create lookup set for fast membership testing
            extra_hours_lookup = set(
                zip(
                    extra_hours_schedules["park_id"], extra_hours_schedules["date_only"]
                )
            )
            # Vectorized check using merge
            extra_hours_df = pd.DataFrame(
                list(extra_hours_lookup), columns=["park_id", "date_only"]
            )
            extra_hours_df["has_extra"] = 1
            # Ensure schedule_date exists for merge
            if "schedule_date" not in df.columns:
                df["schedule_date"] = df["date_local"]
            df_extra = df.merge(
                extra_hours_df,
                left_on=["parkId", "schedule_date"],
                right_on=["park_id", "date_only"],
                how="left",
            )
            df["has_extra_hours"] = df_extra["has_extra"].fillna(0).astype(int)

    # Clean up temporary columns (after all merges are done)
    df = df.drop(columns=["schedule_date"], errors="ignore")

    # Correction Logic: Override "Closed" if we have evidence of "Open" (vectorized)
    # 1. Training Override: Target data (waitTime) indicates open
    if "waitTime" in df.columns:
        mask_wait_time = (
            (df["is_park_open"] == 0) & df["waitTime"].notna() & (df["waitTime"] > 0)
        )
        df.loc[mask_wait_time, "is_park_open"] = 1

    # 2. Inference Override: Live Context (currentWaitTimes) indicates open
    if feature_context and "currentWaitTimes" in feature_context:
        cw = feature_context["currentWaitTimes"]
        if "attractionId" in df.columns:
            mask_context = (
                (df["is_park_open"] == 0)
                & df["attractionId"].isin(cw.keys())
                & df["attractionId"].apply(
                    lambda x: cw.get(x, 0) > 0 if x in cw else False
                )
            )
            df.loc[mask_context, "is_park_open"] = 1

    return df


def resample_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resample data to 5-minute intervals to handle delta-compressed storage.

    The database only saves data when:
    1. Wait time changes > 5 minutes
    2. Status changes
    Resample wait time data to consistent hourly intervals.

    This handles:
    - Delta-compressed data (only changes stored)
    - Gaps in data (parks closed, attractions down)
    - Multiple readings per hour (averaged)

    Strategy:
    - Resample to 1-hour buckets (perfect for hourly predictions)
    - Use mean for wait times (represents hourly average)
    - Forward fill up to 2 hours for minor gaps

    OPTIMIZATION: Uses chunked processing to avoid memory explosion
    from holding 2275 DataFrames in memory simultaneously
    """
    if df.empty:
        return df

    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    print(f"   Rows before resampling: {len(df):,}")

    # CRITICAL FIX: Process in chunks to avoid 15GB memory spike
    CHUNK_SIZE = 100  # Process 100 attractions at a time
    resampled_chunks = []

    # Get all groups first (this is fast, just creates tuples)
    groups = list(df.groupby(["attractionId", "parkId"]))
    total_groups = len(groups)

    # Process in chunks
    for chunk_idx in range(0, total_groups, CHUNK_SIZE):
        chunk_groups = groups[chunk_idx : chunk_idx + CHUNK_SIZE]
        chunk_parts = []

        for (attraction_id, park_id), group in chunk_groups:
            # Set sorted timestamp index
            group = group.set_index("timestamp").sort_index()

            # Identify columns to aggregate
            numeric_cols = group.select_dtypes(include=np.number).columns.tolist()
            # Exclude 'waitTime' from the 'first' aggregation if it's in numeric_cols
            # as it will be handled by 'mean'
            if "waitTime" in numeric_cols:
                numeric_cols.remove("waitTime")

            non_numeric_cols = group.select_dtypes(exclude=np.number).columns.tolist()

            # Define aggregation dictionary
            agg_dict = {"waitTime": "mean"}
            for col in numeric_cols:
                agg_dict[col] = (
                    "first"  # Take the first value for other numeric columns
                )
            for col in non_numeric_cols:
                agg_dict[col] = "first"  # Take the first value for non-numeric columns

            # Resample to 30-minute intervals (sweet spot for hourly predictions)
            # Mean: Average wait time within 30 mins
            # Forward fill: Handle gaps up to 2 hours (4 * 30min = 2h)
            resampled = group.resample("30min").agg(agg_dict).ffill(limit=4)

            # Restore identifiers
            resampled["attractionId"] = attraction_id
            resampled["parkId"] = park_id
            resampled = resampled.reset_index()

            chunk_parts.append(resampled)

        # Concat this chunk and add to final list
        if chunk_parts:
            chunk_df = pd.concat(chunk_parts, ignore_index=True)
            resampled_chunks.append(chunk_df)
            del chunk_parts  # Explicit cleanup to free memory

    if not resampled_chunks:
        return pd.DataFrame()

    # Final concat of chunks (much smaller memory footprint)
    df_resampled = pd.concat(resampled_chunks, ignore_index=True)

    # Drop rows that weren't filled (original NaNs or beyond limit)
    df_resampled = df_resampled.dropna(subset=["waitTime", "parkId"])

    print(f"   Rows after resampling: {len(df_resampled):,}")
    return df_resampled


def add_bridge_day_feature(
    df: pd.DataFrame,
    parks_metadata: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    feature_context: Dict = None,
    cached_holidays_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Add bridge day feature

    Bridge Day = Friday after a Thursday holiday OR Monday before a Tuesday holiday.
    Significantly affects crowds (families take long weekends).

    Args:
        df: DataFrame
        parks_metadata: Metadata
        start_date: Start date for holiday fetch
        end_date: End date for holiday fetch
        feature_context: Optional context for real-time override
    """
    # No need for copy - merge operations create new DataFrames
    df["is_bridge_day"] = 0

    # 1. Use Feature Context if available (Inference)
    if feature_context and "isBridgeDay" in feature_context:
        bridge_map = feature_context["isBridgeDay"]
        for park_id, is_bridge in bridge_map.items():
            mask = df["parkId"] == park_id
            df.loc[mask, "is_bridge_day"] = int(is_bridge)
        return df

    # 2. Compare with historical holidays (Training)
    # Use cached holidays if provided, otherwise fetch
    if cached_holidays_df is not None:
        holidays_df = cached_holidays_df.copy()
    else:
        # Be robust: Fetch holidays 5 days before/after range to cover boundary conditions
        search_start = start_date - timedelta(days=5)
        search_end = end_date + timedelta(days=5)

        # Get relevant countries
        all_countries = set()
        for _, row in parks_metadata.iterrows():
            all_countries.add(row["country"])

        holidays_df = fetch_holidays(list(all_countries), search_start, search_end)

    if holidays_df.empty:
        return df

    holidays_df["date"] = pd.to_datetime(holidays_df["date"]).dt.date

    # Lookup: {(country, date): type}
    holiday_lookup = {}
    for _, row in holidays_df.iterrows():
        # Only care about public holidays for bridge days
        if row["holiday_type"] == "public":
            holiday_lookup[(row["country"], row["date"])] = True

    # Vectorized approach: Pre-compute bridge dates per country using holiday_utils
    # Create country mapping for df
    if "country" not in df.columns:
        # Merge parks_metadata to get country
        df_country = df.merge(
            parks_metadata[["park_id", "country"]],
            left_on="parkId",
            right_on="park_id",
            how="left",
        )
        df["country"] = df_country["country"]

    # Pre-compute bridge dates per country using holiday_utils
    # Build holiday map per country for utility function
    bridge_dates = set()
    for country in set(c for c, _ in holiday_lookup.keys()):
        # Build holiday map for this country (date string -> holiday type)
        # Use "public" to indicate public holidays for bridge day logic
        country_holiday_map = {}
        for (c, holiday_date), _ in holiday_lookup.items():
            if c == country:
                # Store as "public" type so bridge day logic works correctly
                country_holiday_map[holiday_date.strftime("%Y-%m-%d")] = "public"

        # Check all dates in the extended range for bridge days
        # Use extended range to catch bridge days at boundaries
        check_start = start_date - timedelta(days=5)
        check_end = end_date + timedelta(days=5)
        current_date = check_start.date()
        while current_date <= check_end.date():
            date_obj = datetime.combine(current_date, time.min)
            is_holiday, holiday_name, is_bridge_day = calculate_holiday_info(
                date_obj, country_holiday_map
            )
            if is_bridge_day:
                bridge_dates.add((country, current_date))
            current_date += timedelta(days=1)

    # Create bridge lookup DataFrame
    if bridge_dates:
        bridge_df = pd.DataFrame(list(bridge_dates), columns=["country", "bridge_date"])
        bridge_df["is_bridge"] = 1

        # Merge with df to find bridge days
        df_bridge = df.merge(
            bridge_df,
            left_on=["country", "date_local"],
            right_on=["country", "bridge_date"],
            how="left",
        )
        df["is_bridge_day"] = df_bridge["is_bridge"].fillna(0).astype(int)
    else:
        df["is_bridge_day"] = 0

    # Clean up temporary country column if we added it
    if "country" not in df.columns or "park_id" in df.columns:
        df = df.drop(columns=["country", "park_id"], errors="ignore")
    return df


def add_park_has_schedule_feature(
    df: pd.DataFrame,
    feature_context: Dict = None,
    cached_schedules_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Add park_has_schedule feature

    Indicates whether a park has schedule data integration.
    Parks WITH schedules have more reliable patterns (ML can trust them more).
    Parks WITHOUT schedules rely on queue data only (more variability).

    This helps ML learn data quality patterns and adjust confidence accordingly.

    Args:
        df: DataFrame with parkId column
        feature_context: Optional dict with parkHasSchedule data from API

    Returns:
        DataFrame with park_has_schedule feature
    """
    # No need for copy - adding simple column

    # Initialize with default (assume schedule exists = better quality)
    df["park_has_schedule"] = 1

    if feature_context and "parkHasSchedule" in feature_context:
        # Inference Mode: Use provided context from Node.js
        park_schedule_map = feature_context["parkHasSchedule"]

        # Map schedule existence to each row based on parkId
        for park_id, has_schedule in park_schedule_map.items():
            mask = df["parkId"] == park_id
            df.loc[mask, "park_has_schedule"] = int(has_schedule)
    else:
        # Training Mode: Check DB for schedule existence
        # Use cached schedules if provided, otherwise query DB
        try:
            park_ids = df["parkId"].unique().tolist()

            if cached_schedules_df is not None and not cached_schedules_df.empty:
                # Use cached schedules to determine which parks have OPERATING schedules
                parks_with_schedule = set(
                    cached_schedules_df[
                        cached_schedules_df["schedule_type"] == "OPERATING"
                    ]["park_id"].unique()
                )
            else:
                # Query DB if no cached data
                from db import get_db
                from sqlalchemy import text

                with get_db() as db:
                    query = text(
                        """
                        SELECT DISTINCT "parkId"::text
                        FROM schedule_entries
                        WHERE "parkId"::text = ANY(:park_ids)
                          AND "scheduleType" = 'OPERATING'
                    """
                    )
                    result = db.execute(query, {"park_ids": park_ids})
                    parks_with_schedule = set(row[0] for row in result.fetchall())

            # Set feature: 1 if has schedule, 0 if not
            for park_id in park_ids:
                mask = df["parkId"] == park_id
                df.loc[mask, "park_has_schedule"] = int(park_id in parks_with_schedule)

        except Exception as e:
            print(f"⚠️  Failed to check schedule existence: {e}")
            # Keep default (1) on error

    return df


def engineer_features(
    df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    feature_context: Dict = None,
) -> pd.DataFrame:
    """
    Complete feature engineering pipeline

    Args:
        df: Raw queue data from fetch_training_data()
        start_date: Training period start
        end_date: Training period end
        feature_context: Optional dict with real-time feature data (Phase 2)

    Returns:
        DataFrame with all features engineered
    """
    import time as time_module

    total_start = time_module.time()

    # 0. Resample to fix delta-compression gaps
    # DISABLED: SQL query (db.py fetch_training_data) now returns HOURLY aggregated data
    # Resampling to 30-min buckets would INCREASE rows (hourly → 30min = 2x explosion!)
    # The SQL aggregation already handles:
    # - Delta compression (PERCENTILE_CONT median)
    # - Gaps (GROUP BY hour)
    # - Multiple readings (aggregation)
    #
    # Only enable if fetching RAW 5-minute data again
    resample_start = time_module.time()
    # df = resample_data(df)  # SKIP: Already aggregated by SQL
    print("   Resampling: SKIPPED (data already hourly-aggregated by SQL)")

    # Fetch park metadata (needed for region-specific weekends & holidays)
    metadata_start = time_module.time()
    parks_metadata = fetch_parks_metadata()
    print(f"   Parks metadata fetch time: {time_module.time() - metadata_start:.2f}s")

    # Cache DB queries to avoid duplicate fetches
    # fetch_holidays() is called in add_holiday_features() and add_bridge_day_feature()
    # fetch_park_schedules() is called in add_park_schedule_features() and add_park_has_schedule_feature()
    cache_start = time_module.time()

    # Get all countries for holiday fetch (need to do this before fetching)
    all_countries = set()
    for _, row in parks_metadata.iterrows():
        all_countries.add(row["country"])
        # Add countries from influencingRegions JSON
        if isinstance(row["influencingRegions"], list):
            for region in row["influencingRegions"]:
                if isinstance(region, dict) and "countryCode" in region:
                    all_countries.add(region["countryCode"])

    # Fetch holidays once (used by add_holiday_features and add_bridge_day_feature)
    # Extend range by 5 days for bridge day calculations
    holidays_search_start = start_date - timedelta(days=5)
    holidays_search_end = end_date + timedelta(days=5)
    cached_holidays_df = fetch_holidays(
        list(all_countries), holidays_search_start, holidays_search_end
    )

    # Fetch schedules once (used by add_park_schedule_features and add_park_has_schedule_feature)
    # Determine date range from local timestamps if available
    if "local_timestamp" in df.columns and not df["local_timestamp"].isna().all():
        start_date_local = df["local_timestamp"].min().date()
        end_date_local = df["local_timestamp"].max().date()
    else:
        start_date_local = start_date.date()
        end_date_local = end_date.date()

    cached_schedules_df = fetch_park_schedules(
        datetime.combine(start_date_local, time.min),
        datetime.combine(end_date_local, time.max),
    )

    print(f"   DB cache fetch time: {time_module.time() - cache_start:.2f}s")

    # Add features (order matters for dependencies) with performance logging
    time_start = time_module.time()
    df = add_time_features(df, parks_metadata)  # Region-specific weekends
    print(f"   Time features: {time_module.time() - time_start:.2f}s")

    weather_start = time_module.time()
    df = add_weather_features(df)
    print(f"   Weather features: {time_module.time() - weather_start:.2f}s")

    holiday_start = time_module.time()
    df = add_holiday_features(
        df, parks_metadata, start_date, end_date, cached_holidays_df
    )
    print(f"   Holiday features: {time_module.time() - holiday_start:.2f}s")

    bridge_start = time_module.time()
    df = add_bridge_day_feature(
        df, parks_metadata, start_date, end_date, feature_context, cached_holidays_df
    )
    print(f"   Bridge day features: {time_module.time() - bridge_start:.2f}s")

    schedule_start = time_module.time()
    df = add_park_schedule_features(df, start_date, end_date, cached_schedules_df)
    print(f"   Schedule features: {time_module.time() - schedule_start:.2f}s")

    # Attraction and Park features (using available data only)
    attraction_start = time_module.time()
    df = add_attraction_type_feature(df)
    df = add_park_attraction_count_feature(df, parks_metadata)
    print(f"   Attraction features: {time_module.time() - attraction_start:.2f}s")

    historical_start = time_module.time()
    df = add_historical_features(df)
    print(f"   Historical features: {time_module.time() - historical_start:.2f}s")

    percentile_start = time_module.time()
    df = add_percentile_features(df)  # Weather extremes
    print(f"   Percentile features: {time_module.time() - percentile_start:.2f}s")

    # Phase 2: Add context features (Training uses internal data, Inference uses feature_context)
    context_start = time_module.time()
    df = add_park_occupancy_feature(df, feature_context)
    df = add_time_since_park_open(df, feature_context)
    df = add_downtime_features(df, feature_context)
    df = add_virtual_queue_feature(df, feature_context)
    df = add_park_has_schedule_feature(df, feature_context, cached_schedules_df)
    print(f"   Context features: {time_module.time() - context_start:.2f}s")

    # Interaction features (must be after all base features are added)
    interaction_start = time_module.time()
    df = add_interaction_features(df)
    print(f"   Interaction features: {time_module.time() - interaction_start:.2f}s")

    total_time = time_module.time() - total_start

    # Performance summary - show slowest features (absolute times)
    resample_time = time_module.time() - resample_start
    metadata_time = time_module.time() - metadata_start
    cache_time = time_module.time() - cache_start
    time_features_time = time_module.time() - time_start
    weather_time = time_module.time() - weather_start
    holiday_time = time_module.time() - holiday_start
    bridge_time = time_module.time() - bridge_start
    schedule_time = time_module.time() - schedule_start
    attraction_time = time_module.time() - attraction_start
    historical_time = time_module.time() - historical_start
    percentile_time = time_module.time() - percentile_start
    context_time = time_module.time() - context_start
    interaction_time = time_module.time() - interaction_start

    # Use absolute times directly (already calculated correctly above)
    feature_times = {
        "Resampling": resample_time,
        "Parks metadata": metadata_time,
        "DB cache fetch": cache_time,
        "Time features": time_features_time,
        "Weather features": weather_time,
        "Holiday features": holiday_time,
        "Bridge day features": bridge_time,
        "Schedule features": schedule_time,
        "Attraction features": attraction_time,
        "Historical features": historical_time,
        "Percentile features": percentile_time,
        "Context features": context_time,
        "Interaction features": interaction_time,
    }

    # Sort by time (slowest first)
    sorted_features = sorted(feature_times.items(), key=lambda x: x[1], reverse=True)

    print(
        f"\n   Total feature engineering time: {total_time:.2f}s ({total_time / 60:.1f} minutes)"
    )
    print("   Slowest features:")
    for name, duration in sorted_features[:5]:  # Show top 5 slowest
        percentage = (duration / total_time) * 100 if total_time > 0 else 0
        print(f"     - {name}: {duration:.2f}s ({percentage:.1f}%)")

    return df


def get_feature_columns() -> List[str]:
    """Return list of feature column names (in order) - Complete feature set with new additions"""
    return [
        # IDs (categorical)
        "parkId",
        "attractionId",
        # Time features (cyclic encoding preserves continuity: 23:00 → 00:00)
        "hour",
        "day_of_week",
        "month",
        "hour_sin",
        "hour_cos",
        "day_of_week_sin",
        "day_of_week_cos",
        "month_sin",
        "month_cos",
        "day_of_year_sin",
        "day_of_year_cos",
        "season",
        "is_weekend",
        "is_peak_season",
        # Weather features (all important for crowd patterns & ride closures)
        "temperature_avg",
        "temperature_deviation",
        "precipitation",
        "precipitation_last_3h",
        "windSpeedMax",  # Explains high ride closures
        "snowfallSum",  # Explains outdoor ride closures
        "weatherCode",
        "is_raining",  # Explicit rain signal
        # Holiday features (cross-border tourism!)
        "is_holiday_primary",
        "is_school_holiday_primary",
        "is_holiday_neighbor_1",
        "is_holiday_neighbor_2",
        "is_holiday_neighbor_3",  # Border parks (Europa-Park → France, etc.)
        "holiday_count_total",
        "school_holiday_count_total",
        "is_school_holiday_any",  # Consolidated school holiday signal (matches inference)
        "is_bridge_day",  # Extended weekends
        # Park schedule features
        "is_park_open",
        "has_special_event",
        "has_extra_hours",
        # Attraction features (NEW - using available data)
        "attraction_type",  # From attractions.attractionType (nullable, defaults to 'UNKNOWN')
        "park_attraction_count",  # Number of attractions in park (indicator of park size)
        # Historical features
        "avg_wait_last_24h",
        "avg_wait_last_1h",
        "avg_wait_same_hour_last_week",
        "avg_wait_same_hour_last_month",
        "rolling_avg_7d",
        "wait_time_velocity",  # Rate of change (momentum)
        "trend_7d",
        "volatility_7d",
        # Percentile-based features
        "is_temp_extreme",
        "is_wind_extreme",  # Extreme wind → ride closures
        # Phase 2: Real-time context features
        "park_occupancy_pct",  # Park-wide crowding (0-200%)
        "time_since_park_open_mins",  # Minutes since opening
        "had_downtime_today",  # Boolean: was DOWN today
        "downtime_minutes_today",  # Total downtime duration
        "has_virtual_queue",  # Boolean: boarding groups active
        "park_has_schedule",
        # Interaction features (NEW - computationally cheap, no extra data needed)
        "hour_weekend_interaction",  # hour * is_weekend (peak times differ on weekends)
        "hour_is_weekend",
        "temp_precip_interaction",  # temperature * precipitation (rain + heat = indoor preference)
        "holiday_occupancy_interaction",  # is_holiday * park_occupancy_pct (holidays + crowds = extreme waits)
        "hour_occupancy_interaction",  # hour * park_occupancy_pct (time + occupancy patterns)
    ]


def get_categorical_features() -> List[str]:
    """Return list of categorical feature names"""
    return ["parkId", "attractionId", "weatherCode", "attraction_type"]
