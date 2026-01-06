"""
Feature engineering for ML model
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List
from db import fetch_holidays, fetch_parks_metadata, fetch_park_schedules
from percentile_features import add_percentile_features
from attraction_features import add_attraction_type_feature, add_park_attraction_count_feature






def convert_to_local_time(df: pd.DataFrame, parks_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Convert UTC timestamps to park-local time.
    Critical for correct hour/day features and date-based lookups (weather/holidays).
    """
    try:
        import pytz
    except ImportError:
        print("⚠️  pytz not installed, using UTC. Install with: pip install pytz")
        return df

    df = df.copy()
    
    # Ensure timestamp is datetime and timezone-aware (as UTC)
    if df['timestamp'].dt.tz is None:
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    else:
        df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

    # Create map {parkId: timezone_str}
    tz_map = parks_metadata.set_index('park_id')['timezone'].to_dict()

    # We need a 'local_timestamp' column for features
    # Vectorized approach per park (much faster than apply)
    df['local_timestamp'] = df['timestamp'] # Default to UTC

    for park_id in df['parkId'].unique():
        tz_name = tz_map.get(park_id)
        if not tz_name:
            continue
            
        try:
            mask = df['parkId'] == park_id
            # Convert to local time
            df.loc[mask, 'local_timestamp'] = df.loc[mask, 'timestamp'].dt.tz_convert(tz_name)
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
    df = df.copy()
    
    # Middle East countries use Friday+Saturday as weekend
    middle_east_countries = ['SA', 'AE', 'BH', 'KW', 'OM', 'QA', 'IL']
    
    # Initialize is_weekend column
    df['is_weekend'] = 0
    
    # For each park, determine weekend based on country
    for park_id in df['parkId'].unique():
        park_info = parks_metadata[parks_metadata['park_id'] == park_id]
        park_mask = df['parkId'] == park_id
        
        if not park_info.empty:
            country = park_info.iloc[0]['country']
            
            if country in middle_east_countries:
                # Middle East: Friday (4) + Saturday (5)
                # dayofweek: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday, 5=Saturday, 6=Sunday
                df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([4, 5]).astype(int)
            else:
                # Western: Saturday (5) + Sunday (6)
                df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([5, 6]).astype(int)
        else:
            # Default: Western weekend (Saturday + Sunday)
            df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([5, 6]).astype(int)
    
    return df


def add_time_features(df: pd.DataFrame, parks_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Add time-based features using LOCAL time.
    """
    # 1. Convert to local time first
    df = convert_to_local_time(df, parks_metadata)

    # 2. Extract features from LOCAL time
    df['hour'] = df['local_timestamp'].dt.hour
    df['month'] = df['local_timestamp'].dt.month
    df['day_of_week'] = df['local_timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday
    
    # Cyclic encoding (essential for tree models to understand continuity)
    # e.g., hour=23 and hour=0 are close, not 23 units apart
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['day_of_week_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_of_week_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    
    # NEW: Day of year (1-365/366) for finer seasonal trends
    df['day_of_year'] = df['local_timestamp'].dt.dayofyear
    df['day_of_year_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365.25)
    df['day_of_year_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365.25)
    
    # Season (derived from month)
    def get_season(month):
        if month in [12, 1, 2]: return 0  # Winter
        if month in [3, 4, 5]: return 1  # Spring
        if month in [6, 7, 8]: return 2  # Summer
        return 3                         # Fall
        
    df['season'] = df['month'].apply(get_season)
    
    # NEW: Peak season indicator (summer months + December holidays)
    # Peak seasons: June-August (summer), December (holidays)
    df['is_peak_season'] = ((df['month'] >= 6) & (df['month'] <= 8)) | (df['month'] == 12)
    df['is_peak_season'] = df['is_peak_season'].astype(int)

    # 3. Use LOCAL date for further lookups (holidays, weather)
    # This prevents using yesterday's weather for today's morning (due to UTC lag)
    df['date_local'] = df['local_timestamp'].dt.date

    # Region-specific weekend detection
    # Use centralized function to avoid code duplication
    df = add_weekend_feature(df, parks_metadata)

    return df


def add_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """Process weather features"""
    df = df.copy()

    # Fill missing weather - numeric features with mean, categorical with mode
    numeric_weather_cols = ['temperatureMax', 'temperatureMin', 'precipitation', 'windSpeedMax', 'snowfallSum']

    for col in numeric_weather_cols:
        if col in df.columns:
            # Fill with park-specific mean, then global mean
            df[col] = df.groupby('parkId')[col].transform(
                lambda x: x.fillna(x.mean())
            ).fillna(df[col].mean())

    # Fill weatherCode (categorical) with mode, then convert to int
    if 'weatherCode' in df.columns:
        # Fill with park-specific mode (most common value), then global mode
        df['weatherCode'] = df.groupby('parkId')['weatherCode'].transform(
            lambda x: x.fillna(x.mode()[0] if len(x.mode()) > 0 else 0)
        )
        # Fill any remaining NaN with global mode
        if df['weatherCode'].isna().any():
            global_mode = df['weatherCode'].mode()
            df['weatherCode'] = df['weatherCode'].fillna(
                global_mode[0] if len(global_mode) > 0 else 0
            )
        # Convert to integer (CatBoost requires int/string for categorical features)
        df['weatherCode'] = df['weatherCode'].astype(int)

    # Temperature average
    if 'temperatureMax' in df.columns and 'temperatureMin' in df.columns:
        df['temperature_avg'] = (df['temperatureMax'] + df['temperatureMin']) / 2

    # Binary rain indicator (explicit signal, valuable for ML)
    if 'precipitation' in df.columns:
        df['is_raining'] = (df['precipitation'] > 0).astype(int)
        
        # NEW: Precipitation last 3 hours (cumulative effect)
        # For training: use rolling window on historical data
        # For inference: will be provided via feature_context
        if 'timestamp' in df.columns:
            df_sorted = df.sort_values('timestamp').set_index('timestamp')
            df['precipitation_last_3h'] = df_sorted.groupby('parkId')['precipitation'] \
                .rolling('3h', closed='left', min_periods=1).sum() \
                .reset_index(level=0, drop=True).values
            df['precipitation_last_3h'] = df['precipitation_last_3h'].fillna(0)
        else:
            df['precipitation_last_3h'] = 0

    # NEW: Temperature deviation (current vs. monthly average)
    # Helps model understand if weather is unusually hot/cold
    if 'temperature_avg' in df.columns and 'month' in df.columns:
        # Calculate monthly average temperature per park
        monthly_avg = df.groupby(['parkId', 'month'])['temperature_avg'].transform('mean')
        df['temperature_deviation'] = df['temperature_avg'] - monthly_avg
        df['temperature_deviation'] = df['temperature_deviation'].fillna(0)
    else:
        df['temperature_deviation'] = 0

    return df



def add_holiday_features(
    df: pd.DataFrame,
    parks_metadata: pd.DataFrame,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame:
    """
    Add multi-country and regional holiday features

    For each park, checks holidays in:
    - Primary country/region (park's location)
    - Influencing regions (from influencingRegions JSON)
    """
    df = df.copy()

    # Initialize holiday columns
    df['is_holiday_primary'] = 0
    df['is_school_holiday_primary'] = 0
    df['is_holiday_neighbor_1'] = 0
    df['is_holiday_neighbor_2'] = 0
    df['is_holiday_neighbor_3'] = 0
    df['holiday_count_total'] = 0
    df['school_holiday_count_total'] = 0
    df['is_school_holiday_any'] = 0 # Consolidated signal (matches Node.js inference feature)

    # Merge park metadata
    df = df.merge(
        parks_metadata[['park_id', 'country', 'region_code', 'influencingRegions']],
        left_on='parkId',
        right_on='park_id',
        how='left'
    )

    # Get unique countries to fetch holidays for
    all_countries = set()
    for _, row in parks_metadata.iterrows():
        all_countries.add(row['country'])
        
        # Add countries from influencingRegions JSON
        if isinstance(row['influencingRegions'], list):
            for region in row['influencingRegions']:
                if isinstance(region, dict) and 'countryCode' in region:
                    all_countries.add(region['countryCode'])
        


    # Fetch all holidays
    holidays_df = fetch_holidays(list(all_countries), start_date, end_date)

    # Convert date column to datetime
    if not holidays_df.empty:
        holidays_df['date'] = pd.to_datetime(holidays_df['date']).dt.date

    # Create optimized holiday lookup
    # Key: (country, region, date) -> holiday_type
    # Also separate lookup for Nationwide holidays: (country, date) -> type
    holiday_map_regional = {}
    holiday_map_national = {}
    
    if not holidays_df.empty:
        for _, row in holidays_df.iterrows():
            h_date = row['date']
            h_country = row['country']
            h_type = row['holiday_type']
            h_region = row['region']
            is_nationwide = row['is_nationwide']

            # Store in National Map if nationwide
            if is_nationwide:
                holiday_map_national[(h_country, h_date)] = h_type
            
            # Store in Regional Map if region specific
            if h_region:
                 holiday_map_regional[(h_country, h_region, h_date)] = h_type

    def check_holidays(row):
        # Use LOCAL date for holiday lookup
        if pd.isna(row['date_local']):
             return row
             
        date = row['date_local']
        primary_country = row['country']
        primary_region = row['region_code'] # e.g. "DE-BW"
        
        # 1. Primary Location Holiday Check
        primary_type = None
        
        # Check specific region match first
        if primary_region:
            primary_type = holiday_map_regional.get((primary_country, primary_region, date))
        
        # If no regional match, check national (fallback/additive)
        if not primary_type:
            primary_type = holiday_map_national.get((primary_country, date))

        # Assign primary features
        row['is_holiday_primary'] = int(primary_type == 'public')
        row['is_school_holiday_primary'] = int(primary_type == 'school')

        # 2. Influencing Regions Check
        # Parse influencing regions from JSON or fallback
        influencing_list = []
        
        raw_regions = row['influencingRegions']
        if isinstance(raw_regions, list) and len(raw_regions) > 0:
            # Use new granular config
            influencing_list = raw_regions # list of {countryCode, regionCode}


        # Check first 3 neighbors
        neighbor_flags = []
        neighbor_school_flags = []
        
        for region_def in influencing_list[:3]:
            try:
                # Handle both object (new) and string (bad data) cases safely
                if not isinstance(region_def, dict): continue
                
                n_country = region_def.get('countryCode')
                n_region = region_def.get('regionCode') # Can be None
                
                n_type = None
                
                # Check region specific
                if n_region:
                    n_type = holiday_map_regional.get((n_country, n_region, date))
                
                # Check national if no regional hit or if region is None (nationwide influence)
                if not n_type:
                    n_type = holiday_map_national.get((n_country, date))
                
                neighbor_flags.append(int(n_type == 'public'))
                neighbor_school_flags.append(int(n_type == 'school'))
                
            except Exception:
                # Safely skip bad entries
                neighbor_flags.append(0)
                neighbor_school_flags.append(0)

        # Assign neighbor features
        if len(neighbor_flags) > 0: row['is_holiday_neighbor_1'] = neighbor_flags[0]
        if len(neighbor_flags) > 1: row['is_holiday_neighbor_2'] = neighbor_flags[1]
        if len(neighbor_flags) > 2: row['is_holiday_neighbor_3'] = neighbor_flags[2]

        # Totals
        row['holiday_count_total'] = row['is_holiday_primary'] + sum(neighbor_flags)
        
        # "Any School Holiday" Logic (Critical for ML parity)
        # Matches Node.js isSchoolHolidayInInfluenceZone: true if ANY influenced region has school holiday
        # OR if local region has school holiday
        has_local_school = (row['is_school_holiday_primary'] == 1)
        has_neighbor_school = (sum(neighbor_school_flags) > 0)
        
        row['school_holiday_count_total'] = row['is_school_holiday_primary'] + sum(neighbor_school_flags)
        row['is_school_holiday_any'] = int(has_local_school or has_neighbor_school)

        return row

    df = df.apply(check_holidays, axis=1)

    # Drop temporary merge columns
    drop_cols = ['park_id', 'country', 'region_code', 'influencingRegions']
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')

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
    df = df.copy()
    if 'timestamp' not in df.columns:
        return df

    # distinct sort for merge_asof
    df = df.sort_values('timestamp')
    
    # 1. Time-based Rolling Features
    # Must use index for time-based rolling
    df_indexed = df.set_index('timestamp').sort_index()
    
    # avg_wait_last_1h: [t-1h, t)
    # closed='left' excludes current timestamp, preventing data leakage
    df['avg_wait_last_1h'] = df_indexed.groupby('attractionId')['waitTime'] \
        .rolling('1h', closed='left', min_periods=1).mean() \
        .reset_index(level=0, drop=True).values

    # avg_wait_last_24h: [t-24h, t)
    df['avg_wait_last_24h'] = df_indexed.groupby('attractionId')['waitTime'] \
        .rolling('24h', closed='left', min_periods=1).mean() \
        .reset_index(level=0, drop=True).values

    # rolling_avg_7d: [t-7d, t)
    df['rolling_avg_7d'] = df_indexed.groupby('attractionId')['waitTime'] \
        .rolling('7d', closed='left', min_periods=1).mean() \
        .reset_index(level=0, drop=True).values

    # 2. Lag Features (Exact time lookups: T-24h, T-1w)
    # Use merge_asof to find the value closest to (timestamp - lag)
    # Group-wise merge_asof is not direct, so we loop or use exact match on shifted time?
    # Approximate match is better for robustness.
    
    # Helper to merge lagged values
    def merge_lag(source_df, lag_delta, col_name):
        target_time = source_df['timestamp'] - lag_delta
        temp = source_df.copy()
        temp['target_ts'] = target_time
        temp = temp.sort_values('target_ts')
        
        lookup = source_df[['attractionId', 'timestamp', 'waitTime']].sort_values('timestamp')
        
        return pd.merge_asof(
            temp,
            lookup,
            left_on='target_ts',
            right_on='timestamp',
            by='attractionId',
            tolerance=pd.Timedelta('15min'), # Allow 15 min slop
            direction='nearest',
            suffixes=('', '_lag')
        )['waitTime_lag']

    # Lag 24h
    df['wait_lag_24h'] = merge_lag(df, pd.Timedelta(hours=24), 'wait_lag_24h')
    
    # Lag 1 week
    df['wait_lag_1w'] = merge_lag(df, pd.Timedelta(days=7), 'wait_lag_1w')
    
    # Lag 1 month (30 days) - NEW: Extended temporal feature
    df['wait_lag_1m'] = merge_lag(df, pd.Timedelta(days=30), 'wait_lag_1m')
    
    # Map features to legacy names if needed or use new ones
    # We'll keep legacy column names where appropriate to minimize model drift if not retraining everything immediately, 
    # but 'avg_wait_same_hour_last_week' essentially maps to 'wait_lag_1w'
    df['avg_wait_same_hour_last_week'] = df['wait_lag_1w']
    df['avg_wait_same_hour_last_month'] = df['wait_lag_1m']  # NEW: Monthly trend

    # 3. Fallback Logic (Impute missing short-term history with long-term patterns)
    # If avg_wait_last_1h is NaN (e.g. morning), fill with wait_lag_24h (Yesterday Same Hour)
    df['avg_wait_last_1h'] = df['avg_wait_last_1h'].fillna(df['wait_lag_24h'])
    
    # If still NaN, fill with Last Week
    df['avg_wait_last_1h'] = df['avg_wait_last_1h'].fillna(df['wait_lag_1w'])

    # Final Fills with Global Means
    hist_cols = ['avg_wait_last_24h', 'avg_wait_last_1h', 'avg_wait_same_hour_last_week', 
                 'avg_wait_same_hour_last_month', 'rolling_avg_7d', 'trend_7d', 'volatility_7d']
    for col in hist_cols:
         if col in df.columns:
             df[col] = df[col].fillna(0)

    # Wait time velocity (Momentum)
    # Logic: Change over last 30 mins
    # (Current - Avg 30 mins ago) ? 
    # For simplicity, we keep the Diff-based logic but ensure it is robust
    df['wait_time_velocity'] = df.groupby('attractionId')['waitTime'].transform(
        lambda x: x.diff().rolling(window=6, min_periods=1).mean().shift(1)
    ).fillna(0)
    
    # Trend features (NEW: 7-day trend slope)
    # Calculate slope of wait times over last 7 days using linear regression
    # Positive = increasing trend, negative = decreasing trend
    df['trend_7d'] = 0.0
    df['volatility_7d'] = 0.0  # NEW: 7-day volatility (standard deviation)
    
    for attraction_id in df['attractionId'].unique():
        mask = df['attractionId'] == attraction_id
        attraction_data = df[mask].sort_values('timestamp')
        
        if len(attraction_data) >= 2:
            # Get last 7 days of data
            if len(attraction_data) > 168:  # More than 7 days of hourly data
                recent_data = attraction_data.tail(168)  # Last 7 days (24h * 7)
            else:
                recent_data = attraction_data
            
            if len(recent_data) >= 2:
                # Calculate linear trend (slope)
                x = np.arange(len(recent_data))
                y = recent_data['waitTime'].values
                
                # Simple linear regression: slope = (n*sum(xy) - sum(x)*sum(y)) / (n*sum(x²) - sum(x)²)
                n = len(x)
                if n > 1:
                    slope = (n * np.sum(x * y) - np.sum(x) * np.sum(y)) / (n * np.sum(x * x) - np.sum(x) ** 2)
                    df.loc[mask, 'trend_7d'] = slope
                    
                    # Calculate volatility (standard deviation) - NEW
                    volatility = np.std(y)
                    df.loc[mask, 'volatility_7d'] = volatility
                else:
                    df.loc[mask, 'trend_7d'] = 0.0
                    df.loc[mask, 'volatility_7d'] = 0.0
            else:
                df.loc[mask, 'trend_7d'] = 0.0
                df.loc[mask, 'volatility_7d'] = 0.0
        else:
            df.loc[mask, 'trend_7d'] = 0.0
            df.loc[mask, 'volatility_7d'] = 0.0

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
    df = df.copy()
    
    # hour * is_weekend: Peak times on weekends are different
    # Weekend mornings (9-11) and afternoons (14-16) are typically busier
    if 'hour' in df.columns and 'is_weekend' in df.columns:
        df['hour_weekend_interaction'] = df['hour'] * df['is_weekend']
    
    # temperature * precipitation: Rain + heat = indoor attractions more popular
    if 'temperature_avg' in df.columns and 'precipitation' in df.columns:
        df['temp_precip_interaction'] = df['temperature_avg'] * df['precipitation']
    
    # is_holiday * park_occupancy_pct: Holidays + high occupancy = extreme wait times
    if 'is_holiday_primary' in df.columns and 'park_occupancy_pct' in df.columns:
        df['holiday_occupancy_interaction'] = df['is_holiday_primary'] * df['park_occupancy_pct']
    
    # hour * park_occupancy_pct: Time of day + occupancy = different patterns
    if 'hour' in df.columns and 'park_occupancy_pct' in df.columns:
        df['hour_occupancy_interaction'] = df['hour'] * df['park_occupancy_pct'] / 100.0  # Normalize
    
    # NEW: Simple hour * is_weekend interaction (complementary to hour_weekend_interaction)
    # This is a simpler binary interaction that may be easier for the model to learn
    if 'hour' in df.columns and 'is_weekend' in df.columns:
        df['hour_is_weekend'] = df['hour'] * df['is_weekend']
    
    return df


def add_park_occupancy_feature(
    df: pd.DataFrame,
    feature_context: Dict = None
) -> pd.DataFrame:
    """
    Add park-wide occupancy percentage feature
    
    Occupancy % = (current avg wait / P90 baseline) * 100
    Helps ML learn park-wide crowd patterns
    
    Args:
        df: DataFrame with parkId column
        feature_context: Optional dict with parkOccupancy data from API
        
    Returns:
        DataFrame with park_occupancy_pct feature
    """
    df = df.copy()
    
    # Initialize with default (100% = typical)
    df['park_occupancy_pct'] = 100.0
    
    if feature_context and 'parkOccupancy' in feature_context:
        # Inference Mode: Use provided real-time context
        park_occupancy_map = feature_context['parkOccupancy']
        
        # Map occupancy to each row based on parkId
        for park_id, occupancy_pct in park_occupancy_map.items():
            mask = df['parkId'] == park_id
            df.loc[mask, 'park_occupancy_pct'] = float(occupancy_pct)
            
    else:
        # Training Mode: Reconstruct historical occupancy from the data itself
        # 1. Calculate Baseline (P90 of wait times per park)
        # Using 90th percentile as a proxy for "Full Capacity"
        park_baselines = df.groupby('parkId')['waitTime'].quantile(0.90)
        
        # 2. Calculate Instantaneous Park Average (per timestamp)
        # Group by Park + Timestamp to get the average wait at that moment
        # Transform ensures we get a value aligned with the original index
        current_park_avg = df.groupby(['parkId', 'timestamp'])['waitTime'].transform('mean')
        
        # 3. Calculate Percentage
        # We process per park to divide by the correct baseline
        for park_id in df['parkId'].unique():
            if park_id not in park_baselines: continue
            
            baseline = park_baselines[park_id]
            if baseline == 0: baseline = 1 # Avoid div by zero
            
            mask = df['parkId'] == park_id
            # Occupancy = (Current Avg / Baseline) * 100
            # Clip to reasonable limits (0-150%)
            occupancy = (current_park_avg.loc[mask] / baseline) * 100
            df.loc[mask, 'park_occupancy_pct'] = occupancy.clip(0, 150)
    
    return df


def add_time_since_park_open(
    df: pd.DataFrame,
    feature_context: Dict = None
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
    df = df.copy()
    
    # Initialize with 0 (unknown)
    df['time_since_park_open_mins'] = 0.0
    
    if feature_context and 'parkOpeningTimes' in feature_context:
        opening_times_map = feature_context['parkOpeningTimes']
        
        for park_id, opening_time_str in opening_times_map.items():
            if not opening_time_str:
                continue
                
            try:
                # Parse opening time
                opening_time = pd.to_datetime(opening_time_str)
                
                # Calculate minutes since opening for this park
                mask = df['parkId'] == park_id
                if mask.any():
                    # Use local_timestamp if available, otherwise timestamp
                    time_col = 'local_timestamp' if 'local_timestamp' in df.columns else 'timestamp'
                    
                    df.loc[mask, 'time_since_park_open_mins'] = (
                        (df.loc[mask, time_col] - opening_time).dt.total_seconds() / 60
                    ).clip(lower=0)  # Negative = park not yet open, clip to 0
                    
            except Exception as e:
                print(f"⚠️  Failed to parse opening time for park {park_id}: {e}")
    
    return df


def add_downtime_features(
    df: pd.DataFrame,
    feature_context: Dict = None
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
    df = df.copy()
    
    # Initialize with defaults (no downtime)
    df['had_downtime_today'] = 0
    df['downtime_minutes_today'] = 0.0
    
    if feature_context and 'downtimeCache' in feature_context:
        downtime_map = feature_context['downtimeCache']
        
        for attraction_id, downtime_mins in downtime_map.items():
            mask = df['attractionId'] == str(attraction_id)
            if mask.any() and downtime_mins > 0:
                df.loc[mask, 'had_downtime_today'] = 1
                df.loc[mask, 'downtime_minutes_today'] = float(downtime_mins)
    
    return df


def add_virtual_queue_feature(
    df: pd.DataFrame,
    feature_context: Dict = None
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
    df = df.copy()
    
    # Initialize with default (no virtual queue)
    df['has_virtual_queue'] = 0
    
    if feature_context and 'queueData' in feature_context:
        queue_data_map = feature_context['queueData']
        
        for attraction_id, queue_info in queue_data_map.items():
            # Check if this attraction has BOARDING_GROUP queue type
            if queue_info and queue_info.get('queueType') == 'BOARDING_GROUP':
                mask = df['attractionId'] == str(attraction_id)
                df.loc[mask, 'has_virtual_queue'] = 1
    
    return df


def add_park_schedule_features(
    df: pd.DataFrame, 
    start_date: datetime, 
    end_date: datetime,
    feature_context: Dict = None 
) -> pd.DataFrame:
    """
    Add features related to park operating schedule
    
    Args:
        df: DataFrame with timestamps and parkId
        start_date: Query start date
        end_date: Query end date
        feature_context: Optional context for live data override
    """
    df = df.copy()
    
    # Determine date range for schedule query using park local timezone
    # Schedules are stored as DATE type (park's local calendar dates)
    # We must query using dates in the park's timezone, not UTC
    # df has 'local_timestamp' column added by convert_to_local_time() earlier
    if 'local_timestamp' in df.columns and not df['local_timestamp'].isna().all():
        # Extract date range from LOCAL timestamps (already in park TZ)
        start_date_local = df['local_timestamp'].min().date()
        end_date_local = df['local_timestamp'].max().date()
    else:
        # Fallback to provided dates (should be in park local timezone already)
        # If not, this may cause boundary date issues
        start_date_local = start_date.date()
        end_date_local = end_date.date()
    
    # Fetch park schedules (using DB helper with local dates)
    schedules_df = fetch_park_schedules(
        datetime.datetime.combine(start_date_local, datetime.time.min),
        datetime.datetime.combine(end_date_local, datetime.time.max)
    )
    
    # Initialize features
    df['is_park_open'] = 0
    df['has_special_event'] = 0  # TICKETED_EVENT or PRIVATE_EVENT
    df['has_extra_hours'] = 0     # EXTRA_HOURS (typically busier)
    df['time_since_park_open_mins'] = 0.0

    if not schedules_df.empty:
        # Pre-process schedule DF for faster lookup
        schedules_df['date'] = pd.to_datetime(schedules_df['date'])
        schedules_df['opening_time'] = pd.to_datetime(schedules_df['opening_time'])
        schedules_df['closing_time'] = pd.to_datetime(schedules_df['closing_time'])

    # For each row, check schedule
    for idx, row in df.iterrows():
        park_id = row['parkId']
        # Use LOCAL timestamp and date for schedule comparison
        # (Schedules in DB are stored as local dates/times and fetch_park_schedules preserves this)
        # Note: 'local_timestamp' is added by engineer_features -> convert_to_local_time
        if 'local_timestamp' in row:
             timestamp = row['local_timestamp']
             date = row['date_local']
        else:
             # Fallback if local_timestamp missing (shouldn't happen in pipeline)
             timestamp = row['timestamp']
             date = timestamp.date()

        schedule_found = False

        if not schedules_df.empty:
            # Find all schedules for this park on this date
            # Filter for Park-Level schedules (attraction_id is Null/None)
            # Nan filtering in pandas: isnull()
            schedules = schedules_df[
                (schedules_df['park_id'] == park_id) & 
                (schedules_df['date'].dt.date == date)
            ]
            
            # Use park-level schedules if available, otherwise fallback to any schedule
            park_schedules = schedules[schedules['attraction_id'].isnull()]
            if not park_schedules.empty:
                schedules = park_schedules

            if not schedules.empty:
                schedule_found = True
                
                # Check if park is open (OPERATING schedule type)
                operating = schedules[schedules['schedule_type'] == 'OPERATING']
                if not operating.empty:
                    # Use first operating schedule entry
                    op_sched = operating.iloc[0]
                    opening = op_sched['opening_time']
                    closing = op_sched['closing_time']

                    # Compare local timestamp with opening/closing
                    # Ensure comparisons are compatible (both datetime objects)
                    # Convert DB datetime64 objects to pandas Timestamp for comparison
                    
                    # Safety check for NaT
                    if pd.notna(opening) and pd.notna(closing):
                        # Convert to pandas Timestamp to handle datetime64 from DB
                        opening = pd.Timestamp(opening)
                        closing = pd.Timestamp(closing)
                        timestamp_ts = pd.Timestamp(timestamp)
                        
                        if opening <= timestamp_ts <= closing:
                            df.at[idx, 'is_park_open'] = 1
                        
                        # Calculate time since open (minutes)
                        mins_since = (timestamp_ts - opening).total_seconds() / 60.0
                        df.at[idx, 'time_since_park_open_mins'] = max(0, mins_since)

                # Check for special events
                if any(schedules['schedule_type'].isin(['TICKETED_EVENT', 'PRIVATE_EVENT'])):
                    df.at[idx, 'has_special_event'] = 1
                    
                if any(schedules['schedule_type'] == 'EXTRA_HOURS'):
                    df.at[idx, 'has_extra_hours'] = 1

        # Correction Logic: Override "Closed" if we have evidence of "Open"
        if df.at[idx, 'is_park_open'] == 0:
            # 1. Training Override: Target data (waitTime) indicates open
            if 'waitTime' in df.columns:
                 wt = row['waitTime']
                 if pd.notna(wt) and wt > 0:
                     df.at[idx, 'is_park_open'] = 1
            
            # 2. Inference Override: Live Context (currentWaitTimes) indicates open
            elif feature_context and 'currentWaitTimes' in feature_context:
                # Check if this attraction has wait times
                if 'attractionId' in row:
                    attr_id = row['attractionId']
                    cw = feature_context['currentWaitTimes'] # dict[attrId, waitTime]
                    if attr_id in cw:
                        curr_wt = cw[attr_id]
                        if curr_wt is not None and curr_wt > 0:
                            df.at[idx, 'is_park_open'] = 1
                            # Also hints that Park is open, but we process per row

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
    """
    if df.empty:
        return df

    # Ensure timestamp is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    print(f"   Rows before resampling: {len(df):,}")

    resampled_parts = []
    
    for (attraction_id, park_id), group in df.groupby(['attractionId', 'parkId']):
        # Set sorted timestamp index
        group = group.set_index('timestamp').sort_index()
        
        # Identify columns to aggregate
        numeric_cols = group.select_dtypes(include=np.number).columns.tolist()
        # Exclude 'waitTime' from the 'first' aggregation if it's in numeric_cols
        # as it will be handled by 'mean'
        if 'waitTime' in numeric_cols:
            numeric_cols.remove('waitTime')
        
        non_numeric_cols = group.select_dtypes(exclude=np.number).columns.tolist()
        
        # Define aggregation dictionary
        agg_dict = {'waitTime': 'mean'}
        for col in numeric_cols:
            agg_dict[col] = 'first' # Take the first value for other numeric columns
        for col in non_numeric_cols:
            agg_dict[col] = 'first' # Take the first value for non-numeric columns
            
        # Resample to 30-minute intervals (sweet spot for hourly predictions)
        # Mean: Average wait time within 30 mins
        # Forward fill: Handle gaps up to 2 hours (4 * 30min = 2h)
        resampled = group.resample('30min').agg(agg_dict).ffill(limit=4)
        
        # Restore identifiers
        resampled['attractionId'] = attraction_id
        resampled['parkId'] = park_id
        resampled = resampled.reset_index()
        
        resampled_parts.append(resampled)
    
    if not resampled_parts:
        return pd.DataFrame()
        
    df_resampled = pd.concat(resampled_parts, ignore_index=True)
    
    # Drop rows that weren't filled (original NaNs or beyond limit)
    df_resampled = df_resampled.dropna(subset=['waitTime', 'parkId'])
    
    # Restore columns created by ffill (parkId etc should be preserved)
    
    print(f"   Rows after resampling: {len(df_resampled):,}")
    return df_resampled



def add_bridge_day_feature(
    df: pd.DataFrame,
    parks_metadata: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    feature_context: Dict = None
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
    df = df.copy()
    df['is_bridge_day'] = 0
    
    # 1. Use Feature Context if available (Inference)
    if feature_context and 'isBridgeDay' in feature_context:
        bridge_map = feature_context['isBridgeDay']
        for park_id, is_bridge in bridge_map.items():
            mask = df['parkId'] == park_id
            df.loc[mask, 'is_bridge_day'] = int(is_bridge)
        return df

    # 2. Compare with historical holidays (Training)
    # Be robust: Fetch holidays 5 days before/after range to cover boundary conditions
    search_start = start_date - timedelta(days=5)
    search_end = end_date + timedelta(days=5)
    
    # Get relevant countries
    all_countries = set()
    for _, row in parks_metadata.iterrows():
        all_countries.add(row['country'])
    
    holidays_df = fetch_holidays(list(all_countries), search_start, search_end)
    
    if holidays_df.empty:
        return df
        
    holidays_df['date'] = pd.to_datetime(holidays_df['date']).dt.date
    
    # Lookup: {(country, date): type}
    holiday_lookup = {}
    for _, row in holidays_df.iterrows():
        # Only care about public holidays for bridge days
        if row['holiday_type'] == 'public':
            holiday_lookup[(row['country'], row['date'])] = True

    # Vectorized check is hard because parks have different countries
    # Use iteration for correctness
    
    # Optimize: Pre-compute bridge dates per country? 
    # Or just iterate rows. Iteration is acceptable for the volume we have.
    
    def check_bridge(row):
        # Local date
        date = row['date_local']
        day_of_week = row['day_of_week'] # 0=Mon, 4=Fri
        
        # Get country
        park_info = parks_metadata[parks_metadata['park_id'] == row['parkId']]
        if park_info.empty:
            return 0
        country = park_info.iloc[0]['country']
        
        # Check Friday (4) -> Thursday (date - 1)
        if day_of_week == 4:
            prev_day = date - timedelta(days=1)
            if holiday_lookup.get((country, prev_day)):
                return 1
                
        # Check Monday (0) -> Tuesday (date + 1)
        elif day_of_week == 0:
            next_day = date + timedelta(days=1)
            if holiday_lookup.get((country, next_day)):
                return 1
                
        return 0

    df['is_bridge_day'] = df.apply(check_bridge, axis=1)
    return df


def add_park_has_schedule_feature(
    df: pd.DataFrame,
    feature_context: Dict = None
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
    df = df.copy()
    
    # Initialize with default (assume schedule exists = better quality)
    df['park_has_schedule'] = 1
    
    if feature_context and 'parkHasSchedule' in feature_context:
        # Inference Mode: Use provided context from Node.js
        park_schedule_map = feature_context['parkHasSchedule']
        
        # Map schedule existence to each row based on parkId
        for park_id, has_schedule in park_schedule_map.items():
            mask = df['parkId'] == park_id
            df.loc[mask, 'park_has_schedule'] = int(has_schedule)
    else:
        # Training Mode: Check DB for schedule existence
        # Query to check which parks have OPERATING schedules
        from db import get_db
        from sqlalchemy import text
        
        try:
            park_ids = df['parkId'].unique().tolist()
            
            with get_db() as db:
                query = text("""
                    SELECT DISTINCT "parkId"::text
                    FROM schedule_entries
                    WHERE "parkId"::text = ANY(:park_ids)
                      AND "scheduleType" = 'OPERATING'
                """)
                result = db.execute(query, {"park_ids": park_ids})
                parks_with_schedule = set(row[0] for row in result.fetchall())
            
            # Set feature: 1 if has schedule, 0 if not
            for park_id in park_ids:
                mask = df['parkId'] == park_id
                df.loc[mask, 'park_has_schedule'] = int(park_id in parks_with_schedule)
                
        except Exception as e:
            print(f"⚠️  Failed to check schedule existence: {e}")
            # Keep default (1) on error
    
    return df


def engineer_features(
    df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    feature_context: Dict = None
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
    # 0. Resample to fix delta-compression gaps
    df = resample_data(df)

    # Fetch park metadata (needed for region-specific weekends & holidays)
    parks_metadata = fetch_parks_metadata()

    # Add features (order matters for dependencies)
    df = add_time_features(df, parks_metadata)  # Region-specific weekends
    df = add_weather_features(df)
    df = add_holiday_features(df, parks_metadata, start_date, end_date)
    df = add_bridge_day_feature(df, parks_metadata, start_date, end_date, feature_context)
    df = add_park_schedule_features(df, start_date, end_date)
    
    # Attraction and Park features (using available data only)
    df = add_attraction_type_feature(df)
    df = add_park_attraction_count_feature(df, parks_metadata)
    
    df = add_historical_features(df)
    df = add_percentile_features(df)  # Weather extremes
    
    # Phase 2: Add context features (Training uses internal data, Inference uses feature_context)
    df = add_park_occupancy_feature(df, feature_context)
    df = add_time_since_park_open(df, feature_context)
    df = add_downtime_features(df, feature_context)
    df = add_virtual_queue_feature(df, feature_context)
    df = add_park_has_schedule_feature(df, feature_context)  # NEW: Data quality indicator
    
    # Interaction features (must be after all base features are added)
    df = add_interaction_features(df)

    return df


def get_feature_columns() -> List[str]:
    """Return list of feature column names (in order) - Complete feature set with new additions"""
    return [
        # IDs (categorical)
        'parkId',
        'attractionId',

        # Time features (cyclic encoding preserves continuity: 23:00 → 00:00)
        'hour',
        'day_of_week',
        'month',
        'hour_sin', 'hour_cos',
        'day_of_week_sin', 'day_of_week_cos',
        'month_sin', 'month_cos',
        'day_of_year_sin', 'day_of_year_cos',  # NEW: Finer seasonal trends
        'season',
        'is_weekend',
        'is_peak_season',  # NEW: Peak season indicator (summer + December)

        # Weather features (all important for crowd patterns & ride closures)
        'temperature_avg',
        'temperature_deviation',  # NEW: Current vs. monthly average (unusual weather)
        'precipitation',
        'precipitation_last_3h',  # NEW: Cumulative rain effect (last 3 hours)
        'windSpeedMax',          # Explains high ride closures
        'snowfallSum',            # Explains outdoor ride closures
        'weatherCode',
        'is_raining',            # Explicit rain signal

        # Holiday features (cross-border tourism!)
        'is_holiday_primary',
        'is_school_holiday_primary',
        'is_holiday_neighbor_1',
        'is_holiday_neighbor_2',
        'is_holiday_neighbor_3',  # Border parks (Europa-Park → France, etc.)
        'holiday_count_total',
        'school_holiday_count_total',
        'is_school_holiday_any',  # Consolidated school holiday signal (matches inference)
        'is_bridge_day',          # Extended weekends

        # Park schedule features
        'is_park_open',
        'has_special_event',
        'has_extra_hours',

        # Attraction features (NEW - using available data)
        'attraction_type',       # From attractions.attractionType (nullable, defaults to 'UNKNOWN')
        'park_attraction_count', # Number of attractions in park (indicator of park size)

        # Historical features
        'avg_wait_last_24h',
        'avg_wait_last_1h',
        'avg_wait_same_hour_last_week',
        'avg_wait_same_hour_last_month',  # NEW: Monthly trend
        'rolling_avg_7d',
        'wait_time_velocity',  # Rate of change (momentum)
        'trend_7d',            # NEW: 7-day trend slope (positive = increasing, negative = decreasing)
        'volatility_7d',        # NEW: 7-day volatility (standard deviation of wait times)

        # Percentile-based features
        'is_temp_extreme',
        'is_wind_extreme',       # Extreme wind → ride closures
        
        # Phase 2: Real-time context features
        'park_occupancy_pct',           # Park-wide crowding (0-200%)
        'time_since_park_open_mins',    # Minutes since opening
        'had_downtime_today',            # Boolean: was DOWN today
        'downtime_minutes_today',        # Total downtime duration
        'has_virtual_queue',             # Boolean: boarding groups active
        'park_has_schedule',             # NEW: Data quality indicator (1=has schedule, 0=no schedule)
        
        # Interaction features (NEW - computationally cheap, no extra data needed)
        'hour_weekend_interaction',      # hour * is_weekend (peak times differ on weekends)
        'hour_is_weekend',              # NEW: Simple hour * is_weekend (complementary interaction)
        'temp_precip_interaction',       # temperature * precipitation (rain + heat = indoor preference)
        'holiday_occupancy_interaction', # is_holiday * park_occupancy_pct (holidays + crowds = extreme waits)
        'hour_occupancy_interaction',    # hour * park_occupancy_pct (time + occupancy patterns)
    ]


def get_categorical_features() -> List[str]:
    """Return list of categorical feature names"""
    return ['parkId', 'attractionId', 'weatherCode', 'attraction_type']
