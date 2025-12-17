"""
Feature engineering for ML model
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List
from db import fetch_holidays, fetch_parks_metadata
from percentile_features import add_percentile_features






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


def add_time_features(df: pd.DataFrame, parks_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Add time-based features using LOCAL time.
    """
    # 1. Convert to local time first
    df = convert_to_local_time(df, parks_metadata)

    # 2. Extract features from LOCAL time
    df['hour'] = df['local_timestamp'].dt.hour
    df['day_of_week'] = df['local_timestamp'].dt.dayofweek
    df['month'] = df['local_timestamp'].dt.month

    # Cyclical time encoding (preserves continuity, e.g. 23:00 -> 00:00)
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['day_of_week_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_of_week_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    
    # Season logic (simplified)
    def get_season(month):
        if month in [12, 1, 2]: return 0 # Winter
        if month in [3, 4, 5]: return 1  # Spring
        if month in [6, 7, 8]: return 2  # Summer
        return 3                         # Fall
        
    df['season'] = df['month'].apply(get_season)

    # 3. Use LOCAL date for further lookups (holidays, weather)
    # This prevents using yesterday's weather for today's morning (due to UTC lag)
    df['date_local'] = df['local_timestamp'].dt.date

    # Region-specific weekend detection
    # Middle East & Israel: Friday (5) + Saturday (6)
    # Western countries: Saturday (6) + Sunday (0)
    middle_east_countries = ['SA', 'AE', 'BH', 'KW', 'OM', 'QA', 'IL']

    # Initialize is_weekend
    df['is_weekend'] = 0

    # For each park, determine weekend based on country
    for park_id in df['parkId'].unique():
        park_info = parks_metadata[parks_metadata['park_id'] == park_id]

        if not park_info.empty:
            country = park_info.iloc[0]['country']
            park_mask = df['parkId'] == park_id

            if country in middle_east_countries:
                # Middle East: Friday (5) + Saturday (6)
                df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([5, 6]).astype(int)
            else:
                # Western: Saturday (6) + Sunday (0)
                df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([0, 6]).astype(int)
        else:
            # Default: Western weekend
            park_mask = df['parkId'] == park_id
            df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([0, 6]).astype(int)

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

    # Binary rain indicator
    if 'precipitation' in df.columns:
        df['is_raining'] = (df['precipitation'] > 0).astype(int)

    return df


def add_holiday_features(
    df: pd.DataFrame,
    parks_metadata: pd.DataFrame,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame:
    """
    Add multi-country holiday features

    For each park, checks holidays in:
    - Primary country (park's country)
    - Neighbor countries (from influencingCountries)
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

    # Merge park metadata
    df = df.merge(
        parks_metadata[['park_id', 'country', 'influencingCountries']],
        left_on='parkId',
        right_on='park_id',
        how='left'
    )

    # Get unique countries we need holidays for
    all_countries = set()
    for _, row in parks_metadata.iterrows():
        all_countries.add(row['country'])
        if row['influencingCountries']:
            all_countries.update(row['influencingCountries'])

    # Fetch all holidays
    holidays_df = fetch_holidays(list(all_countries), start_date, end_date)

    # Convert date column to datetime if needed
    if not holidays_df.empty:
        holidays_df['date'] = pd.to_datetime(holidays_df['date'])

    # Create holiday lookup: {(country, date): type}
    # type can be 'public', 'school', 'observance', 'bank'
    holiday_lookup = {}
    if not holidays_df.empty:
        for _, row in holidays_df.iterrows():
            key = (row['country'], row['date'].date())
            # prioritize public over others if duplicates (simple approach) or store list
            # simple lookup: store the implementation type
            holiday_lookup[key] = row['holiday_type']

    # Process each row


    def check_holidays(row):
        # Use LOCAL date for holiday lookup
        date = row['date_local']
        primary_country = row['country']
        influencing = row['influencingCountries'] or []

        # Primary country holiday
        h_type = holiday_lookup.get((primary_country, date))
        row['is_holiday_primary'] = int(h_type == 'public')
        row['is_school_holiday_primary'] = int(h_type == 'school')

        # Neighbor holidays (count public holidays)
        # We focus on public holidays for neighbors as school holidays are less impactful across borders usually,
        # but for high impact we could count them. For now let's just count public/school mixed or just public?
        # User asked "School holidays exactly so?". 
        # Let's count public holidays for neighbors as 'is_holiday_neighbor_X'
        
        neighbor_types = [holiday_lookup.get((country, date)) for country in influencing[:3]]
        
        holiday_flags = [int(t == 'public') for t in neighbor_types]
        
        if len(holiday_flags) > 0:
            row['is_holiday_neighbor_1'] = holiday_flags[0]
        if len(holiday_flags) > 1:
            row['is_holiday_neighbor_2'] = holiday_flags[1]
        if len(holiday_flags) > 2:
            row['is_holiday_neighbor_3'] = holiday_flags[2]

        # Total holiday count (Public)
        row['holiday_count_total'] = sum([row['is_holiday_primary']] + holiday_flags)
        
        # Total school holiday count (Primary + Neighbors school holidays)
        school_flags = [int(t == 'school') for t in neighbor_types]
        row['school_holiday_count_total'] = sum([row['is_school_holiday_primary']] + school_flags)

        return row

    df = df.apply(check_holidays, axis=1)

    # Drop temporary merge columns
    df = df.drop(columns=['park_id', 'country', 'influencingCountries'], errors='ignore')

    return df


def add_historical_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add historical wait time features

    Features:
    - avg_wait_last_24h: Average wait time in last 24 hours (per attraction)
    - avg_wait_same_hour_last_week: Average wait at same hour, 7 days ago
    - rolling_avg_7d: 7-day rolling average
    """
    df = df.copy()
    df = df.sort_values(['attractionId', 'timestamp'])

    # Last 24h average (per attraction)
    df['avg_wait_last_24h'] = df.groupby('attractionId')['waitTime'].transform(
        lambda x: x.rolling(window=24, min_periods=1).mean().shift(1)
    )

    # Last 1h average (per attraction) - captures immediate trends
    # 5 min intervals -> window=12
    df['avg_wait_last_1h'] = df.groupby('attractionId')['waitTime'].transform(
        lambda x: x.rolling(window=12, min_periods=1).mean().shift(1)
    )

    # Same hour last week (168 hours ago)
    df['avg_wait_same_hour_last_week'] = df.groupby('attractionId')['waitTime'].transform(
        lambda x: x.shift(168)
    )

    # 7-day rolling average
    df['rolling_avg_7d'] = df.groupby('attractionId')['waitTime'].transform(
        lambda x: x.rolling(window=168, min_periods=1).mean().shift(1)
    )

    # Fill NaN with attraction mean
    hist_cols = ['avg_wait_last_24h', 'avg_wait_last_1h', 'avg_wait_same_hour_last_week', 'rolling_avg_7d']
    for col in hist_cols:
        df[col] = df.groupby('attractionId')[col].transform(
            lambda x: x.fillna(x.mean())
        ).fillna(0)

    # Wait time velocity (rate of change / momentum)
    # Positive = queues building up, Negative = queues clearing
    # Uses last 6 observations (~30 min at 5-min intervals) to calculate trend
    df['wait_time_velocity'] = df.groupby('attractionId')['waitTime'].transform(
        lambda x: x.diff().rolling(window=6, min_periods=1).mean().shift(1)
    )
    
    # Fill NaN velocity with 0 (no change)
    df['wait_time_velocity'] = df.groupby('attractionId')['wait_time_velocity'].transform(
        lambda x: x.fillna(0)
    ).fillna(0)

    return df


def add_park_schedule_features(
    df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame:
    """
    Add park schedule features (is_park_open, has_special_event)

    Special events like Wintertraum at Phantasialand typically have higher crowds.

    Args:
        df: DataFrame with timestamp and parkId columns
        start_date: Training period start
        end_date: Training period end

    Returns:
        DataFrame with schedule features added
    """
    from db import fetch_park_schedules

    # Fetch park schedules
    schedules_df = fetch_park_schedules(start_date, end_date)

    if schedules_df.empty:
        # No schedule data - assume park always open, no special events
        df['is_park_open'] = 1
        df['has_special_event'] = 0
        df['has_extra_hours'] = 0
        return df

    # Convert to datetime for comparison
    schedules_df['opening_time'] = pd.to_datetime(schedules_df['opening_time'])
    schedules_df['closing_time'] = pd.to_datetime(schedules_df['closing_time'])
    schedules_df['date'] = pd.to_datetime(schedules_df['date'])

    # Initialize features
    df['is_park_open'] = 0
    df['has_special_event'] = 0  # TICKETED_EVENT or PRIVATE_EVENT
    df['has_extra_hours'] = 0     # EXTRA_HOURS (typically busier)

    # For each row, check schedule
    for idx, row in df.iterrows():
        park_id = row['parkId']
        # Use LOCAL timestamp and date for schedule comparison
        # (Schedules in DB are stored as local dates/times)
        timestamp = row['local_timestamp']
        date = row['date_local']

        # Find all schedules for this park on this date
        schedules = schedules_df[
            (schedules_df['park_id'] == park_id) &
            (schedules_df['date'].dt.date == date)
        ]

        if not schedules.empty:
            # Check if park is open (OPERATING schedule type)
            operating = schedules[schedules['schedule_type'] == 'OPERATING']
            if not operating.empty:
                opening = operating.iloc[0]['opening_time']
                closing = operating.iloc[0]['closing_time']

                # Compare local timestamp with opening/closing (which are timezone-naive but imply local time)
                # Ensure timestamp is timezone-naive for comparison if necessary, or localize opening/closing
                ts_naive = timestamp.replace(tzinfo=None)
                
                if opening <= ts_naive <= closing:
                    df.at[idx, 'is_park_open'] = 1

            # Check for special events (typically attract more visitors)
            if any(schedules['schedule_type'].isin(['TICKETED_EVENT', 'PRIVATE_EVENT'])):
                df.at[idx, 'has_special_event'] = 1

            # Check for extra hours (extended operating, typically busier)
            if 'EXTRA_HOURS' in schedules['schedule_type'].values:
                df.at[idx, 'has_extra_hours'] = 1

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


def engineer_features(
    df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame:
    """
    Complete feature engineering pipeline

    Args:
        df: Raw queue data from fetch_training_data()
        start_date: Training period start
        end_date: Training period end

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
    df = add_park_schedule_features(df, start_date, end_date)
    df = add_historical_features(df)
    df = add_percentile_features(df)  # Weather extremes

    return df


def get_feature_columns() -> List[str]:
    """Return list of feature column names (in order)"""
    return [
        # IDs (categorical)
        'parkId',
        'attractionId',

        # Time features
        'hour',
        'day_of_week',
        'month',
        'hour_sin', 'hour_cos',
        'day_of_week_sin', 'day_of_week_cos',
        'month_sin', 'month_cos',
        'season',
        'is_weekend',

        # Weather features
        'temperature_avg',
        'precipitation',
        'windSpeedMax',
        'snowfallSum',
        'weatherCode',
        'is_raining',

        # Holiday features
        'is_holiday_primary',
        'is_school_holiday_primary',
        'is_holiday_neighbor_1',
        'is_holiday_neighbor_2',
        'is_holiday_neighbor_3',
        'holiday_count_total',
        'school_holiday_count_total',

        # Park schedule features
        'is_park_open',
        'has_special_event',
        'has_extra_hours',

        # Historical features
        'avg_wait_last_24h',
        'avg_wait_last_1h',
        'avg_wait_same_hour_last_week',
        'rolling_avg_7d',
        'wait_time_velocity',  # Rate of change (momentum)

        # Percentile-based features (Phase 3)
        'is_temp_extreme',
        'is_wind_extreme',
    ]


def get_categorical_features() -> List[str]:
    """Return list of categorical feature names"""
    return ['parkId', 'attractionId', 'weatherCode']
