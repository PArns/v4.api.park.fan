"""
Feature engineering for ML model
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List
from db import fetch_holidays, fetch_parks_metadata, fetch_park_schedules
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
    
    # Season (derived from month)
    def get_season(month):
        if month in [12, 1, 2]: return 0  # Winter
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

    # Binary rain indicator (explicit signal, valuable for ML)
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
    
    # Fetch park schedules (using DB helper)
    schedules_df = fetch_park_schedules(start_date, end_date)
    
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
                    # Schedules are Naive or Aware? DB returns Naive usually (representing Local)
                    # local_timestamp is Naive (representing Local)
                    
                    # Safety check for NaT
                    if pd.notna(opening) and pd.notna(closing):
                        if opening <= timestamp <= closing:
                            df.at[idx, 'is_park_open'] = 1
                        
                        # Calculate time since open (minutes)
                        mins_since = (timestamp - opening).total_seconds() / 60.0
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
    df = add_historical_features(df)
    df = add_percentile_features(df)  # Weather extremes
    
    # Phase 2: Add context features (Training uses internal data, Inference uses feature_context)
    df = add_park_occupancy_feature(df, feature_context)
    df = add_time_since_park_open(df, feature_context)
    df = add_downtime_features(df, feature_context)
    df = add_virtual_queue_feature(df, feature_context)

    return df


def get_feature_columns() -> List[str]:
    """Return list of feature column names (in order) - Complete 42 feature set"""
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
        'season',
        'is_weekend',

        # Weather features (all important for crowd patterns & ride closures)
        'temperature_avg',
        'precipitation',
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
        'is_bridge_day',          # Extended weekends

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

        # Percentile-based features
        'is_temp_extreme',
        'is_wind_extreme',       # Extreme wind → ride closures
        
        # Phase 2: Real-time context features
        'park_occupancy_pct',           # Park-wide crowding (0-200%)
        'time_since_park_open_mins',    # Minutes since opening
        'had_downtime_today',            # Boolean: was DOWN today
        'downtime_minutes_today',        # Total downtime duration
        'has_virtual_queue',             # Boolean: boarding groups active
    ]


def get_categorical_features() -> List[str]:
    """Return list of categorical feature names"""
    return ['parkId', 'attractionId', 'weatherCode']
