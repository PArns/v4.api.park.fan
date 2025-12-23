"""
Prediction logic for hourly and daily forecasts
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from sqlalchemy import text

from model import WaitTimeModel
from features import engineer_features, get_feature_columns
from percentile_features import add_percentile_features
from db import fetch_parks_metadata, get_db, fetch_holidays
from config import get_settings

settings = get_settings()


def round_to_nearest_5(value: float) -> int:
    """
    Round wait time to nearest 5 minutes for UX consistency
    
    Theme parks typically display wait times in 5-minute increments.
    This provides better user experience and consistency with actual queue displays.
    
    Args:
        value: Raw prediction value (any float)
    
    Returns:
        Rounded integer in 5-minute increments
    
    Examples:
        >>> round_to_nearest_5(7.2)
        5
        >>> round_to_nearest_5(8.9)
        10
        >>> round_to_nearest_5(12.4)
        10
        >>> round_to_nearest_5(34.7)
        35
    """
    if value < 2.5:
        return 0  # Very short wait → 0
    
    # Add 2.5 and floor divide by 5, then multiply by 5
    # This ensures consistent rounding: 2.5→5, 7.5→10, 12.5→15
    return int((value + 2.5) // 5) * 5


def fetch_recent_wait_times(attraction_ids: List[str], lookback_days: int = 730) -> pd.DataFrame:
    """
    Fetch recent wait times aggregated by day for historical features

    Args:
        attraction_ids: List of attraction IDs
        lookback_days: How many days to look back (default: 730 = 2 years)

    Returns:
        DataFrame with daily aggregated queue data
    """
    if not attraction_ids:
        return pd.DataFrame()

    # Aggregate to daily values for efficiency (2 years of hourly data = too much)
    # Calculate: avg per day, avg per hour-of-day, avg per day-of-week
    query = text("""
        SELECT
            "attractionId"::text as "attractionId",
            DATE(timestamp) as date,
            EXTRACT(HOUR FROM timestamp) as hour,
            EXTRACT(DOW FROM timestamp) as day_of_week,
            AVG("waitTime") as avg_wait,
            COUNT(*) as data_points
        FROM queue_data
        WHERE "attractionId"::text = ANY(:attraction_ids)
            AND timestamp >= NOW() - INTERVAL :lookback_days DAY
            AND "waitTime" IS NOT NULL
            AND status = 'OPERATING'
            AND "queueType" = 'STANDBY'
        GROUP BY "attractionId", DATE(timestamp), EXTRACT(HOUR FROM timestamp), EXTRACT(DOW FROM timestamp)
        ORDER BY "attractionId", date DESC, hour
    """)

    with get_db() as db:
        result = db.execute(query, {
            "attraction_ids": attraction_ids,
            "lookback_days": f'{lookback_days} days'
        })
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return df


def generate_future_timestamps(
    base_time: datetime,
    prediction_type: str
) -> List[datetime]:
    """
    Generate future timestamps for predictions

    Args:
        base_time: Starting time
        prediction_type: 'hourly' or 'daily'

    Returns:
        List of future timestamps
    """
    if prediction_type == 'hourly':
        # Round base_time to the NEXT full hour (not current hour)
        # This ensures all predictions have timestamps like "2024-01-15T14:00:00"
        # If it's 14:37, round up to 15:00 (next hour)
        rounded_base = base_time.replace(minute=0, second=0, microsecond=0)
        if base_time.minute > 0 or base_time.second > 0 or base_time.microsecond > 0:
            rounded_base = rounded_base + timedelta(hours=1)
        
        # Next 24 hours from rounded base (starting from next full hour)
        return [rounded_base + timedelta(hours=i) for i in range(settings.HOURLY_PREDICTIONS)]
    elif prediction_type == 'daily':
        # Next 14 days (at 14:00 each day, typical peak time)
        return [
            (base_time + timedelta(days=i)).replace(hour=14, minute=0, second=0, microsecond=0)
            for i in range(1, settings.DAILY_PREDICTIONS + 1)
        ]
    else:
        raise ValueError(f"Unknown prediction_type: {prediction_type}")


def create_prediction_features(
    attraction_ids: List[str],
    park_ids: List[str],
    timestamps: List[datetime],
    base_time: datetime,
    weather_forecast: List[Any] = None,
    current_wait_times: Dict[str, int] = None,
    recent_wait_times: Dict[str, int] = None,
    feature_context: Dict[str, Any] = None
) -> pd.DataFrame:
    """
    Create feature DataFrame for predictions with all features from DB

    Args:
        attraction_ids: List of attraction IDs
        park_ids: Corresponding park IDs
        timestamps: Future timestamps to predict for
        base_time: Current time for fetching historical data
        weather_forecast: Optional list of hourly weather forecast items
        current_wait_times: Optional dict of {attractionId: waitTime} for current state
    
    Returns:
        DataFrame with features ready for prediction
    """
    # Create base DataFrame
    rows = []
    for attraction_id, park_id in zip(attraction_ids, park_ids):
        for ts in timestamps:
            rows.append({
                'attractionId': attraction_id,
                'parkId': park_id,
                'timestamp': ts,
            })

    df = pd.DataFrame(rows)

    # Convert parkId and attractionId to strings (for CatBoost)
    df['parkId'] = df['parkId'].astype(str)
    df['attractionId'] = df['attractionId'].astype(str)

    # Add time features
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['month'] = df['timestamp'].dt.month
    
    # Cyclical time encoding
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['day_of_week_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_of_week_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

    df['season'] = df['timestamp'].dt.month.apply(lambda m: (m % 12) // 3)

    # Region-specific weekends
    parks_metadata = fetch_parks_metadata()
    middle_east_countries = ['SA', 'AE', 'BH', 'KW', 'OM', 'QA', 'IL']
    df['is_weekend'] = 0

    for park_id in df['parkId'].unique():
        park_info = parks_metadata[parks_metadata['park_id'] == park_id]
        if not park_info.empty:
            country = park_info.iloc[0]['country']
            park_mask = df['parkId'] == park_id

            if country in middle_east_countries:
                # Middle East: Friday (4) + Saturday (5) in dayofweek (0=Mon)
                df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([4, 5]).astype(int)
            else:
                # Western: Saturday (5) + Sunday (6)
                df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([5, 6]).astype(int)
        else:
            # Default: Western weekend
            park_mask = df['parkId'] == park_id
            df.loc[park_mask, 'is_weekend'] = df.loc[park_mask, 'day_of_week'].isin([5, 6]).astype(int)

    # Weather features 
    # Logic: Use provided hourly forecast if available, otherwise fallback to historical daily averages
    
    use_forecast = False
    if weather_forecast and len(weather_forecast) > 0:
        try:
            # Convert forecast objects to dicts if needed
            wf_data = [w.dict() if hasattr(w, 'dict') else w for w in weather_forecast]
            wf_df = pd.DataFrame(wf_data)
            wf_df['time'] = pd.to_datetime(wf_df['time'])
            
            # Normalize both columns to timezone-naive UTC for robust merging
            # 1. Handle Weather Forecast DataFrame
            if wf_df['time'].dt.tz is not None:
                # Convert to UTC then remove timezone info
                wf_df['time'] = wf_df['time'].dt.tz_convert('UTC').dt.tz_localize(None)
            
            # 2. Handle Prediction DataFrame
            # Create join_time and ensure it is also timezone-naive UTC
            df['join_time'] = df['timestamp'].dt.round('h')
            if df['join_time'].dt.tz is not None:
                df['join_time'] = df['join_time'].dt.tz_convert('UTC').dt.tz_localize(None)
            
            # Merge logic
            # Note: weather_forecast is assumed to apply to all parks in this batch (usually same park)
            df = df.merge(wf_df, left_on='join_time', right_on='time', how='left')
            
            # Map columns and fill defaults
            df['temperature_avg'] = df['temperature'].fillna(20.0)
            df['precipitation'] = df['precipitation'].fillna(0.0)
            df['windSpeedMax'] = df['windSpeed'].fillna(0.0)
            df['snowfallSum'] = df['snowfall'].fillna(0.0)
            df['weatherCode'] = df['weatherCode'].fillna(0).astype(int)
            
            # Cleanup join columns
            df = df.drop(columns=['join_time', 'time', 'temperature', 'windSpeed', 'snowfall', 'rain'], errors='ignore')
            use_forecast = True
        except Exception as e:
            print(f"⚠️ Failed to use weather forecast in features: {e}. Falling back to DB.")
            use_forecast = False

    if not use_forecast:
        # Weather features (use seasonal averages from DB for better accuracy)
        # Get the month we're predicting for
        prediction_month = timestamps[0].month if timestamps else datetime.now(timezone.utc).month
        
        weather_query = text("""
            SELECT
                "parkId"::text as "parkId",
                AVG("temperatureMax" + "temperatureMin") / 2 as temp_avg,
                AVG("precipitationSum") as precip_avg,
                AVG("windSpeedMax") as wind_avg,
                AVG("snowfallSum") as snow_avg,
                MODE() WITHIN GROUP (ORDER BY "weatherCode") as weather_code_mode
            FROM weather_data
            WHERE "parkId"::text = ANY(:park_ids)
                AND EXTRACT(MONTH FROM date) = :month  -- Same month from historical data
                AND date >= CURRENT_DATE - INTERVAL '3 years'  -- Use 3 
            GROUP BY "parkId"
        """)
    
        with get_db() as db:
            result = db.execute(weather_query, {
                "park_ids": list(set(park_ids)),
                "month": prediction_month
            })
            weather_df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
        # Merge weather data
        if not weather_df.empty:
            df = df.merge(weather_df, left_on='parkId', right_on='parkId', how='left')
            df['temperature_avg'] = df['temp_avg'].fillna(20.0)
            df['precipitation'] = df['precip_avg'].fillna(0.0)
            df['windSpeedMax'] = df['wind_avg'].fillna(0.0)
            df['snowfallSum'] = df['snow_avg'].fillna(0.0)
            df['weatherCode'] = df['weather_code_mode'].fillna(0).astype(int)
            df = df.drop(columns=['temp_avg', 'precip_avg', 'wind_avg', 'snow_avg', 'weather_code_mode'], errors='ignore')
        else:
            df['temperature_avg'] = 20.0
            df['precipitation'] = 0.0
            df['windSpeedMax'] = 0.0
            df['snowfallSum'] = 0.0
            df['weatherCode'] = 0

    df['is_raining'] = (df['precipitation'] > 0).astype(int)

    # Holiday features
    df_start = df['timestamp'].min()
    df_end = df['timestamp'].max()

    all_countries = set()
    for _, park in parks_metadata.iterrows():
        all_countries.add(park['country'])
        if park['influencingCountries']:
            all_countries.update(park['influencingCountries'])

    holidays_df = fetch_holidays(list(all_countries), df_start, df_end)

    if not holidays_df.empty:
        holidays_df['date'] = pd.to_datetime(holidays_df['date'])
        
        # Create lookup with type: {(country, date): type}
        holiday_lookup = {}
        for _, row in holidays_df.iterrows():
            key = (row['country'], row['date'].date())
            holiday_lookup[key] = row['holiday_type']
    else:
        holiday_lookup = {}

    # Initialize holiday columns
    df['is_holiday_primary'] = 0
    df['is_school_holiday_primary'] = 0
    df['is_holiday_neighbor_1'] = 0
    df['is_holiday_neighbor_2'] = 0
    df['is_holiday_neighbor_3'] = 0
    df['holiday_count_total'] = 0
    df['school_holiday_count_total'] = 0

    # Check holidays for each row
    for idx, row in df.iterrows():
        date = row['timestamp'].date()
        park_info = parks_metadata[parks_metadata['park_id'] == row['parkId']]

        if not park_info.empty:
            primary_country = park_info.iloc[0]['country']
            influencing = park_info.iloc[0]['influencingCountries'] or []

            # Primary country holiday
            h_type = holiday_lookup.get((primary_country, date))
            df.at[idx, 'is_holiday_primary'] = int(h_type == 'public')
            df.at[idx, 'is_school_holiday_primary'] = int(h_type == 'school')

            # Neighbor holidays (Public only for now for neighbors)
            neighbor_types = [holiday_lookup.get((country, date)) for country in influencing[:3]]
            neighbor_flags = [int(t == 'public') for t in neighbor_types]
            school_flags = [int(t == 'school') for t in neighbor_types]

            if len(neighbor_flags) > 0:
                df.at[idx, 'is_holiday_neighbor_1'] = neighbor_flags[0]
            if len(neighbor_flags) > 1:
                df.at[idx, 'is_holiday_neighbor_2'] = neighbor_flags[1]
            if len(neighbor_flags) > 2:
                df.at[idx, 'is_holiday_neighbor_3'] = neighbor_flags[2]

            df.at[idx, 'holiday_count_total'] = sum([df.at[idx, 'is_holiday_primary']] + neighbor_flags)
            df.at[idx, 'school_holiday_count_total'] = sum([df.at[idx, 'is_school_holiday_primary']] + school_flags)

    # Park schedule features (check if park is open at predicted time)
    schedule_query = text("""
        SELECT
            "parkId"::text as "parkId",
            "attractionId"::text as "attractionId",
            date,
            "scheduleType",
            "openingTime",
            "closingTime"
        FROM schedule_entries
        WHERE "parkId"::text = ANY(:park_ids)
            AND date BETWEEN :start_date AND :end_date
            AND (
                ("openingTime" IS NOT NULL AND "closingTime" IS NOT NULL)
                OR "scheduleType" IN ('MAINTENANCE', 'CLOSED', 'INFO', 'TICKETED_EVENT', 'PRIVATE_EVENT')
            )
    """)

    with get_db() as db:
        result = db.execute(schedule_query, {
            "park_ids": list(set(park_ids)),
            "start_date": df_start.date(),
            "end_date": df_end.date()
        })
        schedules_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Initialize schedule features and status
    df['is_park_open'] = 1  # Assume open if no schedule found
    df['has_special_event'] = 0
    df['has_extra_hours'] = 0
    df['status'] = 'OPERATING'

    if not schedules_df.empty:
        schedules_df['openingTime'] = pd.to_datetime(schedules_df['openingTime'])
        schedules_df['closingTime'] = pd.to_datetime(schedules_df['closingTime'])
        schedules_df['date'] = pd.to_datetime(schedules_df['date'])
        
        # Ensure attractionId is treated as string (handle NaN/None)
        if 'attractionId' in schedules_df.columns:
            schedules_df['attractionId'] = schedules_df['attractionId'].fillna('nan').astype(str)

        for idx, row in df.iterrows():
            park_id = row['parkId']
            attraction_id = str(row['attractionId'])
            timestamp = row['timestamp']

            # Convert timestamp to date for comparison
            date_only = pd.Timestamp(timestamp.date())

            # Filter schedules for this park/date
            # We want: 
            # 1. Park schedules (attractionId is null/nan)
            # 2. Attraction schedules (attractionId matches)
            park_schedules = schedules_df[
                (schedules_df['parkId'] == park_id) &
                (schedules_df['date'] == date_only)
            ]
            
            if not park_schedules.empty:
                # 1. Check Park Status & Opening Hours
                # Park-wide schedules usually have attractionId = None (or 'nan' after string conversion)
                global_schedules = park_schedules[
                    (park_schedules['attractionId'] == 'nan') | 
                    (park_schedules['attractionId'] == 'None') |
                    (park_schedules['attractionId'].isna())
                ]
                
                if not global_schedules.empty:
                    # Operating hours
                    operating = global_schedules[global_schedules['scheduleType'] == 'OPERATING']
                    if not operating.empty:
                        opening = operating.iloc[0]['openingTime']
                        closing = operating.iloc[0]['closingTime']
                        
                        # Robust comparison handling mixed timezones
                        ts_compare = timestamp
                        
                        # case 1: DB has timezone, we need to match it
                        if getattr(opening, 'tzinfo', None) is not None:
                            if ts_compare.tzinfo is None:
                                ts_compare = pd.Timestamp(ts_compare).tz_localize('UTC')
                            ts_compare = ts_compare.tz_convert(opening.tzinfo)
                        # case 2: DB is naive, we must be naive (UTC)
                        else:
                            if ts_compare.tzinfo is not None:
                                ts_compare = ts_compare.tz_convert('UTC').tz_localize(None)
                        
                        is_open = opening <= ts_compare <= closing
                        df.at[idx, 'is_park_open'] = int(is_open)
                        
                        if not is_open:
                            df.at[idx, 'status'] = 'CLOSED'

                    # Special events / Extra hours
                    if any(global_schedules['scheduleType'].isin(['TICKETED_EVENT', 'PRIVATE_EVENT'])):
                        df.at[idx, 'has_special_event'] = 1
                    
                    if 'EXTRA_HOURS' in global_schedules['scheduleType'].values:
                        df.at[idx, 'has_extra_hours'] = 1

                # 2. Check Specific Attraction Status (Overrides park status if strictly closed/maintenance)
                # But typically if park is closed, attraction is closed. 
                # If park is open, attraction might be maintenance.
                attr_schedules = park_schedules[park_schedules['attractionId'] == attraction_id]
                
                if not attr_schedules.empty:
                    # Check for MAINTENANCE or CLOSED
                    if any(attr_schedules['scheduleType'].isin(['MAINTENANCE', 'CLOSED'])):
                        df.at[idx, 'status'] = 'CLOSED' # Or 'MAINTENANCE' specifically if we want distinct status
                        # If attraction is closed, is_park_open feature for the model implies "can guests ride?" 
                        # technically park is open but this ride isn't. 
                        # But we are overriding prediction anyway, so feature value matters less.


    # Historical features (most important!)
    # Fetch up to 2 years of aggregated daily data (efficient for large datasets)
    recent_data = fetch_recent_wait_times(attraction_ids, lookback_days=730)

    # Initialize with defaults
    df['avg_wait_last_24h'] = 30.0
    df['avg_wait_last_1h'] = 30.0
    df['avg_wait_same_hour_last_week'] = 35.0
    df['rolling_avg_7d'] = 32.0

    if not recent_data.empty:
        recent_data['date'] = pd.to_datetime(recent_data['date'])

        # Convert base_time to pandas Timestamp for consistent comparisons
        base_time_pd = pd.Timestamp(base_time)

        for attraction_id in attraction_ids:
            attraction_data = recent_data[recent_data['attractionId'] == attraction_id]

            if not attraction_data.empty:
                # Overall average (all data)
                overall_avg = attraction_data['avg_wait'].mean()

                # Last 7 days average (rolling_avg_7d)
                cutoff_7d = base_time_pd - timedelta(days=7)
                last_7_days = attraction_data[attraction_data['date'] >= cutoff_7d]
                rolling_7d = last_7_days['avg_wait'].mean() if len(last_7_days) > 0 else overall_avg

                # Last 24h average (approximation: today + yesterday average)
                cutoff_24h = base_time_pd - timedelta(days=1)
                last_24h = attraction_data[attraction_data['date'] >= cutoff_24h]
                avg_24h = last_24h['avg_wait'].mean() if len(last_24h) > 0 else rolling_7d
                
                # Last 1h average
                # Since we don't have hourly data aggregated in 'recent_data' (it's daily-ish but query has hour),
                # let's check if we can approximate from the query result which DOES have 'hour'.
                # The query groups by date AND hour.
                last_1h_data = attraction_data[
                    (attraction_data['date'] == base_time_pd.date()) & 
                    (attraction_data['hour'] == (base_time.hour - 1))
                ]
                avg_1h = last_1h_data['avg_wait'].mean() if not last_1h_data.empty else avg_24h

                # Same hour last week (7 days ago, same hour)
                last_week_date = base_time_pd - timedelta(days=7)
                current_hour = base_time.hour

                same_hour_last_week = attraction_data[
                    (attraction_data['date'] == last_week_date.normalize()) &
                    (attraction_data['hour'] == current_hour)
                ]
                avg_same_hour = same_hour_last_week['avg_wait'].mean() if len(same_hour_last_week) > 0 else rolling_7d

                # Apply to all rows for this attraction
                mask = df['attractionId'] == attraction_id
                df.loc[mask, 'avg_wait_last_24h'] = avg_24h
                df.loc[mask, 'avg_wait_last_1h'] = avg_1h
                df.loc[mask, 'avg_wait_same_hour_last_week'] = avg_same_hour
                df.loc[mask, 'rolling_avg_7d'] = rolling_7d

    # Calculate wait time velocity (momentum) BEFORE overriding lags
    # Initialize with default (no change)
    df['wait_time_velocity'] = 0.0
    
    # Override lags with current wait times if available (Autoregression)
    if current_wait_times:
        for attraction_id, wait_time in current_wait_times.items():
            if wait_time is not None:
                mask = df['attractionId'] == str(attraction_id)
                if mask.any():
                    # Calculate velocity: 
                    # Training uses: rolling(6).mean() of differences (avg change per 5 mins over 30 mins)
                    # Inference approximation: (Current - Recent30min) / 6.0
                    
                    velocity = 0.0
                    
                    # 1. Try explicit recent wait time (passed from API, ~30 mins ago)
                    if recent_wait_times and str(attraction_id) in recent_wait_times:
                        recent_val = recent_wait_times[str(attraction_id)]
                        if recent_val is not None:
                            velocity = (float(wait_time) - float(recent_val)) / 6.0
                    
                    # 2. Fallback to using 1h avg from DB if explicit recent not available
                    elif not pd.isna(df.loc[mask, 'avg_wait_last_1h'].iloc[0]):
                        recent_avg = df.loc[mask, 'avg_wait_last_1h'].iloc[0]
                        velocity = (float(wait_time) - recent_avg) / 6.0
                    
                    df.loc[mask, 'wait_time_velocity'] = velocity
                    
                    # Now override the lag feature with current value
                    df.loc[mask, 'avg_wait_last_1h'] = float(wait_time)
    
    # Add percentile features (Weather extremes)
    df = add_percentile_features(df)
    
    # Phase 2: Add real-time context features
    if feature_context:
        from features import (
            add_park_occupancy_feature,
            add_time_since_park_open,
            add_downtime_features,
            add_virtual_queue_feature,
            add_bridge_day_feature
        )
        
        df = add_park_occupancy_feature(df, feature_context)
        df = add_time_since_park_open(df, feature_context)
        df = add_downtime_features(df, feature_context)
        df = add_virtual_queue_feature(df, feature_context)
        
        # Bridge day needs metadata refetch ideally, but for inference we rely on feature_context
        # If feature_context has it, great. If not, add_bridge_day_feature will attempt fallback or skip.
        # But add_bridge_day_feature requires parks_metadata, start_date, end_date for fallback.
        # For inference, if we lack context, we might skip expensive fallback or fetch metadata.
        # Let's pass what we have.
        # Note: add_bridge_day_feature signature: (df, parks_metadata, start_date, end_date, feature_context)
        # We need to fetch metadata if not available (already imported at module level)
        parks_metadata = fetch_parks_metadata()
        start = df['timestamp'].min()
        end = df['timestamp'].max()
        df = add_bridge_day_feature(df, parks_metadata, start, end, feature_context)
    else:
        # Defaults if no feature_context provided
        df['park_occupancy_pct'] = 100.0
        df['time_since_park_open_mins'] = 0.0
        df['had_downtime_today'] = 0
        df['downtime_minutes_today'] = 0.0
        df['has_virtual_queue'] = 0
        df['is_bridge_day'] = 0

    return df


def predict_wait_times(
    model: WaitTimeModel,
    attraction_ids: List[str],
    park_ids: List[str],
    prediction_type: str = 'hourly',
    base_time: datetime = None,
    weather_forecast: List[Any] = None,
    current_wait_times: Dict[str, int] = None,
    recent_wait_times: Dict[str, int] = None,
    feature_context: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    """
    Predict wait times for attractions

    Args:
        model: Trained WaitTimeModel
        attraction_ids: List of attraction IDs to predict for
        park_ids: Corresponding park IDs
        prediction_type: 'hourly' or 'daily'
        base_time: Starting time (defaults to now)
        weather_forecast: Optional hourly weather forecast
        current_wait_times: Optional current wait times for autoregression

    Returns:
        List of predictions:
        [
            {
                'attractionId': str,
                'predictedTime': datetime,
                'predictedWaitTime': int,
                'predictionType': str,
                'confidence': float (0-100)
            }
        ]
    """
    if base_time is None:
        base_time = datetime.now(timezone.utc)

    # Generate future timestamps
    timestamps = generate_future_timestamps(base_time, prediction_type)

    # Create features with all DB-loaded data
    features_df = create_prediction_features(
        attraction_ids,
        park_ids,
        timestamps,
        base_time,
        weather_forecast,
        current_wait_times,
        recent_wait_times,
        feature_context
    )

    # Predict with uncertainty estimation
    try:
        uncertainty_results = model.predict_with_uncertainty(features_df)
        predictions = uncertainty_results['predictions']
        uncertainties = uncertainty_results['uncertainty']
        use_uncertainty = True
    except Exception as e:
        # Fallback to regular predictions if uncertainty estimation fails
        print(f"⚠️  Uncertainty estimation failed, using regular predictions: {e}")
        predictions = model.predict(features_df)
        uncertainties = np.zeros(len(predictions))
        use_uncertainty = False

    # Format results
    results = []
    for idx, row in features_df.iterrows():
        pred_wait = round_to_nearest_5(predictions[idx])

        # Calculate combined confidence (60% time-based + 40% model-based)
        hours_ahead = (row['timestamp'] - base_time).total_seconds() / 3600

        # Time-based confidence (60% weight)
        if prediction_type == 'hourly':
            time_confidence = max(50, 95 - (hours_ahead * 2))  # 95% at t+1h, drops to 50%
        else:
            # Daily: 85% at t+1d, drops to 30% at t+365d
            days_ahead = hours_ahead / 24
            time_confidence = max(30, 85 - (days_ahead * 0.15))

        # Model-based confidence from uncertainty (40% weight)
        if use_uncertainty and pred_wait > 0:
            # Calculate relative uncertainty (uncertainty / prediction)
            relative_uncertainty = uncertainties[idx] / max(pred_wait, 1)
            # Convert to confidence: low uncertainty = high confidence
            # Cap relative uncertainty at 100% (1.0) for confidence calculation
            model_confidence = max(30, 100 * (1 - min(relative_uncertainty, 1.0)))
        else:
            # Fallback: use time confidence if no uncertainty available
            model_confidence = time_confidence

        # Combined confidence (weighted average)
        confidence = 0.6 * time_confidence + 0.4 * model_confidence

        # Calculate crowd level based on predicted wait time vs baseline (rolling_avg_7d)
        baseline = row.get('rolling_avg_7d', 30.0)  # Default to 30 min if missing

        if baseline > 0:
            ratio = pred_wait / baseline
        else:
            ratio = 1.0  # Default if no baseline

        # Categorize crowd level
        if ratio < 0.2:
            crowd_level = 'very_low'
        elif ratio < 0.4:
            crowd_level = 'low'
        elif ratio < 0.7:
            crowd_level = 'moderate'
        elif ratio < 1.0:
            crowd_level = 'high'
        elif ratio < 1.3:
            crowd_level = 'very_high'
        else:
            crowd_level = 'extreme'

        results.append({
            'attractionId': row['attractionId'],
            'parkId': row['parkId'],  # Include parkId for schedule filtering
            'predictedTime': row['timestamp'].isoformat(),
            'predictedWaitTime': pred_wait,
            'predictionType': prediction_type,
            'confidence': round(confidence, 1),
            'crowdLevel': crowd_level,
            'baseline': round(baseline, 1),
            'modelVersion': model.version,
            'status': row['status'],
            'trend': 'stable' # Default
        })
        
        # Calculate Trend
        # Compare current prediction window to previous window or current actual
        # Here we only have the current prediction "row", so we need context.
        # But we can calculate trend if we look at the sequence of predictions for this attraction.
        # Since we are iterating row by row, this is hard.
        # A simpler way is to calc trend AFTER collecting all results for an attraction.
        # BUT, for single-point prediction (e.g. next 1h), we compare to current_wait_times!
        
        if current_wait_times and row['attractionId'] in current_wait_times:
            current_actual = current_wait_times[row['attractionId']]
            diff = pred_wait - current_actual
            if diff > 5:
                results[-1]['trend'] = 'increasing'
            elif diff < -5:
                results[-1]['trend'] = 'decreasing'
            else:
                results[-1]['trend'] = 'stable'
        elif idx > 0 and results[-2]['attractionId'] == row['attractionId']:
            # If no current actual, compare to previous hour prediction
            prev_pred = results[-2]['predictedWaitTime']
            diff = pred_wait - prev_pred
            if diff > 5:
                results[-1]['trend'] = 'increasing'
            elif diff < -5:
                results[-1]['trend'] = 'decreasing'
            else:
                results[-1]['trend'] = 'stable'
        
        # Override if status is CLOSED
        if row['status'] == 'CLOSED':
            results[-1]['predictedWaitTime'] = 0
            results[-1]['confidence'] = 100.0
            results[-1]['crowdLevel'] = 'closed'

    # NOTE: Schedule filtering will be applied after this function returns
    # See filter_predictions_by_schedule() for operating hours filtering
    return results


def predict_for_park(
    model: WaitTimeModel,
    park_id: str,
    prediction_type: str = 'hourly'
) -> List[Dict[str, Any]]:
    """
    Predict wait times for all attractions in a park

    Args:
        model: Trained model
        park_id: Park ID
        prediction_type: 'hourly' or 'daily'

    Returns:
        List of predictions
    """
    # TODO: Fetch attraction IDs for this park from database
    # For now, placeholder
    attraction_ids = []  # Would fetch from DB
    park_ids = [park_id] * len(attraction_ids)

    if len(attraction_ids) == 0:
        return []

    return predict_wait_times(
        model,
        attraction_ids,
        park_ids,
        prediction_type
    )
