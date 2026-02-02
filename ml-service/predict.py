"""
Prediction logic for hourly and daily forecasts
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from sqlalchemy import text

from model import WaitTimeModel
from percentile_features import add_percentile_features
from db import fetch_parks_metadata, get_db, fetch_holidays, convert_df_types
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


def fetch_recent_wait_times(
    attraction_ids: List[str], lookback_days: int = 730
) -> pd.DataFrame:
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
    query = text(
        """
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
    """
    )

    with get_db() as db:
        result = db.execute(
            query,
            {
                "attraction_ids": attraction_ids,
                "lookback_days": f"{lookback_days} days",
            },
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return convert_df_types(df)


def generate_future_timestamps(
    base_time: datetime, prediction_type: str
) -> List[datetime]:
    """
    Generate future timestamps for predictions

    Args:
        base_time: Starting time
        prediction_type: 'hourly' or 'daily'

    Returns:
        List of future timestamps
    """
    if prediction_type == "hourly":
        # Round base_time to the NEXT full hour (not current hour)
        # This ensures all predictions have timestamps like "2024-01-15T14:00:00"
        # If it's 14:37, round up to 15:00 (next hour)
        rounded_base = base_time.replace(minute=0, second=0, microsecond=0)
        if base_time.minute > 0 or base_time.second > 0 or base_time.microsecond > 0:
            rounded_base = rounded_base + timedelta(hours=1)

        # Next 24 hours from rounded base (starting from next full hour)
        return [
            rounded_base + timedelta(hours=i)
            for i in range(settings.HOURLY_PREDICTIONS)
        ]
    elif prediction_type == "daily":
        # Next 14 days (at 14:00 each day, typical peak time)
        return [
            (base_time + timedelta(days=i)).replace(
                hour=14, minute=0, second=0, microsecond=0
            )
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
    feature_context: Optional[Dict[str, Any]] = None,
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
            rows.append(
                {
                    "attractionId": attraction_id,
                    "parkId": park_id,
                    "timestamp": ts,
                }
            )

    df = pd.DataFrame(rows)

    # Convert parkId and attractionId to strings (for CatBoost)
    df["parkId"] = df["parkId"].astype(str)
    df["attractionId"] = df["attractionId"].astype(str)

    # Convert UTC timestamps to local park time using helper function
    # This matches the training pipeline in features.py::engineer_features()
    # Without this, hour/day_of_week/month features would be wrong (UTC instead of local)
    from features import convert_to_local_time

    # Fetch park metadata for timezone conversion (fetches all parks)
    # OPTIMIZATION: Cache this to avoid multiple DB queries
    parks_metadata = fetch_parks_metadata()

    df = convert_to_local_time(df, parks_metadata)

    # Add time features from LOCAL timestamp (not UTC!)
    df["hour"] = df["local_timestamp"].dt.hour
    df["day_of_week"] = df["local_timestamp"].dt.dayofweek
    df["month"] = df["local_timestamp"].dt.month

    # Cyclical time encoding
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    df["day_of_week_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["day_of_week_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    # NEW: Day of year (1-365/366) for finer seasonal trends
    df["day_of_year"] = df["local_timestamp"].dt.dayofyear
    df["day_of_year_sin"] = np.sin(2 * np.pi * df["day_of_year"] / 365.25)
    df["day_of_year_cos"] = np.cos(2 * np.pi * df["day_of_year"] / 365.25)

    # Season calculation - MUST use local_timestamp to match training pipeline
    # Using UTC timestamp would cause season to be wrong for parks in different timezones
    # Example: 23:00 UTC Feb 28 = 08:00 JST Mar 1 → Season jumps from Winter to Spring
    df["season"] = df["local_timestamp"].dt.month.apply(lambda m: (m % 12) // 3)

    # NEW: Peak season indicator (summer months + December holidays)
    df["is_peak_season"] = ((df["month"] >= 6) & (df["month"] <= 8)) | (
        df["month"] == 12
    )
    df["is_peak_season"] = df["is_peak_season"].astype(int)

    # Region-specific weekends
    # OPTIMIZATION: Use centralized function from features.py to avoid code duplication
    from features import add_weekend_feature

    df = add_weekend_feature(df, parks_metadata)

    # Weather features
    # Logic: Use provided hourly forecast if available, otherwise fallback to historical daily averages

    use_forecast = False
    if weather_forecast and len(weather_forecast) > 0:
        try:
            # Convert forecast objects to dicts if needed
            wf_data = [w.dict() if hasattr(w, "dict") else w for w in weather_forecast]
            wf_df = pd.DataFrame(wf_data)
            wf_df["time"] = pd.to_datetime(wf_df["time"])

            # Normalize both columns to timezone-naive UTC for robust merging
            # 1. Handle Weather Forecast DataFrame
            if wf_df["time"].dt.tz is not None:
                # Convert to UTC then remove timezone info
                wf_df["time"] = wf_df["time"].dt.tz_convert("UTC").dt.tz_localize(None)

            # 2. Handle Prediction DataFrame
            # Create join_time and ensure it is also timezone-naive UTC
            df["join_time"] = df["timestamp"].dt.round("h")
            if df["join_time"].dt.tz is not None:
                df["join_time"] = (
                    df["join_time"].dt.tz_convert("UTC").dt.tz_localize(None)
                )

            # Merge logic
            # Note: weather_forecast is assumed to apply to all parks in this batch (usually same park)
            df = df.merge(wf_df, left_on="join_time", right_on="time", how="left")

            # Map columns and fill defaults
            df["temperature_avg"] = df["temperature"].fillna(20.0)
            df["precipitation"] = df["precipitation"].fillna(0.0)
            df["windSpeedMax"] = df["windSpeed"].fillna(0.0)
            df["snowfallSum"] = df["snowfall"].fillna(0.0)
            df["weatherCode"] = df["weatherCode"].fillna(0).astype(int)

            # NEW: Weather interaction features
            # Precipitation last 3h (for forecast, we approximate with current precipitation * 3)
            df["precipitation_last_3h"] = (
                df["precipitation"] * 3.0
            )  # Approximation for forecast
            # Temperature deviation will be calculated later after we have monthly averages

            # Cleanup join columns
            df = df.drop(
                columns=[
                    "join_time",
                    "time",
                    "temperature",
                    "windSpeed",
                    "snowfall",
                    "rain",
                ],
                errors="ignore",
            )
            use_forecast = True
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f"Failed to use weather forecast in features: {e}. Falling back to DB."
            )
            use_forecast = False

    if not use_forecast:
        # Weather features (use seasonal averages from DB for better accuracy)
        # Get the month we're predicting for (use local time for accurate month)
        # Use the first timestamp's local month if available, otherwise UTC month
        if timestamps:
            # Try to use local timestamp if available (more accurate for timezone-aware predictions)
            if "local_timestamp" in df.columns and len(df) > 0:
                prediction_month = df["local_timestamp"].iloc[0].month
            else:
                prediction_month = timestamps[0].month
        else:
            prediction_month = datetime.now(timezone.utc).month

        weather_query = text(
            """
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
                AND date >= CURRENT_DATE - INTERVAL '3 years'  -- Use 3 years of historical data
            GROUP BY "parkId"
        """
        )

        try:
            with get_db() as db:
                result = db.execute(
                    weather_query,
                    {"park_ids": list(set(park_ids)), "month": prediction_month},
                )
                weather_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                weather_df = convert_df_types(weather_df)
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f"Failed to fetch historical weather data: {e}. Using defaults."
            )
            weather_df = pd.DataFrame()

        # Merge weather data
        if not weather_df.empty:
            df = df.merge(weather_df, left_on="parkId", right_on="parkId", how="left")
            df["temperature_avg"] = df["temp_avg"].fillna(20.0)
            df["precipitation"] = df["precip_avg"].fillna(0.0)
            df["windSpeedMax"] = df["wind_avg"].fillna(0.0)
            df["snowfallSum"] = df["snow_avg"].fillna(0.0)
            df["weatherCode"] = df["weather_code_mode"].fillna(0).astype(int)

            # NEW: Temperature deviation (current vs. monthly average)
            df["temperature_deviation"] = df["temperature_avg"] - df["temp_avg"].fillna(
                20.0
            )

            # NEW: Precipitation last 3h (for historical data, approximate with daily average)
            df["precipitation_last_3h"] = (
                df["precipitation"] * 0.125
            )  # Daily / 24h * 3h approximation

            df = df.drop(
                columns=[
                    "temp_avg",
                    "precip_avg",
                    "wind_avg",
                    "snow_avg",
                    "weather_code_mode",
                ],
                errors="ignore",
            )
        else:
            df["temperature_avg"] = 20.0
            df["precipitation"] = 0.0
            df["windSpeedMax"] = 0.0
            df["snowfallSum"] = 0.0
            df["weatherCode"] = 0
            df["temperature_deviation"] = 0.0
            df["precipitation_last_3h"] = 0.0

    # Calculate temperature deviation if not already set (for forecast data)
    if (
        "temperature_deviation" not in df.columns
        or df["temperature_deviation"].isna().any()
    ):
        # Get monthly average for each park
        if "month" in df.columns:
            monthly_avg = df.groupby(["parkId", "month"])["temperature_avg"].transform(
                "mean"
            )
            df["temperature_deviation"] = df["temperature_avg"] - monthly_avg
            df["temperature_deviation"] = df["temperature_deviation"].fillna(0)
        else:
            df["temperature_deviation"] = 0.0

    # Ensure precipitation_last_3h is set
    if "precipitation_last_3h" not in df.columns:
        df["precipitation_last_3h"] = df["precipitation"] * 0.125  # Approximation

    df["is_raining"] = (df["precipitation"] > 0).astype(int)

    # Holiday features
    df_start = df["timestamp"].min()
    df_end = df["timestamp"].max()

    all_countries = set()
    for _, park in parks_metadata.iterrows():
        all_countries.add(park["country"])

        # Legacy fallback

        # New JSON support
        raw_influences = park.get("influencingRegions")
        import json

        if isinstance(raw_influences, str):
            try:
                regions = json.loads(raw_influences)
                if regions:
                    all_countries.update(
                        [r["countryCode"] for r in regions if r.get("countryCode")]
                    )
            except Exception:
                pass
        elif isinstance(raw_influences, list):
            all_countries.update(
                [r["countryCode"] for r in raw_influences if r.get("countryCode")]
            )

    holidays_df = fetch_holidays(list(all_countries), df_start, df_end)

    if not holidays_df.empty:
        holidays_df["date"] = pd.to_datetime(holidays_df["date"])

        # Create lookup with type: {(country, region, date): type} AND {(country, date): type}
        holiday_lookup = {}
        for _, row in holidays_df.iterrows():
            # Key 1: With specific region (if available) -> (country, region, date)
            if row.get("region"):
                key_regional = (row["country"], row["region"], row["date"].date())
                holiday_lookup[key_regional] = row["holiday_type"]
            else:
                # Key 2: National holiday (no region) -> (country, None, date)??
                # Actually, my new loop checks (country, region, date) then (country, date).
                # So we should populate (country, date) for national holidays.
                key_national = (row["country"], row["date"].date())
                holiday_lookup[key_national] = row["holiday_type"]
    else:
        holiday_lookup = {}

    # Initialize holiday columns
    df["is_holiday_primary"] = 0
    df["is_school_holiday_primary"] = 0
    df["is_holiday_neighbor_1"] = 0
    df["is_holiday_neighbor_2"] = 0
    df["is_holiday_neighbor_3"] = 0
    df["holiday_count_total"] = 0
    df["school_holiday_count_total"] = 0
    df["is_school_holiday_any"] = 0  # NEW: Consolidated signal

    # Check holidays for each row
    for idx, row in df.iterrows():
        # Use LOCAL date for holiday lookup (matches training pipeline)
        date = row["local_timestamp"].date()
        park_info = parks_metadata[parks_metadata["park_id"] == row["parkId"]]

        if not park_info.empty:
            primary_country = park_info.iloc[0]["country"]

            # Parse influencing regions (JSON) or fallback to countries list
            influencing_regions = []
            raw_influences = park_info.iloc[0].get("influencingRegions")

            if isinstance(raw_influences, list):
                influencing_regions = raw_influences
            elif isinstance(raw_influences, str):
                import json

                try:
                    influencing_regions = json.loads(raw_influences)
                except Exception:
                    influencing_regions = []

            # Fallback for backward compatibility (older DB records)

            # Primary country holiday
            # Note: For prediction, we might not always have granular region for the park itself in metadata
            # unless we fetched it. db.fetch_parks_metadata was updated to include region_code.
            primary_region = park_info.iloc[0].get("region_code")

            primary_type = None
            # Check specific region match first
            if primary_region:
                primary_type = holiday_lookup.get(
                    (primary_country, primary_region, date)
                )  # Need to update lookup keys first!

            # If no regional match, check national (fallback/additive)
            if not primary_type:
                # Fallback to (country, None) or (country, date) depending on lookup structure
                # The existing lookup in predict.py (lines 318-321) uses (country, date).
                # We need to update that lookup construction too!
                primary_type = holiday_lookup.get((primary_country, date))

            df.at[idx, "is_holiday_primary"] = int(primary_type == "public")
            df.at[idx, "is_school_holiday_primary"] = int(primary_type == "school")

            # Neighbor holidays
            neighbor_public_flags = []
            neighbor_school_flags = []

            for region in influencing_regions[:3]:
                r_country = region["countryCode"]
                r_code = region["regionCode"]

                # Check specific region
                h_type = None
                if r_code:
                    h_type = holiday_lookup.get((r_country, r_code, date))

                # Fallback to national
                if not h_type:
                    h_type = holiday_lookup.get((r_country, date))

                neighbor_public_flags.append(int(h_type == "public"))
                neighbor_school_flags.append(int(h_type == "school"))

            # Fill neighbor columns (up to 3)
            if len(neighbor_public_flags) > 0:
                df.at[idx, "is_holiday_neighbor_1"] = neighbor_public_flags[0]
            if len(neighbor_public_flags) > 1:
                df.at[idx, "is_holiday_neighbor_2"] = neighbor_public_flags[1]
            if len(neighbor_public_flags) > 2:
                df.at[idx, "is_holiday_neighbor_3"] = neighbor_public_flags[2]

            df.at[idx, "holiday_count_total"] = sum(
                [df.at[idx, "is_holiday_primary"]] + neighbor_public_flags
            )
            df.at[idx, "school_holiday_count_total"] = sum(
                [df.at[idx, "is_school_holiday_primary"]] + neighbor_school_flags
            )

            # "Any School Holiday" Logic (Critical for ML parity)
            has_local_school = df.at[idx, "is_school_holiday_primary"] == 1
            has_neighbor_school = sum(neighbor_school_flags) > 0
            df.at[idx, "is_school_holiday_any"] = int(
                has_local_school or has_neighbor_school
            )

    # Park schedule features (check if park is open at predicted time)
    schedule_query = text(
        """
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
    """
    )

    with get_db() as db:
        # Determine date range for schedule query using park local timezone
        # Schedules are stored as DATE type (park's local calendar dates)
        # We must query using dates in the park's timezone, not UTC
        # df has 'local_timestamp' column added by convert_to_local_time() earlier
        if "local_timestamp" in df.columns:
            # Extract date range from LOCAL timestamps (already in park TZ)
            start_date_local = df["local_timestamp"].min().date()
            end_date_local = df["local_timestamp"].max().date()
        else:
            # Fallback to UTC dates (should not happen in production)
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                "No local_timestamp column - using UTC for schedule dates (may miss boundary dates)"
            )
            # FIX: Use df['timestamp'] instead of undefined df_start/df_end
            start_date_local = df["timestamp"].min().date()
            end_date_local = df["timestamp"].max().date()

        result = db.execute(
            schedule_query,
            {
                "park_ids": list(set(park_ids)),
                "start_date": start_date_local,
                "end_date": end_date_local,
            },
        )
        schedules_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Initialize schedule features and status
    df["is_park_open"] = 1  # Assume open if no schedule found
    df["has_special_event"] = 0
    df["has_extra_hours"] = 0
    df["status"] = "OPERATING"

    if not schedules_df.empty:
        schedules_df["openingTime"] = pd.to_datetime(schedules_df["openingTime"])
        schedules_df["closingTime"] = pd.to_datetime(schedules_df["closingTime"])
        schedules_df["date"] = pd.to_datetime(schedules_df["date"])

        # Ensure attractionId is treated as string (handle NaN/None)
        if "attractionId" in schedules_df.columns:
            schedules_df["attractionId"] = (
                schedules_df["attractionId"].fillna("nan").astype(str)
            )

        for idx, row in df.iterrows():
            park_id = row["parkId"]
            attraction_id = str(row["attractionId"])
            timestamp = row["timestamp"]

            # Use local timestamp for schedule date comparison
            # Schedules are stored as local calendar dates (DATE type in DB)
            # We must compare with the date in the park's timezone, not UTC
            if "local_timestamp" in row.index and pd.notna(row["local_timestamp"]):
                local_ts = row["local_timestamp"]
                date_only = pd.Timestamp(local_ts.date())
            else:
                # Fallback to UTC date (defensive, should not happen)
                date_only = pd.Timestamp(timestamp.date())

            # Filter schedules for this park/date
            # We want:
            # 1. Park schedules (attractionId is null/nan)
            # 2. Attraction schedules (attractionId matches)
            park_schedules = schedules_df[
                (schedules_df["parkId"] == park_id)
                & (schedules_df["date"] == date_only)
            ]

            if not park_schedules.empty:
                # 1. Check Park Status & Opening Hours
                # Park-wide schedules usually have attractionId = None (or 'nan' after string conversion)
                global_schedules = park_schedules[
                    (park_schedules["attractionId"] == "nan")
                    | (park_schedules["attractionId"] == "None")
                    | (park_schedules["attractionId"].isna())
                ]

                if not global_schedules.empty:
                    # Operating hours
                    operating = global_schedules[
                        global_schedules["scheduleType"] == "OPERATING"
                    ]
                    if not operating.empty:
                        opening = operating.iloc[0]["openingTime"]
                        closing = operating.iloc[0]["closingTime"]

                        # Robust comparison handling mixed timezones
                        ts_compare = timestamp

                        # case 1: DB has timezone, we need to match it
                        if getattr(opening, "tzinfo", None) is not None:
                            if ts_compare.tzinfo is None:
                                ts_compare = pd.Timestamp(ts_compare).tz_localize("UTC")
                            ts_compare = ts_compare.tz_convert(opening.tzinfo)
                        # case 2: DB is naive, we must be naive (UTC)
                        else:
                            if ts_compare.tzinfo is not None:
                                ts_compare = ts_compare.tz_convert("UTC").tz_localize(
                                    None
                                )

                        is_open = opening <= ts_compare <= closing
                        df.at[idx, "is_park_open"] = int(is_open)

                        if not is_open:
                            df.at[idx, "status"] = "CLOSED"

                    # Special events / Extra hours
                    if any(
                        global_schedules["scheduleType"].isin(
                            ["TICKETED_EVENT", "PRIVATE_EVENT"]
                        )
                    ):
                        df.at[idx, "has_special_event"] = 1

                    if "EXTRA_HOURS" in global_schedules["scheduleType"].values:
                        df.at[idx, "has_extra_hours"] = 1

                # 2. Check Specific Attraction Status (Overrides park status if strictly closed/maintenance)
                # But typically if park is closed, attraction is closed.
                # If park is open, attraction might be maintenance.
                attr_schedules = park_schedules[
                    park_schedules["attractionId"] == attraction_id
                ]

                if not attr_schedules.empty:
                    # Check for MAINTENANCE or CLOSED
                    if any(
                        attr_schedules["scheduleType"].isin(["MAINTENANCE", "CLOSED"])
                    ):
                        df.at[idx, "status"] = (
                            "CLOSED"  # Or 'MAINTENANCE' specifically if we want distinct status
                        )
                        # If attraction is closed, is_park_open feature for the model implies "can guests ride?"
                        # technically park is open but this ride isn't.
                        # But we are overriding prediction anyway, so feature value matters less.

    # Historical features (most important!)
    # Fetch up to 2 years of aggregated daily data (efficient for large datasets)
    recent_data = fetch_recent_wait_times(attraction_ids, lookback_days=730)

    # Initialize with defaults
    df["avg_wait_last_24h"] = 30.0
    df["avg_wait_last_1h"] = 30.0
    df["avg_wait_same_hour_last_week"] = 35.0
    df["avg_wait_same_hour_last_month"] = 35.0  # NEW: Monthly trend
    df["rolling_avg_7d"] = 32.0
    df["trend_7d"] = 0.0  # NEW: 7-day trend (0 = no trend)

    if not recent_data.empty:
        recent_data["date"] = pd.to_datetime(recent_data["date"])
        # Ensure timezone-naive for comparison with cutoff dates
        if recent_data["date"].dt.tz is not None:
            recent_data["date"] = recent_data["date"].dt.tz_localize(None)

        # OPTIMIZATION: Use local time for historical feature lookups
        # The data in recent_data is aggregated by UTC hour (from fetch_recent_wait_times)
        # We convert base_time to local time for each park to improve accuracy
        # Note: For perfect accuracy, fetch_recent_wait_times() should aggregate by local hour,
        # but this approximation works well for most use cases.

        # Convert base_time to pandas Timestamp (timezone-aware UTC)
        base_time_pd = pd.Timestamp(base_time)
        if base_time_pd.tzinfo is None:
            base_time_pd = base_time_pd.tz_localize("UTC")
        else:
            base_time_pd = base_time_pd.tz_convert("UTC")

        # Get park timezone for each attraction
        attraction_to_park = dict(zip(attraction_ids, park_ids))
        park_timezones = {}
        for park_id in set(park_ids):
            park_info = parks_metadata[parks_metadata["park_id"] == park_id]
            if not park_info.empty:
                park_timezones[park_id] = park_info.iloc[0]["timezone"]

        for attraction_id in attraction_ids:
            attraction_data = recent_data[recent_data["attractionId"] == attraction_id]
            park_id = attraction_to_park.get(attraction_id)
            park_tz = park_timezones.get(park_id) if park_id else None

            if not attraction_data.empty:
                # Overall average (all data)
                overall_avg = attraction_data["avg_wait"].mean()

                # Convert base_time to local time for this park
                if park_tz:
                    try:
                        import pytz

                        tz = pytz.timezone(park_tz)
                        base_time_local = base_time_pd.tz_convert(tz)
                    except Exception:
                        # Fallback to UTC if timezone conversion fails
                        base_time_local = base_time_pd
                else:
                    base_time_local = base_time_pd

                # Last 7 days average (rolling_avg_7d) - use local date
                cutoff_7d_local = (base_time_local - timedelta(days=7)).date()
                last_7_days = attraction_data[
                    pd.to_datetime(attraction_data["date"]).dt.date >= cutoff_7d_local
                ]
                rolling_7d = (
                    last_7_days["avg_wait"].mean()
                    if len(last_7_days) > 0
                    else overall_avg
                )

                # Last 24h average (approximation: today + yesterday average) - use local date
                cutoff_24h_local = (base_time_local - timedelta(days=1)).date()
                last_24h = attraction_data[
                    pd.to_datetime(attraction_data["date"]).dt.date >= cutoff_24h_local
                ]
                avg_24h = (
                    last_24h["avg_wait"].mean() if len(last_24h) > 0 else rolling_7d
                )

                # Last 1h average (Lag 1) - use local hour
                # Note: Data is still UTC-aggregated, but we use local hour for lookup
                # This is an approximation - for perfect accuracy, data should be aggregated by local hour
                prev_hour_dt_local = base_time_local - timedelta(hours=1)
                prev_hour_local = prev_hour_dt_local.hour
                prev_hour_date_local = prev_hour_dt_local.date()

                # Try local hour first
                last_1h_data = attraction_data[
                    (
                        pd.to_datetime(attraction_data["date"]).dt.date
                        == prev_hour_date_local
                    )
                    & (attraction_data["hour"] == prev_hour_local)
                ]

                # Fallback: Try UTC hour if local hour doesn't match (timezone offset)
                if last_1h_data.empty:
                    prev_hour_utc = (base_time_pd - timedelta(hours=1)).hour
                    prev_hour_date_utc = (base_time_pd - timedelta(hours=1)).date()
                    last_1h_data = attraction_data[
                        (
                            pd.to_datetime(attraction_data["date"]).dt.date
                            == prev_hour_date_utc
                        )
                        & (attraction_data["hour"] == prev_hour_utc)
                    ]

                # Intermediate Fallback: Same time yesterday (better than daily average)
                yesterday_prev_hour_date = prev_hour_date_local - timedelta(days=1)
                yesterday_fallback = attraction_data[
                    (
                        pd.to_datetime(attraction_data["date"]).dt.date
                        == yesterday_prev_hour_date
                    )
                    & (attraction_data["hour"] == prev_hour_local)
                ]
                avg_yesterday_fallback = (
                    yesterday_fallback["avg_wait"].mean()
                    if not yesterday_fallback.empty
                    else avg_24h
                )

                avg_1h = (
                    last_1h_data["avg_wait"].mean()
                    if not last_1h_data.empty
                    else avg_yesterday_fallback
                )

                # Same hour last week (7 days ago, same hour in local time)
                last_week_date_local = (base_time_local - timedelta(days=7)).date()
                current_hour_local = base_time_local.hour

                same_hour_last_week = attraction_data[
                    (
                        pd.to_datetime(attraction_data["date"]).dt.date
                        == last_week_date_local
                    )
                    & (attraction_data["hour"] == current_hour_local)
                ]
                avg_same_hour = (
                    same_hour_last_week["avg_wait"].mean()
                    if len(same_hour_last_week) > 0
                    else rolling_7d
                )

                # Same hour last month (30 days ago, same hour in local time) - NEW
                last_month_date_local = (base_time_local - timedelta(days=30)).date()
                same_hour_last_month = attraction_data[
                    (
                        pd.to_datetime(attraction_data["date"]).dt.date
                        == last_month_date_local
                    )
                    & (attraction_data["hour"] == current_hour_local)
                ]
                avg_same_hour_month = (
                    same_hour_last_month["avg_wait"].mean()
                    if len(same_hour_last_month) > 0
                    else rolling_7d
                )

                # Calculate 7-day trend (slope) - NEW
                # Simple approximation: compare last 7 days average to previous 7 days average
                cutoff_14d_local = (base_time_local - timedelta(days=14)).date()
                last_14_days = attraction_data[
                    pd.to_datetime(attraction_data["date"]).dt.date >= cutoff_14d_local
                ]

                trend_7d = 0.0
                if len(last_14_days) >= 2:
                    # Split into two 7-day periods
                    # mid_point = cutoff_7d_local (unused)

                    recent_7d = last_14_days[
                        pd.to_datetime(last_14_days["date"]).dt.date >= cutoff_7d_local
                    ]
                    previous_7d = last_14_days[
                        pd.to_datetime(last_14_days["date"]).dt.date < cutoff_7d_local
                    ]

                    if len(recent_7d) > 0 and len(previous_7d) > 0:
                        recent_avg = recent_7d["avg_wait"].mean()
                        previous_avg = previous_7d["avg_wait"].mean()
                        # Trend = difference per day (approximation)
                        trend_7d = (recent_avg - previous_avg) / 7.0

                # Calculate volatility (std of last 7d), dampened with log(1+x) to match training
                volatility_7d = 0.0
                if len(last_7_days) > 1:
                    raw_std = last_7_days["avg_wait"].std()
                    if not pd.isna(raw_std) and raw_std >= 0:
                        volatility_7d = np.log1p(raw_std)

                # Apply to all rows for this attraction
                mask = df["attractionId"] == attraction_id
                df.loc[mask, "avg_wait_last_24h"] = avg_24h
                df.loc[mask, "avg_wait_last_1h"] = avg_1h
                df.loc[mask, "avg_wait_same_hour_last_week"] = avg_same_hour
                df.loc[mask, "avg_wait_same_hour_last_month"] = avg_same_hour_month
                df.loc[mask, "rolling_avg_7d"] = rolling_7d
                df.loc[mask, "trend_7d"] = trend_7d
                df.loc[mask, "volatility_7d"] = volatility_7d

    # Calculate wait time velocity (momentum) BEFORE overriding lags
    # Initialize with default (no change)
    df["wait_time_velocity"] = 0.0

    # Initialize trend and volatility if not already set
    if "trend_7d" not in df.columns:
        df["trend_7d"] = 0.0
    if "volatility_7d" not in df.columns:
        df["volatility_7d"] = 0.0

    # Override lags with current wait times if available (Autoregression)
    if current_wait_times:
        for attraction_id, wait_time in current_wait_times.items():
            if wait_time is not None:
                mask = df["attractionId"] == str(attraction_id)
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
                    elif not pd.isna(df.loc[mask, "avg_wait_last_1h"].iloc[0]):
                        recent_avg = df.loc[mask, "avg_wait_last_1h"].iloc[0]
                        velocity = (float(wait_time) - recent_avg) / 6.0

                    df.loc[mask, "wait_time_velocity"] = velocity

                    # Now override the lag feature with current value
                    df.loc[mask, "avg_wait_last_1h"] = float(wait_time)

    # Add percentile features (Weather extremes)
    df = add_percentile_features(df)

    # Add attraction and park features (using available data only)
    from attraction_features import (
        add_attraction_type_feature,
        add_park_attraction_count_feature,
    )

    df = add_attraction_type_feature(df)
    df = add_park_attraction_count_feature(df, parks_metadata)

    # Phase 2: Add real-time context features
    if feature_context:
        from features import (
            add_park_occupancy_feature,
            add_time_since_park_open,
            add_downtime_features,
            add_virtual_queue_feature,
            add_bridge_day_feature,
            add_park_has_schedule_feature,
            add_interaction_features,
        )

        df = add_park_occupancy_feature(df, feature_context)
        df = add_time_since_park_open(df, feature_context)
        df = add_downtime_features(df, feature_context)
        df = add_virtual_queue_feature(df, feature_context)
        df = add_park_has_schedule_feature(
            df, feature_context
        )  # NEW: Data quality indicator

        # Bridge day needs metadata refetch ideally, but for inference we rely on feature_context
        # If feature_context has it, great. If not, add_bridge_day_feature will attempt fallback or skip.
        # But add_bridge_day_feature requires parks_metadata, start_date, end_date for fallback.
        # For inference, if we lack context, we might skip expensive fallback or fetch metadata.
        # Let's pass what we have.
        # Note: add_bridge_day_feature signature: (df, parks_metadata, start_date, end_date, feature_context)
        # OPTIMIZATION: Reuse parks_metadata from line 174 instead of fetching again
        start = df["timestamp"].min()
        end = df["timestamp"].max()
        df = add_bridge_day_feature(df, parks_metadata, start, end, feature_context)

        # Interaction features (must be after all base features are added)
        df = add_interaction_features(df)
    else:
        # Defaults if no feature_context provided
        df["park_occupancy_pct"] = 100.0
        df["time_since_park_open_mins"] = 0.0
        df["had_downtime_today"] = 0
        df["downtime_minutes_today"] = 0.0
        df["has_virtual_queue"] = 0
        df["is_bridge_day"] = 0
        df["park_has_schedule"] = (
            1  # NEW: Default to 1 (assume schedule exists for better quality)
        )

        # Interaction features with defaults
        from features import add_interaction_features

        df = add_interaction_features(df)

    return df


def predict_wait_times(
    model: WaitTimeModel,
    attraction_ids: List[str],
    park_ids: List[str],
    prediction_type: str = "hourly",
    base_time: Optional[datetime] = None,
    weather_forecast: Optional[List[Dict[str, Any]]] = None,
    current_wait_times: Optional[Dict[str, int]] = None,
    recent_wait_times: Optional[Dict[str, int]] = None,
    feature_context: Optional[Dict[str, Any]] = None,
    p50_baseline: Optional[float] = None,  # NEW: P50 baseline for crowd level
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
    # Reduced logging - only log summary, not details

    # Features
    features_df = create_prediction_features(
        attraction_ids,
        park_ids,
        timestamps,
        base_time,
        weather_forecast,
        current_wait_times,
        recent_wait_times,
        feature_context,
    )

    # Predict with uncertainty estimation
    try:
        uncertainty_results = model.predict_with_uncertainty(features_df)
        predictions = uncertainty_results["predictions"]
        uncertainties = uncertainty_results["uncertainty"]
        use_uncertainty = True
    except Exception as e:
        # Fallback to regular predictions if uncertainty estimation fails
        # Reduced logging - only log if it's a real issue
        if "missing" not in str(e).lower():
            print(f"⚠️  Uncertainty estimation failed: {e}")
        predictions = model.predict(features_df)
        uncertainties = np.zeros(len(predictions))
        use_uncertainty = False

    # Format results
    results = []
    for idx, row in features_df.iterrows():
        pred_wait = round_to_nearest_5(predictions[idx])

        # Calculate combined confidence (60% time-based + 40% model-based)
        hours_ahead = (row["timestamp"] - base_time).total_seconds() / 3600

        # Time-based confidence (60% weight)
        if prediction_type == "hourly":
            time_confidence = max(
                50, 95 - (hours_ahead * 2)
            )  # 95% at t+1h, drops to 50%
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

        # Calculate crowd level based on predicted wait time vs P50 baseline
        # P50 BASELINE SYSTEM:
        # - Uses P50 (median) from TypeScript service (passed via API)
        # - Replaces internal rolling_avg_7d calculation
        # - Ensures TypeScript and Python produce IDENTICAL crowd levels
        #
        # Fallback chain (graceful degradation):
        #   1. P50 baseline from API (preferred)
        #   2. rolling_avg_7d from features (legacy)
        #   3. Default 30 min (safeguard)
        if p50_baseline is not None and p50_baseline > 0:
            baseline = p50_baseline  # Use P50 baseline from TypeScript
        else:
            # Fallback to rolling_avg_7d during migration period
            baseline = row.get("rolling_avg_7d", 30.0)

        if baseline > 0:
            ratio = pred_wait / baseline
        else:
            ratio = 1.0  # Default if no baseline

        # Categorize crowd level using P50-RELATIVE THRESHOLDS
        # MUST MATCH TypeScript determineCrowdLevel() EXACTLY!
        # TypeScript: 50, 79, 120, 170, 250
        # P50 (100%) = "moderate" (expected/typical baseline)
        occupancy_pct = ratio * 100
        if occupancy_pct <= 50:
            crowd_level = "very_low"
        elif occupancy_pct <= 79:
            crowd_level = "low"
        elif occupancy_pct <= 120:  # 80-120%: ±20% around P50 = moderate
            crowd_level = "moderate"
        elif occupancy_pct <= 170:
            crowd_level = "high"
        elif occupancy_pct <= 250:
            crowd_level = "very_high"
        else:
            crowd_level = "extreme"

        results.append(
            {
                "attractionId": row["attractionId"],
                "parkId": row["parkId"],  # Include parkId for schedule filtering
                "predictedTime": row["timestamp"].isoformat(),
                "predictedWaitTime": pred_wait,
                "predictionType": prediction_type,
                "confidence": round(confidence, 1),
                "crowdLevel": crowd_level,
                "baseline": round(baseline, 1),
                "modelVersion": model.version,
                "status": row["status"],
                "trend": "stable",  # Default
            }
        )

        # Calculate Trend
        # Compare current prediction window to previous window or current actual
        # Here we only have the current prediction "row", so we need context.
        # But we can calculate trend if we look at the sequence of predictions for this attraction.
        # Since we are iterating row by row, this is hard.
        # A simpler way is to calc trend AFTER collecting all results for an attraction.
        # BUT, for single-point prediction (e.g. next 1h), we compare to current_wait_times!

        if current_wait_times and row["attractionId"] in current_wait_times:
            current_actual = current_wait_times[row["attractionId"]]
            diff = pred_wait - current_actual
            if diff > 5:
                results[-1]["trend"] = "increasing"
            elif diff < -5:
                results[-1]["trend"] = "decreasing"
            else:
                results[-1]["trend"] = "stable"
        elif idx > 0 and results[-2]["attractionId"] == row["attractionId"]:
            # If no current actual, compare to previous hour prediction
            prev_pred = results[-2]["predictedWaitTime"]
            diff = pred_wait - prev_pred
            if diff > 5:
                results[-1]["trend"] = "increasing"
            elif diff < -5:
                results[-1]["trend"] = "decreasing"
            else:
                results[-1]["trend"] = "stable"

        # Override if status is CLOSED
        if row["status"] == "CLOSED":
            results[-1]["predictedWaitTime"] = 0
            results[-1]["confidence"] = 100.0
            results[-1]["crowdLevel"] = "closed"

    # NOTE: Schedule filtering will be applied after this function returns
    # See filter_predictions_by_schedule() for operating hours filtering
    return results


def predict_for_park(
    model: WaitTimeModel, park_id: str, prediction_type: str = "hourly"
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

    return predict_wait_times(model, attraction_ids, park_ids, prediction_type)
