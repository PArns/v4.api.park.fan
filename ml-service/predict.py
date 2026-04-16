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
from holiday_utils import normalize_region_code

settings = get_settings()

# Cache for weather historical data (1 hour TTL)
_weather_historical_cache = {}
_weather_historical_cache_ttl = 3600  # 1 hour


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


def add_attraction_type_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add attraction type heuristics based on attraction name.
    """
    if "attractionName" not in df.columns:
        df["is_coaster"] = 0
        df["is_water_ride"] = 0
        df["is_indoor"] = 0
        df["is_wind_sensitive"] = 0
        return df

    import re

    # 1. Coasters (Achterbahnen)
    coaster_patterns = [
        r"coaster",
        r"achterbahn",
        r"montaña rusa",
        r"grand huit",
        r"express",
        r"mountain",
        r"roller",
        r"spinning",
        r"inverted",
        r"hyper",
        r"giga",
        r"strata",
        r"wooden",
        r"steel",
        r"taron",
        r"f\.l\.y\.",
        r"baron",
        r"mamba",
        r"karacho",
        r"desert",
        r"helix",
        r"shambhala",
        r"kondaa",
        r"untamed",
        r"velocicoaster",
    ]
    coaster_regex = re.compile("|".join(coaster_patterns), re.IGNORECASE)

    # 2. Water Rides (Wasserbahnen)
    water_patterns = [
        r"water",
        r"wasser",
        r"splash",
        r"river",
        r"flume",
        r"rapids",
        r"log",
        r"chiapas",
        r"rafting",
        r"pirates",
        r"falls",
        r"tidal",
        r"wave",
        r"bayou",
        r"lagoon",
        r"monsoon",
        r"plunge",
        r"chutes",
        r"viking",
        r"atlantis",
    ]
    water_regex = re.compile("|".join(water_patterns), re.IGNORECASE)

    # 3. Indoor Rides (Themenfahrten / Shows)
    indoor_patterns = [
        r"cinema",
        r"4d",
        r"3d",
        r"5d",
        r"theater",
        r"indoor",
        r"dark",
        r"mansion",
        r"mous au chocolat",
        r"hotel",
        r"museum",
        r"quest",
        r"voyage",
        r"haunted",
        r"ghost",
        r"secret",
        r"adventure",
        r"expedition",
        r"tales",
        r"piraten",
        r"small world",
        r"buzz lightyear",
        r"spiderman",
        r"transformers",
    ]
    indoor_regex = re.compile("|".join(indoor_patterns), re.IGNORECASE)

    # Apply heuristics
    names = df["attractionName"].astype(str)
    df["is_coaster"] = names.apply(
        lambda x: 1 if coaster_regex.search(x) else 0
    ).astype(int)
    df["is_water_ride"] = names.apply(
        lambda x: 1 if water_regex.search(x) else 0
    ).astype(int)
    df["is_indoor"] = names.apply(lambda x: 1 if indoor_regex.search(x) else 0).astype(
        int
    )

    # Wind sensitive: mostly high coasters and towers
    wind_patterns = [
        r"tower",
        r"sky",
        r"high",
        r"drop",
        r"starflyer",
        r"giant",
        r"loop",
        r"swing",
        r"flyer",
        r"wheel",
    ]
    wind_regex = re.compile("|".join(wind_patterns), re.IGNORECASE)
    df["is_wind_sensitive"] = (
        (df["is_coaster"] == 1)
        | names.apply(lambda x: 1 if wind_regex.search(x) else 0)
    ).astype(int)

    return df


# Cache for recent wait times (short-lived, 2 minutes)
_recent_wait_times_cache = {}
_recent_wait_times_cache_ttl = 120  # 2 minutes


def fetch_recent_wait_times(
    attraction_ids: List[str], lookback_days: int = 730
) -> pd.DataFrame:
    """
    Fetch recent wait times aggregated by day for historical features

    OPTIMIZATION:
    - Caches results for 2 minutes (burst request protection)
    - Pre-computes rolling averages in DB instead of Python
    - Uses window functions for efficiency

    Args:
        attraction_ids: List of attraction IDs
        lookback_days: How many days to look back (default: 730 = 2 years)

    Returns:
        DataFrame with daily aggregated queue data + pre-computed rolling averages
    """
    if not attraction_ids:
        return pd.DataFrame()

    # Create cache key from sorted attraction IDs
    cache_key = f"{','.join(sorted(attraction_ids))}:{lookback_days}"

    # Check cache
    if cache_key in _recent_wait_times_cache:
        cached_data, cache_time = _recent_wait_times_cache[cache_key]
        import time

        if time.time() - cache_time < _recent_wait_times_cache_ttl:
            return cached_data.copy()

    # OPTIMIZATION: Pre-compute rolling averages in DB using window functions
    # This avoids shipping raw data to Python and doing calculations there
    query = text(
        """
        WITH hourly_agg AS (
            SELECT
                qd."attractionId"::text as "attractionId",
                a.name as "attractionName",
                DATE(qd.timestamp AT TIME ZONE p.timezone) as date,
                EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE p.timezone) as hour,
                EXTRACT(DOW FROM qd.timestamp AT TIME ZONE p.timezone) as day_of_week,
                AVG(qd."waitTime") as avg_wait,
                COUNT(*) as data_points
            FROM queue_data qd
            INNER JOIN attractions a ON a.id = qd."attractionId"
            INNER JOIN parks p ON p.id = a."parkId"
            LEFT JOIN schedule_entries se
                ON se."parkId" = a."parkId"
                AND se.date = DATE(qd.timestamp AT TIME ZONE p.timezone)
                AND se."attractionId" IS NULL
            WHERE qd."attractionId"::text = ANY(:attraction_ids)
                AND qd.timestamp >= NOW() - :lookback_days * INTERVAL '1 day'
                AND qd."waitTime" IS NOT NULL
                AND qd."waitTime" >= 5
                AND qd.status = 'OPERATING'
                AND qd."queueType" = 'STANDBY'
                AND (se.id IS NULL OR se."scheduleType" = 'OPERATING')
            GROUP BY qd."attractionId", a.name, DATE(qd.timestamp AT TIME ZONE p.timezone),
                     EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE p.timezone),
                     EXTRACT(DOW FROM qd.timestamp AT TIME ZONE p.timezone)
        )
        SELECT
            "attractionId",
            "attractionName",
            date,
            hour,
            day_of_week,
            avg_wait,
            data_points,
            -- Pre-compute 7-day rolling average in DB (window function)
            AVG(avg_wait) OVER (
                PARTITION BY "attractionId"
                ORDER BY date, hour
                ROWS BETWEEN 167 PRECEDING AND CURRENT ROW  -- 7 days * 24 hours = 168 rows
            ) as rolling_avg_7d,
            -- Pre-compute standard deviation for volatility
            STDDEV(avg_wait) OVER (
                PARTITION BY "attractionId"
                ORDER BY date, hour
                ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
            ) as rolling_std_7d,
            -- Weekday (Mon-Fri, DOW 1-5) rolling average: help model distinguish load patterns
            AVG(CASE WHEN day_of_week BETWEEN 1 AND 5 THEN avg_wait END) OVER (
                PARTITION BY "attractionId"
                ORDER BY date, hour
                ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
            ) as rolling_avg_weekday,
            -- Weekend (Sat-Sun, DOW 0 and 6 in Postgres) rolling average
            AVG(CASE WHEN day_of_week IN (0, 6) THEN avg_wait END) OVER (
                PARTITION BY "attractionId"
                ORDER BY date, hour
                ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
            ) as rolling_avg_weekend,
            -- Weekend/weekday standard deviation (for split volatility)
            STDDEV(CASE WHEN day_of_week BETWEEN 1 AND 5 THEN avg_wait END) OVER (
                PARTITION BY "attractionId"
                ORDER BY date, hour
                ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
            ) as rolling_std_weekday,
            STDDEV(CASE WHEN day_of_week IN (0, 6) THEN avg_wait END) OVER (
                PARTITION BY "attractionId"
                ORDER BY date, hour
                ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
            ) as rolling_std_weekend
        FROM hourly_agg
        ORDER BY "attractionId", date DESC, hour
    """
    )

    with get_db() as db:
        result = db.execute(
            query,
            {
                "attraction_ids": attraction_ids,
                "lookback_days": lookback_days,
            },
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        df = convert_df_types(df)

        # Update cache
        import time

        _recent_wait_times_cache[cache_key] = (df.copy(), time.time())

        return df


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

    # Fetch historical occupancy for future prediction rows (avoids using stale real-time value)
    from db import fetch_historical_park_occupancy

    unique_park_ids = list(set(park_ids))
    hist_occ = fetch_historical_park_occupancy(unique_park_ids)
    if feature_context is not None:
        feature_context = dict(feature_context)  # don't mutate caller's dict
        feature_context["historicalOccupancy"] = hist_occ
        feature_context["baseTime"] = base_time
    # When feature_context is None, hist_occ is used directly in the else branch
    # so only park_occupancy_pct benefits (bridge day / other features keep their defaults)

    df = convert_to_local_time(df, parks_metadata)

    # Add time features from LOCAL timestamp (not UTC!)
    df["hour"] = df["local_timestamp"].dt.hour
    df["day_of_week"] = df["local_timestamp"].dt.dayofweek
    df["month"] = df["local_timestamp"].dt.month
    df["date_local"] = df["local_timestamp"].dt.date

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

    # Initialize bridge day flag (overwritten by context or logic below)
    df["is_bridge_day"] = 0

    # Region-specific weekends
    # OPTIMIZATION: Use centralized function from features.py to avoid code duplication
    from features import add_weekend_feature

    df = add_weekend_feature(df, parks_metadata)

    # Weather features
    # Logic: Use provided hourly forecast if available, otherwise fallback to historical daily averages

    use_forecast = False
    historical_temp_avg = {}  # Map for temperature deviation baseline

    if weather_forecast and len(weather_forecast) > 0:
        try:
            # Convert forecast objects to dicts if needed
            wf_data = [w.dict() if hasattr(w, "dict") else w for w in weather_forecast]
            wf_df = pd.DataFrame(wf_data)
            wf_df["time"] = pd.to_datetime(wf_df["time"])

            # Normalize both columns to timezone-naive UTC for robust merging
            if wf_df["time"].dt.tz is not None:
                wf_df["time"] = wf_df["time"].dt.tz_convert("UTC").dt.tz_localize(None)

            df["join_time"] = df["timestamp"].dt.round("h")
            if df["join_time"].dt.tz is not None:
                df["join_time"] = (
                    df["join_time"].dt.tz_convert("UTC").dt.tz_localize(None)
                )

            # Merge logic
            df = df.merge(wf_df, left_on="join_time", right_on="time", how="left")

            # Map columns and fill defaults
            # Use sinusoidal interpolation if we have hour and potentially min/max
            df["temperature_avg"] = df["temperature"].fillna(20.0)
            df["precipitation"] = df["precipitation"].fillna(0.0)
            df["windSpeedMax"] = df["windSpeed"].fillna(0.0)
            df["snowfallSum"] = df["snowfall"].fillna(0.0)
            df["weatherCode"] = df["weatherCode"].fillna(0).astype(int)

            # NEW: Weather interaction features
            df["precipitation_last_3h"] = df["precipitation"] * 3.0  # Approx

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

    if True:  # Always fetch DB weather for deviation baseline
        # Get the month we're predicting for
        if "local_timestamp" in df.columns and len(df) > 0:
            prediction_month = df["local_timestamp"].iloc[0].month
        elif timestamps:
            prediction_month = timestamps[0].month
        else:
            prediction_month = datetime.now(timezone.utc).month

        # OPTIMIZATION: Cache weather historical averages (1 hour TTL)
        cache_key = f"{','.join(sorted(set(park_ids)))}:{prediction_month}"
        weather_db_df = None

        if cache_key in _weather_historical_cache:
            cached_data, cache_time = _weather_historical_cache[cache_key]
            import time

            if time.time() - cache_time < _weather_historical_cache_ttl:
                weather_db_df = cached_data.copy()

        if weather_db_df is None:
            # Cache miss - load from DB
            # We now fetch Max/Min even for predict to allow sinusoidal fallback
            weather_query = text("""
                SELECT
                    "parkId"::text as "parkId",
                    AVG("temperatureMax") as temp_max,
                    AVG("temperatureMin") as temp_min,
                    AVG("temperatureMax" + "temperatureMin") / 2 as temp_avg,
                    AVG("precipitationSum") as precip_avg,
                    AVG("windSpeedMax") as wind_avg,
                    AVG("snowfallSum") as snow_avg,
                    MODE() WITHIN GROUP (ORDER BY "weatherCode") as weather_code_mode
                FROM weather_data
                WHERE "parkId"::text = ANY(:park_ids)
                    AND EXTRACT(MONTH FROM date) = :month
                    AND date >= CURRENT_DATE - INTERVAL '3 years'
                GROUP BY "parkId"
            """)

            try:
                with get_db() as db:
                    result = db.execute(
                        weather_query,
                        {"park_ids": list(set(park_ids)), "month": prediction_month},
                    )
                    weather_db_df = pd.DataFrame(
                        result.fetchall(), columns=result.keys()
                    )
                    weather_db_df = convert_df_types(weather_db_df)
                    import time

                    _weather_historical_cache[cache_key] = (
                        weather_db_df.copy(),
                        time.time(),
                    )
            except Exception as e:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Failed to fetch historical weather data: {e}. Using defaults."
                )
                weather_db_df = pd.DataFrame()

        if not weather_db_df.empty:
            historical_temp_avg = weather_db_df.set_index("parkId")[
                "temp_avg"
            ].to_dict()

            if not use_forecast:
                # Merge historical data as primary
                df = df.merge(weather_db_df, on="parkId", how="left")

                # Apply sinusoidal interpolation to historical fallback
                # Min at 4am, Max at 2pm (14:00)
                temp_min = df["temp_max"].fillna(25.0)  # Use max if min missing
                temp_min = df["temp_min"].fillna(15.0)
                temp_max = df["temp_max"].fillna(25.0)
                temp_range = temp_max - temp_min

                normalized_time = ((df["hour"] - 14) / 12) * np.pi
                interpolation_factor = np.cos(normalized_time) * -0.5 + 0.5
                df["temperature_avg"] = temp_min + (interpolation_factor * temp_range)

                df["precipitation"] = df["precip_avg"].fillna(0.0)
                df["windSpeedMax"] = df["wind_avg"].fillna(0.0)
                df["snowfallSum"] = df["snow_avg"].fillna(0.0)
                df["weatherCode"] = df["weather_code_mode"].fillna(0).astype(int)
                df["precipitation_last_3h"] = df["precipitation"] * 0.125

                # Cleanup historical source columns but KEEP temperature_avg
                df = df.drop(
                    columns=[
                        "temp_max",
                        "temp_min",
                        "temp_avg",
                        "precip_avg",
                        "wind_avg",
                        "snow_avg",
                        "weather_code_mode",
                    ],
                    errors="ignore",
                )

    # Ensure all weather columns are initialized with defaults
    if "temperature_avg" not in df.columns:
        df["temperature_avg"] = 20.0
    if "precipitation" not in df.columns:
        df["precipitation"] = 0.0
    if "windSpeedMax" not in df.columns:
        df["windSpeedMax"] = 0.0
    if "snowfallSum" not in df.columns:
        df["snowfallSum"] = 0.0
    if "weatherCode" not in df.columns:
        df["weatherCode"] = 0

    # Calculate temperature deviation CORRECTLY against long-term averages
    # Differentiates from simple variance within the forecasted batch.
    df["_hist_temp_baseline"] = df["parkId"].map(historical_temp_avg).fillna(20.0)
    df["temperature_deviation"] = df["temperature_avg"] - df["_hist_temp_baseline"]
    df = df.drop(columns=["_hist_temp_baseline"])

    # Ensure precipitation_last_3h is set
    if "precipitation_last_3h" not in df.columns:
        df["precipitation_last_3h"] = df["precipitation"] * 0.125

    df["is_raining"] = (df["precipitation"] > 0).astype(int)

    # Rain Trend Features (starting/stopping) - matches features.py logic
    # Sort by (parkId, timestamp) to correctly find state changes
    original_index = df.index
    df = df.sort_values(["parkId", "timestamp"])

    # In inference, we might only have a single row or a short window.
    # We use shift(1) per park to find changes.
    was_raining = df.groupby("parkId")["is_raining"].shift(1).fillna(0)
    df["is_rain_starting"] = ((df["is_raining"] == 1) & (was_raining == 0)).astype(int)
    df["is_rain_stopping"] = ((df["is_raining"] == 0) & (was_raining == 1)).astype(int)

    # Restore original order
    df = df.loc[original_index]

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

    # OPTIMIZATION: Vectorized holiday lookups (replaces slow loop)
    # Pre-process parks metadata to create lookup structures
    park_country_map = parks_metadata.set_index("park_id")[
        ["country", "region_code"]
    ].to_dict("index")

    # Parse influencing regions once per park (not per row!)
    park_influences_map = {}
    for _, park_row in parks_metadata.iterrows():
        park_id = park_row["park_id"]
        raw_influences = park_row.get("influencingRegions")

        influencing_regions = []
        if isinstance(raw_influences, list):
            influencing_regions = raw_influences
        elif isinstance(raw_influences, str):
            import json

            try:
                influencing_regions = json.loads(raw_influences)
            except Exception:
                influencing_regions = []

        park_influences_map[park_id] = influencing_regions[:3]  # Max 3

    # Add local date column for holiday matching
    df["local_date"] = df["local_timestamp"].dt.date

    # Convert holidays_df to easier lookup format
    if not holidays_df.empty:
        # Create separate DataFrames for regional and national holidays
        holidays_df["date_only"] = pd.to_datetime(holidays_df["date"]).dt.date

        # Regional holidays (with region) — normalize region codes to match training
        holidays_regional = holidays_df[holidays_df["region"].notna()].copy()
        holidays_regional["region_normalized"] = holidays_regional["region"].apply(
            normalize_region_code
        )
        holidays_regional["lookup_key"] = (
            holidays_regional["country"]
            + "|"
            + holidays_regional["region_normalized"].fillna("")
            + "|"
            + holidays_regional["date_only"].astype(str)
        )

        # National holidays (no region)
        holidays_national = holidays_df[holidays_df["region"].isna()].copy()
        holidays_national["lookup_key"] = (
            holidays_national["country"]
            + "||"
            + holidays_national["date_only"].astype(str)
        )

        # Combine for fast lookup
        holiday_type_lookup = {}
        for _, row in holidays_regional.iterrows():
            holiday_type_lookup[row["lookup_key"]] = row["holiday_type"]
        for _, row in holidays_national.iterrows():
            holiday_type_lookup[row["lookup_key"]] = row["holiday_type"]

    # Initialize holiday columns (vectorized)
    df["is_holiday_primary"] = 0
    df["is_school_holiday_primary"] = 0
    df["is_holiday_neighbor_1"] = 0
    df["is_holiday_neighbor_2"] = 0
    df["is_holiday_neighbor_3"] = 0
    df["holiday_count_total"] = 0
    df["school_holiday_count_total"] = 0
    df["is_school_holiday_any"] = 0

    if not holidays_df.empty:
        # FULLY VECTORIZED: Build lookup keys without loops
        # Map park metadata to DataFrame columns
        df["park_country"] = df["parkId"].map(
            lambda pid: park_country_map.get(pid, {}).get("country", "")
        )
        # Normalize region codes (e.g. "DE-NW" -> "NW") to match training feature logic
        df["park_region"] = df["parkId"].map(
            lambda pid: (
                normalize_region_code(
                    park_country_map.get(pid, {}).get("region_code", "") or None
                )
                or ""
            )
        )
        df["date_str"] = df["local_date"].astype(str)

        # Primary key: country|region|date or country||date (if no region)
        df["primary_key"] = df.apply(
            lambda row: (
                f"{row['park_country']}|{row['park_region']}|{row['date_str']}"
                if row["park_region"]
                else f"{row['park_country']}||{row['date_str']}"
            ),
            axis=1,
        )

        # Neighbor keys: Extract from park_influences_map (region codes normalized)
        def get_neighbor_key(row, index):
            influences = park_influences_map.get(row["parkId"], [])
            if index < len(influences):
                inf = influences[index]
                country = inf.get("countryCode", "")
                region = normalize_region_code(inf.get("regionCode", "") or None) or ""
                if region:
                    return f"{country}|{region}|{row['date_str']}"
                else:
                    return f"{country}||{row['date_str']}"
            return ""

        df["neighbor_1_key"] = df.apply(lambda row: get_neighbor_key(row, 0), axis=1)
        df["neighbor_2_key"] = df.apply(lambda row: get_neighbor_key(row, 1), axis=1)
        df["neighbor_3_key"] = df.apply(lambda row: get_neighbor_key(row, 2), axis=1)

        # Map to holiday types
        df["primary_type"] = df["primary_key"].map(holiday_type_lookup)
        df["neighbor_1_type"] = df["neighbor_1_key"].map(holiday_type_lookup)
        df["neighbor_2_type"] = df["neighbor_2_key"].map(holiday_type_lookup)
        df["neighbor_3_type"] = df["neighbor_3_key"].map(holiday_type_lookup)

        # Fallback: If regional lookup failed, try national
        mask_no_primary = df["primary_type"].isna()
        if mask_no_primary.any():
            # Extract country from primary_key and try national lookup
            df.loc[mask_no_primary, "primary_fallback_key"] = df.loc[
                mask_no_primary, "primary_key"
            ].str.replace(r"\|.*?\|", "||", regex=True)
            df.loc[mask_no_primary, "primary_type"] = df.loc[
                mask_no_primary, "primary_fallback_key"
            ].map(holiday_type_lookup)

        # Convert to binary flags (vectorized)
        df["is_holiday_primary"] = (df["primary_type"] == "public").astype(int)
        df["is_school_holiday_primary"] = (df["primary_type"] == "school").astype(int)
        df["is_holiday_neighbor_1"] = (df["neighbor_1_type"] == "public").astype(int)
        df["is_holiday_neighbor_2"] = (df["neighbor_2_type"] == "public").astype(int)
        df["is_holiday_neighbor_3"] = (df["neighbor_3_type"] == "public").astype(int)

        # Calculate totals (vectorized)
        df["holiday_count_total"] = (
            df["is_holiday_primary"]
            + df["is_holiday_neighbor_1"]
            + df["is_holiday_neighbor_2"]
            + df["is_holiday_neighbor_3"]
        )

        df["school_holiday_count_total"] = (
            (df["primary_type"] == "school").astype(int)
            + (df["neighbor_1_type"] == "school").astype(int)
            + (df["neighbor_2_type"] == "school").astype(int)
            + (df["neighbor_3_type"] == "school").astype(int)
        )

        # "Any School Holiday" Logic (vectorized)
        df["is_school_holiday_any"] = (df["school_holiday_count_total"] > 0).astype(int)

        # Cleanup temporary columns
        df = df.drop(
            columns=[
                "local_date",
                "primary_key",
                "neighbor_1_key",
                "neighbor_2_key",
                "neighbor_3_key",
                "primary_type",
                "neighbor_1_type",
                "neighbor_2_type",
                "neighbor_3_type",
            ],
            errors="ignore",
        )
    else:
        # No holidays data - keep defaults (all zeros)
        df = df.drop(columns=["local_date"], errors="ignore")

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
                OR "scheduleType" IN ('MAINTENANCE', 'CLOSED', 'INFO', 'TICKETED_EVENT', 'PRIVATE_EVENT', 'UNKNOWN')
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

        # Parks without schedule integration: no OPERATING rows at all → treat as "no schedule"
        park_level = schedules_df[
            (schedules_df["attractionId"].isin(["nan", "None"]))
            | (schedules_df["attractionId"].isna())
        ]

        # Determine park schedule integration (at least one OPERATING row)
        park_has_operating = (
            park_level.groupby(park_level["parkId"].astype(str))["scheduleType"]
            .apply(lambda x: (x == "OPERATING").any())
            .to_dict()
        )

        # Ensure index alignment and type matching for merges
        df = df.reset_index(drop=True)
        df["parkId"] = df["parkId"].astype(str)
        if "attractionId" in df.columns:
            df["attractionId"] = df["attractionId"].astype(str)

        # Ensure local_timestamp and date_local exist in df
        if "local_timestamp" not in df.columns:
            df["local_timestamp"] = pd.to_datetime(df["timestamp"])
        df["local_timestamp"] = pd.to_datetime(df["local_timestamp"])
        df["schedule_date"] = pd.to_datetime(df["local_timestamp"].dt.date)

        # Create lookup structure for park-level schedules
        park_schedules = schedules_df[
            (schedules_df["attractionId"].isin(["nan", "None"]))
            | (schedules_df["attractionId"].isna())
        ].copy()

        # Prepare operating schedules
        operating_schedules = park_schedules[
            park_schedules["scheduleType"] == "OPERATING"
        ].copy()

        if not operating_schedules.empty:
            operating_schedules["date_only"] = pd.to_datetime(
                operating_schedules["date"].dt.date
            )
            # Keep first operating schedule per park/date
            operating_schedules = (
                operating_schedules.groupby(["parkId", "date_only"])
                .first()
                .reset_index()
            )

            # Merge df with operating_schedules
            df_merged = df.merge(
                operating_schedules[
                    ["parkId", "date_only", "openingTime", "closingTime"]
                ],
                left_on=["parkId", "schedule_date"],
                right_on=["parkId", "date_only"],
                how="left",
            )
            # Set index to match df
            df_merged.index = df.index

            # Use local_timestamp since openingTime/closingTime from DB are timezone-aware (UTC stored in DB as TIMESTAMPTZ)
            # Make sure df_merged openingTime/closingTime and df["timestamp"] are comparable.
            ts_compare = pd.to_datetime(df_merged["timestamp"], utc=True)
            opening = df_merged["openingTime"]
            closing = df_merged["closingTime"]

            # Set tz to UTC to compare if opening has tzinfo
            if getattr(opening.dt, "tz", None) is not None:
                opening = opening.dt.tz_convert("UTC")
                closing = closing.dt.tz_convert("UTC")
            else:
                # If opening time from DB is naive, make ts_compare naive (UTC) as well
                ts_compare = ts_compare.dt.tz_localize(None)

            mask_valid = opening.notna() & closing.notna()
            mask_open = mask_valid & (ts_compare >= opening) & (ts_compare <= closing)

            df.loc[mask_open, "is_park_open"] = 1
            mask_closed_operating_day = mask_valid & ~mask_open
            df.loc[mask_closed_operating_day, "status"] = "CLOSED"

        # Handle non-operating schedules (CLOSED / UNKNOWN) on dates with NO operating schedule
        # First, find the dates where operating schedules were missing
        if not operating_schedules.empty:
            has_operating = (
                df.merge(
                    operating_schedules[["parkId", "date_only"]],
                    left_on=["parkId", "schedule_date"],
                    right_on=["parkId", "date_only"],
                    how="left",
                    indicator=True,
                )["_merge"]
                == "both"
            )
            has_operating.index = df.index
        else:
            has_operating = pd.Series(False, index=df.index)

        mask_no_operating = ~has_operating

        if mask_no_operating.any():
            # For these rows, we check if park_has_operating is True.
            # If so, set status to UNKNOWN or CLOSED depending on what exists, and is_park_open = 0

            # Map park_has_operating flag to df
            df_park_has_operating = df["parkId"].map(park_has_operating).fillna(False)
            df_park_has_operating.index = df.index

            # Look up if schedule has UNKNOWN for that park/date
            unknown_schedules = park_schedules[
                park_schedules["scheduleType"] == "UNKNOWN"
            ].copy()
            if not unknown_schedules.empty:
                unknown_schedules["date_only"] = pd.to_datetime(
                    unknown_schedules["date"].dt.date
                )
                unknown_schedules = (
                    unknown_schedules.groupby(["parkId", "date_only"])
                    .first()
                    .reset_index()
                )

                has_unknown = (
                    df.merge(
                        unknown_schedules[["parkId", "date_only"]],
                        left_on=["parkId", "schedule_date"],
                        right_on=["parkId", "date_only"],
                        how="left",
                        indicator=True,
                    )["_merge"]
                    == "both"
                )
                has_unknown.index = df.index
            else:
                has_unknown = pd.Series(False, index=df.index)

            # Look up if schedule has CLOSED for that park/date
            closed_schedules = park_schedules[
                park_schedules["scheduleType"] == "CLOSED"
            ].copy()
            if not closed_schedules.empty:
                closed_schedules["date_only"] = pd.to_datetime(
                    closed_schedules["date"].dt.date
                )
                closed_schedules = (
                    closed_schedules.groupby(["parkId", "date_only"])
                    .first()
                    .reset_index()
                )

                has_closed = (
                    df.merge(
                        closed_schedules[["parkId", "date_only"]],
                        left_on=["parkId", "schedule_date"],
                        right_on=["parkId", "date_only"],
                        how="left",
                        indicator=True,
                    )["_merge"]
                    == "both"
                )
                has_closed.index = df.index
            else:
                has_closed = pd.Series(False, index=df.index)

            mask_apply_non_op = (
                mask_no_operating & df_park_has_operating & (has_unknown | has_closed)
            )

            # Default to CLOSED if no operating, park_has_operating, and it has some non-op schedule
            df.loc[mask_apply_non_op, "is_park_open"] = 0
            df.loc[mask_apply_non_op, "status"] = "CLOSED"

            # Overwrite with UNKNOWN if it has an UNKNOWN entry
            mask_apply_unknown = mask_apply_non_op & has_unknown
            df.loc[mask_apply_unknown, "status"] = "UNKNOWN"

        # Check for special events
        event_schedules = park_schedules[
            park_schedules["scheduleType"].isin(["TICKETED_EVENT", "PRIVATE_EVENT"])
        ].copy()
        if not event_schedules.empty:
            event_schedules["date_only"] = pd.to_datetime(
                event_schedules["date"].dt.date
            )
            event_schedules = (
                event_schedules.groupby(["parkId", "date_only"]).first().reset_index()
            )
            has_event = (
                df.merge(
                    event_schedules[["parkId", "date_only"]],
                    left_on=["parkId", "schedule_date"],
                    right_on=["parkId", "date_only"],
                    how="left",
                    indicator=True,
                )["_merge"]
                == "both"
            )
            has_event.index = df.index
            df.loc[has_event, "has_special_event"] = 1

        # Check for extra hours
        extra_hours_schedules = park_schedules[
            park_schedules["scheduleType"] == "EXTRA_HOURS"
        ].copy()
        if not extra_hours_schedules.empty:
            extra_hours_schedules["date_only"] = pd.to_datetime(
                extra_hours_schedules["date"].dt.date
            )
            extra_hours_schedules = (
                extra_hours_schedules.groupby(["parkId", "date_only"])
                .first()
                .reset_index()
            )
            has_extra = (
                df.merge(
                    extra_hours_schedules[["parkId", "date_only"]],
                    left_on=["parkId", "schedule_date"],
                    right_on=["parkId", "date_only"],
                    how="left",
                    indicator=True,
                )["_merge"]
                == "both"
            )
            has_extra.index = df.index
            df.loc[has_extra, "has_extra_hours"] = 1

        # Check specific attraction status (Maintenance or Closed)
        if "attractionId" in df.columns:
            attr_schedules = schedules_df[
                schedules_df["attractionId"].notna()
                & ~schedules_df["attractionId"].isin(["nan", "None"])
            ].copy()
            if not attr_schedules.empty:
                attr_schedules["date_only"] = pd.to_datetime(
                    attr_schedules["date"].dt.date
                )
                attr_schedules_closed = attr_schedules[
                    attr_schedules["scheduleType"].isin(["MAINTENANCE", "CLOSED"])
                ]
                if not attr_schedules_closed.empty:
                    attr_schedules_closed = (
                        attr_schedules_closed.groupby(
                            ["parkId", "attractionId", "date_only"]
                        )
                        .first()
                        .reset_index()
                    )
                    has_attr_closed = (
                        df.merge(
                            attr_schedules_closed[
                                ["parkId", "attractionId", "date_only"]
                            ],
                            left_on=["parkId", "attractionId", "schedule_date"],
                            right_on=["parkId", "attractionId", "date_only"],
                            how="left",
                            indicator=True,
                        )["_merge"]
                        == "both"
                    )
                    has_attr_closed.index = df.index
                    df.loc[has_attr_closed, "status"] = "CLOSED"

        df = df.drop(columns=["schedule_date"], errors="ignore")

    # Live park status override: if NestJS determined a park is OPERATING via ride data,
    # correct is_park_open for rows with UNKNOWN schedule (no data from wiki).
    # Only overrides UNKNOWN — never CLOSED, which reflects an explicit schedule entry.
    # This fixes future prediction timestamps (hourly: next 24h) where currentWaitTimes won't help.
    if feature_context and "parkLiveStatus" in feature_context:
        for park_id, live_status in feature_context["parkLiveStatus"].items():
            if live_status == "OPERATING":
                mask = (
                    (df["parkId"] == park_id)
                    & (df["status"] == "UNKNOWN")
                    & (df["is_park_open"] == 0)
                )
                if mask.any():
                    df.loc[mask, "is_park_open"] = 1
                    df.loc[mask, "status"] = "OPERATING"

    # 3. Holiday Distance Features (Arrival & Departure signals)
    # Strategy: Find the next AND previous public holiday/bridge day per country
    df["days_until_next_holiday"] = 7
    df["days_since_last_holiday"] = 7
    df["is_long_weekend"] = 0  # Part of a 3+ day block

    # We need country mapping for distance calculation
    df_temp = df.merge(
        parks_metadata[["park_id", "country"]],
        left_on="parkId",
        right_on="park_id",
        how="left",
    )

    # Filter all event days (Holidays + Bridge Days) from our current dataframe
    event_days = df_temp[
        (df_temp["is_holiday_primary"] == 1) | (df_temp.get("is_bridge_day", 0) == 1)
    ][["country", "date_local"]].drop_duplicates()

    if not event_days.empty:
        event_days["event_date"] = pd.to_datetime(event_days["date_local"])
        event_days = event_days.sort_values("event_date")

        # Prepare main df for merge_asof
        df["_temp_ts"] = pd.to_datetime(df["date_local"])
        original_order = df.index
        df["country"] = df_temp["country"]
        df = df.sort_values("_temp_ts")

        # A. Next Event (Arrival Signal)
        df = pd.merge_asof(
            df,
            event_days[["country", "event_date"]],
            left_on="_temp_ts",
            right_on="event_date",
            by="country",
            direction="forward",
        )
        df["days_until_next_holiday"] = (
            (df["event_date"] - df["_temp_ts"]).dt.days.fillna(7).clip(0, 7)
        )
        df = df.drop(columns=["event_date"])

        # B. Previous Event (Departure Signal)
        df = pd.merge_asof(
            df,
            event_days[["country", "event_date"]],
            left_on="_temp_ts",
            right_on="event_date",
            by="country",
            direction="backward",
        )
        df["days_since_last_holiday"] = (
            (df["_temp_ts"] - df["event_date"]).dt.days.fillna(7).clip(0, 7)
        )
        df = df.drop(columns=["event_date"])

        # C. Long Weekend Flag (Heuristic)
        is_event = (df["is_holiday_primary"] == 1) | (df["is_bridge_day"] == 1)
        df["is_long_weekend"] = (
            is_event
            | ((df["is_weekend"] == 1) & (df["days_until_next_holiday"] <= 1))
            | ((df["is_weekend"] == 1) & (df["days_since_last_holiday"] <= 1))
        ).astype(int)

        # Cleanup and restore original row order.
        # IMPORTANT: use .loc[original_order], NOT df.index = ... + sort_index().
        # After sort_values + merge_asof the rows are in _temp_ts order;
        # overwriting the index labels and sorting would assign wrong labels.
        df = df.drop(columns=["_temp_ts", "country"])
        df = df.loc[original_order]

    # Historical features (most important!)
    # OPTIMIZATION: fetch_recent_wait_times now pre-computes rolling averages in DB
    # This avoids expensive Python calculations for every attraction
    recent_data = fetch_recent_wait_times(attraction_ids, lookback_days=730)

    # Initialize with defaults (0.0 to match training fallback)
    df["avg_wait_last_24h"] = 0.0
    df["avg_wait_last_1h"] = 0.0
    df["avg_wait_same_hour_last_week"] = 0.0
    df["avg_wait_same_hour_last_month"] = 0.0
    df["rolling_avg_7d"] = 0.0
    df["trend_7d"] = 0.0
    df["volatility_7d"] = 0.0

    if not recent_data.empty:
        # Map attraction names for type heuristic
        attr_names_map = (
            recent_data.groupby("attractionId")["attractionName"].first().to_dict()
        )
        df["attractionName"] = df["attractionId"].map(attr_names_map)
        df = add_attraction_type_features(df)

        recent_data["date"] = pd.to_datetime(recent_data["date"])
        # Ensure timezone-naive for comparison with cutoff dates
        if recent_data["date"].dt.tz is not None:
            recent_data["date"] = recent_data["date"].dt.tz_localize(None)

        # Historical feature lookups use local park time.
        # fetch_recent_wait_times() now aggregates by local park hour/DOW (via p.timezone JOIN),
        # so lookups keyed by local hour/DOW are self-consistent with the stored data.

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

                # OPTIMIZATION: Use pre-computed rolling_avg_7d from DB (window function)
                # This avoids expensive Python aggregation for every attraction
                cutoff_7d_local = (base_time_local - timedelta(days=7)).date()
                last_7_days = attraction_data[
                    pd.to_datetime(attraction_data["date"]).dt.date >= cutoff_7d_local
                ]

                # Use DB-computed rolling average if available
                if "rolling_avg_7d" in attraction_data.columns:
                    rolling_7d = (
                        attraction_data["rolling_avg_7d"].iloc[-1]
                        if len(attraction_data) > 0
                        and not pd.isna(attraction_data["rolling_avg_7d"].iloc[-1])
                        else overall_avg
                    )
                else:
                    # Fallback to Python calculation (backwards compatibility)
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

                # Calculate 7-day trend (momentum) - matches features.py logic
                trend_7d = avg_24h - rolling_7d

                # OPTIMIZATION: Use pre-computed rolling_std_7d from DB (window function)
                # Volatility: std of last 7d, log1p-dampened and capped to match training
                cap_std = get_settings().VOLATILITY_CAP_STD_MINUTES
                volatility_7d = 0.0
                if "rolling_std_7d" in attraction_data.columns:
                    # Use DB-computed standard deviation
                    raw_std = (
                        attraction_data["rolling_std_7d"].iloc[-1]
                        if len(attraction_data) > 0
                        and not pd.isna(attraction_data["rolling_std_7d"].iloc[-1])
                        else 0.0
                    )
                    if raw_std > 0:
                        volatility_7d = min(np.log1p(raw_std), np.log1p(cap_std))
                elif len(last_7_days) > 1:
                    # Fallback to Python calculation (backwards compatibility)
                    raw_std = last_7_days["avg_wait"].std()
                    if not pd.isna(raw_std) and raw_std >= 0:
                        volatility_7d = min(np.log1p(raw_std), np.log1p(cap_std))

                # Split volatility: weekday vs weekend (matches training-side calculate_trend_volatility)
                def _dampened_vol_pred(raw_std_val):
                    if raw_std_val is None or pd.isna(raw_std_val) or raw_std_val <= 0:
                        return 0.0
                    return min(np.log1p(raw_std_val), np.log1p(cap_std))

                volatility_weekday = _dampened_vol_pred(
                    attraction_data["rolling_std_weekday"].iloc[-1]
                    if "rolling_std_weekday" in attraction_data.columns
                    and len(attraction_data) > 0
                    else None
                )
                volatility_weekend = _dampened_vol_pred(
                    attraction_data["rolling_std_weekend"].iloc[-1]
                    if "rolling_std_weekend" in attraction_data.columns
                    and len(attraction_data) > 0
                    else None
                )

                # Weekday / weekend rolling averages (from DB window functions)
                rolling_avg_weekday = (
                    attraction_data["rolling_avg_weekday"].iloc[-1]
                    if "rolling_avg_weekday" in attraction_data.columns
                    and len(attraction_data) > 0
                    and not pd.isna(attraction_data["rolling_avg_weekday"].iloc[-1])
                    else rolling_7d
                )
                rolling_avg_weekend = (
                    attraction_data["rolling_avg_weekend"].iloc[-1]
                    if "rolling_avg_weekend" in attraction_data.columns
                    and len(attraction_data) > 0
                    and not pd.isna(attraction_data["rolling_avg_weekend"].iloc[-1])
                    else rolling_7d
                )

                # Apply uniform features to all rows for this attraction
                mask = df["attractionId"] == attraction_id
                df.loc[mask, "avg_wait_last_24h"] = avg_24h
                df.loc[mask, "avg_wait_last_1h"] = avg_1h
                df.loc[mask, "avg_wait_same_hour_last_week"] = avg_same_hour
                df.loc[mask, "avg_wait_same_hour_last_month"] = avg_same_hour_month
                df.loc[mask, "rolling_avg_7d"] = rolling_7d
                df.loc[mask, "rolling_avg_weekday"] = rolling_avg_weekday
                df.loc[mask, "rolling_avg_weekend"] = rolling_avg_weekend
                df.loc[mask, "trend_7d"] = trend_7d
                df.loc[mask, "volatility_7d"] = volatility_7d
                df.loc[mask, "volatility_weekday"] = volatility_weekday
                df.loc[mask, "volatility_weekend"] = volatility_weekend

                # avg_wait_same_dow_4w: mean of last 4 same-day-of-week lookups (1w/2w/3w/4w).
                # Must be computed per prediction timestamp because future timestamps span
                # multiple days (e.g. Sunday 19:00 → Monday 18:00), each needing its own
                # same-DOW historical anchor.
                #
                # PERFORMANCE: Pre-build a (date, hour) → avg_wait dict once per attraction
                # so each per-row lookup is O(1) instead of O(len(attraction_data)).
                # This avoids saturating all uvicorn workers with DataFrame scans.
                _attr_lookup: dict = (
                    attraction_data.groupby(
                        [
                            pd.to_datetime(attraction_data["date"]).dt.date,
                            attraction_data["hour"].astype(int),
                        ]
                    )["avg_wait"]
                    .mean()
                    .to_dict()
                )

                def _same_dow_avg_fast(ts_local, weeks: list):
                    vals = []
                    for w in weeks:
                        key = (
                            (ts_local - timedelta(days=7 * w)).date(),
                            int(ts_local.hour),
                        )
                        if key in _attr_lookup:
                            vals.append(_attr_lookup[key])
                    return float(np.mean(vals)) if vals else rolling_7d

                # For future prediction rows (> 24h ahead), avg_wait_last_24h and
                # avg_wait_last_1h lose meaning — they just reflect current/recent
                # conditions, not conditions at the predicted time. Replace them with
                # DOW/hour-based historical profiles so the model can differentiate
                # weekdays from weekends and low-season from peak-season future dates.
                if park_tz:
                    import pytz as _pytz_future

                    _tz_future = _pytz_future.timezone(park_tz)
                    future_24h_cutoff = base_time_pd + timedelta(hours=24)

                    # Ensure timezone aware timestamps locally
                    ts_utc = pd.to_datetime(df.loc[mask, "timestamp"])
                    if ts_utc.dt.tz is None:
                        ts_utc = ts_utc.dt.tz_localize("UTC")
                    ts_local = ts_utc.dt.tz_convert(_tz_future)

                    # Condition: only apply where time is > 24h ahead
                    future_mask = ts_utc > future_24h_cutoff

                    # Calculate same-DOW-4-week avg via vectorized apply over the local timestamps
                    # Since _same_dow_avg_fast requires complex dictionary lookup, we apply it.
                    same_dow_vals = ts_local.apply(
                        lambda x: _same_dow_avg_fast(x, [1, 2, 3, 4])
                    )

                    # Update 'avg_wait_same_dow_4w' for all mask rows
                    df.loc[mask, "avg_wait_same_dow_4w"] = same_dow_vals.values

                    # Update 'avg_wait_last_24h' and 'avg_wait_last_1h' only for future mask rows
                    # (where we drop real-time recent values)
                    future_indices = df.loc[mask].index[future_mask]
                    if len(future_indices) > 0:
                        df.loc[future_indices, "avg_wait_last_24h"] = same_dow_vals[
                            future_mask
                        ].values

                        ts_local_future = ts_local[future_mask]
                        row_hours = ts_local_future.dt.hour.values
                        row_is_weekends = (ts_local_future.dt.dayofweek >= 5).values

                        # Start with default weekday/weekend rolling averages
                        hour_hists = np.where(
                            row_is_weekends, rolling_avg_weekend, rolling_avg_weekday
                        )

                        # Lookup from _attr_lookup if key exists
                        ts_minus_7d = ts_local_future - pd.Timedelta(days=7)
                        dates = ts_minus_7d.dt.date.values

                        for i, idx in enumerate(future_indices):
                            hour_key = (dates[i], row_hours[i])
                            if hour_key in _attr_lookup:
                                hour_hists[i] = _attr_lookup[hour_key]

                        df.loc[future_indices, "avg_wait_last_1h"] = hour_hists
                else:
                    df.loc[mask, "avg_wait_same_dow_4w"] = _same_dow_avg_fast(
                        base_time_local, [1, 2, 3, 4]
                    )

    # Calculate wait time velocity (momentum) BEFORE overriding lags
    # Initialize with default (no change)
    df["wait_time_velocity"] = 0.0

    # Initialize trend and volatility if not already set
    if "trend_7d" not in df.columns:
        df["trend_7d"] = 0.0
    if "volatility_7d" not in df.columns:
        df["volatility_7d"] = 0.0
    if "volatility_weekday" not in df.columns:
        df["volatility_weekday"] = 0.0
    if "volatility_weekend" not in df.columns:
        df["volatility_weekend"] = 0.0
    if "rolling_avg_weekday" not in df.columns:
        df["rolling_avg_weekday"] = df.get("rolling_avg_7d", 0.0)
    if "rolling_avg_weekend" not in df.columns:
        df["rolling_avg_weekend"] = df.get("rolling_avg_7d", 0.0)
    if "avg_wait_same_dow_4w" not in df.columns:
        df["avg_wait_same_dow_4w"] = df.get("avg_wait_same_hour_last_week", 0.0)

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
        add_park_attraction_count_feature,
    )

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
        # Defaults if no feature_context provided — use historical occupancy profile instead of flat 100%
        from features import add_park_occupancy_feature

        occ_context = {"historicalOccupancy": hist_occ, "baseTime": base_time}
        df = add_park_occupancy_feature(df, occ_context)
        df["time_since_park_open_mins"] = 0.0
        df["had_downtime_today"] = 0
        df["downtime_minutes_today"] = 0.0
        df["has_virtual_queue"] = 0
        df["is_bridge_day"] = 0
        df["park_has_schedule"] = (
            1  # Default to 1 (assume schedule exists for better quality)
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

    # OPTIMIZATION: Skip ML inference for CLOSED days when schedule exists.
    # UNKNOWN days are kept because they should receive predictions (Calendar expects ML or fallback "moderate" for them).
    #
    # CRITICAL - Parks with "quasi keine" schedules still get predictions:
    # - No schedule in DB for prediction range → status stays OPERATING (default) → we keep all rows
    # - Only UNKNOWN/CLOSED in range (no OPERATING) → park_has_operating=False → we keep default
    df_inference = features_df[features_df["status"].isin(["OPERATING", "UNKNOWN"])]
    if df_inference.empty:
        return []  # All days explicitly closed; filter_predictions_by_schedule would return [] anyway

    # Predict with uncertainty estimation (only for OPERATING rows)
    try:
        uncertainty_results = model.predict_with_uncertainty(df_inference)
        predictions = uncertainty_results["predictions"]
        uncertainties = uncertainty_results["uncertainty"]
        use_uncertainty = True
    except Exception as e:
        # Fallback to regular predictions if uncertainty estimation fails
        # Reduced logging - only log if it's a real issue
        if "missing" not in str(e).lower():
            print(f"⚠️  Uncertainty estimation failed: {e}")
        predictions = model.predict(df_inference)
        uncertainties = np.zeros(len(predictions))
        use_uncertainty = False

    # Format results (OPERATING and UNKNOWN rows; CLOSED never reach the client)
    results = []
    for i, (idx, row) in enumerate(df_inference.iterrows()):
        pred_wait = round_to_nearest_5(predictions[i])

        # Enforce a minimum 10 min wait if the ride is considered operating by the model
        # or zero if the model thinks it's closed anyway.
        if row["status"] in ["UNKNOWN", "OPERATING"] and pred_wait > 0:
            pred_wait = max(10, pred_wait)

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
            relative_uncertainty = uncertainties[i] / max(pred_wait, 1)
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
        # TypeScript: 60, 89, 110, 150, 200
        # P50 (100%) = "moderate" (expected/typical baseline)
        occupancy_pct = ratio * 100
        if occupancy_pct <= 60:
            crowd_level = "very_low"
        elif occupancy_pct <= 89:
            crowd_level = "low"
        elif occupancy_pct <= 110:  # 90-110%: ±10% around P50 = moderate
            crowd_level = "moderate"
        elif occupancy_pct <= 150:
            crowd_level = "high"
        elif occupancy_pct <= 200:
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
        elif len(results) > 1 and results[-2]["attractionId"] == row["attractionId"]:
            # If no current actual, compare to previous hour prediction
            prev_pred = results[-2]["predictedWaitTime"]
            diff = pred_wait - prev_pred
            if diff > 5:
                results[-1]["trend"] = "increasing"
            elif diff < -5:
                results[-1]["trend"] = "decreasing"
            else:
                results[-1]["trend"] = "stable"

    # NOTE: CLOSED/UNKNOWN rows were excluded before inference (no ML call for closed days).
    # Schedule filtering will be applied after this function returns.
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
