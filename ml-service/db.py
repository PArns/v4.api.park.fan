"""
Database connection and queries for ML training
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Generator, List, Dict
import datetime
import pandas as pd
import decimal
import uuid
from config import get_settings

settings = get_settings()


def convert_df_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert problematic database types to ML-compatible formats.
    - decimal.Decimal -> float
    - uuid.UUID -> str
    - datetime.date/datetime.datetime -> pd.to_datetime
    """
    if df.empty:
        return df

    for col in df.columns:
        if df[col].dtype == "object":
            sample = df[col].dropna()
            if sample.empty:
                continue

            first_val = sample.iloc[0]

            try:
                # 1. Decimal -> Float
                if isinstance(first_val, decimal.Decimal):
                    df[col] = df[col].astype(float)
                # 2. UUID -> String
                elif isinstance(first_val, uuid.UUID):
                    df[col] = df[col].astype(str)
                # 3. Date/Datetime -> Timestamp (ML features need standard pandas types)
                elif isinstance(first_val, (datetime.date, datetime.datetime)):
                    try:
                        df[col] = pd.to_datetime(df[col], utc=True)
                    except Exception as conv_error:
                        print(
                            f"⚠️  Warning: Failed to convert datetime column {col}: {conv_error}"
                        )
                        # Keep original values if conversion fails
                        pass
            except Exception as e:
                print(f"⚠️  Warning: Failed to convert column {col}: {e}")

    return df


def get_db_url() -> str:
    """Build PostgreSQL connection URL"""
    return (
        f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )


engine = create_engine(get_db_url(), pool_pre_ping=True, pool_size=10)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_db() -> Generator:
    """Get database session context manager"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def fetch_training_data(
    start_date: datetime.datetime, end_date: datetime.datetime
) -> pd.DataFrame:
    """
    Fetch historical queue data, weather, holidays for training

    Returns DataFrame with columns:
    - attraction_id
    - park_id
    - timestamp
    - wait_time (target)
    - hour, day_of_week, month, season
    - temperature_max, precipitation, weather_code
    - is_weekend, is_holiday_primary, is_holiday_neighbor_1/2/3
    - avg_wait_last_24h, avg_wait_same_hour_last_week
    """

    query = text("""
        WITH queue_with_park AS (
            SELECT
                qd.id,
                qd."attractionId",
                a."parkId",
                a."attractionType",
                qd.timestamp,
                qd."waitTime",
                EXTRACT(HOUR FROM qd.timestamp) as hour,
                EXTRACT(DOW FROM qd.timestamp) as day_of_week,
                EXTRACT(MONTH FROM qd.timestamp) as month,
                CASE
                    WHEN EXTRACT(MONTH FROM qd.timestamp) IN (12, 1, 2) THEN 0  -- Winter
                    WHEN EXTRACT(MONTH FROM qd.timestamp) IN (3, 4, 5) THEN 1   -- Spring
                    WHEN EXTRACT(MONTH FROM qd.timestamp) IN (6, 7, 8) THEN 2   -- Summer
                    ELSE 3                                                        -- Fall
                END as season
            FROM queue_data qd
            INNER JOIN attractions a ON a.id = qd."attractionId"
            WHERE qd."queueType" = 'STANDBY'  -- Use index-friendly order: queueType first
                AND qd.status = 'OPERATING'
                AND qd.timestamp BETWEEN :start_date AND :end_date
                AND qd."waitTime" IS NOT NULL
                AND qd."waitTime" >= 0
        ),
        weather_daily AS (
            SELECT
                "parkId",
                date,
                "temperatureMax",
                "temperatureMin",
                "precipitationSum",
                "snowfallSum",
                "windSpeedMax",
                "weatherCode"
            FROM weather_data
            WHERE date BETWEEN :start_date AND :end_date
                AND "dataType" = 'historical'
        )
        SELECT
            qwp.*,
            wd."temperatureMax",
            wd."temperatureMin",
            wd."precipitationSum" as precipitation,
            wd."snowfallSum" as "snowfallSum",
            wd."windSpeedMax" as "windSpeedMax",
            wd."weatherCode"
        FROM queue_with_park qwp
        LEFT JOIN weather_daily wd ON wd."parkId" = qwp."parkId"
            AND DATE(qwp.timestamp) = wd.date
        ORDER BY qwp.timestamp
    """)

    import time

    start_time = time.time()

    with get_db() as db:
        result = db.execute(query, {"start_date": start_date, "end_date": end_date})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        query_time = time.time() - start_time
        print(f"   Query execution time: {query_time:.2f}s")
        return convert_df_types(df)


def fetch_queue_aggregates(
    attraction_ids: List[str],
    target_hour: datetime.datetime,
    lookback_hours: List[int] = None,
) -> pd.DataFrame:
    """
    Fetch pre-computed percentiles from queue_data_aggregates

    Used for temporal percentile lookups in ML features:
    - wait_p50_same_hour_yesterday (lookback: 24h)
    - wait_p90_same_hour_last_week (lookback: 168h)
    - wait_p75_same_hour_4w_ago (lookback: 672h)

    Args:
        attraction_ids: List of attraction IDs to fetch
        target_hour: The hour to look back from (usually current prediction time)
        lookback_hours: List of hours to look back (e.g., [24, 168, 672])
                       If None, fetches all aggregates for last 7 days

    Returns:
        DataFrame with columns:
        - attraction_id
        - hour (timestamp of aggregated hour)
        - p25, p50, p75, p90, p95, p99
        - iqr, std_dev, mean, sample_count
        - hours_ago (calculated from target_hour)
    """
    if lookback_hours is None:
        # Default: last 7 days for general ML features
        lookback_hours = list(range(0, 24 * 7, 24))  # Every 24h for 7 days

    # Calculate exact timestamps to fetch
    target_timestamps = []
    for hours in lookback_hours:
        ts = target_hour - datetime.timedelta(hours=hours)
        # Truncate to hour
        ts = ts.replace(minute=0, second=0, microsecond=0)
        target_timestamps.append(ts)

    query = text("""
        SELECT
            "attractionId" as attraction_id,
            hour,
            p25,
            p50,
            p75,
            p90,
            p95,
            p99,
            iqr,
            "stdDev" as std_dev,
            mean,
            "sampleCount" as sample_count,
            EXTRACT(EPOCH FROM (:target_hour - hour)) / 3600 as hours_ago
        FROM queue_data_aggregates
        WHERE "attractionId" = ANY(:attraction_ids)
          AND hour = ANY(:target_hours)
        ORDER BY "attractionId", hour DESC
    """)

    with engine.connect() as conn:
        result = conn.execute(
            query,
            {
                "attraction_ids": attraction_ids,
                "target_hours": target_timestamps,
                "target_hour": target_hour,
            },
        )
        data = pd.DataFrame(result.fetchall(), columns=result.keys())

    return convert_df_types(data)


def fetch_park_influencing_countries() -> Dict[str, List[str]]:
    """
    Fetch park influencing countries mapping

    Returns: {park_id: [country_codes]}
    """
    query = text("""
        SELECT
            id as park_id,
            COALESCE("influencingCountries", ARRAY["countryCode"]) as countries
        FROM parks
        WHERE "countryCode" IS NOT NULL
    """)

    with get_db() as db:
        result = db.execute(query)
        return {row.park_id: row.countries for row in result}


def fetch_holidays(
    country_codes: List[str], start_date: datetime.datetime, end_date: datetime.datetime
) -> pd.DataFrame:
    """
    Fetch holidays for specified countries

    Returns DataFrame with columns:
    - date
    - country
    - region (Nullable)
    - holiday_type
    - is_nationwide
    """
    query = text("""
        SELECT
            date,
            country,
            region,
            "holidayType" as holiday_type,
            "isNationwide" as is_nationwide
        FROM holidays
        WHERE country = ANY(:countries)
            AND date BETWEEN :start_date AND :end_date
    """)

    with get_db() as db:
        result = db.execute(
            query,
            {
                "countries": country_codes,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return convert_df_types(df)


# Cache for parks metadata (5 minute TTL)
_parks_metadata_cache = None
_parks_metadata_cache_time = None
_parks_metadata_cache_ttl = 300  # 5 minutes in seconds


def fetch_parks_metadata(use_cache: bool = True) -> pd.DataFrame:
    """
    Fetch park metadata (country code, influencing regions, etc.)

    OPTIMIZATION: Caches result for 5 minutes to avoid repeated DB queries
    during prediction batches.

    Args:
        use_cache: If True, use cached result if available and fresh

    Returns DataFrame with park details
    """
    global _parks_metadata_cache, _parks_metadata_cache_time

    # Check cache if enabled
    if (
        use_cache
        and _parks_metadata_cache is not None
        and _parks_metadata_cache_time is not None
    ):
        import time

        age = time.time() - _parks_metadata_cache_time
        if age < _parks_metadata_cache_ttl:
            return _parks_metadata_cache.copy()

    query = text("""
        SELECT
            p.id as park_id,
            p.name,
            p."countryCode" as country,
            p."regionCode" as region_code,
            p.timezone,
            p."influencingRegions", 
            p."influenceRadiusKm",
            p.latitude,
            p.longitude,
            COUNT(DISTINCT a.id) as attraction_count
        FROM parks p
        LEFT JOIN attractions a ON a."parkId" = p.id
        GROUP BY p.id, p.name, p."countryCode", p."regionCode", p.timezone, 
                 p."influencingRegions", p."influenceRadiusKm", p.latitude, p.longitude
    """)

    with get_db() as db:
        result = db.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        df = convert_df_types(df)

        # Update cache
        if use_cache:
            import time

            _parks_metadata_cache = df.copy()
            _parks_metadata_cache_time = time.time()

        return df


def fetch_park_schedules(
    start_date: datetime.datetime, end_date: datetime.datetime
) -> pd.DataFrame:
    """
    Fetch park opening hours/schedules including special events

    IMPORTANT: This function expects start_date and end_date to represent dates
    in the park's local timezone (not UTC). The dates are converted using .date()
    which extracts the calendar date. If UTC datetime objects are passed, the
    date extraction will be in UTC, which may not match the park's local calendar date.

    For timezone-aware usage, ensure dates are already converted to park local time
    before calling this function, or use the DataFrame-based extraction in
    add_park_schedule_features() which uses local_timestamp.

    Returns DataFrame with columns:
    - park_id
    - date
    - schedule_type (OPERATING, TICKETED_EVENT, EXTRA_HOURS, etc.)
    - opening_time
    - closing_time
    """
    query = text("""
        SELECT
            "parkId" as park_id,
            "attractionId" as attraction_id,
            date,
            "scheduleType" as schedule_type,
            "openingTime" as opening_time,
            "closingTime" as closing_time,
            "isHoliday" as is_holiday,
            "isBridgeDay" as is_bridge_day
        FROM schedule_entries
        WHERE date BETWEEN :start_date AND :end_date
            -- For park schedules: opening/closing must be present (OPERATING) 
            -- OR it can be an event/info/maintenance without times
            AND (
                ("openingTime" IS NOT NULL AND "closingTime" IS NOT NULL)
                OR "scheduleType" IN ('MAINTENANCE', 'CLOSED', 'INFO', 'TICKETED_EVENT', 'PRIVATE_EVENT')
            )
        ORDER BY "parkId", date, "scheduleType"
    """)

    with get_db() as db:
        # Extract date() from datetime objects for SQL query
        # Note: If start_date/end_date are timezone-aware, .date() extracts the date
        # in that timezone. For correct behavior, ensure dates are in park local timezone.
        # The caller (add_park_schedule_features) should handle timezone conversion.
        result = db.execute(
            query, {"start_date": start_date.date(), "end_date": end_date.date()}
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return convert_df_types(df)


def fetch_active_model_version() -> str:
    """
    Fetch the active model version from the database

    Returns:
        Model version string (e.g., 'v1.1.0')
        Falls back to 'v1.0.0' if no active model found
    """
    query = text("""
        SELECT version
        FROM ml_models
        WHERE "isActive" = true
        LIMIT 1
    """)

    try:
        with get_db() as db:
            result = db.execute(query)
            row = result.fetchone()
            if row:
                return row[0]
            else:
                print("⚠️  No active model found in database, using default v1.0.0")
                return "v1.0.0"
    except Exception as e:
        print(f"⚠️  Failed to fetch active model from database: {e}")
        print("   Using default v1.0.0")
        return "v1.0.0"


def fetch_prediction_errors_for_training(
    start_date: datetime.datetime, end_date: datetime.datetime
) -> pd.DataFrame:
    """
    Fetch prediction accuracy errors to calculate sample weights for training

    Returns DataFrame with columns:
    - attractionId
    - timestamp (targetTime from prediction_accuracy)
    - absolute_error
    - percentage_error

    Used to weight training samples: higher weights for samples with high prediction errors
    """
    query = text("""
        SELECT
            pa."attraction_id" as "attractionId",
            pa."target_time" as timestamp,
            pa."absolute_error",
            pa."percentage_error"
        FROM prediction_accuracy pa
        WHERE pa."comparison_status" = 'COMPLETED'
            AND pa."target_time" BETWEEN :start_date AND :end_date
            AND pa."absolute_error" IS NOT NULL
            AND pa."absolute_error" >= 0
    """)

    try:
        with get_db() as db:
            result = db.execute(query, {"start_date": start_date, "end_date": end_date})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return convert_df_types(df)
    except Exception as e:
        print(f"⚠️  Failed to fetch prediction errors: {e}")
        print("   Training without sample weights (using uniform weights)")
        return pd.DataFrame()
