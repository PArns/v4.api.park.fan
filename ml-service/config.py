"""
Configuration for ML Service
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings"""

    # Database
    DB_HOST: str = "postgres"
    DB_PORT: int = 5432
    DB_NAME: str = "parkfan"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"

    # Model Configuration
    MODEL_DIR: str = "/app/models"
    MODEL_VERSION: str = "v1.1.0"

    # CatBoost Hyperparameters
    CATBOOST_ITERATIONS: int = 1000
    CATBOOST_LEARNING_RATE: float = 0.03
    CATBOOST_DEPTH: int = 6
    CATBOOST_L2_LEAF_REG: float = 3.0
    CATBOOST_RANDOM_SEED: int = 42
    CATBOOST_THREAD_COUNT: int = (
        -1
    )  # -1 = use all available CPU cores, 0 = use CPU count
    CATBOOST_TASK_TYPE: str = "CPU"  # "CPU" or "GPU" (if GPU available)

    # Training Configuration
    TRAIN_LOOKBACK_YEARS: int = 2
    TRAIN_TEST_SPLIT: float = 0.8
    VALIDATION_DAYS: int = (
        30  # Used for large datasets (>60 days); smaller datasets use adaptive % split
    )

    # Sample Weight Configuration (Feedback Loop)
    # DISABLED: Currently deactivated due to performance concerns with large prediction_accuracy table.
    # The JOIN on prediction_accuracy significantly slows down training queries.
    #
    # Future Alternative (when > 6 months of data):
    # Instead of timestamp-level matching, use attraction-level aggregated weights:
    #   SELECT attractionId, AVG(absolute_error) as avg_error FROM prediction_accuracy GROUP BY attractionId
    # This reduces the weight calculation to O(attractions) instead of O(training_rows).
    #
    # For now: Train with uniform weights. 59 features + 2 years lookback is sufficient.
    ENABLE_SAMPLE_WEIGHTS: bool = False  # Disabled for performance
    SAMPLE_WEIGHT_FACTOR: float = (
        0.3  # Conservative default (0.3 = 30% boost, weights: 1.0 - 1.3)
    )
    MIN_DATA_DAYS_FOR_WEIGHTS: int = (
        30  # Minimum days of data before weights are actually used
    )

    # Prediction Configuration
    HOURLY_PREDICTIONS: int = 24  # Next 24 hours (internal use)
    DAILY_PREDICTIONS: int = 365  # Next 365 days (1 year)

    # Volatility feature cap (7d std in minutes). Values above this are capped so
    # volatility_7d doesn't dominate feature importance; occupancy/time stay primary.
    # Lowered from 40 → 15 to reduce the dominance of volatility_7d (was 32.91% importance)
    # and allow temporal/holiday features to contribute meaningfully.
    VOLATILITY_CAP_STD_MINUTES: float = 15

    # Occupancy dropout rate for training (0.0 = disabled, 0.3 = 30% of rows).
    # For dropout rows, real-time park_occupancy_pct is replaced with the DOW×hour
    # historical mean. This teaches the model to rely on hour/day_of_week features
    # when only an approximate occupancy is available — matching the inference scenario
    # for future predictions (tomorrow, next week) where real-time occupancy is unknown.
    OCCUPANCY_DROPOUT_RATE: float = 0.50

    # Rolling Average Dropout Rate
    # For dropout rows, avg_wait_last_24h and avg_wait_last_1h are replaced with
    # rolling_avg_7d and rolling_avg_weekday/weekend respectively. This teaches the
    # model to predict from other signals (holidays, season, hour) when real-time
    # rolling averages are unavailable — matching the inference scenario for future
    # predictions (next week, next month) where we use historical DOW/hour profiles.
    ROLLING_AVG_DROPOUT_RATE: float = 0.40

    # Multi-Country Holiday Radius
    DEFAULT_INFLUENCE_RADIUS_KM: int = 200

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Ignore extra fields from .env


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
