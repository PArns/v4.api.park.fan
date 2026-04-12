"""
Configuration for ML Service
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
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
    CATBOOST_ITERATIONS: int = (
        2000  # Raised from 1000: best_iteration was 998/1000 → model was still learning
    )
    CATBOOST_LEARNING_RATE: float = 0.03  # 0.03 works well with 2000 iterations
    CATBOOST_DEPTH: int = (
        7  # Raised from 6: more depth for complex holiday×season interactions
    )
    CATBOOST_BORDER_COUNT: int = (
        254  # Increased binning for features for greater precision
    )
    CATBOOST_L2_LEAF_REG: float = 3.0
    CATBOOST_RANDOM_SEED: int = 42
    CATBOOST_THREAD_COUNT: int = (
        -1
    )  # -1 = use all available CPU cores, 0 = use CPU count
    CATBOOST_TASK_TYPE: str = "CPU"  # "CPU" or "GPU" (if GPU available)

    # Training Configuration
    TRAIN_LOOKBACK_YEARS: int = 2
    TRAIN_TEST_SPLIT: float = 0.85
    VALIDATION_DAYS: int = (
        30  # Used for large datasets (>60 days); smaller datasets use adaptive % split
    )

    # Sample Weight Configuration (Feedback Loop)
    # Re-enabled: Now uses efficient attraction-level aggregated MAE from
    # attraction_accuracy_stats instead of row-level prediction_accuracy JOIN.
    ENABLE_SAMPLE_WEIGHTS: bool = True
    SAMPLE_WEIGHT_FACTOR: float = (
        0.5  # 0.5 = up to 50% boost for high-error rides (weights: 1.0 - 1.5)
    )
    MIN_DATA_DAYS_FOR_WEIGHTS: int = (
        7  # Reduced from 30: attraction stats are pre-aggregated anyway
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
    # Raised to 0.75: break dominance of real-time occupancy.
    OCCUPANCY_DROPOUT_RATE: float = 0.75

    # Rolling Average Dropout Rate
    # Raised to 0.65: force model to rely on structural features (DOW, Hour, Holidays).
    ROLLING_AVG_DROPOUT_RATE: float = 0.65

    # rolling_avg_7d Dropout Rate
    # Raised to 0.55: ensure features like attractionId and season gain importance.
    ROLLING_7D_DROPOUT_RATE: float = 0.55

    # Multi-Country Holiday Radius
    DEFAULT_INFLUENCE_RADIUS_KM: int = 200

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
