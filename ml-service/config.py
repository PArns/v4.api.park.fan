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
    VALIDATION_DAYS: int = 30

    # Sample Weight Configuration (Feedback Loop)
    # Feature is enabled by default, but only activates when sufficient data is available
    # With limited data (<30 days), sample weights are automatically disabled to avoid overfitting
    ENABLE_SAMPLE_WEIGHTS: bool = (
        True  # Feature enabled, but requires MIN_DATA_DAYS_FOR_WEIGHTS
    )
    SAMPLE_WEIGHT_FACTOR: float = (
        0.3  # Conservative default (0.3 = 30% boost, weights: 1.0 - 1.3)
    )
    MIN_DATA_DAYS_FOR_WEIGHTS: int = (
        30  # Minimum days of data before weights are actually used
    )

    # Prediction Configuration
    HOURLY_PREDICTIONS: int = 24  # Next 24 hours (internal use)
    DAILY_PREDICTIONS: int = 365  # Next 365 days (1 year)

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
