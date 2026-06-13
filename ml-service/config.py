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
    CATBOOST_RSM: float = (
        0.8  # Random subspace method: use 80% of features per split to reduce
        # overfitting; helps generalize on 71-feature space
    )
    CATBOOST_MIN_DATA_IN_LEAF: int = (
        20  # Minimum samples per leaf; prevents overfitting on rare holiday rows
    )
    CATBOOST_RANDOM_SEED: int = 42
    CATBOOST_THREAD_COUNT: int = (
        -1
    )  # -1 = use all available CPU cores, 0 = use CPU count
    # Cores left free for serving while the 06:00 training subprocess runs. With
    # THREAD_COUNT=-1 training otherwise pins every core and starves the uvicorn
    # workers, so concurrent /predict calls queued behind it hit the 120s client
    # timeout. Reserving a couple of cores keeps serving responsive during training.
    CATBOOST_TRAINING_RESERVED_CORES: int = 2
    # Threads per inference call. Predicts now run concurrently in a threadpool, so a
    # high count would oversubscribe the CPU across simultaneous requests. Inference
    # batches are tiny (a few attractions × N timestamps) so a low count is plenty.
    CATBOOST_INFERENCE_THREAD_COUNT: int = 2
    CATBOOST_TASK_TYPE: str = "CPU"  # "CPU" or "GPU" (if GPU available)
    # GPU-only hyperparameters. The CPU-tuned regularizers rsm + min_data_in_leaf are
    # not usable on the GPU backend as-is (rsm: GPU only for pairwise losses;
    # min_data_in_leaf: GPU only with a non-symmetric grow policy). Dropping both
    # outright produced an over-fit, over-predicting model (MAE 5.6 → 26, R² −1.15).
    # So on GPU we re-express the regularization: a non-symmetric grow policy to keep
    # min_data_in_leaf, plus row bagging (subsample + Bernoulli bootstrap) as the
    # rsm substitute. All env-overridable for tuning. Only applied when TASK_TYPE=GPU.
    CATBOOST_GPU_GROW_POLICY: str = "Depthwise"  # enables min_data_in_leaf on GPU
    CATBOOST_GPU_SUBSAMPLE: float = 0.8  # row bagging (rsm has no GPU equivalent)
    CATBOOST_GPU_BOOTSTRAP_TYPE: str = "Bernoulli"  # required for `subsample`
    CATBOOST_GPU_RAM_PART: str = ""  # e.g. "0.5" to leave VRAM for nf-service; "" = CatBoost default
    # On GPU the model lives in VRAM but data quantization still needs CPU RAM (~8.5g
    # here), which exceeds the 8g CPU cap and floods the log with "using more CPU RAM
    # than the limit" warnings. The container has 20g, so raise the CPU-RAM cap on GPU.
    CATBOOST_GPU_USED_RAM_LIMIT: str = "14gb"
    # Hard cap on RAM the CatBoost trainer may use. Training now runs as an isolated
    # subprocess (see main.py), so an OOM kill only tears down that process. Budget:
    #   container mem_limit 20g
    #   - 2 uvicorn workers  ~4g (serving)
    #   - subprocess Python + data  ~5g
    #   - CatBoost this limit  ~8g
    #   headroom  ~3g
    # Empty = unlimited (unsafe on this host).
    CATBOOST_USED_RAM_LIMIT: str = "8gb"
    # Loss function. Default RMSEWithUncertainty (predicts the conditional mean +
    # VirtEnsembles uncertainty). Set to e.g. "Quantile:alpha=0.7" to predict an
    # upper conditional quantile instead — directly lifts the under-predicted busy
    # tail (the model regresses busy days to the mean). Quantile disables the
    # (empirically near-zero) VirtEnsembles uncertainty. Env-gated so the nightly
    # cron is unaffected unless explicitly overridden.
    # Production default = "MultiQuantile:alpha=0.5,0.8,0.95" — predicts three
    # conditional quantiles at once for PER-PURPOSE serving (predict.py):
    #   - q0.5 (median) → the wait-time DISPLAY. Backtest (2026-06-13, 21d holdout,
    #     392k slots): vs the old single q0.8, the median cut ALL MAE 5.15→3.93
    #     (−24%) and quiet MAE 4.03→2.39 (−41%), and removed the +3.4-min quiet
    #     over-read (bias +3.46→+0.60). The honest central estimate.
    #   - q0.8 → the CROWD-LEVEL signal. Same backtest: q0.8 is near-perfectly
    #     calibrated on busy (bias +0.62) — better than q0.95, which OVER-shoots
    #     busy massively (bias +16.5). So the crowd quantile is 0.8, NOT 0.95.
    # NOTE: the busy-tail MAE itself is inherent variance, not a bias a quantile can
    # fix — measured & rejected (q0.95 made busy worse; a CatBoost+TFT daily ensemble
    # also lost to TFT alone). See docs/ml/tft-vs-catboost-clean-comparison.md.
    # Override to "Quantile:alpha=0.8" or "RMSEWithUncertainty" to revert.
    CATBOOST_LOSS_FUNCTION: str = "MultiQuantile:alpha=0.5,0.8,0.95"
    CATBOOST_POSTERIOR_SAMPLING: bool = True  # only used for RMSEWithUncertainty
    # Per-purpose serving quantiles (must be present in the MultiQuantile alpha list).
    SERVING_WAIT_QUANTILE: float = 0.5  # median → honest wait display
    SERVING_CROWD_QUANTILE: float = 0.8  # busy-calibrated → crowd-level signal
    # Busyness weighting: down-weight the ~72% quiet rows / up-weight busy rows so
    # the loss attends to the under-fit busy tail. Improves overall calibration on
    # top of the quantile lift (q0.7w/q0.8w had the best holdout MAE). Default ON.
    CATBOOST_BUSY_WEIGHT: bool = True

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
    # Daily prediction = PEAK, not a single 14:00 value. We predict these peak-window
    # hours per day and collapse to the per-day MAX (≈ the daily P90 peak that the
    # calendar's typical-day-peak baseline uses). A single 14:00 value systematically
    # under-read the peak (backtest: bias −13..−45 on busy headliner days). Comma list.
    DAILY_PEAK_HOURS: str = "12,14,16"

    # Volatility feature cap (7d std in minutes). Values above this are capped so
    # volatility_7d doesn't dominate feature importance; occupancy/time stay primary.
    # Raised back to 30 (was briefly 15 which suppressed all volatility signal).
    VOLATILITY_CAP_STD_MINUTES: float = 30

    # Occupancy dropout rate for training (0.0 = disabled, 0.5 = 50% of rows).
    # Simulates future predictions where real-time occupancy is unknown.
    # Reverted from 0.75 (too aggressive — destroyed short-term signal, caused R²≈0).
    OCCUPANCY_DROPOUT_RATE: float = 0.50

    # Rolling Average Dropout Rate (avg_wait_last_24h)
    # Simulates next-week predictions where avg_wait_last_24h is irrelevant.
    # Reverted from 0.65 (too aggressive — see above).
    ROLLING_AVG_DROPOUT_RATE: float = 0.40

    # Short-window Rolling Average Dropout Rate (avg_wait_last_1h)
    # Higher than ROLLING_AVG_DROPOUT_RATE because this feature had 21% importance
    # during live analysis (too dominant; model should not rely on it for future preds).
    ROLLING_1H_DROPOUT_RATE: float = 0.60

    # rolling_avg_7d Dropout Rate
    # Reverted from 0.55 (too aggressive — see above).
    ROLLING_7D_DROPOUT_RATE: float = 0.30

    # Holiday dropout rate: on public-holiday rows, additionally replace rolling
    # averages + occupancy with historical DOW proxies so the model is forced to
    # learn is_holiday_primary / is_school_holiday_primary as independent signals
    # rather than relying on rolling averages that already reflect holiday crowding.
    HOLIDAY_DROPOUT_RATE: float = 0.70

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
