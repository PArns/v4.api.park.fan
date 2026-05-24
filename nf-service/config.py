"""Settings for the NeuralForecast service. Reuses the same DB_* env vars as
ml-service so it can point at the same Postgres without extra config."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database (same names as ml-service)
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "parkfan"
    DB_PASSWORD: str = ""
    DB_NAME: str = "parkfan"

    MODEL_DIR: str = "/app/models"

    # --- Forecast scope (PoC: one park, daily-peak) ---
    # Comma-separated park UUIDs to include. Empty = all parks (full panel).
    NF_PARK_IDS: str = ""
    # Daily-peak target percentile (matches the calendar peak-vs-peak contract).
    NF_TARGET_PERCENTILE: float = 0.9
    # Minimum wait filter (aligns with ml-service training rules).
    NF_MIN_WAIT: int = 5
    NF_WINDOW_DAYS: int = 730  # ~2 years of history for the daily model

    # --- Model config ---
    # Forecast horizon (days) = 30: TFT serves the calendar's near-term daily crowd
    # levels (days 1-30); CatBoost serves day 31-365 + intraday. 30 is a product call
    # (a month of calendar coverage) traded against data-fit: per-headliner history is
    # short + gappy (median 72 operating-day points, p25=36), so a window needing ~h
    # real horizon points trains 79% of series at h=30 vs 93% at h=14. Validated by the
    # headliner backtest before wiring (accuracy at h=30 must stay close to the h=14
    # baseline of MAE 11.1). Re-evaluate as history accumulates — the ceiling on h
    # rises with more seasonal coverage.
    NF_HORIZON: int = 30
    # Context window (days). Kept at ~90 to match the ~150d of daily history we
    # actually have — 365 would be almost all start-padding (see challenger doc).
    NF_INPUT_SIZE: int = 90
    NF_MAX_STEPS: int = 500     # PoC training steps (tune up later)
    NF_HIDDEN_SIZE: int = 64
    NF_LEARNING_RATE: float = 1e-3
    NF_LEVELS: str = "80,90"    # prediction-interval levels
    # CI mode: when False (default), disable the interactive tqdm/Lightning progress
    # bar — its carriage-return TUI is unreadable in Coolify's log viewer. We emit
    # plain per-epoch log lines instead. Set True locally for the live TUI.
    NF_PROGRESS_BAR: bool = False
    # Dataloader workers for multi-core data loading. Viable again now that training
    # is CHUNKED (small per-chunk panel keeps the shared-memory footprint bounded) and
    # runs in its own process. On the full panel these OOM-killed the fit; per chunk
    # they're fine. 0 = single-thread loading.
    NF_NUM_WORKERS: int = 4
    # Parks per training chunk. Combined with the small windows_batch_size this keeps
    # each fit comfortably in memory; also lets the dataloader workers run.
    NF_PARK_CHUNK_SIZE: int = 10
    # Windows/series per training batch — THE memory lever. NeuralForecast's default
    # is large and made TFT attention spike >14g at fit start (OOM, independent of
    # series count). The hourly PoC ran fine at 128. Keep small.
    NF_WINDOWS_BATCH_SIZE: int = 128
    NF_BATCH_SIZE: int = 16
    # Static (per-series) covariates for the TFT static encoder — the #1 lever per
    # the TFT paper (static-var selection ↑ accuracy). Feeds country/region (encoded)
    # as stat_exog. Enabled: the headliner backtest showed TFT already beats CatBoost
    # on the daily peak; static covariates are the next lever (no coverage risk —
    # static features don't affect windowing).
    NF_USE_STATIC: bool = True

    # Loss for the daily-peak TFT. "studentt" (DistributionLoss, median + 80/90
    # intervals) is the production default. "quantile" (upper conditional quantile,
    # CatBoost-q0.8 analog) was tested via the headliner sweep and REJECTED: TFT's busy
    # bias is only ~−8 (CatBoost's is −29), so a high quantile over-steers — it lifts
    # busy but over-inflates the dominant quiet bucket, so ALL MAE rises monotonically
    # (studentt 11.1 → q0.7 13.0 → q0.8 15.6 → q0.9 24.9) AND it would regress the
    # calendar typical-day-peak calibration (quiet days read too high). Env-gated.
    NF_LOSS: str = "studentt"
    NF_QUANTILE: float = 0.8

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def park_ids(self) -> list[str]:
        return [p.strip() for p in self.NF_PARK_IDS.split(",") if p.strip()]

    @property
    def levels(self) -> list[int]:
        return [int(x) for x in self.NF_LEVELS.split(",") if x.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
