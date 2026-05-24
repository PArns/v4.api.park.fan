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
    # Forecast horizon (days). 14, not 30/90: bounded by the WEATHER forecast horizon —
    # Open-Meteo gives 16 days, so beyond ~14 the weather futr_exog would be all-NaN
    # (filled → noise). 14 also maximises trainable series (each window needs
    # input_size + h real points; our per-series daily history is short + gappy) and
    # covers the high-value near-term the calendar/scoreboard consume. CatBoost still
    # serves the long horizon.
    NF_HORIZON: int = 14
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
