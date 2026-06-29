"""Settings for the Shape service (Tageskurven-Expansion — see
docs/ml/shape-model-design.md).

Reuses the same DB_* env vars as ml-/nf-/pcn-service so it points at the same Postgres
without extra config. Phase 0 only needs the DB + the profile-shaping knobs; a learned
candidate's hyperparameters land here once the bake-off (Phase 2) picks one.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database (same names as ml-/nf-/pcn-service)
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "parkfan"
    DB_PASSWORD: str = ""
    DB_NAME: str = "parkfan"

    MODEL_DIR: str = "/app/models"

    # --- Scope ---
    # Comma-separated park UUIDs. Empty = all parks. Profiles are ALWAYS per ride within a
    # park (the daily form is a within-ride signal); this just selects which parks to build.
    SHAPE_PARK_IDS: str = ""
    # History window (days). 548 (~1.5y) is forward-compatible — caps at what exists today
    # (~6-7mo) and auto-grows as history accumulates (same pattern as PCN/nf).
    SHAPE_WINDOW_DAYS: int = 548

    # --- Slot grid (must mirror the serving grid) ---
    SHAPE_SLOT_MINUTES: int = 15  # 96 slots/day; CatBoost/nf bin to 15-min too
    SHAPE_MIN_WAIT: int = 5  # min STANDBY wait to count as a real observation

    # --- What counts as a shape-defining operating day ---
    # A day defines a ride's form only if it actually operated: a real daily peak and enough
    # slots. Near-empty/closed days would otherwise inject flat noise into the profile.
    SHAPE_MIN_DAY_PEAK: int = 5  # daily peak (the normaliser) must be >= this
    SHAPE_MIN_DAY_SLOTS: int = 8  # >= this many operating slots in the day

    # --- Conditioning ---
    # Day-of-week split: 'wend' = weekend vs weekday (robust default); 'full' = per-DOW.
    SHAPE_DOW_MODE: str = "wend"
    # Crowd buckets from terciles of EACH ride's own daily-peak distribution (quiet/mid/busy)
    # — self-calibrating, needs no external crowd source. (Phase 0 fixes this at 3.)
    SHAPE_CROWD_BUCKETS: int = 3
    # A (ride, crowd, dow) cell is trusted only with >= this many distinct days; otherwise the
    # render falls back to a coarser cell (see profiles.pick_curve).
    SHAPE_MIN_OBS_PER_CELL: int = 5
    # Statistic used to normalise each day's curve (the "level"). 'peak' = daily max. Serving
    # rescales to whatever statistic the LCM predicts (peak/P90 factor) — see design §6.
    SHAPE_LEVEL_STAT: str = "peak"

    # Additive-shrinkage render weights (grid-searched 2026-06-29, §8a): the served form is
    # ride_base + α·(crowd_curve − base) + β·(daytype_curve − base). Both calendar factors
    # (crowd + the weekend/holiday/ferien/bridge/season daytype) help additively; shrinking
    # (<1) regularises the noisy per-cell deviations. β>α because daytype carries the most
    # busy-tail signal. Multiplicative crowd×daytype and weather were tested and rejected
    # (data-walled / no form signal).
    SHAPE_ALPHA_CROWD: float = 0.5
    SHAPE_BETA_DAYTYPE: float = 0.6
    # Smooth the served form over ±N adjacent 15-min slots. The per-slot mean-form is noisy
    # (each cell averages ~18 days); neighbouring slots are smooth, so a ±2-slot (±30 min)
    # moving average denoises it for a free ~4% busy-MAE drop (backtest 2026-06-29).
    SHAPE_SMOOTH_SLOTS: int = 2

    # --- Shadow producer (Phase 1) ---
    # How many forward days to render per run (matures over this many days for scoring).
    SHAPE_FORECAST_DAYS: int = 14

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def park_ids(self) -> list[str]:
        return [p.strip() for p in self.SHAPE_PARK_IDS.split(",") if p.strip()]

    @property
    def slots_per_day(self) -> int:
        return (24 * 60) // self.SHAPE_SLOT_MINUTES

    @property
    def slot_freq(self) -> str:
        return f"{self.SHAPE_SLOT_MINUTES}min"


@lru_cache
def get_settings() -> Settings:
    return Settings()
