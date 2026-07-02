"""Settings for the PCN service (Park-Crowd Nowcaster — see
docs/ml/custom-intraday-model-design.md).

Reuses the same DB_* env vars as ml-service / nf-service so it points at the same
Postgres without extra config. Phase 0 only needs the DB + a handful of
panel-shaping knobs; model hyperparameters land here once the bake-off picks a
backbone (GP-STGNN / AGCRN / Graph WaveNet …).
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database (same names as ml-service / nf-service)
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "parkfan"
    DB_PASSWORD: str = ""
    DB_NAME: str = "parkfan"

    MODEL_DIR: str = "/app/models"

    # --- Cross-ride tensor scope (Phase 0) ---
    # Comma-separated park UUIDs. Empty = all parks. The tensor is ALWAYS assembled
    # per park (the park-wide crowd state is a within-park signal — rides in different
    # parks don't share a crowd), so this just selects which parks to build.
    PCN_PARK_IDS: str = ""
    # History window (days). 548 (~1.5y) is forward-compatible: today only ~6-7 months
    # exist so it caps at what's there, and as history accumulates the tensor auto-grows
    # without a re-tune (same pattern as nf-service BT_WINDOW_DAYS).
    PCN_WINDOW_DAYS: int = 548
    # Slot resolution. CatBoost serves 15-min slots and the intraday backtest bins to
    # 15 min, so the tensor mirrors that grid exactly (96 slots/day).
    PCN_SLOT_MINUTES: int = 15
    # Minimum wait to count as a real STANDBY observation (aligns with ml/nf services).
    PCN_MIN_WAIT: int = 5
    # Park-open heuristic at slot granularity: a slot counts as "park open" if at least
    # this many rides report a real observation (or a DOWN signal) in it. Mirrors the
    # ml-service operating-day heuristic (>=3 attractions), applied per 15-min slot.
    PCN_MIN_RIDES_OPEN: int = 3

    # --- Model / training (the bake-off candidate; quantile serving) ---
    # graphwavenet (dilated TCN, parallel over time) trains ~6× faster than the recurrent
    # gpstgnn AND saturates the GPU (8.6s vs 54s/park, ~97% vs ~35% util → ~16min vs ~1.7h
    # for 111 parks). gpstgnn unrolls L sequentially → GPU-launch bound. Busy-tail QUALITY
    # is still arbitrated by run_bakeoff.py + the live shadow board; switch back via env if
    # gpstgnn proves materially better on the busy segment.
    PCN_ARCH: str = "graphwavenet"     # 'graphwavenet' (TCN) | 'gpstgnn' (RNN) | 'localgru' (ablation)
    PCN_LOSS: str = "quantile"         # 'quantile' (per-purpose serving) | 'tweedie'
    # Context slots (L). The recurrent AGCRN encoder unrolls L SEQUENTIALLY, so L drives
    # train time; 192 = 2 days of 15-min (96/day) keeps the recent trajectory + daily
    # seasonality while ~2.5× faster than the old 5-day (480) context.
    PCN_INPUT_SIZE: int = 192
    PCN_HORIZON: int = 48              # forecast slots (H) = 12 h
    PCN_HIDDEN_SIZE: int = 64
    PCN_MAX_STEPS: int = 500
    # Quantiles to PERSIST for shadow serving: q0.5 = displayed wait, q0.8 = crowd
    # signal (per-purpose serving, mirroring CatBoost's MultiQuantile split).
    PCN_SERVE_QUANTILES: str = "0.5,0.8"
    # Skip forecasting a park whose freshest 15-min slot is older than this (hours).
    # Seasonally-closed / dead-data parks have a stale latest slot (weeks/months old);
    # forecasting forward from there only writes rows whose targets fall outside any future
    # score window → unscoreable + pcn_forecasts bloat. 36h clears normal overnight/short
    # gaps (≤~16h) with margin while catching genuinely stale parks.
    PCN_MAX_ORIGIN_AGE_HOURS: int = 36
    # Panel window (days) for the every-15-min FORECAST tick. Inference only needs the
    # L-slot context (2 days) plus ffill warm-up — refetching the full PCN_WINDOW_DAYS
    # history each tick re-aggregated ~1.5y of queue_data per park per run for nothing.
    # Training keeps PCN_WINDOW_DAYS. Must comfortably exceed L slots (192 = 2 days).
    PCN_FORECAST_WINDOW_DAYS: int = 7
    # Scoring lookback (hours). Must be >= 48 so the hourly scorer's full-day contract
    # holds: board rows are only (re)written from a window that fully covers their
    # target_date — see score._matched_frame. (24h caused the rolling-window overwrite
    # that degraded matured board days to their last hour.)
    PCN_SCORE_LOOKBACK_HOURS: int = 48
    # Prune pcn_forecasts rows whose created_at is older than this (days). Scoring reads
    # only the last ~2 days of targets and serving the last 3h, so matured origins are
    # dead weight at ~10^7 rows/day. 14d keeps enough for ad-hoc lead-curve analysis.
    PCN_FORECAST_RETENTION_DAYS: int = 14
    # Graph WaveNet temporal depth: receptive field = 1 + sum(2^i) over layers (kernel 2)
    # → 2 layers = 4 slots (1h!), 6 = 64 slots (16h), 8 = 256 slots (64h > L). Flipped
    # 2→8 on 2026-07-02 after the bake-off (8 busy parks, input-size=192, quantile): gwn@8
    # beat gwn@2 on EVERY segment/lead incl. busy (18.8 vs 20.1 MAE), beat the full-context
    # gpstgnn RNN on busy (18.8 vs 19.4) at ~6× less compute, AND flipped the short-lead
    # loss vs persistence (≤3h 9.8 vs 10.4). Serving-safe: load() honours each checkpoint's
    # own trained arch_kwargs, so old layers=2 checkpoints keep serving until retrained.
    PCN_GWN_LAYERS: int = 8

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def park_ids(self) -> list[str]:
        return [p.strip() for p in self.PCN_PARK_IDS.split(",") if p.strip()]

    @property
    def slot_freq(self) -> str:
        """Pandas offset alias for the slot grid, e.g. '15min'."""
        return f"{self.PCN_SLOT_MINUTES}min"

    @property
    def serve_quantiles(self) -> list[float]:
        return [float(q) for q in self.PCN_SERVE_QUANTILES.split(",") if q.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
