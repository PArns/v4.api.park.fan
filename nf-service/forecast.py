"""TFT (+ NHITS baseline) training and prediction over the daily-peak panel,
with holidays/calendar as KNOWN-FUTURE covariates (the architectural reason for
this service). Offline/batch oriented: train nightly, predict the horizon, the
NestJS side consumes the cached daily forecast.
"""

from __future__ import annotations

import logging
import os

import pandas as pd

from config import get_settings
import db

logger = logging.getLogger("nf.forecast")
settings = get_settings()

_MODEL_PATH = os.path.join(settings.MODEL_DIR, "nf_daily")


def _build_models():
    # Imported lazily so the module loads even before torch is present (e.g. for
    # /health on a half-built image).
    from neuralforecast.models import TFT
    from neuralforecast.losses.pytorch import DistributionLoss

    # CI mode (default): no tqdm progress bar (Coolify can't render its TUI).
    # NOTE: do NOT inject a custom Lightning callback here — a Callback subclass from
    # `lightning.pytorch` is incompatible with neuralforecast's `pytorch_lightning`
    # Trainer and crashed fit at start (the runs that died at "Modules in train mode",
    # at ~49MB — not OOM). Phase-timing logs (Panel/covariates/Fit done) give progress.
    trainer_kw = {"enable_progress_bar": settings.NF_PROGRESS_BAR}

    # Parallel dataloader so CPU-bound fit isn't starved by single-threaded windowing.
    dl_kwargs = {}
    if settings.NF_NUM_WORKERS > 0:
        dl_kwargs = {
            "num_workers": settings.NF_NUM_WORKERS,
            "persistent_workers": True,
        }

    common = dict(
        h=settings.NF_HORIZON,
        input_size=settings.NF_INPUT_SIZE,
        futr_exog_list=db.FUTR_EXOG,
        scaler_type="robust",  # required by NeuralForecast when using exog
        loss=DistributionLoss(distribution="StudentT", level=settings.levels),
        max_steps=settings.NF_MAX_STEPS,
        # THE memory lever: windows per batch. NeuralForecast's default is large, and
        # TFT attention over input_size × windows_batch_size × hidden spiked >14g at
        # fit start → OOM, independent of series count (the hourly PoC with 128 ran
        # fine). Keep it small; this is what actually fixes the OOM.
        batch_size=settings.NF_BATCH_SIZE,
        windows_batch_size=settings.NF_WINDOWS_BATCH_SIZE,
        inference_windows_batch_size=settings.NF_WINDOWS_BATCH_SIZE,
        # Our daily history is short (~5 months) with gaps, so some attraction
        # series are shorter than input_size. Pad them instead of dropping/erroring.
        start_padding_enabled=True,
        dataloader_kwargs=dl_kwargs,
        **trainer_kw,
    )
    # TFT only — it's the model under evaluation; NHITS was an unused baseline whose
    # training just doubled time + memory. (Re-add if a baseline is needed later.)
    return [
        TFT(**common, hidden_size=settings.NF_HIDDEN_SIZE,
            learning_rate=settings.NF_LEARNING_RATE),
    ]


def build_panel(park_ids: list[str]):
    """Returns (panel_with_covariates, meta, holidays) for the given parks. Empty
    frames if the parks have no data in the window (caller skips the chunk)."""
    panel = db.fetch_daily_peak_panel(park_ids)
    if panel.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    meta = db.fetch_attraction_meta(park_ids)
    countries = sorted({c for c in meta["country"].dropna().unique()})
    # include influencing countries
    for inf in meta["influencing"].dropna():
        items = inf if isinstance(inf, list) else []
        countries += [d.get("countryCode") for d in items if d.get("countryCode")]
    countries = sorted(set(countries))
    holidays = db.fetch_holidays(countries)
    panel = db.add_calendar_covariates(panel, meta, holidays)
    return panel, meta, holidays


def train_and_forecast(version: str) -> pd.DataFrame:
    """Train + forecast ITERATIVELY over park chunks, returning the concatenated
    forward forecast.

    Why chunked: the full ~2759-series panel spikes >14g at fit start and gets
    OOM-killed (cgroup oom_kill, memory.peak hit the cap). Training ~NF_PARK_CHUNK_SIZE
    parks at a time bounds the per-fit footprint to ~1-2g, which fits comfortably AND
    makes parallel dataloader workers (NF_NUM_WORKERS) viable again. Each chunk trains
    its own TFT — fine for per-attraction forecasting (own history + calendar drive it).
    No nf.save() (the DistributionLoss deep-copy save bug); fit+predict per chunk.
    """
    import time
    import gc
    from neuralforecast import NeuralForecast

    t0 = time.time()
    park_ids = settings.park_ids or db.fetch_park_ids()
    size = max(1, settings.NF_PARK_CHUNK_SIZE)
    chunks = [park_ids[i:i + size] for i in range(0, len(park_ids), size)]
    logger.info(
        "Iterative training: %d parks in %d chunk(s) of %d (workers=%d)",
        len(park_ids), len(chunks), size, settings.NF_NUM_WORKERS,
    )

    cols = ["unique_id", "ds", "y"] + db.FUTR_EXOG
    parts = []
    for ci, chunk in enumerate(chunks, 1):
        tc = time.time()
        panel, meta, holidays = build_panel(chunk)
        if panel.empty:
            logger.info("chunk %d/%d: no data, skip", ci, len(chunks))
            continue
        nf = NeuralForecast(models=_build_models(), freq="D")
        nf.fit(df=panel[cols])
        futr = db.build_future_frame(panel, meta, holidays, settings.NF_HORIZON)
        yh = nf.predict(df=panel[cols], futr_df=futr)
        parts.append(yh.reset_index() if yh.index.name else yh)
        logger.info(
            "chunk %d/%d done: %d series, %d rows (%.1fs, total %.1fs)",
            ci, len(chunks), panel["unique_id"].nunique(), len(parts[-1]),
            time.time() - tc, time.time() - t0,
        )
        del nf, panel, futr, meta, holidays
        gc.collect()

    if not parts:
        raise RuntimeError("No chunk produced a forecast — check data / park scope.")
    out = pd.concat(parts, ignore_index=True)
    logger.info("All chunks done: %d forecast rows in %.1fs", len(out), time.time() - t0)
    return out
