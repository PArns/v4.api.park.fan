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


def _epoch_log_callback(chunk_idx: int, n_chunks: int):
    """Plain per-epoch progress for CI mode (no tqdm TUI), with an OVERALL % across
    all chunks. Imported from pytorch_lightning (the package neuralforecast's Trainer
    uses) so the Callback type matches. Returns None if unavailable → training runs."""
    try:
        from pytorch_lightning.callbacks import Callback
    except Exception:
        try:
            from lightning.pytorch.callbacks import Callback
        except Exception:
            return None

    class _Progress(Callback):
        def on_train_epoch_end(self, trainer, pl_module):
            step = int(trainer.global_step)
            mx = int(trainer.max_steps) if trainer.max_steps and trainer.max_steps > 0 else 1
            overall = ((chunk_idx - 1) * mx + min(step, mx)) * 100.0 / (max(n_chunks, 1) * mx)
            m = trainer.callback_metrics
            loss = m.get("train_loss") or m.get("train_loss_epoch") or m.get("train_loss_step")
            try:
                loss = float(loss)
            except Exception:
                loss = float("nan")
            logger.info(
                "progress: chunk %d/%d, step %d/%d (%.0f%% total) loss=%.3f",
                chunk_idx, n_chunks, step, mx, overall, loss,
            )
            # Also persist a small progress file so /train/status (and the admin
            # system-health endpoint) can surface the live % in a UI.
            try:
                import json
                import os
                import time as _t
                prog = {
                    "chunk": chunk_idx, "n_chunks": n_chunks,
                    "step": step, "max_steps": mx, "pct": round(overall, 1),
                    "loss": round(loss, 3) if loss == loss else None,
                    "updated_at": _t.time(),
                }
                with open(os.path.join(settings.MODEL_DIR, "nf_progress.json"), "w") as f:
                    json.dump(prog, f)
            except Exception:
                pass

    return _Progress()


def _build_loss():
    """The configured loss. Quantile (upper conditional quantile, CatBoost-q0.8
    analog) by default; StudentT distribution otherwise."""
    from neuralforecast.losses.pytorch import DistributionLoss, QuantileLoss

    if settings.NF_LOSS == "quantile":
        return QuantileLoss(q=settings.NF_QUANTILE)
    return DistributionLoss(distribution="StudentT", level=settings.levels)


def _build_models(chunk_idx: int = 1, n_chunks: int = 1, stat_exog=None, loss=None):
    # Imported lazily so the module loads even before torch is present (e.g. for
    # /health on a half-built image).
    from neuralforecast.models import TFT

    # CI mode (default): no tqdm progress bar (Coolify can't render its TUI), and mute
    # Lightning's param-summary table — our progress callback + phase logs are enough.
    trainer_kw = {
        "enable_progress_bar": settings.NF_PROGRESS_BAR,
        "enable_model_summary": False,
        # auto-selects GPU (CUDA) when available, falls back to CPU.
        "accelerator": "auto",
    }
    if not settings.NF_PROGRESS_BAR:
        _cb = _epoch_log_callback(chunk_idx, n_chunks)
        if _cb is not None:
            trainer_kw["callbacks"] = [_cb]

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
        loss=loss if loss is not None else _build_loss(),
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
    if stat_exog:
        common["stat_exog_list"] = stat_exog
    # TFT only — it's the model under evaluation; NHITS was an unused baseline whose
    # training just doubled time + memory. (Re-add if a baseline is needed later.)
    return [
        TFT(**common, hidden_size=settings.NF_HIDDEN_SIZE,
            learning_rate=settings.NF_LEARNING_RATE),
    ]


def _build_static_df(meta: pd.DataFrame, panel: pd.DataFrame):
    """Static per-series covariates for the TFT static encoder: country/region as
    integer codes (categorical embeddings). Only for series present in the panel."""
    if meta.empty:
        return None, None
    sdf = meta[["unique_id", "country", "region"]].drop_duplicates("unique_id").copy()
    sdf["unique_id"] = sdf["unique_id"].astype(str)
    sdf = sdf[sdf["unique_id"].isin(panel["unique_id"].astype(str).unique())]
    if sdf.empty:
        return None, None
    sdf["country_code"] = pd.factorize(sdf["country"].fillna("NA"))[0]
    sdf["region_code"] = pd.factorize(sdf["region"].fillna("NA"))[0]
    return sdf[["unique_id", "country_code", "region_code"]], ["country_code", "region_code"]


def build_panel(park_ids: list[str]):
    """Returns (panel_with_covariates, meta, holidays, weather) for the given parks.
    Empty frames if the parks have no data in the window (caller skips the chunk)."""
    panel = db.fetch_daily_peak_panel(park_ids)
    if panel.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    meta = db.fetch_attraction_meta(park_ids)
    countries = sorted({c for c in meta["country"].dropna().unique()})
    # include influencing countries
    for inf in meta["influencing"].dropna():
        items = inf if isinstance(inf, list) else []
        countries += [d.get("countryCode") for d in items if d.get("countryCode")]
    countries = sorted(set(countries))
    holidays = db.fetch_holidays(countries)
    weather = db.fetch_weather(park_ids)
    panel = db.add_calendar_covariates(panel, meta, holidays, weather)
    return panel, meta, holidays, weather


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

    # Rich startup banner (mirrors the CatBoost ml-service training logs so the TFT
    # run is just as observable in the worker output / admin dashboard).
    logger.info("=" * 60)
    logger.info("🚀 Training TFT Daily-Peak Forecaster")
    logger.info("   Version: %s", version)
    logger.info("=" * 60)

    # Device / GPU
    try:
        import torch
        if torch.cuda.is_available():
            _dev = torch.cuda.get_device_name(0)
            _vram = torch.cuda.get_device_properties(0).total_memory / 1e9
            logger.info("🖥️  Device: GPU — %s (%.1f GB VRAM)", _dev, _vram)
        else:
            logger.info("🖥️  Device: CPU (no CUDA)")
    except Exception as e:  # noqa: BLE001
        logger.info("🖥️  Device: unknown (torch probe failed: %s)", e)

    # Memory
    try:
        import psutil
        _mem0 = psutil.Process().memory_info().rss / 1e9
        logger.info("💾 Initial Memory: %.2f GB", _mem0)
    except Exception:
        _mem0 = None

    # Hyperparameters (the levers tuned for GPU — see tft-gpu-tuning-plan)
    logger.info("🤖 Model config:")
    logger.info("   Loss:               %s", settings.NF_LOSS)
    logger.info("   Horizon:            %d days", settings.NF_HORIZON)
    logger.info("   Input size:         %d days", settings.NF_INPUT_SIZE)
    logger.info("   Hidden size:        %d", settings.NF_HIDDEN_SIZE)
    logger.info("   Max steps:          %d", settings.NF_MAX_STEPS)
    logger.info("   Batch size:         %d", settings.NF_BATCH_SIZE)
    logger.info("   Windows batch size: %d", settings.NF_WINDOWS_BATCH_SIZE)
    logger.info("   Learning rate:      %s", settings.NF_LEARNING_RATE)
    logger.info("   Static covariates:  %s", "on" if settings.NF_USE_STATIC else "off")
    logger.info(
        "📊 Panel: %d parks → %d chunk(s) of %d (dataloader workers=%d)",
        len(park_ids), len(chunks), size, settings.NF_NUM_WORKERS,
    )

    cols = ["unique_id", "ds", "y"] + db.FUTR_EXOG
    parts = []
    skipped = 0
    for ci, chunk in enumerate(chunks, 1):
        tc = time.time()
        panel, meta, holidays, weather = build_panel(chunk)
        if panel.empty:
            logger.info("chunk %d/%d: no data, skip", ci, len(chunks))
            continue
        # Per-chunk panel stats (like CatBoost's per-step row/feature counts).
        try:
            _ns = panel["unique_id"].nunique()
            _rows = len(panel)
            _spd = panel.groupby("unique_id").size()
            logger.info(
                "🔧 chunk %d/%d: %d series, %d rows (series len min/median/max: %d/%d/%d)",
                ci, len(chunks), _ns, _rows,
                int(_spd.min()), int(_spd.median()), int(_spd.max()),
            )
        except Exception:
            pass
        # One bad chunk (e.g. all series too short → NeuralForecast "No windows
        # available for training") must not abort the whole run — skip it, keep going.
        try:
            static_df, stat_exog = (None, None)
            if settings.NF_USE_STATIC:
                static_df, stat_exog = _build_static_df(meta, panel)
            nf = NeuralForecast(
                models=_build_models(ci, len(chunks), stat_exog), freq="D"
            )
            nf.fit(df=panel[cols], static_df=static_df)
            futr = db.build_future_frame(panel, meta, holidays, settings.NF_HORIZON, weather)
            # static_df must be passed to predict too (else: "static exogenous
            # variables not found in input dataset"); None when static is off.
            yh = nf.predict(df=panel[cols], static_df=static_df, futr_df=futr)
            parts.append(yh.reset_index() if yh.index.name else yh)
            logger.info(
                "chunk %d/%d done: %d series, %d rows (%.1fs, total %.1fs)",
                ci, len(chunks), panel["unique_id"].nunique(), len(parts[-1]),
                time.time() - tc, time.time() - t0,
            )
        except Exception as e:  # noqa: BLE001
            skipped += 1
            logger.warning(
                "chunk %d/%d (%d series) skipped: %s",
                ci, len(chunks), panel["unique_id"].nunique(), e,
            )
        finally:
            gc.collect()

    logger.info("Chunks: %d ok, %d skipped", len(parts), skipped)

    if not parts:
        raise RuntimeError("No chunk produced a forecast — check data / park scope.")
    out = pd.concat(parts, ignore_index=True)
    # unique_id can carry Python UUID objects from the DB → pyarrow/to_parquet can't
    # serialize them. Force str (also what persist_forecast expects).
    out["unique_id"] = out["unique_id"].astype(str)

    # Rich completion summary (mirrors CatBoost's final metrics block). TFT has no
    # inline val MAE — quality is measured by the headliner backtest — so we report
    # the forecast distribution + run stats, which is what's observable here.
    _elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info("✅ TFT Training Complete!")
    logger.info("   Version:          %s", version)
    logger.info("   Chunks:           %d ok, %d skipped", len(parts), skipped)
    logger.info("   Forecast rows:    %d (%d series × %d-day horizon)",
                len(out), out["unique_id"].nunique(), settings.NF_HORIZON)
    logger.info("   Duration:         %.1fs (%.1f min)", _elapsed, _elapsed / 60)
    try:
        _pcol = _point_forecast_column(list(out.columns))
        if _pcol:
            s = out[_pcol]
            logger.info("📊 Forecast wait-time distribution (%s):", _pcol)
            logger.info("   min/median/max:   %.1f / %.1f / %.1f min",
                        float(s.min()), float(s.median()), float(s.max()))
            logger.info("   mean:             %.1f min", float(s.mean()))
    except Exception as e:  # noqa: BLE001
        logger.info("   (forecast distribution unavailable: %s)", e)
    if _mem0 is not None:
        try:
            import psutil
            _mem1 = psutil.Process().memory_info().rss / 1e9
            logger.info("💾 Final Memory:     %.2f GB (peak delta +%.2f GB)",
                        _mem1, _mem1 - _mem0)
        except Exception:
            pass
    try:
        import torch
        if torch.cuda.is_available():
            logger.info("🖥️  Peak VRAM:       %.2f GB",
                        torch.cuda.max_memory_allocated() / 1e9)
    except Exception:
        pass
    logger.info("=" * 60)
    return out


def _point_forecast_column(cols: list[str]) -> str | None:
    """The TFT point/MEDIAN forecast column. NeuralForecast names it after the model
    (e.g. 'TFT', 'TFT-median'); fall back to the first TFT* column.

    Semantics: this column becomes `predicted_peak`. The training target y is the
    daily P90 of waitTime (NF_TARGET_PERCENTILE), so this MEDIAN forecast is the
    expected/typical daily-peak, i.e. predicted_peak = E[daily-P90]. It is the
    CENTRAL forecast of a P90 target — NOT a P90/upper quantile of the forecast
    distribution (those are the TFT '-lo-'/'-hi-' band columns). This is intentional:
    it makes the day-by-day comparison to the realised daily P90 apples-to-apples."""
    for c in ("TFT", "TFT-median", "TFT-q-50", "TFT-loc"):
        if c in cols:
            return c
    tft_cols = [c for c in cols if c.startswith("TFT")]
    return tft_cols[0] if tft_cols else None
