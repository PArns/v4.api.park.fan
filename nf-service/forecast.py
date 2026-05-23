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
    from neuralforecast.models import TFT, NHITS
    from neuralforecast.losses.pytorch import DistributionLoss

    common = dict(
        h=settings.NF_HORIZON,
        input_size=settings.NF_INPUT_SIZE,
        futr_exog_list=db.FUTR_EXOG,
        scaler_type="robust",  # required by NeuralForecast when using exog
        loss=DistributionLoss(distribution="StudentT", level=settings.levels),
        max_steps=settings.NF_MAX_STEPS,
        # Our daily history is short (~5 months) with gaps, so some attraction
        # series are shorter than input_size. Pad them instead of dropping/erroring.
        start_padding_enabled=True,
    )
    return [
        TFT(**common, hidden_size=settings.NF_HIDDEN_SIZE,
            learning_rate=settings.NF_LEARNING_RATE),
        NHITS(**common, learning_rate=settings.NF_LEARNING_RATE),
    ]


def build_panel():
    """Returns (panel_with_covariates, meta, holidays)."""
    park_ids = settings.park_ids
    panel = db.fetch_daily_peak_panel(park_ids)
    if panel.empty:
        raise RuntimeError("Empty panel — check NF_PARK_IDS / data window.")
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
    """Fit + predict in ONE process and return the forward forecast.

    Deliberately does NOT nf.save(): NeuralForecast's save() deep-copies the model
    incl. the DistributionLoss, which raises with the StudentT loss (confirmed in
    the PoC). The nightly job trains + forecasts + persists in a single pass anyway,
    so a save→load split buys nothing and only reintroduces that bug.
    """
    import time
    from neuralforecast import NeuralForecast

    t0 = time.time()
    logger.info("Building panel…")
    panel, meta, holidays = build_panel()
    logger.info(
        "Panel: %d rows, %d series, %s..%s (%.1fs)",
        len(panel), panel["unique_id"].nunique(),
        panel["ds"].min().date(), panel["ds"].max().date(), time.time() - t0,
    )

    cols = ["unique_id", "ds", "y"] + db.FUTR_EXOG
    nf = NeuralForecast(models=_build_models(), freq="D")
    t_fit = time.time()
    logger.info("Fitting %d model(s)…", len(nf.models))
    nf.fit(df=panel[cols])
    logger.info("Fit done in %.1fs", time.time() - t_fit)

    t_pred = time.time()
    futr = db.build_future_frame(panel, meta, holidays, settings.NF_HORIZON)
    y_hat = nf.predict(df=panel[cols], futr_df=futr)
    logger.info(
        "Forecast done in %.1fs (total %.1fs)", time.time() - t_pred, time.time() - t0,
    )
    return y_hat.reset_index() if y_hat.index.name else y_hat
