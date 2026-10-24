"""Shared DB→tensor step used by train / forecast / bake-off, so the panel-fetch +
tensor-assembly is defined once."""

from __future__ import annotations

import logging

import pandas as pd

import db
import tensor as tns
from config import get_settings

logger = logging.getLogger("pcn.pipeline")
settings = get_settings()


def park_timezone(park_id: str) -> str | None:
    meta = db.fetch_attraction_meta([park_id])
    if meta.empty:
        return None
    return meta["timezone"].iloc[0]


def build_park_tensor(
    park_id: str, window_days: int | None = None, in_force: bool | None = None
):
    """Build the cross-ride tensor for one park over the configured window (or an
    explicit override — the forecast tick passes its short inference window), or None
    if the park has no attractions / no queue data in the window. `in_force` overrides
    PCN_TARGET_MODE (forecast.py passes the loaded checkpoint's own mode)."""
    tz = park_timezone(park_id)
    if tz is None:
        logger.warning("park %s: no attractions", park_id)
        return None
    panel = db.fetch_cross_ride_panel(park_id, tz, window_days=window_days)
    if panel.empty:
        logger.warning("park %s (tz=%s): no queue data in window", park_id, tz)
        return None
    readings = windows = None
    if settings.in_force_targets if in_force is None else in_force:
        # Same span as the panel (NOW() - window days), so the grids line up.
        hi = pd.Timestamp.now(tz="UTC")
        lo = hi - pd.Timedelta(days=window_days or settings.PCN_WINDOW_DAYS)
        readings = db.fetch_in_force_readings(
            park_id, tz, lo.to_pydatetime(), hi.to_pydatetime())
        windows = db.fetch_operating_windows(
            park_id, tz, lo.tz_convert(tz).date(), hi.tz_convert(tz).date())
    return tns.build(
        panel, park_id=park_id, freq=settings.slot_freq,
        min_rides_open=settings.PCN_MIN_RIDES_OPEN,
        readings=readings, windows=windows, min_wait=settings.PCN_MIN_WAIT,
    )


def scope_park_ids(explicit: list[str] | None = None) -> list[str]:
    if explicit:
        return explicit
    return settings.park_ids or db.fetch_park_ids()
