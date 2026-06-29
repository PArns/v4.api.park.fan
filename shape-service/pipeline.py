"""Shared DB→profiles step used by the builder / (later) serving, so the panel-fetch +
profile-assembly is defined once."""

from __future__ import annotations

import logging

import daytypes
import db
import profiles as P
from config import get_settings

settings = get_settings()
logger = logging.getLogger("shape.pipeline")


def build_park_profiles(park_id: str):
    """Build the shape profiles for one park over the configured window, or None if the
    park has no timezone / no operating wait data. The second conditioner is the holiday-
    driven daytype (weekend/feiertag/ferien/brücke/saison); falls back to weekend/weekday
    if the park has no country holidays."""
    meta = db.park_meta(park_id)
    if meta is None or not meta.get("timezone"):
        logger.warning("park %s: no timezone", park_id)
        return None
    panel = db.fetch_shape_panel(park_id, meta["timezone"])
    if panel.empty:
        logger.warning("park %s: no operating wait data in window", park_id)
        return None
    day_label = None
    if meta.get("country"):
        hol = db.fetch_holidays(meta["country"])
        day_label = daytypes.daytype_map(hol, meta.get("region"), panel["day"])
    return P.build_profiles(
        panel,
        park_id=park_id,
        slot_count=settings.slots_per_day,
        dow_mode=settings.SHAPE_DOW_MODE,
        n_buckets=settings.SHAPE_CROWD_BUCKETS,
        min_day_peak=settings.SHAPE_MIN_DAY_PEAK,
        min_day_slots=settings.SHAPE_MIN_DAY_SLOTS,
        min_obs=settings.SHAPE_MIN_OBS_PER_CELL,
        day_label=day_label,
        alpha=settings.SHAPE_ALPHA_CROWD,
        beta=settings.SHAPE_BETA_DAYTYPE,
    )


def scope_park_ids(explicit: list[str] | None = None) -> list[str]:
    if explicit:
        return explicit
    return settings.park_ids or db.fetch_park_ids()
