"""Shadow producer — render the next N days' 15-min curves and durably store them.

For each park: load its built profiles, take the served DAILY forecast as the level per
(ride, day), expand it into a calibrated 15-min curve via the additive crowd⊕daytype form,
and write shape_forecasts (the going-forward shadow snapshot). SHADOW only — it never
touches the served champion; score.py later compares it to CatBoost + actuals.

    python3 forecast.py [PARK_UUID ...]
"""

from __future__ import annotations

import logging
import os
import sys
import time

import pandas as pd

import daytypes
import db
import pipeline
from config import get_settings
from profiles import ShapeProfiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("shape.forecast")
settings = get_settings()


def _profiles_path(park_id: str) -> str:
    return os.path.join(settings.MODEL_DIR, f"shape_{park_id}.pkl")


def forecast_park(park_id: str, version: str) -> int:
    meta = db.park_meta(park_id)
    if meta is None or not meta.get("timezone"):
        return 0
    path = _profiles_path(park_id)
    if not os.path.exists(path):
        logger.warning("park %s: no built profiles — run /build first", park_id)
        return 0
    prof: ShapeProfiles = ShapeProfiles.load(path)
    tz = meta["timezone"]
    today = pd.Timestamp.now(tz=tz).normalize().tz_localize(None)
    horizon = settings.SHAPE_FORECAST_DAYS
    lo = today + pd.Timedelta(days=1)  # forecast FORWARD (from tomorrow)
    hi = today + pd.Timedelta(days=horizon)

    levels = db.fetch_daily_levels(park_id, tz, lo.date(), hi.date())
    if levels.empty:
        return 0
    dfn = daytypes.build_daytype_fn(
        db.fetch_holidays(meta["country"]) if meta.get("country") else None,
        meta.get("region"),
    )
    step = pd.Timedelta(minutes=settings.SHAPE_SLOT_MINUTES)
    origin = today.date()
    rows = []
    for r in levels.itertuples():
        day = pd.Timestamp(r.day).normalize()
        curve = prof.serve_curve(r.unique_id, float(r.level), dfn(day))
        for slot, val in enumerate(curve):
            if val == val and val > 0:  # not NaN, positive
                rows.append(
                    {
                        "aid": r.unique_id,
                        "ts": (day + slot * step).to_pydatetime(),
                        "od": origin,
                        "pw": float(val),
                    }
                )
    n = db.write_shape_forecasts(rows, version)
    logger.info(
        "park %s: wrote %d shape_forecasts (%d ride-days, horizon %dd)",
        park_id,
        n,
        len(levels),
        horizon,
    )
    return n


def forecast_all(version: str, park_ids: list[str] | None = None) -> dict:
    parks = pipeline.scope_park_ids(park_ids)
    t0 = time.time()
    total = 0
    for pid in parks:
        try:
            total += forecast_park(pid, version)
        except Exception as e:  # noqa: BLE001
            logger.warning("park %s forecast failed: %s", pid, e)
    logger.info(
        "forecast done: %d rows across %d parks in %.1fs", total, len(parks), time.time() - t0
    )
    return {"rows": total, "parks": len(parks), "version": version}


if __name__ == "__main__":
    import datetime as _dt

    ver = f"shape{_dt.datetime.now(_dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    forecast_all(ver, sys.argv[1:] or None)
