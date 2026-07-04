"""Shadow scorer — score matured shape forecasts vs realised actuals AND vs CatBoost, on
the matched (attraction, 15-min slot) population, pooled ACROSS ALL PARKS (design §7).

Mirrors the (fixed) pcn-service scorer: INNER-join the durable forward shape forecast to the
realised 15-min median and to CatBoost's freshest pred for the same slot, then write segmented
MAE/bias (quiet/mid/busy × lead-in-days) to shape_comparisons → /v1/admin/ml-comparison. The
board is keyed without a park dimension, so rows are pooled park-wide before the upsert.

    python3 score.py [--lookback-hours 96] [PARK_UUID ...]
"""

from __future__ import annotations

import argparse
import logging
import time

import numpy as np
import pandas as pd

import db
import pipeline
from config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("shape.score")
settings = get_settings()

_SEGMENTS = [
    ("all", lambda a: np.ones(len(a), dtype=bool)),
    ("quiet", lambda a: a < 30),
    ("mid", lambda a: (a >= 30) & (a < 60)),
    ("busy", lambda a: a >= 60),
]
_LEADS = [
    ("all", lambda ld: np.ones(len(ld), dtype=bool)),
    ("<=3d", lambda ld: ld <= 3),
    ("4-7d", lambda ld: (ld > 3) & (ld <= 7)),
    (">7d", lambda ld: ld > 7),
]


def aggregate_comparison(df: pd.DataFrame, models: list[str]) -> list[dict]:
    """Pure: from a matched frame (target_slot, lead_d, actual, + one col per model) build
    shape_comparisons rows per target_date × model × segment × lead bucket. Matched
    population is identical across models (same rows), so n is comparable."""
    if df.empty:
        return []
    df = df.dropna(subset=["actual"]).copy()
    if df.empty:
        return []
    df["target_date"] = pd.to_datetime(df["target_slot"]).dt.date
    rows: list[dict] = []
    for td, g in df.groupby("target_date"):
        actual = g["actual"].to_numpy(dtype=float)
        lead = g["lead_d"].to_numpy(dtype=float)
        for model in models:
            if model not in g.columns:
                continue
            pred = g[model].to_numpy(dtype=float)
            for seg_label, seg_fn in _SEGMENTS:
                smask = seg_fn(actual)
                for lead_label, lead_fn in _LEADS:
                    keep = smask & lead_fn(lead) & np.isfinite(pred) & np.isfinite(actual)
                    n = int(keep.sum())
                    if n == 0:
                        continue
                    d = pred[keep] - actual[keep]
                    rows.append(
                        {
                            "target_date": td,
                            "model": model,
                            "segment": seg_label,
                            "lead_bucket": lead_label,
                            "n": n,
                            "mae": float(np.abs(d).mean()),
                            "bias": float(d.mean()),
                            "mean_actual": float(actual[keep].mean()),
                            "mean_pred": float(pred[keep].mean()),
                        }
                    )
    return rows


def full_day_window(
    now_local: pd.Timestamp, lookback_hours: int, slot_freq: str
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """(lo, hi) park-local scoring window honouring the FULL-DAY CONTRACT (same fix
    as pcn-service score.py): the board upserts per target_date, so a date's cells may
    only be (re)written from a window that covers the date COMPLETELY (so far) —
    otherwise the rolling lookback degrades matured days run by run (the last write
    kept only the tail of the day, and skipped n=0 cells went stale). lo = first
    FULLY-covered local day (96h default → last 3 full days + today); hi = now floored
    to the slot grid (the current slot's realised median is still partial)."""
    hi = now_local.floor(slot_freq)
    lo = min(
        (now_local - pd.Timedelta(hours=lookback_hours)).normalize()
        + pd.Timedelta(days=1),                       # first FULLY-covered local day
        now_local.normalize(),                        # never later than today 00:00
    )
    return lo, hi


# Easternmost real park tz offset (+14 safe upper bound). The board is pooled across all
# parks with no park dimension, so a local target_date ages out of the per-park window
# EAST→WEST: a matured day re-scored late is rewritten by only the still-in-window WESTERN
# subset, and aggregate_comparison's n=0-skip freezes the dropped eastern cells stale
# (→ leak + collapse, same bug as pcn-service). Freeze a date once it leaves the
# easternmost park's window; cutting at +14h freezes at full cross-park coverage.
_MAX_TZ_OFFSET_HOURS = 14


def freeze_cutoff_date(lookback_hours: int, now_utc: pd.Timestamp | None = None):
    """Oldest target_date still safe to (re)write. Older dates have left some park's window
    and must stay frozen at their full-coverage state (see _MAX_TZ_OFFSET_HOURS)."""
    now_utc = now_utc if now_utc is not None else pd.Timestamp.now(tz="UTC")
    now_east = now_utc.tz_convert(None) + pd.Timedelta(hours=_MAX_TZ_OFFSET_HOURS)
    lo, _ = full_day_window(now_east, lookback_hours, settings.slot_freq)
    return lo.date()


def _freeze_old_days(rows: list[dict], lookback_hours: int) -> list[dict]:
    """Drop board rows for dates that have aged out of the easternmost park's window."""
    cutoff = freeze_cutoff_date(lookback_hours)
    return [r for r in rows if r["target_date"] >= cutoff]


def _park_matched(park_id: str, lookback_hours: int) -> pd.DataFrame:
    tz = db.park_timezone(park_id)
    if tz is None:
        return pd.DataFrame()
    now_utc = pd.Timestamp.now(tz="UTC")
    lo_utc = (now_utc - pd.Timedelta(hours=lookback_hours)).to_pydatetime()
    hi_utc = now_utc.to_pydatetime()
    now_local = pd.Timestamp.now(tz=tz).tz_localize(None)
    lo_local, hi_local = full_day_window(now_local, lookback_hours, settings.slot_freq)

    shape = db.fetch_shape_forecasts_window(
        park_id, lo_local.to_pydatetime(), hi_local.to_pydatetime())
    if shape.empty:
        return pd.DataFrame()
    shape = shape.rename(columns={"predicted_wait": "shape"})
    shape["lead_d"] = (
        shape["target_slot"].dt.normalize() - pd.to_datetime(shape["origin_date"])
    ).dt.days

    actuals = db.fetch_actuals_slots(park_id, tz, lo_utc, hi_utc)
    if actuals.empty:
        return pd.DataFrame()
    cat = db.fetch_catboost_slots(park_id, tz, lo_utc, hi_utc)
    if cat.empty:
        return pd.DataFrame()
    # INNER on both: shape and CatBoost scored on exactly the same matched slots.
    m = shape.merge(actuals, on=["unique_id", "target_slot"], how="inner").merge(
        cat, on=["unique_id", "target_slot"], how="inner"
    )
    return m.rename(columns={"cat_pred": "catboost"})


def score_all(lookback_hours: int = 96, park_ids: list[str] | None = None) -> dict:
    """Pool every park's matched (shape ⋈ actual ⋈ CatBoost) slots, aggregate across all
    parks into the global board in one upsert (no park dimension in the PK)."""
    parks = pipeline.scope_park_ids(park_ids)
    t0 = time.time()
    frames = []
    for pid in parks:
        try:
            m = _park_matched(pid, lookback_hours)
            if not m.empty:
                frames.append(m)
        except Exception as e:  # noqa: BLE001
            logger.warning("park %s scoring failed: %s", pid, e)
    pruned = db.prune_shape_forecasts(settings.SHAPE_FORECAST_RETENTION_DAYS)
    if not frames:
        logger.info(
            "scoring: 0 matched slots across %d parks in %.1fs (pruned %d)",
            len(parks), time.time() - t0, pruned,
        )
        return {"rows": 0, "matched_slots": 0, "parks": len(parks), "pruned": pruned}
    combined = pd.concat(frames, ignore_index=True)
    rows = _freeze_old_days(
        aggregate_comparison(combined, models=["shape", "catboost"]), lookback_hours)
    n = db.upsert_shape_comparisons(rows)
    logger.info(
        "scoring done: %d board rows from %d matched slots across %d parks in %.1fs "
        "(pruned %d old forecasts)",
        n,
        len(combined),
        len(parks),
        time.time() - t0,
        pruned,
    )
    return {"rows": n, "matched_slots": int(len(combined)), "parks": len(parks),
            "pruned": pruned}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Shape shadow scorer")
    ap.add_argument("--lookback-hours", type=int, default=96)
    ap.add_argument("parks", nargs="*")
    args = ap.parse_args()
    score_all(args.lookback_hours, args.parks or None)
