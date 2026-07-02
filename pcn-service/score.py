"""Shadow scorer — score matured PCN forecasts vs realised actuals AND vs CatBoost,
on the matched (attraction, 15-min slot) population (design doc §12).

Mirrors nf-service's score-comparison, at intraday grain: INNER-join PCN's durable
forward forecast (q0.5 = the displayed wait) to the realised 15-min median and to
CatBoost's freshest pred for the same slot, then write segmented MAE/bias (quiet/mid/
busy × lead bucket) to pcn_intraday_comparisons → surfaced on /v1/admin/system-health.

The aggregation is a pure function (unit-tested); the DB join is correct-by-construction
(mirrors the verified backtest queries).

    python3 score.py [--lookback-hours 24] [PARK_UUID ...]
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
logger = logging.getLogger("pcn.score")
settings = get_settings()

_SEGMENTS = [
    ("all", lambda a: np.ones(len(a), dtype=bool)),
    ("quiet", lambda a: a < 30),
    ("mid", lambda a: (a >= 30) & (a < 60)),
    ("busy", lambda a: a >= 60),
]
_LEADS = [
    ("all", lambda lh: np.ones(len(lh), dtype=bool)),
    ("<=3h", lambda lh: lh <= 3),
    ("3-6h", lambda lh: (lh > 3) & (lh <= 6)),
    (">6h", lambda lh: lh > 6),
]


def aggregate_comparison(df: pd.DataFrame, models: list[str]) -> list[dict]:
    """Pure: from a matched frame (columns: target_slot, lead_h, actual, + one column
    per model) build pcn_intraday_comparisons rows — per target_date × model × segment ×
    lead bucket. Skips empty cells. The matched population is identical across models by
    construction (same rows), so n is comparable."""
    if df.empty:
        return []
    df = df.dropna(subset=["actual"]).copy()
    if df.empty:
        return []
    df["target_date"] = pd.to_datetime(df["target_slot"]).dt.date
    rows: list[dict] = []
    for td, g in df.groupby("target_date"):
        actual = g["actual"].to_numpy(dtype=float)
        lead = g["lead_h"].to_numpy(dtype=float)
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
                    rows.append({
                        "target_date": td,
                        "model": model,
                        "segment": seg_label,
                        "lead_bucket": lead_label,
                        "n": n,
                        "mae": float(np.abs(d).mean()),
                        "bias": float(d.mean()),
                        "mean_actual": float(actual[keep].mean()),
                        "mean_pred": float(pred[keep].mean()),
                    })
    return rows


def full_day_window(
    now_local: pd.Timestamp, lookback_hours: int, slot_freq: str
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """(lo, hi) park-local scoring window honouring the FULL-DAY CONTRACT.

    The board is upserted per target_date, so a date's cells must only ever be
    (re)written from a window that covers that date COMPLETELY (so far). A naive
    rolling lookback shrank matured days run by run — the last write for a day covered
    only its final hour, and skipped (n=0) cells kept stale values from earlier
    windows (visible as lead-bucket n sums exceeding the 'all' row on the live board).

    lo = the first local day the lookback covers FULLY (with the 48h default that is
    yesterday 00:00; today's rows grow monotonically and yesterday's final rewrite
    covers the whole day before the date drops out of scope). hi = now floored to the
    slot grid — the CURRENT slot's realised median is still accumulating, and scoring
    it would compare a full-slot forecast against a partial actual."""
    hi = now_local.floor(slot_freq)
    lo = min(
        (now_local - pd.Timedelta(hours=lookback_hours)).normalize()
        + pd.Timedelta(days=1),                       # first FULLY-covered local day
        now_local.normalize(),                        # never later than today 00:00
    )
    return lo, hi


def _matched_frame(park_id: str, tz: str, lookback_hours: int) -> pd.DataFrame:
    """Join matured PCN q0.5 forecasts ⋈ actuals ⋈ CatBoost on (attraction, slot),
    windowed by the full-day contract (see full_day_window)."""
    # UTC window for the queue_data / wait_time_predictions queries: pass tz-AWARE
    # datetimes (the columns are timestamptz) — matches backtest_intraday_nowcast and
    # avoids a naive-vs-timestamptz comparison that depends on the session TZ.
    now_utc = pd.Timestamp.now(tz="UTC")
    lo_utc = (now_utc - pd.Timedelta(hours=lookback_hours)).to_pydatetime()
    hi_utc = now_utc.to_pydatetime()
    # Local window for pcn_forecasts.target_slot (a naive park-local `timestamp`).
    now_local = pd.Timestamp.now(tz=tz).tz_localize(None)
    lo_local, hi_local = full_day_window(now_local, lookback_hours, settings.slot_freq)

    pcn = db.fetch_pcn_forecasts_window(
        park_id, lo_local.to_pydatetime(), hi_local.to_pydatetime())
    if pcn.empty:
        return pd.DataFrame()
    pcn = pcn[np.isclose(pcn["quantile"], 0.5)]            # q0.5 = displayed wait
    if pcn.empty:
        return pd.DataFrame()
    pcn = pcn.rename(columns={"predicted_wait": "pcn"})
    pcn["lead_h"] = (pcn["target_slot"] - pcn["origin_slot"]) / pd.Timedelta(hours=1)

    actuals = db.fetch_actuals_local(park_id, tz, lo_utc, hi_utc)
    if actuals.empty:
        return pd.DataFrame()
    cat = db.fetch_catboost_local(park_id, tz, lo_utc, hi_utc)
    if cat.empty:
        # No CatBoost pred in this park's window → no fair head-to-head possible here.
        return pd.DataFrame()
    # INNER-join BOTH actuals and CatBoost: PCN and CatBoost are scored on exactly the SAME
    # matched (attraction, 15-min slot) population, so their MAE/bias are comparable and n
    # is identical (design doc §12 — a fair head-to-head). The previous LEFT-join on CatBoost
    # kept PCN-only slots, producing misleading unequal-n cells across the two models.
    m = (pcn.merge(actuals, on=["unique_id", "target_slot"], how="inner")
            .merge(cat, on=["unique_id", "target_slot"], how="inner"))
    return m.rename(columns={"cat_pred": "catboost"})


def _park_matched(park_id: str, lookback_hours: int) -> pd.DataFrame:
    tz = pipeline.park_timezone(park_id)
    if tz is None:
        return pd.DataFrame()
    return _matched_frame(park_id, tz, lookback_hours)


def score_all(lookback_hours: int | None = None, park_ids: list[str] | None = None) -> dict:
    """Pool every park's matched (PCN q0.5 ⋈ actual ⋈ CatBoost) slots, then aggregate
    ACROSS ALL PARKS into the global board in ONE upsert.

    pcn_intraday_comparisons is keyed by (target_date, model, segment, lead_bucket) with NO
    park dimension, so the rows MUST be pooled park-wide before the upsert. A per-park
    upsert had each park overwrite the previous park's cells (last-writer-wins), so the
    board reflected a single arbitrary park's slots instead of the whole estate.

    Needs lookback >= 48h so yesterday is always fully covered (see _matched_frame's
    full-day contract). Also prunes pcn_forecasts past the retention window — scoring
    only ever reads recent targets, and serving only reads the last few hours, so
    matured origins are dead weight that would grow unbounded (~10^7 rows/day)."""
    lookback_hours = lookback_hours or settings.PCN_SCORE_LOOKBACK_HOURS
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
    pruned = db.prune_pcn_forecasts(settings.PCN_FORECAST_RETENTION_DAYS)
    if not frames:
        logger.info("scoring: 0 matched slots across %d parks in %.1fs (pruned %d)",
                    len(parks), time.time() - t0, pruned)
        return {"rows": 0, "matched_slots": 0, "parks": len(parks), "pruned": pruned}
    combined = pd.concat(frames, ignore_index=True)
    rows = aggregate_comparison(combined, models=["pcn", "catboost"])
    n = db.upsert_pcn_comparisons(rows)
    logger.info("scoring done: %d board rows from %d matched slots across %d parks "
                "in %.1fs (pruned %d old forecasts)",
                n, len(combined), len(parks), time.time() - t0, pruned)
    return {"rows": n, "matched_slots": int(len(combined)), "parks": len(parks),
            "pruned": pruned}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="PCN shadow scorer")
    ap.add_argument("--lookback-hours", type=int, default=None)
    ap.add_argument("parks", nargs="*")
    args = ap.parse_args()
    score_all(args.lookback_hours, args.parks or None)
