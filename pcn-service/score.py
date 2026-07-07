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

# Lead-curve forecast horizons (hours). The scorer picks, per target, the origin made
# ~L hours before it — measuring the longer leads the UI serves for rest-of-day, which
# the freshest-origin main board never sees (its 3-6h/>6h buckets fill only from stale
# producer phases). Persistence (wait unchanged from L hours ago) is the honest baseline.
LEADCURVE_LEADS_H = [1.0, 3.0, 6.0]
# Persistence-blend horizon (hours) for the shadow `pcn_blend` model: the weight on the
# origin's realised wait decays 1→0 over this lead, so short-lead predictions fall back
# toward "current wait" — which beats the raw PCN forecast at ≤1h (§7.7 review). Scored on
# the board as a 3rd model BEFORE any serving change, so the blend is validated first.
LEADCURVE_BLEND_HORIZON_H = 3.0


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


def aggregate_leadcurve(df: pd.DataFrame, models: list[str]) -> list[dict]:
    """Pure: from a lead-curve matched frame (columns: target_slot, lead_bucket
    (FORCED '1h'/'3h'/'6h'), actual, + one column per model) build
    pcn_lead_curve_comparisons rows — per target_date × model × segment × lead_bucket.

    Unlike aggregate_comparison, lead_bucket is a column (the forced forecast horizon
    the origin was picked for), not derived from a natural lead — and there is no 'all'
    lead roll-up (each L is scored on its own matched population, which differs per L)."""
    if df.empty:
        return []
    df = df.dropna(subset=["actual"]).copy()
    if df.empty:
        return []
    df["target_date"] = pd.to_datetime(df["target_slot"]).dt.date
    rows: list[dict] = []
    for (td, lead_bucket), g in df.groupby(["target_date", "lead_bucket"]):
        actual = g["actual"].to_numpy(dtype=float)
        for model in models:
            if model not in g.columns:
                continue
            pred = g[model].to_numpy(dtype=float)
            for seg_label, seg_fn in _SEGMENTS:
                keep = seg_fn(actual) & np.isfinite(pred) & np.isfinite(actual)
                n = int(keep.sum())
                if n == 0:
                    continue
                d = pred[keep] - actual[keep]
                rows.append({
                    "target_date": td,
                    "model": model,
                    "segment": seg_label,
                    "lead_bucket": lead_bucket,
                    "n": n,
                    "mae": float(np.abs(d).mean()),
                    "bias": float(d.mean()),
                    "mean_actual": float(actual[keep].mean()),
                    "mean_pred": float(pred[keep].mean()),
                })
    return rows


def serve_round(x: np.ndarray) -> np.ndarray:
    """Serve-side wait quantization — mirror of ml-service round_to_nearest_5 + the
    operating min-10 rule (and of the NestJS roundServedWait the PCN override uses).
    CatBoost's stored preds are ALREADY quantized like this, so the board must score
    PCN's raw q0.5 through the same boundary or the head-to-head compares a served
    number against an unserved one (raw stays in pcn_forecasts on purpose)."""
    x = np.asarray(x, dtype=float)
    rounded = np.floor((x + 2.5) / 5.0) * 5.0
    return np.where(rounded > 0, np.maximum(rounded, 10.0), 0.0)


def persistence_blend(pcn, persist, lead_h, horizon_h: float | None = None) -> np.ndarray:
    """Blend the served PCN wait toward `persist` (the origin's realised wait) at short
    lead: weight = max(0, 1 − lead/horizon), so lead 0 → pure persistence and lead ≥
    horizon → pure PCN. Serve-rounded to mirror serving. Vectorized; NaN in either input
    propagates (a missing persist ⇒ NaN blend, dropped for the pcn_blend model)."""
    horizon_h = horizon_h or LEADCURVE_BLEND_HORIZON_H
    lead = np.asarray(lead_h, dtype=float)
    alpha = np.maximum(0.0, 1.0 - lead / horizon_h)
    blended = alpha * np.asarray(persist, dtype=float) + (1.0 - alpha) * np.asarray(pcn, dtype=float)
    # serve_round coerces NaN→0 (its np.where(nan>0) is False); keep NaN so a missing
    # persist stays NaN and is dropped for the pcn_blend model, not scored as a 0 wait.
    return np.where(np.isnan(blended), np.nan, serve_round(blended))


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


# Easternmost real park tz offset (Pacific/Auckland is +12/+13; +14 is the safe upper
# bound). A local target_date ages out of the per-park window EAST→WEST, but the board is
# pooled across parks with no park dimension — so a matured day re-scored late is rewritten
# by only the still-in-window WESTERN subset, and aggregate_comparison's n=0-skip freezes
# the dropped eastern cells stale (→ the leak + ~7% collapse seen live on 07-01/02). Freeze
# a date once it has exited the easternmost park's window: past that point every remaining
# write is a shrinking subset. Cutting at +14 freezes at ~D+1 10:00 UTC — after the
# westernmost park (~−8) has finished the day and before any real park sheds it, so the
# frozen row holds FULL cross-park coverage.
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
    pcn["pcn"] = serve_round(pcn["pcn"].to_numpy())        # score what is SERVED
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


def _leadcurve_matched_frame(park_id: str, tz: str, lookback_hours: int) -> pd.DataFrame:
    """Lead-curve matched frame: PCN@{1h,3h,6h} ⋈ actual@target, plus a persistence
    baseline = actual @ (target − L). Same full-day window as the main scorer."""
    now_utc = pd.Timestamp.now(tz="UTC")
    # Widen the actuals window by the longest lead so persistence@6h at the window's
    # first target still finds its (target − 6h) realised wait.
    max_lead = max(LEADCURVE_LEADS_H) if LEADCURVE_LEADS_H else 0.0
    lo_utc = (now_utc - pd.Timedelta(hours=lookback_hours + max_lead)).to_pydatetime()
    hi_utc = now_utc.to_pydatetime()
    now_local = pd.Timestamp.now(tz=tz).tz_localize(None)
    lo_local, hi_local = full_day_window(now_local, lookback_hours, settings.slot_freq)

    fan = db.fetch_pcn_leadcurve_window(
        park_id, lo_local.to_pydatetime(), hi_local.to_pydatetime(), LEADCURVE_LEADS_H)
    if fan.empty:
        return pd.DataFrame()
    fan["pcn"] = serve_round(fan["predicted_wait"].to_numpy())     # score what is SERVED
    fan["lead_bucket"] = fan["target_lead_h"].map(lambda h: f"{int(round(h))}h")

    actuals = db.fetch_actuals_local(park_id, tz, lo_utc, hi_utc)
    if actuals.empty:
        return pd.DataFrame()
    m = fan.merge(actuals, on=["unique_id", "target_slot"], how="inner")   # truth @ target
    if m.empty:
        return pd.DataFrame()
    # Persistence baseline: the realised wait L hours before the target (no-change nowcast).
    persist_src = actuals.rename(
        columns={"target_slot": "persist_slot", "actual": "persist"})
    m["persist_slot"] = m["target_slot"] - pd.to_timedelta(m["target_lead_h"], unit="h")
    m = m.merge(persist_src, on=["unique_id", "persist_slot"], how="left")
    # Shadow persistence-blend (§7.7 follow-up): blend the served PCN toward the origin's
    # realised wait (= persist), decaying to pure PCN by the horizon. Since origin =
    # target − lead, `persist` IS the value serving would blend with, so this measures the
    # exact served blend. NaN where persist is missing (dropped for this model downstream).
    m["pcn_blend"] = persistence_blend(
        m["pcn"].to_numpy(), m["persist"].to_numpy(), m["target_lead_h"].to_numpy())
    return m[["target_slot", "lead_bucket", "actual", "pcn", "persist", "pcn_blend"]]


def _park_leadcurve(park_id: str, lookback_hours: int) -> pd.DataFrame:
    tz = pipeline.park_timezone(park_id)
    if tz is None:
        return pd.DataFrame()
    return _leadcurve_matched_frame(park_id, tz, lookback_hours)


def score_leadcurve_all(
    lookback_hours: int | None = None, park_ids: list[str] | None = None
) -> dict:
    """Pool every park's lead-curve matched slots and aggregate PCN@{1h,3h,6h} vs actual
    + persistence into pcn_lead_curve_comparisons (one pooled upsert, like score_all).
    Measures the longer leads the UI serves for rest-of-day — the main board's
    freshest-origin join only ever sees ~15-min leads (§7.7)."""
    lookback_hours = lookback_hours or settings.PCN_SCORE_LOOKBACK_HOURS
    parks = pipeline.scope_park_ids(park_ids)
    t0 = time.time()
    frames = []
    for pid in parks:
        try:
            m = _park_leadcurve(pid, lookback_hours)
            if not m.empty:
                frames.append(m)
        except Exception as e:  # noqa: BLE001
            logger.warning("park %s lead-curve scoring failed: %s", pid, e)
    if not frames:
        logger.info("lead-curve: 0 matched slots across %d parks in %.1fs",
                    len(parks), time.time() - t0)
        return {"rows": 0, "matched_slots": 0, "parks": len(parks)}
    combined = pd.concat(frames, ignore_index=True)
    rows = _freeze_old_days(
        aggregate_leadcurve(combined, models=["pcn", "persist", "pcn_blend"]),
        lookback_hours)
    n = db.upsert_pcn_leadcurve(rows)
    logger.info("lead-curve done: %d board rows from %d matched slots across %d parks "
                "in %.1fs", n, len(combined), len(parks), time.time() - t0)
    return {"rows": n, "matched_slots": int(len(combined)), "parks": len(parks)}


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
        result = {"rows": 0, "matched_slots": 0, "parks": len(parks), "pruned": pruned}
    else:
        combined = pd.concat(frames, ignore_index=True)
        rows = _freeze_old_days(
            aggregate_comparison(combined, models=["pcn", "catboost"]), lookback_hours)
        n = db.upsert_pcn_comparisons(rows)
        logger.info("scoring done: %d board rows from %d matched slots across %d parks "
                    "in %.1fs (pruned %d old forecasts)",
                    n, len(combined), len(parks), time.time() - t0, pruned)
        result = {"rows": n, "matched_slots": int(len(combined)), "parks": len(parks),
                  "pruned": pruned}
    # Lead-curve board (§7.7) — best-effort, independent of the main board's success so a
    # lead-curve failure never blocks the served-freshest swap-gate evidence.
    try:
        result["lead_curve"] = score_leadcurve_all(lookback_hours, park_ids)
    except Exception as e:  # noqa: BLE001
        logger.warning("lead-curve scoring failed: %s", e)
        result["lead_curve"] = {"error": str(e)}
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="PCN shadow scorer")
    ap.add_argument("--lookback-hours", type=int, default=None)
    ap.add_argument("parks", nargs="*")
    args = ap.parse_args()
    score_all(args.lookback_hours, args.parks or None)
