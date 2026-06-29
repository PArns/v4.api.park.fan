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


def _matched_frame(park_id: str, tz: str, lookback_hours: int) -> pd.DataFrame:
    """Join matured PCN q0.5 forecasts ⋈ actuals ⋈ CatBoost on (attraction, slot)."""
    now_utc = pd.Timestamp.now(tz="UTC")
    lo_utc = (now_utc - pd.Timedelta(hours=lookback_hours)).tz_localize(None)
    hi_utc = now_utc.tz_localize(None)
    now_local = pd.Timestamp.now(tz=tz).tz_localize(None)
    lo_local = now_local - pd.Timedelta(hours=lookback_hours)

    pcn = db.fetch_pcn_forecasts_window(park_id, lo_local, now_local)
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

    m = pcn.merge(actuals, on=["unique_id", "target_slot"], how="inner")
    if not cat.empty:
        m = m.merge(cat, on=["unique_id", "target_slot"], how="left")
    else:
        m["cat_pred"] = np.nan
    return m.rename(columns={"cat_pred": "catboost"})


def score_park(park_id: str, lookback_hours: int = 24) -> int:
    tz = pipeline.park_timezone(park_id)
    if tz is None:
        return 0
    m = _matched_frame(park_id, tz, lookback_hours)
    if m.empty:
        return 0
    rows = aggregate_comparison(m, models=["pcn", "catboost"])
    n = db.upsert_pcn_comparisons(rows)
    logger.info("park %s: scored %d comparison rows (matched slots=%d)",
                park_id, n, len(m))
    return n


def score_all(lookback_hours: int = 24, park_ids: list[str] | None = None) -> dict:
    parks = pipeline.scope_park_ids(park_ids)
    t0 = time.time()
    total = 0
    for pid in parks:
        try:
            total += score_park(pid, lookback_hours)
        except Exception as e:  # noqa: BLE001
            logger.warning("park %s scoring failed: %s", pid, e)
    logger.info("scoring done: %d rows across %d parks in %.1fs",
                total, len(parks), time.time() - t0)
    return {"rows": total, "parks": len(parks)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="PCN shadow scorer")
    ap.add_argument("--lookback-hours", type=int, default=24)
    ap.add_argument("parks", nargs="*")
    args = ap.parse_args()
    score_all(args.lookback_hours, args.parks or None)
