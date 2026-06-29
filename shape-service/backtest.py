"""Offline shape backtest — does conditioning the day-FORM measurably help, GIVEN the true
daily level? (design §7, metric 1: isolate SHAPE quality from level error.)

Train the profiles on history minus the last EVAL_DAYS; then for each held-out (ride, day)
render each conditioning variant's normalised form × the day's TRUE peak and score per-slot
MAE vs the realised curve, broken out by busy segment (the day's peak). This answers two
questions at once:
  1. Does form-conditioning (crowd / dow / weather / holiday) beat the ride-global form?
  2. Does it stop helping because cells go sparse → the honest "need more data" signal.

Pure given a panel + a day-condition table, so it is unit-testable and reusable as the
bake-off instrument once richer conditioners land.

    python3 backtest.py [PARK_UUID ...] [--eval-days 14]
"""

from __future__ import annotations

import argparse
import logging

import numpy as np
import pandas as pd

import profiles as P

logger = logging.getLogger("shape.backtest")

# Busy segments by the day's realised peak (the level we condition on).
_SEGMENTS = [
    ("all", lambda peak: True),
    ("quiet<30", lambda peak: peak < 30),
    ("mid30-59", lambda peak: 30 <= peak < 60),
    ("busy>=60", lambda peak: peak >= 60),
]


def _fill(curve: np.ndarray, *fallbacks: np.ndarray) -> np.ndarray:
    """Fill NaN slots of a variant curve from coarser curves so every operating slot gets a
    prediction (fair across variants)."""
    out = curve.copy()
    for fb in fallbacks:
        if fb is None:
            continue
        m = np.isnan(out)
        out[m] = fb[m]
    return out


def _variant_curve(prof: P.ShapeProfiles, ride: str, crowd: int, dow: str, variant: str):
    """Normalised form for a named conditioning variant (no within-variant fallback except
    the shared park/ride fill applied by the caller)."""
    g_park = prof.g_park
    g_r = prof.g_r.get((ride,), (None,))[0]
    if variant == "additive":
        # the chosen model: smooth(ride_base + α·(crowd−base) + β·(daytype−base)) (§8a/§8c).
        base = prof._ride_base(ride)
        cdev = prof._dev(prof.g_rc, (ride, crowd), base)
        ddev = prof._dev(prof.g_rd, (ride, dow), base)
        form = np.clip(base + prof.alpha * cdev + prof.beta * ddev, 0.0, 2.0)
        return P.smooth_curve(form, prof.smooth)
    table = {
        "park": prof.g_park,
        "ride": g_r,
        "dow": prof.g_rd.get((ride, dow), (None,))[0],
        "crowd": prof.g_rc.get((ride, crowd), (None,))[0],
        "crowd_dow": prof.g_rcd.get((ride, crowd, dow), (None,))[0],
    }
    primary = table.get(variant)
    if primary is None:
        primary = np.full(prof.slot_count, np.nan)
    return _fill(primary, g_r, g_park)


def run_backtest(
    panel: pd.DataFrame, *, park_id: str, slot_count: int, eval_days: int = 14,
    dow_mode: str = "wend", n_buckets: int = 3, min_day_peak: float = 5.0,
    min_day_slots: int = 8, min_obs: int = 5, day_label: dict | None = None,
    alpha: float = 0.5, beta: float = 0.6, smooth: int = 0,
    variants=("park", "ride", "dow", "crowd", "crowd_dow", "additive"),
) -> pd.DataFrame:
    """Train on all-but-last-eval_days, score each variant on the held-out days. Returns a
    tidy frame: variant × segment → (n_slots, mae). When `day_label` is given the second
    conditioner (and the 'dow'/'additive' variants) use the daytype label, not weekday."""
    if panel.empty:
        return pd.DataFrame()
    days = np.sort(panel["day"].unique())
    if days.size <= eval_days + 5:
        return pd.DataFrame()
    cutoff = days[-eval_days]
    train = panel[panel["day"] < cutoff]
    ev = panel[panel["day"] >= cutoff]
    prof = P.build_profiles(
        train, park_id=park_id, slot_count=slot_count, dow_mode=dow_mode,
        n_buckets=n_buckets, min_day_peak=min_day_peak, min_day_slots=min_day_slots,
        min_obs=min_obs, day_label=day_label, alpha=alpha, beta=beta, smooth=smooth,
    )
    if prof is None:
        return pd.DataFrame()

    # accumulate per (variant, segment) the abs errors
    err: dict[tuple, list] = {}
    for (ride, day), g in ev.groupby(["unique_id", "day"], observed=True):
        peak = float(g["y"].max())
        if peak < min_day_peak or len(g) < min_day_slots:
            continue
        if (ride,) not in prof.g_r and prof.g_park is None:
            continue
        crowd = prof.level_to_crowd(ride, peak)
        dow = ((day_label.get(pd.Timestamp(day).normalize()) if day_label else None)
               or P.dow_bucket(int(pd.Timestamp(day).dayofweek), dow_mode))
        actual = np.full(slot_count, np.nan)
        actual[g["slot"].to_numpy(dtype=int)] = g["y"].to_numpy(dtype=float)
        obs = np.isfinite(actual)
        for v in variants:
            curve = _variant_curve(prof, ride, crowd, dow, v)
            pred = peak * curve
            d = np.abs(pred[obs] - actual[obs])
            d = d[np.isfinite(d)]
            for seg_label, seg_fn in _SEGMENTS:
                if seg_fn(peak):
                    err.setdefault((v, seg_label), []).append(d)

    rows = []
    for (v, seg), chunks in err.items():
        alld = np.concatenate(chunks) if chunks else np.array([])
        if alld.size == 0:
            continue
        rows.append({"variant": v, "segment": seg, "n_slots": int(alld.size),
                     "mae": float(alld.mean())})
    return pd.DataFrame(rows)


def main():
    import pipeline
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="Shape backtest (form quality, given true level)")
    ap.add_argument("parks", nargs="*")
    ap.add_argument("--eval-days", type=int, default=14)
    args = ap.parse_args()

    import daytypes
    import db
    from config import get_settings
    s = get_settings()
    parks = pipeline.scope_park_ids(args.parks or None)
    frames = []
    for pid in parks:
        meta = db.park_meta(pid)
        if meta is None or not meta.get("timezone"):
            continue
        panel = db.fetch_shape_panel(pid, meta["timezone"])
        day_label = None
        if meta.get("country"):
            day_label = daytypes.daytype_map(
                db.fetch_holidays(meta["country"]), meta.get("region"), panel["day"])
        res = run_backtest(
            panel, park_id=pid, slot_count=s.slots_per_day, eval_days=args.eval_days,
            dow_mode=s.SHAPE_DOW_MODE, n_buckets=s.SHAPE_CROWD_BUCKETS,
            min_day_peak=s.SHAPE_MIN_DAY_PEAK, min_day_slots=s.SHAPE_MIN_DAY_SLOTS,
            min_obs=s.SHAPE_MIN_OBS_PER_CELL, day_label=day_label,
            alpha=s.SHAPE_ALPHA_CROWD, beta=s.SHAPE_BETA_DAYTYPE,
            smooth=s.SHAPE_SMOOTH_SLOTS,
        )
        if not res.empty:
            res["park_id"] = pid
            frames.append(res)
    if not frames:
        logger.info("no backtest results")
        return
    allres = pd.concat(frames, ignore_index=True)
    # pool across parks: n-weighted MAE per variant × segment
    pooled = (allres.assign(se=allres["mae"] * allres["n_slots"])
              .groupby(["segment", "variant"], as_index=False)
              .agg(n=("n_slots", "sum"), se=("se", "sum")))
    pooled["mae"] = pooled["se"] / pooled["n"]
    order = {"all": 0, "busy>=60": 1, "mid30-59": 2, "quiet<30": 3}
    pooled = pooled.sort_values(by=["segment", "mae"],
                                key=lambda c: c.map(order) if c.name == "segment" else c)
    for seg in sorted(pooled["segment"].unique(), key=lambda x: order.get(x, 9)):
        sub = pooled[pooled["segment"] == seg]
        line = "  ".join(f"{r.variant}={r.mae:.2f}(n{int(r.n)})" for r in sub.itertuples())
        logger.info("[%s] %s", seg, line)


if __name__ == "__main__":
    main()
