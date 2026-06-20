"""Fair INTRADAY NOWCAST backtest at CatBoost's NATIVE 15-MIN resolution.

CatBoost serves 15-minute slots (predictionType='hourly' is a misnomer — verified:
predictedTime spacing = 15 min), refreshed every 15 min with current features. So a
fair TFT-vs-CatBoost intraday comparison must predict the SAME 15-min slots.

Design (mirrors how the live nowcast works):
  - freq='15min'. Target = 15-min MEDIAN wait per attraction (CatBoost's target).
  - available_mask: closed/missing slots ffilled for context but masked OUT of the
    loss (no 0-fill underbias — the flaw that broke the 2026-05-23 hourly verdict).
  - Rolling origin: train ONCE on history < earliest base, then for each base predict
    forward using the panel UP TO base (encoder sees the current level) = "train
    nightly, predict live". CatBoost's stored 15-min preds are matched at the SAME
    origin (freshest createdAt <= base) → both forecast each slot from info at `base`.
  - GPU (idle ~99%/day): input_size=480 (5d), horizon=48 (12h), hidden=64.

Output: MAE/bias by lead bucket and busy segment, TFT vs CatBoost, identical
(attraction, 15-min slot) population.

  docker exec <nf> python3 /app/backtest_intraday_nowcast.py [PARK_ID] [N_BASES]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from sqlalchemy import text

import db

PARK = sys.argv[1] if len(sys.argv) > 1 else "6ba1074b-68b6-4646-bbf7-58f3be700444"
N_BASES = int(sys.argv[2]) if len(sys.argv) > 2 else 5
# MODE: "plain" (futr_exog only) vs "occ" (+ park-wide occupancy as hist_exog).
# The HONEST lever for busy accuracy (NOT loss-weighting/forcing): a univariate TFT's
# encoder already captures the lagged target (avg_wait_last_1h, velocity, lag_24h) from
# each ride's own y-window — but it is BLIND to the cross-ride, park-wide busyness that
# explains WHY a slot is busy (CatBoost's #2 feature park_occupancy_pct, ~17-20% imp).
# Feeding realized park occupancy as hist_exog lets TFT learn busy because it understands
# the regime — not because we tilted the loss. Measures plain vs +occ on busy bias.
MODE = sys.argv[3] if len(sys.argv) > 3 else "plain"
# LOSS: "studentt" (default, distribution median) vs "q0.8"/"q0.9" (upper conditional
# quantile — the FORCING lever the user rejected; measured here for reference only, to
# quantify the busy-fix-vs-quiet-inflation trade-off intraday).
LOSS = sys.argv[4] if len(sys.argv) > 4 else "studentt"

SLOT = "15min"
HORIZON = 48        # 12h of 15-min slots
INPUT_SIZE = 480    # 5 days of 15-min context
import os
MAX_STEPS = int(os.getenv("BT_STEPS", "500"))   # default 500; doc default is 1000
HIDDEN = int(os.getenv("BT_HIDDEN", "64"))       # doc default 128
# Data-fetch horizon. Set to 1.5y (forward-compatible): today we only have ~150d so it
# caps at what exists, but as history accumulates the model auto-uses more (no re-tune).
# Production config.py already uses NF_WINDOW_DAYS=730. The real limiter is input_size.
WINDOW_DAYS = int(os.getenv("BT_WINDOW_DAYS", "548"))
WINDOWS_BATCH = int(os.getenv("BT_WB", "128"))   # lower for larger hidden (GPU mem)
WB_FLOOR = int(os.getenv("BT_WB_FLOOR", "16"))   # OOM auto-fallback won't go below this
BASE_HOUR_LOCAL = 11  # anchor each base at 11:00 local

# Postgres bins to local 15-min slots: date_bin('15 minutes', ts_local, origin)
BIN = "date_bin('15 minutes', {col} AT TIME ZONE :tz, TIMESTAMP '2000-01-01 00:00:00')"


def _add_slot(df: pd.DataFrame) -> pd.DataFrame:
    """Cyclic time-of-day at 15-min granularity (96 slots) + hour, so the model can
    learn both the coarse daily shape and the fine intraday ramp."""
    slot = df["ds"].dt.hour * 4 + df["ds"].dt.minute // 15  # 0..95
    df["slot_sin"] = np.sin(2 * np.pi * slot / 96)
    df["slot_cos"] = np.cos(2 * np.pi * slot / 96)
    h = df["ds"].dt.hour
    df["hour_sin"] = np.sin(2 * np.pi * h / 24)
    df["hour_cos"] = np.cos(2 * np.pi * h / 24)
    return df


def fetch_panel(tz: str) -> pd.DataFrame:
    sql = text(
        f"""
        SELECT qd."attractionId"::text AS unique_id,
               {BIN.format(col='qd.timestamp')} AS ds,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") AS y
        FROM queue_data qd JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = :park
          AND qd.timestamp >= NOW() - (:w || ' days')::interval
          AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" >= 5
        GROUP BY 1, 2 HAVING COUNT(*) >= 1
        """
    )
    with db._engine.connect() as c:
        df = pd.read_sql(sql, c, params={"park": PARK, "tz": tz, "w": str(WINDOW_DAYS)})
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = df["y"].astype(float)
    return df


def regularize_with_mask(panel: pd.DataFrame, end_local: pd.Timestamp) -> pd.DataFrame:
    out = []
    for uid, g in panel.groupby("unique_id"):
        g = g.set_index("ds").sort_index()
        idx = pd.date_range(g.index.min(), end_local, freq=SLOT)
        g = g.reindex(idx)
        mask = g["y"].notna().astype(float)
        y = g["y"].ffill().fillna(0.0)
        out.append(pd.DataFrame(
            {"unique_id": uid, "ds": idx, "y": y.values, "available_mask": mask.values}))
    return pd.concat(out, ignore_index=True)


def fetch_actuals(tz: str, lo_utc, hi_utc) -> pd.DataFrame:
    sql = text(
        f"""
        SELECT qd."attractionId"::text AS unique_id,
               {BIN.format(col='qd.timestamp')} AS ds_local,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") AS actual
        FROM queue_data qd JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = :park AND qd.timestamp >= :lo AND qd.timestamp < :hi
          AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY' AND qd."waitTime" >= 5
        GROUP BY 1, 2 HAVING COUNT(*) >= 1
        """
    )
    with db._engine.connect() as c:
        df = pd.read_sql(sql, c, params={"park": PARK, "tz": tz,
                                         "lo": lo_utc.to_pydatetime(), "hi": hi_utc.to_pydatetime()})
    df["ds_local"] = pd.to_datetime(df["ds_local"])
    df["actual"] = df["actual"].astype(float)
    return df


def fetch_catboost(tz: str, bases) -> pd.DataFrame:
    """CatBoost's stored 15-min preds at the SAME origin as each base: freshest
    forecast made AT/BEFORE base for slots in (base, base+H]. bases = list of
    (base_local, base_utc)."""
    sql = text(
        f"""
        SELECT DISTINCT ON (wp."attractionId", {BIN.format(col='wp."predictedTime"')})
            wp."attractionId"::text AS unique_id,
            {BIN.format(col='wp."predictedTime"')} AS ds_local,
            wp."predictedWaitTime"::float AS cat_pred
        FROM wait_time_predictions wp JOIN attractions a ON a.id = wp."attractionId"
        WHERE a."parkId" = :park AND wp."predictionType" = 'hourly'
          AND wp."createdAt" <= :base AND wp."predictedTime" > :base AND wp."predictedTime" <= :hi
        ORDER BY wp."attractionId", {BIN.format(col='wp."predictedTime"')}, wp."createdAt" DESC
        """
    )
    frames = []
    with db._engine.connect() as c:
        for base_local, base_utc in bases:
            hi = base_utc + pd.Timedelta(minutes=15 * HORIZON)
            d = pd.read_sql(sql, c, params={"park": PARK, "tz": tz,
                                            "base": base_utc.to_pydatetime(), "hi": hi.to_pydatetime()})
            if not d.empty:
                d["base"] = base_local
                frames.append(d)
            print(f"    cat base {base_local}: {len(d)} preds", flush=True)
    if not frames:
        return pd.DataFrame(columns=["unique_id", "ds_local", "cat_pred", "base"])
    out = pd.concat(frames, ignore_index=True)
    out["ds_local"] = pd.to_datetime(out["ds_local"])
    return out


def main():
    from neuralforecast import NeuralForecast
    from neuralforecast.models import TFT
    from neuralforecast.losses.pytorch import DistributionLoss

    meta = db.fetch_attraction_meta([PARK])
    tz = meta["timezone"].iloc[0]
    country = meta["country"].iloc[0]
    print(f"park={PARK} tz={tz} country={country} rides={meta['unique_id'].nunique()}", flush=True)

    now_local = pd.Timestamp.now(tz=tz).tz_localize(None)
    today = now_local.normalize()
    bases_local = [today - pd.Timedelta(days=k) + pd.Timedelta(hours=BASE_HOUR_LOCAL)
                   for k in range(1, N_BASES + 1)]
    horizon_hours = HORIZON * 15 / 60
    bases_local = [b for b in bases_local if b < now_local - pd.Timedelta(hours=horizon_hours)]
    bases_local.sort()
    earliest = bases_local[0]
    bases = [(b, b.tz_localize(tz, nonexistent="shift_forward", ambiguous=True).tz_convert("UTC"))
             for b in bases_local]
    print(f"bases (local {BASE_HOUR_LOCAL}:00): {[str(b) for b in bases_local]}", flush=True)

    raw = fetch_panel(tz)
    print(f"raw rows={len(raw)} series={raw['unique_id'].nunique()} "
          f"span={raw['ds'].min()}..{raw['ds'].max()}", flush=True)
    holidays = db.fetch_holidays([country])
    exog = list(db.FUTR_EXOG) + ["slot_sin", "slot_cos", "hour_sin", "hour_cos"]
    hist = ["park_occ"] if MODE == "occ" else []

    def add_occupancy(p):
        """park_occ = cross-ride park-wide busyness at each slot = mean wait over the
        REAL (available_mask==1) rides in that slot. This is the observed-only signal a
        univariate TFT cannot derive from a single ride's y-window (each encoder sees
        only its own series). Fed as hist_exog so the model sees the current regime."""
        real = p[p["available_mask"] > 0]
        occ = real.groupby("ds")["y"].mean().rename("park_occ")
        p = p.merge(occ, on="ds", how="left")
        p["park_occ"] = p["park_occ"].fillna(0.0)
        return p

    def build(p, with_occ=False):
        p = db.add_calendar_covariates(p, meta, holidays)
        p = _add_slot(p)
        if with_occ and MODE == "occ":
            p = add_occupancy(p)
        return p

    train_panel = build(regularize_with_mask(
        raw[raw["ds"] < earliest], earliest - pd.Timedelta(minutes=15)), with_occ=True)
    # hist_exog goes in the historical df only (NOT futr_df — it's observed-only).
    cols = ["unique_id", "ds", "y", "available_mask"] + exog + hist

    if LOSS.startswith("q"):
        from neuralforecast.losses.pytorch import QuantileLoss
        loss_obj = QuantileLoss(q=float(LOSS[1:]))
    elif LOSS == "tweedie":
        # Tweedie = the HONEST distribution for right-skewed, spiky positive data
        # (compound Poisson-Gamma). Matches the likelihood to the actual wait shape
        # instead of tilting it (not forcing). rho in (1,2); 1.5 = balanced default.
        loss_obj = DistributionLoss(distribution="Tweedie", level=[80, 90], rho=1.5)
    elif LOSS == "mqloss":
        from neuralforecast.losses.pytorch import MQLoss
        loss_obj = MQLoss(level=[80, 90])
    elif LOSS == "hubermqloss":
        from neuralforecast.losses.pytorch import HuberMQLoss
        loss_obj = HuberMQLoss(level=[80, 90])
    else:
        loss_obj = DistributionLoss(distribution="StudentT", level=[80, 90])
    def _build(wb: int):
        common = dict(
            h=HORIZON, input_size=INPUT_SIZE, futr_exog_list=exog, scaler_type="robust",
            loss=loss_obj,
            max_steps=MAX_STEPS, start_padding_enabled=True,
            batch_size=8, windows_batch_size=wb, inference_windows_batch_size=wb,
            enable_progress_bar=False)
        if hist:
            common["hist_exog_list"] = hist
        return NeuralForecast(
            models=[TFT(**common, hidden_size=HIDDEN, learning_rate=1e-3)], freq=SLOT)

    # Auto-fallback on CUDA OOM: the TFT attention matrix scales with
    # windows_batch_size (× input_size²), so on an out-of-memory error halve it
    # and retry — down to WB_FLOOR — clearing the CUDA cache between attempts.
    # inference_windows_batch_size is rebuilt to match, so prediction shrinks too.
    # Keeps the harness from hard-failing as the 15-min panel grows.
    import gc
    import torch

    wb = WINDOWS_BATCH
    nf = None
    while True:
        try:
            print(f"training TFT (15-min) on {len(train_panel)} rows "
                  f"({train_panel['available_mask'].mean():.2f} available, "
                  f"windows_batch_size={wb})…", flush=True)
            nf = _build(wb)
            nf.fit(df=train_panel[cols])
            break
        except RuntimeError as e:  # torch.OutOfMemoryError subclasses RuntimeError
            if "out of memory" not in str(e).lower() or wb <= WB_FLOOR:
                raise
            nf = None
            gc.collect()
            torch.cuda.empty_cache()
            new_wb = max(WB_FLOOR, wb // 2)
            print(f"  ⚠️  CUDA OOM at windows_batch_size={wb} — "
                  f"retrying at {new_wb}", flush=True)
            wb = new_wb

    def tcol(cs):
        for x in cs:
            if x in ("TFT", "TFT-median"):
                return x
        return next((x for x in cs if x.startswith("TFT") and "-lo-" not in x and "-hi-" not in x), None)

    preds = []
    for base_local, _ in bases:
        ctx = build(regularize_with_mask(
            raw[raw["ds"] < base_local], base_local - pd.Timedelta(minutes=15)), with_occ=True)
        fds = pd.date_range(base_local, periods=HORIZON, freq=SLOT)
        fut = build(pd.concat([pd.DataFrame({"unique_id": uid, "ds": fds})
                               for uid in ctx["unique_id"].unique()], ignore_index=True))
        yh = nf.predict(df=ctx[cols], futr_df=fut[["unique_id", "ds"] + exog])
        yh = yh.reset_index() if yh.index.name else yh
        tc = tcol(list(yh.columns))
        yh = yh[["unique_id", "ds", tc]].rename(columns={tc: "tft_pred", "ds": "ds_local"})
        yh["base"] = base_local
        yh["lead_h"] = ((yh["ds_local"] - base_local) / pd.Timedelta(hours=1))
        preds.append(yh)
        print(f"  base {base_local}: {len(yh)} tft preds", flush=True)
    tft = pd.concat(preds, ignore_index=True)

    lo_utc = min(b for _, b in bases)
    hi_utc = max(b for _, b in bases) + pd.Timedelta(minutes=15 * HORIZON)
    actuals = fetch_actuals(tz, lo_utc, hi_utc)
    cat = fetch_catboost(tz, bases)

    m = tft.merge(actuals, on=["unique_id", "ds_local"], how="inner")

    # --- Naive baselines (the honest reference any model must beat; no CatBoost needed,
    #     since CatBoost's stored intraday preds are dedup-destroyed for past origins) ---
    # persistence: last real wait strictly before base, held flat over the horizon.
    persist = {}
    for base_local, _ in bases:
        last = (raw[raw["ds"] < base_local].sort_values("ds")
                .groupby("unique_id")["y"].last())
        for uid, v in last.items():
            persist[(uid, base_local)] = float(v)
    m["persist"] = [persist.get((u, b), np.nan) for u, b in zip(m["unique_id"], m["base"])]
    # yesterday-same-slot: actual 24h earlier (seasonal-naive).
    raw_idx = raw.set_index(["unique_id", "ds"])["y"]
    m["yest"] = [
        raw_idx.get((u, d - pd.Timedelta(days=1)), np.nan)
        for u, d in zip(m["unique_id"], m["ds_local"])
    ]
    print(f"\nmatched rows={len(m)} (tft∩actual); persist={m['persist'].notna().sum()} "
          f"yest={m['yest'].notna().sum()}", flush=True)

    def _mb(df, col):
        d = df.dropna(subset=[col])
        if not len(d):
            return float("nan"), float("nan")
        return (d[col] - d["actual"]).abs().mean(), (d[col] - d["actual"]).mean()

    def seg(df, label):
        n = len(df)
        if n == 0:
            return
        tm, tb = _mb(df, "tft_pred")
        pm, pb = _mb(df, "persist")
        ym, yb = _mb(df, "yest")
        print(f"  {label:14} n={n:5d}  TFT {tm:5.1f}/{tb:+6.1f}   "
              f"persist {pm:5.1f}/{pb:+6.1f}   yest {ym:5.1f}/{yb:+6.1f}", flush=True)

    print("\n=== by lead bucket (MAE / bias) — TFT vs naive baselines ===", flush=True)
    seg(m[m["lead_h"] <= 3], "lead <=3h")
    seg(m[(m["lead_h"] > 3) & (m["lead_h"] <= 6)], "lead 3-6h")
    seg(m[m["lead_h"] > 6], "lead >6h")
    print("\n=== by busy segment (actual median) ===", flush=True)
    seg(m[m["actual"] < 30], "quiet <30")
    seg(m[m["actual"].between(30, 59)], "30-59")
    seg(m[m["actual"] >= 60], "busy >=60")
    print("\n=== ALL ===", flush=True)
    seg(m, "ALL")


if __name__ == "__main__":
    main()
