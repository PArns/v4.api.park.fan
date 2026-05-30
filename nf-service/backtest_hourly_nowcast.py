"""Fair INTRADAY NOWCAST backtest: TFT vs CatBoost on the hourly surface.

Fixes the three flaws that made the 2026-05-23 hourly PoC verdict inconclusive
(see docs/ml/busy-day-prediction-challenger.md "Hourly TFT vs CatBoost"):

  1. **0-fill underbias → `available_mask`.** Closed hours kept the grid regular but
     0-filled `y`, so the StudentT mean was dragged down (~−6.6 bias) and the loss
     was trained on spurious zeros. Here closed hours are forward-filled for the
     encoder context but carry `available_mask=0` → excluded from the loss; eval
     scores ONLY real operating hours.
  2. **lean config → GPU capacity.** The old run was host-RAM-limited (hidden=32,
     input=120, max_steps=300). The GPU is idle ~99% of the day, so we use it:
     hidden=64, input_size=168 (1 week), max_steps=500.
  3. **single base_time → rolling origin.** A nowcast is recomputed every 15 min on
     the CURRENT state. We mirror that: train ONCE on history < the earliest base,
     then for each base predict the next H hours using the panel UP TO that base
     (the encoder sees the current level) — "train nightly, predict live". CatBoost's
     stored hourly predictions (generate-hourly cron, */15) are matched at the SAME
     origin so both models forecast each target hour from info available at `base`.

Target = hourly MEDIAN wait per attraction (= CatBoost's PERCENTILE_CONT(0.5) intraday
target). Output: MAE/bias by lead bucket and by busy segment, TFT vs CatBoost, on the
identical (attraction, hour) population.

Run inside the nf-service container (GPU):
  docker exec <nf> python3 /app/backtest_hourly_nowcast.py [PARK_ID] [N_BASES]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from sqlalchemy import text

import db

# Shanghai Disneyland — busy (p90 ~75, ~26 rides, rich busy tail), good history.
PARK = sys.argv[1] if len(sys.argv) > 1 else "6ba1074b-68b6-4646-bbf7-58f3be700444"
N_BASES = int(sys.argv[2]) if len(sys.argv) > 2 else 5

TZ = None  # filled from park meta
HORIZON = 24  # forecast next 24h from each base (matches CatBoost hourly horizon)
INPUT_SIZE = 168  # 1 week of hourly context (GPU — no lean constraint)
MAX_STEPS = 500
HIDDEN = 64
WINDOW_DAYS = 150
WINDOWS_BATCH = 256  # GPU headroom; the dominant TFT memory lever
BASE_HOUR_LOCAL = 11  # anchor each base at 11:00 local (mid-morning, queue building)


def _add_hour(df: pd.DataFrame) -> pd.DataFrame:
    h = df["ds"].dt.hour
    df["hour_sin"] = np.sin(2 * np.pi * h / 24)
    df["hour_cos"] = np.cos(2 * np.pi * h / 24)
    return df


def fetch_hourly_median_panel(tz: str) -> pd.DataFrame:
    """Hourly median wait per attraction, park-local hour buckets. Only OPERATING/
    STANDBY/wait>=5 — these are the REAL (available) observations."""
    sql = text(
        """
        SELECT qd."attractionId"::text AS unique_id,
               date_trunc('hour', qd.timestamp AT TIME ZONE :tz) AS ds,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") AS y
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = :park
          AND qd.timestamp >= NOW() - (:w || ' days')::interval
          AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" >= 5
        GROUP BY 1, 2
        HAVING COUNT(*) >= 1
        """
    )
    with db._engine.connect() as c:
        df = pd.read_sql(sql, c, params={"park": PARK, "tz": tz, "w": str(WINDOW_DAYS)})
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = df["y"].astype(float)
    return df


def regularize_with_mask(panel: pd.DataFrame, end_local: pd.Timestamp) -> pd.DataFrame:
    """Continuous hourly grid per series [min..end_local]. Real operating hours →
    available_mask=1 (real y). Closed/missing hours → available_mask=0, y forward-
    filled (smooth encoder context) then 0 for any leading gap. The mask keeps these
    rows OUT of the loss, so no 0-fill underbias."""
    out = []
    for uid, g in panel.groupby("unique_id"):
        g = g.set_index("ds").sort_index()
        idx = pd.date_range(g.index.min(), end_local, freq="h")
        g = g.reindex(idx)
        mask = g["y"].notna().astype(float)
        y = g["y"].ffill().fillna(0.0)
        out.append(
            pd.DataFrame(
                {"unique_id": uid, "ds": idx, "y": y.values, "available_mask": mask.values}
            )
        )
    return pd.concat(out, ignore_index=True)


def fetch_actuals(tz: str, lo_utc: pd.Timestamp, hi_utc: pd.Timestamp) -> pd.DataFrame:
    """Realised hourly median per attraction over the eval window (real ops only)."""
    sql = text(
        """
        SELECT qd."attractionId"::text AS unique_id,
               date_trunc('hour', qd.timestamp AT TIME ZONE :tz) AS ds_local,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") AS actual
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = :park
          AND qd.timestamp >= :lo AND qd.timestamp < :hi
          AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" >= 5
        GROUP BY 1, 2
        HAVING COUNT(*) >= 1
        """
    )
    with db._engine.connect() as c:
        df = pd.read_sql(
            sql, c,
            params={"park": PARK, "tz": tz,
                    "lo": lo_utc.to_pydatetime(), "hi": hi_utc.to_pydatetime()},
        )
    df["ds_local"] = pd.to_datetime(df["ds_local"])
    df["actual"] = df["actual"].astype(float)
    return df


def fetch_catboost_hourly(tz: str, bases_utc, horizon_h: int) -> pd.DataFrame:
    """CatBoost's stored hourly predictions (generate-hourly */15 cron) matched at the
    SAME origin as each TFT base: for each base, the freshest forecast made AT/BEFORE
    base (createdAt <= base) for target hours in (base, base+H]. → both models forecast
    each target hour from info available at `base`."""
    frames = []
    sql = text(
        """
        SELECT DISTINCT ON (wp."attractionId", date_trunc('hour', wp."predictedTime" AT TIME ZONE :tz))
            wp."attractionId"::text AS unique_id,
            date_trunc('hour', wp."predictedTime" AT TIME ZONE :tz) AS ds_local,
            wp."predictedWaitTime"::float AS cat_pred
        FROM wait_time_predictions wp
        JOIN attractions a ON a.id = wp."attractionId"
        WHERE a."parkId" = :park
          AND wp."predictionType" = 'hourly'
          AND wp."createdAt" <= :base
          AND wp."predictedTime" >  :base
          AND wp."predictedTime" <= :hi
        ORDER BY wp."attractionId",
                 date_trunc('hour', wp."predictedTime" AT TIME ZONE :tz),
                 wp."createdAt" DESC
        """
    )
    with db._engine.connect() as c:
        for base in bases_utc:
            hi = base + pd.Timedelta(hours=horizon_h)
            d = pd.read_sql(
                sql, c,
                params={"park": PARK, "tz": tz,
                        "base": base.to_pydatetime(), "hi": hi.to_pydatetime()},
            )
            if not d.empty:
                d["base"] = base
                frames.append(d)
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

    # Rolling-origin bases: last N_BASES days at BASE_HOUR_LOCAL, all in the PAST so
    # actuals + CatBoost stored preds exist.
    now_local = pd.Timestamp.now(tz=tz).tz_localize(None)
    today = now_local.normalize()
    bases_local = [
        today - pd.Timedelta(days=k) + pd.Timedelta(hours=BASE_HOUR_LOCAL)
        for k in range(1, N_BASES + 1)
    ]
    bases_local = [b for b in bases_local if b < now_local - pd.Timedelta(hours=HORIZON)]
    bases_local.sort()
    earliest = bases_local[0]
    bases_utc = [
        b.tz_localize(tz, nonexistent="shift_forward", ambiguous=True).tz_convert("UTC")
        for b in bases_local
    ]
    print(f"bases (local {BASE_HOUR_LOCAL}:00): {[str(b) for b in bases_local]}", flush=True)

    # --- Panel + covariates (history up to NOW; we slice per base at predict time) ---
    raw = fetch_hourly_median_panel(tz)
    print(f"raw rows={len(raw)} series={raw['unique_id'].nunique()} "
          f"span={raw['ds'].min()}..{raw['ds'].max()}", flush=True)
    holidays = db.fetch_holidays([country])

    exog = list(db.FUTR_EXOG) + ["hour_sin", "hour_cos"]

    def build_with_cov(panel_masked: pd.DataFrame) -> pd.DataFrame:
        p = db.add_calendar_covariates(panel_masked, meta, holidays)
        p = _add_hour(p)
        return p

    # Train ONCE on history strictly before the earliest base (true OOS for every base).
    train_panel = regularize_with_mask(
        raw[raw["ds"] < earliest], earliest - pd.Timedelta(hours=1)
    )
    train_panel = build_with_cov(train_panel)
    cols = ["unique_id", "ds", "y", "available_mask"] + exog

    common = dict(
        h=HORIZON, input_size=INPUT_SIZE, futr_exog_list=exog,
        scaler_type="robust",
        loss=DistributionLoss(distribution="StudentT", level=[80, 90]),
        max_steps=MAX_STEPS, start_padding_enabled=True,
        batch_size=16, windows_batch_size=WINDOWS_BATCH,
        inference_windows_batch_size=WINDOWS_BATCH,
        enable_progress_bar=False,
    )
    models = [TFT(**common, hidden_size=HIDDEN, learning_rate=1e-3)]
    nf = NeuralForecast(models=models, freq="h")
    print(f"training TFT on {len(train_panel)} rows "
          f"({train_panel['available_mask'].mean():.2f} available)…", flush=True)
    nf.fit(df=train_panel[cols])

    def tcol(cs):
        for x in cs:
            if x == "TFT" or x == "TFT-median":
                return x
        for x in cs:
            if x.startswith("TFT") and "-lo-" not in x and "-hi-" not in x:
                return x
        return None

    # --- Rolling-origin predictions ---
    preds = []
    for base_local, base_utc in zip(bases_local, bases_utc):
        # Encoder context = panel UP TO base (the "current state" the nowcast sees).
        ctx = regularize_with_mask(
            raw[raw["ds"] < base_local], base_local - pd.Timedelta(hours=1)
        )
        ctx = build_with_cov(ctx)
        # Future frame: H hourly rows from base_local, covariates filled.
        fds = pd.date_range(base_local, periods=HORIZON, freq="h")
        fut = pd.concat(
            [pd.DataFrame({"unique_id": uid, "ds": fds})
             for uid in ctx["unique_id"].unique()],
            ignore_index=True,
        )
        fut = build_with_cov(fut)
        yh = nf.predict(df=ctx[cols], futr_df=fut[["unique_id", "ds"] + exog])
        yh = yh.reset_index() if yh.index.name else yh
        tc = tcol(list(yh.columns))
        yh = yh[["unique_id", "ds", tc]].rename(columns={tc: "tft_pred", "ds": "ds_local"})
        yh["base"] = base_local
        yh["lead_h"] = ((yh["ds_local"] - base_local) / pd.Timedelta(hours=1)).round().astype(int)
        preds.append(yh)
        print(f"  base {base_local}: {len(yh)} tft preds", flush=True)

    tft = pd.concat(preds, ignore_index=True)

    # --- Actuals + CatBoost, matched on (attraction, target hour, base) ---
    lo_utc = min(bases_utc)
    hi_utc = max(bases_utc) + pd.Timedelta(hours=HORIZON)
    actuals = fetch_actuals(tz, lo_utc, hi_utc)
    cat = fetch_catboost_hourly(tz, bases_utc, HORIZON)
    cat_base_local = {b_utc: b_loc for b_utc, b_loc in zip(bases_utc, bases_local)}
    if not cat.empty:
        cat["base"] = cat["base"].map(cat_base_local)

    m = tft.merge(actuals, on=["unique_id", "ds_local"], how="inner")
    if not cat.empty:
        m = m.merge(cat[["unique_id", "ds_local", "base", "cat_pred"]],
                    on=["unique_id", "ds_local", "base"], how="left")
    else:
        m["cat_pred"] = np.nan
    print(f"\nmatched rows={len(m)} (tft∩actual), with cat_pred={m['cat_pred'].notna().sum()}", flush=True)

    # --- Report: MAE/bias by lead bucket and busy segment ---
    def seg(df, label):
        n = len(df)
        if n == 0:
            return
        t_mae = (df["tft_pred"] - df["actual"]).abs().mean()
        t_bias = (df["tft_pred"] - df["actual"]).mean()
        c = df.dropna(subset=["cat_pred"])
        c_mae = (c["cat_pred"] - c["actual"]).abs().mean() if len(c) else float("nan")
        c_bias = (c["cat_pred"] - c["actual"]).mean() if len(c) else float("nan")
        print(f"  {label:16} n={n:5d}  TFT {t_mae:5.1f}/{t_bias:+5.1f}   "
              f"CatBoost {c_mae:5.1f}/{c_bias:+5.1f}  (cat n={len(c)})", flush=True)

    print("\n=== by lead bucket (MAE / bias, min) ===", flush=True)
    seg(m[m["lead_h"].between(1, 3)], "lead 1-3h")
    seg(m[m["lead_h"].between(4, 12)], "lead 4-12h")
    seg(m[m["lead_h"].between(13, 24)], "lead 13-24h")

    print("\n=== by busy segment (actual median, min) ===", flush=True)
    seg(m[m["actual"] < 30], "quiet <30")
    seg(m[m["actual"].between(30, 59)], "30-59")
    seg(m[m["actual"] >= 60], "busy >=60")

    print("\n=== ALL ===", flush=True)
    seg(m, "ALL")


if __name__ == "__main__":
    main()
