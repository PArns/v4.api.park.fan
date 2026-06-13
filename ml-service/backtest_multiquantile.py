"""Offline backtest: MultiQuantile vs the current single-Quantile(0.8) champion.

Goal (per-purpose serving hypothesis): does a MultiQuantile model give us
- a MEDIAN (q0.5) that is at least as good as q0.8 on quiet slots (honest display), AND
- a HIGH quantile (q0.95) that materially drops busy-tail MAE vs q0.8?

Trains both models on an identical chronological split (hold-out = last N days,
never seen in training), predicts on the hold-out, and reports MAE/bias segmented
by quiet(<30) / mid(30-60) / busy(>=60) — the only metric that matters here.

OFFLINE ONLY. Does not touch the production model, config, or DB writes.

  docker exec <ml> python3 /app/backtest_multiquantile.py [HOLDOUT_DAYS]
"""

from __future__ import annotations

import sys
from datetime import timedelta

import numpy as np
import pandas as pd

from config import get_settings
from db import fetch_training_data
from features import engineer_features, get_feature_columns
from model import WaitTimeModel
from data_validation import validate_training_data
from train import remove_anomalies

settings = get_settings()

HOLDOUT_DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 21
VAL_DAYS = 10  # tail of the training window used for early stopping


def _seg(y: np.ndarray, p: np.ndarray, mask: np.ndarray) -> str:
    if mask.sum() == 0:
        return "n=0"
    a, q = y[mask], p[mask]
    mae = float(np.abs(q - a).mean())
    bias = float((q - a).mean())
    return f"n={int(mask.sum()):<6d} MAE={mae:6.2f}  bias={bias:+6.2f}"


def main() -> None:
    print(f"=== MultiQuantile backtest (hold-out = last {HOLDOUT_DAYS}d) ===", flush=True)

    # 1) Load + engineer features over the available window (mirror train_model).
    from db import get_db
    from sqlalchemy import text

    with get_db() as db:
        row = db.execute(
            text(
                "SELECT MIN(timestamp) lo, MAX(timestamp) hi FROM queue_data "
                "WHERE \"queueType\"='STANDBY' AND status='OPERATING' AND \"waitTime\" IS NOT NULL"
            )
        ).fetchone()
    data_start = max(row.lo, row.hi - timedelta(days=settings.TRAIN_LOOKBACK_YEARS * 365))
    end_date = row.hi + timedelta(days=1)
    print(f"data: {data_start:%Y-%m-%d} → {row.hi:%Y-%m-%d}", flush=True)

    df = fetch_training_data(data_start, end_date)
    df, _ = validate_training_data(df)
    df = remove_anomalies(df)
    df = engineer_features(df, data_start, end_date)
    df = df.dropna(subset=["waitTime"]).reset_index(drop=True)
    print(f"rows after features: {len(df):,}", flush=True)

    # 2) Chronological split: train < cutoff_holdout, hold-out = last HOLDOUT_DAYS.
    hi = df["timestamp"].max()
    holdout_cut = hi - pd.Timedelta(days=HOLDOUT_DAYS)
    val_cut = holdout_cut - pd.Timedelta(days=VAL_DAYS)
    df_holdout = df[df["timestamp"] >= holdout_cut]
    df_val = df[(df["timestamp"] >= val_cut) & (df["timestamp"] < holdout_cut)]
    df_train = df[df["timestamp"] < val_cut]
    print(
        f"train={len(df_train):,}  val={len(df_val):,}  holdout={len(df_holdout):,}",
        flush=True,
    )

    feats = get_feature_columns()
    Xtr, ytr = df_train[feats], df_train["waitTime"]
    Xv, yv = df_val[feats], df_val["waitTime"]
    Xho, yho = df_holdout[feats], df_holdout["waitTime"].to_numpy()

    quiet = yho < 30
    mid = (yho >= 30) & (yho < 60)
    busy = yho >= 60

    def report(label: str, preds: np.ndarray) -> None:
        print(f"\n--- {label} ---", flush=True)
        print(f"  ALL    {_seg(yho, preds, np.ones_like(yho, bool))}", flush=True)
        print(f"  quiet  {_seg(yho, preds, quiet)}", flush=True)
        print(f"  mid    {_seg(yho, preds, mid)}", flush=True)
        print(f"  busy   {_seg(yho, preds, busy)}", flush=True)

    # 3) Champion: single Quantile(0.8).
    settings.CATBOOST_LOSS_FUNCTION = "Quantile:alpha=0.8"
    champ = WaitTimeModel()
    champ.train(Xtr, ytr, Xv, yv)
    report("CHAMPION Quantile:0.8", np.maximum(champ.predict(Xho), 0))

    # 4) Challenger: MultiQuantile(0.5, 0.8, 0.95).
    settings.CATBOOST_LOSS_FUNCTION = "MultiQuantile:alpha=0.5,0.8,0.95"
    chal = WaitTimeModel()
    chal.train(Xtr, ytr, Xv, yv)
    q = chal.predict_quantiles(Xho)
    for a in sorted(q):
        report(f"MultiQuantile q{a}", q[a])

    print(
        "\n=== Decision rule: a high quantile (q0.95) should drop busy MAE materially "
        "vs CHAMPION, while q0.5 keeps quiet MAE ≤ champion. ===",
        flush=True,
    )


if __name__ == "__main__":
    main()
