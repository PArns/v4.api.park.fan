"""Hourly TFT (+NHITS) out-of-sample forecast for Disney's Animal Kingdom.

Target = hourly MEDIAN wait per attraction (matches the CatBoost training target
PERCENTILE_CONT(0.5)). Trains strictly on data < BASE_TIME, forecasts the next 72h
with calendar + hour-of-day as known-future covariates. fit+predict in-process
(avoids the DistributionLoss save-deepcopy bug). Closed hours are 0-filled to keep
the hourly grid regular; the eval join later keeps only real operating hours.

Run: docker run --rm --env-file nf.env parkfan-nf:poc python3 poc_eval_hourly.py
Writes /tmp/ak_tft_eval.csv : unique_id,dsUTC,TFT,NHITS
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text

import db

AK = "8c91d61b-811a-457f-803d-a02700b09a1b"
BASE_UTC = pd.Timestamp("2026-05-21T04:00:00Z")  # 2026-05-21 00:00 America/New_York
TZ = "America/New_York"
HORIZON = 72
INPUT_SIZE = 120          # 5 days context (was 168; trimmed for the host's ~11 GiB headroom)
MAX_STEPS = 300
WINDOW_DAYS = 150
WINDOWS_BATCH = 128       # sampled windows per step — the dominant TFT memory lever
OUT = "/tmp/ak_tft_eval.csv"


def _add_hour(df: pd.DataFrame) -> pd.DataFrame:
    h = df["ds"].dt.hour
    df["hour_sin"] = np.sin(2 * np.pi * h / 24)
    df["hour_cos"] = np.cos(2 * np.pi * h / 24)
    return df


def fetch_hourly_median_panel() -> pd.DataFrame:
    sql = text(
        """
        SELECT qd."attractionId" AS unique_id,
               date_trunc('hour', qd.timestamp AT TIME ZONE p.timezone) AS ds,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") AS y
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE a."parkId" = :ak
          AND qd.timestamp >= NOW() - (:w || ' days')::interval
          AND qd.timestamp < :base
          AND qd.status = 'OPERATING'
          AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" >= 5
        GROUP BY 1, 2
        HAVING COUNT(*) >= 2
        """
    )
    with db._engine.connect() as c:
        df = pd.read_sql(
            sql, c, params={"ak": AK, "w": str(WINDOW_DAYS), "base": BASE_UTC.to_pydatetime()}
        )
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = df["y"].astype(float)
    return df


def regularize(panel: pd.DataFrame, end_local: pd.Timestamp) -> pd.DataFrame:
    """Reindex each series to a continuous hourly grid [min .. end_local], 0-fill
    closed hours so NeuralForecast sees a regular freq='h' grid."""
    out = []
    for uid, g in panel.groupby("unique_id"):
        g = g.set_index("ds").sort_index()
        idx = pd.date_range(g.index.min(), end_local, freq="h")
        g = g.reindex(idx)
        g["y"] = g["y"].fillna(0.0)
        g["unique_id"] = uid
        g = g.rename_axis("ds").reset_index()
        out.append(g[["unique_id", "ds", "y"]])
    return pd.concat(out, ignore_index=True)


def main():
    from neuralforecast import NeuralForecast
    from neuralforecast.models import TFT, NHITS
    from neuralforecast.losses.pytorch import DistributionLoss

    base_local = BASE_UTC.tz_convert(TZ).tz_localize(None)  # 2026-05-21 00:00 naive
    end_local = base_local - pd.Timedelta(hours=1)

    raw = fetch_hourly_median_panel()
    print(f"raw rows={len(raw)} series={raw['unique_id'].nunique()} "
          f"span={raw['ds'].min()}..{raw['ds'].max()}", flush=True)
    panel = regularize(raw, end_local)

    meta = db.fetch_attraction_meta([AK])
    holidays = db.fetch_holidays(["US"])
    panel = db.add_calendar_covariates(panel, meta, holidays)
    panel = _add_hour(panel)

    exog = list(db.FUTR_EXOG) + ["hour_sin", "hour_cos"]
    cols = ["unique_id", "ds", "y"] + exog

    common = dict(
        h=HORIZON, input_size=INPUT_SIZE, futr_exog_list=exog,
        scaler_type="robust",
        loss=DistributionLoss(distribution="StudentT", level=[80, 90]),
        max_steps=MAX_STEPS, start_padding_enabled=True,
        batch_size=16, windows_batch_size=WINDOWS_BATCH,
        inference_windows_batch_size=WINDOWS_BATCH,
        enable_progress_bar=False,
    )
    # TFT only (the model under evaluation) — keeps peak memory under the host's
    # ~11 GiB headroom while ml-service holds ~8.5 GiB.
    models = [TFT(**common, hidden_size=32, learning_rate=1e-3)]
    nf = NeuralForecast(models=models, freq="h")
    nf.fit(df=panel[cols])

    # Future frame: 72 hourly rows per series starting at base_local.
    rows = []
    for uid in panel["unique_id"].unique():
        fds = pd.date_range(base_local, periods=HORIZON, freq="h")
        rows.append(pd.DataFrame({"unique_id": uid, "ds": fds}))
    fut = pd.concat(rows, ignore_index=True)
    fut = db.add_calendar_covariates(fut, meta, holidays)
    fut = _add_hour(fut)

    yhat = nf.predict(df=panel[cols], futr_df=fut[["unique_id", "ds"] + exog])
    yhat = yhat.reset_index() if yhat.index.name else yhat
    c = list(yhat.columns)

    def pick(m):
        for x in c:
            if x == m or x == m + "-median":
                return x
        for x in c:
            if x.startswith(m) and "-lo-" not in x and "-hi-" not in x:
                return x
        return None

    tcol = pick("TFT")
    print("RESULT_COLS:", c, "TFT=", tcol, flush=True)

    out = yhat[["unique_id", "ds", tcol]].copy()
    out["dsUTC"] = (
        out["ds"].dt.tz_localize(TZ, nonexistent="shift_forward", ambiguous=True)
        .dt.tz_convert("UTC")
    )
    out[["unique_id", "dsUTC", tcol]].rename(columns={tcol: "TFT"}).to_csv(
        OUT, index=False
    )
    print(f"wrote {len(out)} rows -> {OUT}", flush=True)


if __name__ == "__main__":
    main()
