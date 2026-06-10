"""Horizon-extension backtest: TFT h=45 (env-overridable) on HEADLINERS.

Variant of backtest_headliners.py: BASE/END/H from env, output segmented by
LEAD bucket (1-15 / 16-30 / 31-45) x busy, TFT vs realised daily P90.
CatBoost forward records are joined where available (stale-lead caveat applies).

  docker exec <nf> sh -c 'BT_BASE=2026-04-26 BT_END=2026-06-10 BT_H=45 python3 /app/backtest_horizon.py'
"""

import os

import numpy as np
import pandas as pd
from sqlalchemy import text

import db
import forecast
from neuralforecast import NeuralForecast

BASE = os.getenv("BT_BASE", "2026-04-26")
END = os.getenv("BT_END", "2026-06-10")
H = int(os.getenv("BT_H", "45"))
forecast.settings.NF_HORIZON = H


def _q(sql, **params):
    with db._engine.connect() as c:
        return pd.read_sql(text(sql), c, params=params)


def mae(a, p):
    return float(np.abs(p - a).mean())


def bias(a, p):
    return float((p - a).mean())


def main():
    hl = _q('SELECT DISTINCT "attractionId"::text aid, "parkId"::text pid FROM headliner_attractions')
    hl_ids = hl["aid"].tolist()
    park_ids = hl["pid"].dropna().unique().tolist()
    print(f"BASE={BASE} END={END} H={H} headliners={len(hl_ids)} parks={len(park_ids)}", flush=True)

    panel = _q(
        """
        SELECT qd."attractionId"::text AS unique_id,
               date_trunc('day', qd.timestamp AT TIME ZONE p.timezone)::date AS ds,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") AS y
        FROM queue_data qd JOIN attractions a ON a.id=qd."attractionId" JOIN parks p ON p.id=a."parkId"
        WHERE qd."attractionId"::text = ANY(:ids)
          AND qd.timestamp >= NOW() - INTERVAL '730 days' AND qd.timestamp < :base
          AND qd.status='OPERATING' AND qd."queueType"='STANDBY' AND qd."waitTime">=5
        GROUP BY 1,2 HAVING COUNT(*) >= 3
        """,
        ids=hl_ids, base=BASE + " 00:00+00",
    )
    panel["ds"] = pd.to_datetime(panel["ds"])
    panel["y"] = panel["y"].astype(float)
    meta = db.fetch_attraction_meta(park_ids)
    countries = sorted({c for c in meta["country"].dropna().unique()})
    for inf in meta["influencing"].dropna():
        items = inf if isinstance(inf, list) else []
        countries += [d.get("countryCode") for d in items if d.get("countryCode")]
    hol = db.fetch_holidays(sorted(set(countries)))
    wx = db.fetch_weather(park_ids)
    panel = db.add_calendar_covariates(panel, meta, hol, wx)
    cols = ["unique_id", "ds", "y"] + db.FUTR_EXOG
    print(f"panel rows={len(panel)} series={panel['unique_id'].nunique()}", flush=True)

    static_df, stat_exog = forecast._build_static_df(meta, panel)

    act = _q(
        """
        SELECT qd."attractionId"::text aid,
               date_trunc('day', qd.timestamp AT TIME ZONE p.timezone)::date d,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") y
        FROM queue_data qd JOIN attractions a ON a.id=qd."attractionId" JOIN parks p ON p.id=a."parkId"
        WHERE qd."attractionId"::text = ANY(:ids) AND qd.timestamp >= :base AND qd.timestamp < :end
          AND qd.status='OPERATING' AND qd."queueType"='STANDBY' AND qd."waitTime">=5
        GROUP BY 1,2 HAVING COUNT(*) >= 3
        """,
        ids=hl_ids, base=BASE + " 00:00+00", end=END + " 00:00+00",
    )
    act["d"] = pd.to_datetime(act["d"]).dt.date

    from neuralforecast.losses.pytorch import DistributionLoss

    lossobj = DistributionLoss(distribution="StudentT", level=forecast.settings.levels)
    nf = NeuralForecast(models=forecast._build_models(1, 1, stat_exog, loss=lossobj), freq="D")
    nf.fit(df=panel[cols], static_df=static_df)
    try:
        futr = nf.make_future_dataframe(df=panel[cols])
    except TypeError:
        futr = nf.make_future_dataframe()
    futr = db.add_calendar_covariates(futr, meta, hol, wx)
    yh = nf.predict(df=panel[cols], static_df=static_df, futr_df=futr)
    yh = yh.reset_index() if yh.index.name else yh
    tcol = next((x for x in yh.columns if x in ("TFT", "TFT-median")), None) or next(
        x for x in yh.columns if x.startswith("TFT") and "-lo-" not in x and "-hi-" not in x
    )
    yh["aid"] = yh["unique_id"].astype(str)
    yh["d"] = pd.to_datetime(yh["ds"]).dt.date
    tft = yh[["aid", "d", tcol]].rename(columns={tcol: "tft"})
    m = act.merge(tft, on=["aid", "d"], how="inner")
    base_d = pd.to_datetime(BASE).date()
    m["lead"] = m["d"].map(lambda x: (x - base_d).days + 1)

    print(f"=== HORIZON BACKTEST h={H}: TFT vs actual daily P90 (headliners) ===", flush=True)
    print(f"matched={len(m)} attractions={m['aid'].nunique()}", flush=True)
    buckets = [("lead %02d-%02d" % (lo, min(lo + 14, H)),
                (m.lead >= lo) & (m.lead <= min(lo + 14, H)))
               for lo in range(1, H + 1, 15)]
    for blbl, bmk in buckets:
        for slbl, smk in [("ALL", m.y >= 0), ("busy>=40", m.y >= 40), ("busy>=70", m.y >= 70)]:
            sub = m[bmk & smk]
            if len(sub):
                print("  %-11s %-9s n=%-5d TFT MAE=%5.1f bias=%+5.1f"
                      % (blbl, slbl, len(sub), mae(sub.y, sub.tft), bias(sub.y, sub.tft)),
                      flush=True)


if __name__ == "__main__":
    main()
