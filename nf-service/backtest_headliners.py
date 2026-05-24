"""OOS backtest: TFT vs CatBoost on HEADLINER attractions.

Train TFT on headliner daily-P90 history < BASE, forecast the holdout [BASE, today),
then compare against CatBoost's genuine forward daily predictions and the realised
actual daily P90 — headliners only (the rides that drive crowd levels).

Run inside nf-service: docker exec <nf> python3 /app/backtest_headliners.py
"""

import numpy as np
import pandas as pd
from sqlalchemy import text

import db
import forecast
from neuralforecast import NeuralForecast

BASE = "2026-05-10"          # train < BASE
END = "2026-05-24"           # holdout = [BASE, END)
H = 14


def _q(sql, **params):
    with db._engine.connect() as c:
        return pd.read_sql(text(sql), c, params=params)


def main():
    hl = _q('SELECT DISTINCT "attractionId"::text aid, "parkId"::text pid FROM headliner_attractions')
    hl_ids = hl["aid"].tolist()
    park_ids = hl["pid"].dropna().unique().tolist()
    print(f"headliners={len(hl_ids)} parks={len(park_ids)}", flush=True)

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
    panel = db.add_calendar_covariates(panel, meta, hol)
    cols = ["unique_id", "ds", "y"] + db.FUTR_EXOG
    print(f"panel rows={len(panel)} series={panel['unique_id'].nunique()}", flush=True)

    # Static covariates (country/region) — measure the lift vs the no-static baseline.
    static_df, stat_exog = forecast._build_static_df(meta, panel)
    print(f"static_exog={stat_exog} static_rows={0 if static_df is None else len(static_df)}", flush=True)
    nf = NeuralForecast(models=forecast._build_models(1, 1, stat_exog), freq="D")
    nf.fit(df=panel[cols], static_df=static_df)
    # Let NeuralForecast define the exact (unique_id, ds) future combos it expects
    # (build_future_frame's per-series-last construction mismatched gappy series),
    # then attach the futr_exog covariates.
    try:
        futr = nf.make_future_dataframe(df=panel[cols])
    except TypeError:
        futr = nf.make_future_dataframe()
    futr = db.add_calendar_covariates(futr, meta, hol)
    yh = nf.predict(df=panel[cols], futr_df=futr)
    yh = yh.reset_index() if yh.index.name else yh
    tcol = next((x for x in yh.columns if x in ("TFT", "TFT-median")), None) or next(
        x for x in yh.columns if x.startswith("TFT") and "-lo-" not in x and "-hi-" not in x
    )
    yh["aid"] = yh["unique_id"].astype(str)
    yh["d"] = pd.to_datetime(yh["ds"]).dt.date
    tft = yh[["aid", "d", tcol]].rename(columns={tcol: "tft"})

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
    cat = _q(
        """
        SELECT DISTINCT ON (wp."attractionId", DATE(wp."predictedTime" AT TIME ZONE p.timezone))
          wp."attractionId"::text aid, DATE(wp."predictedTime" AT TIME ZONE p.timezone) d,
          wp."predictedWaitTime"::float cb
        FROM wait_time_predictions wp JOIN attractions a ON a.id=wp."attractionId" JOIN parks p ON p.id=a."parkId"
        WHERE wp."attractionId"::text = ANY(:ids) AND wp."predictionType"='daily'
          AND DATE(wp."predictedTime" AT TIME ZONE p.timezone) >= :bd
          AND DATE(wp."predictedTime" AT TIME ZONE p.timezone) < :ed
          AND DATE(wp."createdAt" AT TIME ZONE p.timezone) < DATE(wp."predictedTime" AT TIME ZONE p.timezone)
        ORDER BY wp."attractionId", DATE(wp."predictedTime" AT TIME ZONE p.timezone), wp."createdAt" DESC
        """,
        ids=hl_ids, bd=BASE, ed=END,
    )
    act["d"] = pd.to_datetime(act["d"]).dt.date
    cat["d"] = pd.to_datetime(cat["d"]).dt.date

    m = act.merge(tft, on=["aid", "d"], how="inner").merge(cat, on=["aid", "d"], how="inner")
    print(f"matched headliner-days={len(m)}  attractions={m['aid'].nunique()}", flush=True)

    def mae(a, p):
        return float(np.abs(p - a).mean())

    def bias(a, p):
        return float((p - a).mean())

    print("=== HEADLINERS: TFT vs CatBoost vs actual daily P90 ===", flush=True)
    print("ALL       n=%-5d TFT MAE=%5.1f bias=%+5.1f | CatBoost MAE=%5.1f bias=%+5.1f"
          % (len(m), mae(m.y, m.tft), bias(m.y, m.tft), mae(m.y, m.cb), bias(m.y, m.cb)), flush=True)
    for lbl, mk in [("quiet<40", m.y < 40), ("busy>=40", m.y >= 40), ("busy>=70", m.y >= 70)]:
        sub = m[mk]
        if len(sub):
            print("%-9s n=%-5d TFT MAE=%5.1f bias=%+5.1f | CatBoost MAE=%5.1f bias=%+5.1f"
                  % (lbl, len(sub), mae(sub.y, sub.tft), bias(sub.y, sub.tft),
                     mae(sub.y, sub.cb), bias(sub.y, sub.cb)), flush=True)


if __name__ == "__main__":
    main()
