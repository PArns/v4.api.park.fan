"""One-shot PoC: fit TFT+NHITS in-process (no save → avoids the DistributionLoss
deepcopy save bug) and print the marquee attraction's daily forecast with
holiday/weekend flags — the direct test of whether TFT lifts holidays.

Run: docker run --rm --env-file nf.env parkfan-nf:poc python3 poc_eval.py
"""

import pandas as pd

import forecast
import db
from config import get_settings
from neuralforecast import NeuralForecast


def main():
    s = get_settings()
    panel, meta, hol = forecast.build_panel()
    print(
        f"PANEL rows={len(panel)} series={panel['unique_id'].nunique()} "
        f"span={panel['ds'].min().date()}..{panel['ds'].max().date()}",
        flush=True,
    )
    top = panel.groupby("unique_id")["y"].median().sort_values().index[-1]
    cols0 = ["unique_id", "ds", "y"] + db.FUTR_EXOG
    nf = NeuralForecast(models=forecast._build_models(), freq="D")
    nf.fit(df=panel[cols0])
    fut = db.build_future_frame(panel, meta, hol, s.NF_HORIZON)
    yhat = nf.predict(df=panel[cols0], futr_df=fut)
    cols = list(yhat.columns)

    def pick(mdl):
        c = [x for x in cols if x == mdl or x == mdl + "-median"]
        if c:
            return c[0]
        c = [x for x in cols if x.startswith(mdl) and "-lo-" not in x and "-hi-" not in x]
        return c[0] if c else None

    tcol, ncol = pick("TFT"), pick("NHITS")
    m = yhat[yhat["unique_id"] == top].merge(
        fut[["unique_id", "ds", "is_holiday_primary", "is_holiday_neighbor", "is_weekend"]],
        on=["unique_id", "ds"], how="left",
    )
    print("RESULT_COLS:", cols, flush=True)
    print("TOP:", top, "tcol:", tcol, "ncol:", ncol, flush=True)
    for _, r in m.iterrows():
        d = pd.to_datetime(r["ds"])
        fl = (
            "HOL" if r.get("is_holiday_primary") == 1
            else ("nb" if r.get("is_holiday_neighbor") == 1
                  else ("we" if r.get("is_weekend") == 1 else "  "))
        )
        print("ROW %s %s %s TFT=%6.1f NHITS=%6.1f" % (
            d.date(), d.strftime("%a"), fl,
            (r[tcol] if tcol else -1), (r[ncol] if ncol else -1)), flush=True)


if __name__ == "__main__":
    main()
