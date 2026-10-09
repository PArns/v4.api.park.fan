"""NeuralForecast candidates (PAR-829) as ml-bench plug-ins: TiDE, TSMixerx, NHITS.

The forecasts are made beforehand by ``nf_precompute`` (GPU, walk-forward month
blocks, see its docstring for the information cut) and read here from the cache;
this class only turns the HOURLY forecast into the 15-minute slots the harness
asks for and hands them in like any other model:

    slot = model_hour(ride, day, hour) × H5(slot) / mean(H5 over the hour's slots)

i.e. the model sets the level of every hour, H5 (built here from the history the
plug-in is given, the same recipe as baseline 4) distributes it inside the hour —
the rope-drop ramp in the opening hour, the close-aligned tail in the last one,
the linear interpolation in between. Where H5 has no value the hour is flat.

The daily level for UC4 / D6 / D7 (``predict_daily``) is the P90 of the model's own
15-minute q50 curve over the published window, for d1–d7; the runner then also
scores ``<name>_x_h5`` (that level on the H5 profile), which isolates the model's
level from its hourly shape.

Run (after the precompute; the cache directory comes from ``MLBENCH_NF_CACHE``):

    python -m mlbench run --model mlbench.models.neuralforecast_models:NFTiDE ...
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

from .base import Model, Origin
from .nf_panel import HOUR0, K

OUT_COLS = ["attraction_id", "slot_start_utc", "q50", "q80", "q95"]


def _pg_dow(dates: pd.Series) -> pd.Series:
    return (pd.to_datetime(dates).dt.dayofweek + 1) % 7


def h5_curve(h: pd.DataFrame, g: pd.DataFrame, min_days_daytype: int = 3, min_days_all: int = 4) -> pd.Series:
    """Baseline 4 (H5 hybrid profile) for the grid ``g`` from the history ``h`` —
    the same steps as ``example_level_h5`` (asserted equal to the built-in SQL)."""
    h = h.copy()
    h["we"] = _pg_dow(h["date"]).isin([0, 6]).astype(int)
    h["hr"] = h["ws"] // 4

    def profile(key: str) -> pd.DataFrame:
        typed = h.groupby(["attraction_id", "we", key])["y"].agg(["median", "count"]).reset_index()
        typed = typed[typed["count"] >= min_days_daytype].rename(columns={"we": "wk", key: "idx"})
        alld = h.groupby(["attraction_id", key])["y"].agg(["median", "count"]).reset_index()
        alld = alld[alld["count"] >= min_days_all].rename(columns={key: "idx"})
        alld["wk"] = 2
        return pd.concat([typed, alld])[["attraction_id", "wk", "idx", "median"]]

    hh = h.groupby(["attraction_id", "we", "hr"]).agg(m=("y", "median"), n=("date", "nunique")).reset_index()
    hh = hh[hh["n"] >= min_days_daytype].rename(columns={"we": "wk", "hr": "idx", "m": "median"})
    ha = h.groupby(["attraction_id", "hr"]).agg(m=("y", "median"), n=("date", "nunique")).reset_index()
    ha = ha[ha["n"] >= min_days_all].rename(columns={"hr": "idx", "m": "median"})
    ha["wk"] = 2
    p_hr = pd.concat([hh, ha])[["attraction_id", "wk", "idx", "median"]]
    we = _pg_dow(g["date"]).isin([0, 6]).astype(int).to_numpy()

    def lookup(p: pd.DataFrame, idx: pd.Series) -> pd.Series:
        t = p.set_index(["attraction_id", "wk", "idx"])["median"]
        a = t.reindex(pd.MultiIndex.from_arrays([g["attraction_id"], we, idx])).to_numpy()
        b = t.reindex(pd.MultiIndex.from_arrays([g["attraction_id"], np.full(len(g), 2), idx])).to_numpy()
        return pd.Series(np.where(np.isnan(a), b, a), index=g.index)

    p_open, p_close = lookup(profile("ko"), g["ko"]), lookup(profile("kc"), g["kc"])
    h0 = lookup(p_hr, g["ws"] // 4)
    hm = lookup(p_hr, g["ws"] // 4 - 1).fillna(h0)
    hp = lookup(p_hr, g["ws"] // 4 + 1).fillna(h0)
    mid = (g["ws"] % 4) * 15 + 7.5
    h_lin = pd.Series(np.where(mid < 30, h0 + ((30 - mid) / 60.0) * (hm - h0),
                               h0 + ((mid - 30) / 60.0) * (hp - h0)), index=g.index)
    edge = pd.Series(np.where(g["ko"] < 4, p_open, np.where(g["kc"] < 4, p_close, np.nan)), index=g.index)
    return edge.fillna(h_lin).fillna(lookup(profile("ws"), g["ws"]))


def hourly_to_slots(g: pd.DataFrame, hourly: pd.DataFrame, h5: pd.Series) -> pd.DataFrame:
    """Spread hourly quantiles over the grid's 15-min slots with H5's within-hour shape."""
    g = g[["attraction_id", "date", "slot_start_utc", "ws"]].copy()
    g["date"] = pd.to_datetime(g["date"]).dt.date
    g["hr"] = np.clip(g["ws"].to_numpy() // 4, HOUR0, HOUR0 + K - 1)
    g["h5"] = h5.to_numpy()
    hm = g.groupby(["attraction_id", "date", "hr"])["h5"].transform("mean")
    f = (g["h5"] / hm).where((hm > 0) & g["h5"].notna(), 1.0).clip(0.25, 4.0)
    hourly = hourly.rename(columns={"aid": "attraction_id"})
    hourly["date"] = pd.to_datetime(hourly["date"]).dt.date
    m = g.assign(f=f.to_numpy()).merge(hourly[["attraction_id", "date", "hr", "q50", "q80", "q95"]],
                                       on=["attraction_id", "date", "hr"], how="inner")
    for q in ("q50", "q80", "q95"):
        m[q] = m[q] * m["f"]
    return m[OUT_COLS + ["date"]]


class NFCached(Model):
    """Base: reads ``$MLBENCH_NF_CACHE/<cache_name>/*.parquet``."""

    name = "nf_cached"
    cache_name = ""
    provides_quantiles = True
    intraday = True
    max_lead_days = 7
    refit_every_days = None
    train_days = 1          # fit() is a no-op (trained in nf_precompute): keep the runner's panel tiny

    def __init__(self) -> None:
        root = Path(os.environ.get("MLBENCH_NF_CACHE", "/data/par-829/cache/current"))
        self.dir = root / (self.cache_name or self.name)
        if not self.dir.exists():
            raise FileNotFoundError(f"{self.dir}: run nf_precompute first (MLBENCH_NF_CACHE)")
        self._last: pd.DataFrame | None = None
        self._files = sorted(self.dir.glob("b*_h*.parquet"))

    def _hourly(self, date, hour: int) -> pd.DataFrame:
        d = pd.Timestamp(date)
        files = [f for f in self._files if int(f.stem.split("_h")[1]) == hour
                 and pd.Timestamp(f.stem[1:9]) <= d]
        if not files:
            return pd.DataFrame(columns=["aid", "date", "hr", "q50", "q80", "q95"])
        f = max(files)       # the block that contains the origin: the latest block start <= origin
        return pd.read_parquet(f, filters=[("origin_date", "==", d)],
                               columns=["aid", "date", "hr", "q50", "q80", "q95"])

    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        if horizon_slots.empty:
            return pd.DataFrame(columns=OUT_COLS)
        hour = 6 if origin.kind == "daily" else int(origin.hour_local)
        hourly = self._hourly(origin.date, hour)
        if hourly.empty:
            return pd.DataFrame(columns=OUT_COLS)
        g = horizon_slots.reset_index(drop=True)
        h5 = h5_curve(origin.history.df(56), g)
        out = hourly_to_slots(g, hourly, h5)
        if origin.kind == "daily":
            self._last = out
        return out[OUT_COLS]

    def predict_daily(self, origin: Origin, horizon_days: pd.DataFrame,
                      known_future_covariates: pd.DataFrame) -> pd.DataFrame | None:
        if self._last is None or self._last.empty:
            return None
        s = self._last
        lv = (s.groupby(["attraction_id", "date"])["q50"]
              .agg(level=lambda v: v.quantile(0.9), n="size").reset_index())
        lv = lv[lv["n"] >= 8]
        days = horizon_days.assign(date=pd.to_datetime(horizon_days["date"]).dt.date)
        out = days.merge(lv, on=["attraction_id", "date"], how="inner")
        out = out[(out["lead_days"] >= 1) & (out["lead_days"] <= self.max_lead_days)]
        return out[["attraction_id", "date", "level"]]


class NFTiDE(NFCached):
    name = "nf_tide"


class NFTiDEWeather(NFCached):
    """TiDE with daily weather ACTUALS as known-future covariates (ORACLE — an upper bound)."""
    name = "nf_tide_wx"
    uses_oracle_weather = True


class NFNHITS(NFCached):
    name = "nf_nhits"


class NFTSMixerx(NFCached):
    name = "nf_tsmixerx"
