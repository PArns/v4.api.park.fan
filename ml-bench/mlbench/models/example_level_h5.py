"""Example plug-in: baseline 5a (level × H5) written against the model interface.

It reproduces the built-in SQL column ``lvlh5_naive`` from nothing but the
inputs a plug-in gets (``origin.history`` and ``horizon_slots``), which makes it
both a template for real models and a check that the plug-in path scores the
same way as the built-in path (tests/test_example_model.py asserts equality).

Copy this file, rename the class and ``name``, replace ``predict`` with your
model. Heavy models: do the expensive work in ``fit`` (called every
``refit_every_days``) and keep ``predict`` to inference.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .base import Model, Origin


def _pg_dow(dates: pd.Series) -> pd.Series:
    """Postgres/DuckDB DOW (0 = Sunday) from dates."""
    return (pd.to_datetime(dates).dt.dayofweek + 1) % 7


class LevelH5Example(Model):
    name = "example_level_h5"
    provides_quantiles = False
    intraday = False
    max_lead_days = 90
    refit_every_days = None

    window_days = 56
    min_days_daytype = 3
    min_days_all = 4
    min_ride_day_slots = 8

    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        h = origin.history.df(self.window_days)
        if h.empty or horizon_slots.empty:
            return pd.DataFrame(columns=["attraction_id", "slot_start_utc", "q50", "q80", "q95"])
        h["dow"] = _pg_dow(h["date"])
        h["we"] = h["dow"].isin([0, 6]).astype(int)

        def profile(key: str) -> pd.DataFrame:
            """median per (ride, wk, idx); wk 0/1 = day type (>= 3 days), 2 = all days (>= 4)."""
            typed = h.groupby(["attraction_id", "we", key])["y"].agg(["median", "count"]).reset_index()
            typed = typed[typed["count"] >= self.min_days_daytype].rename(columns={"we": "wk", key: "idx"})
            alld = h.groupby(["attraction_id", key])["y"].agg(["median", "count"]).reset_index()
            alld = alld[alld["count"] >= self.min_days_all].rename(columns={key: "idx"})
            alld["wk"] = 2
            return pd.concat([typed, alld])[["attraction_id", "wk", "idx", "median"]]

        h["hr"] = h["ws"] // 4
        prof = {k: profile(k) for k in ("ko", "kc", "hr")}
        # hourly profile counts DAYS, not slots
        hh = h.groupby(["attraction_id", "we", "hr"]).agg(m=("y", "median"), n=("date", "nunique")).reset_index()
        hh = hh[hh["n"] >= self.min_days_daytype].rename(columns={"we": "wk", "hr": "idx", "m": "median"})
        ha = h.groupby(["attraction_id", "hr"]).agg(m=("y", "median"), n=("date", "nunique")).reset_index()
        ha = ha[ha["n"] >= self.min_days_all].rename(columns={"hr": "idx", "m": "median"})
        ha["wk"] = 2
        prof["hr"] = pd.concat([hh, ha])[["attraction_id", "wk", "idx", "median"]]
        wsp = profile("ws")

        g = horizon_slots.copy()
        g["dow"] = _pg_dow(g["date"])
        g["we"] = g["dow"].isin([0, 6]).astype(int)

        def lookup(p: pd.DataFrame, idx: pd.Series) -> pd.Series:
            t = p.set_index(["attraction_id", "wk", "idx"])["median"]
            k_typed = pd.MultiIndex.from_arrays([g["attraction_id"], g["we"], idx])
            k_all = pd.MultiIndex.from_arrays([g["attraction_id"], np.full(len(g), 2), idx])
            a = t.reindex(k_typed).to_numpy()
            b = t.reindex(k_all).to_numpy()
            return pd.Series(np.where(np.isnan(a), b, a), index=g.index)

        p_open = lookup(prof["ko"], g["ko"])
        p_close = lookup(prof["kc"], g["kc"])
        h0 = lookup(prof["hr"], g["ws"] // 4)
        hm = lookup(prof["hr"], g["ws"] // 4 - 1).fillna(h0)
        hp = lookup(prof["hr"], g["ws"] // 4 + 1).fillna(h0)
        mid = (g["ws"] % 4) * 15 + 7.5
        h_lin = np.where(mid < 30, h0 + ((30 - mid) / 60.0) * (hm - h0), h0 + ((mid - 30) / 60.0) * (hp - h0))
        h_lin = pd.Series(h_lin, index=g.index)
        wt = lookup(wsp, g["ws"])
        first = g["ko"].between(0, 3)
        last = g["kc"].between(0, 3)
        edge = pd.Series(np.where(first, p_open, np.where(last, p_close, np.nan)), index=g.index)
        h5 = edge.fillna(h_lin).fillna(wt)

        # levels: window ride-day P90s
        rd = (h.groupby(["attraction_id", "date"])
              .agg(p90=("y", lambda s: s.quantile(0.9)), n=("y", "size"), dow=("dow", "first"),
                   we=("we", "first")).reset_index())
        rd = rd[rd["n"] >= self.min_ride_day_slots]
        rd = rd.sort_values("date", ascending=False)
        rd["rn"] = rd.groupby(["attraction_id", "dow"]).cumcount() + 1
        n4 = rd[rd["rn"] <= 4].groupby(["attraction_id", "dow"])["p90"].agg(["median", "count"])
        n4 = n4[n4["count"] >= 2]["median"]
        dlt = rd.groupby(["attraction_id", "we"])["p90"].agg(["median", "count"])
        dlt = dlt[dlt["count"] >= self.min_days_daytype]["median"]
        dla = rd.groupby("attraction_id")["p90"].agg(["median", "count"])
        dla = dla[dla["count"] >= self.min_days_all]["median"]
        lvl = n4.reindex(pd.MultiIndex.from_arrays([g["attraction_id"], g["dow"]])).to_numpy()
        ref_t = dlt.reindex(pd.MultiIndex.from_arrays([g["attraction_id"], g["we"]])).to_numpy()
        ref_a = dla.reindex(g["attraction_id"]).to_numpy()
        ref = np.where(np.isnan(ref_t), ref_a, ref_t)
        with np.errstate(invalid="ignore", divide="ignore"):
            scaled = np.where(ref > 0, h5.to_numpy() * lvl / ref, np.nan)
        q50 = np.where(first, h5.to_numpy(), scaled)
        out = pd.DataFrame({"attraction_id": g["attraction_id"], "slot_start_utc": g["slot_start_utc"],
                            "q50": q50})
        return out[np.isfinite(out["q50"])]
