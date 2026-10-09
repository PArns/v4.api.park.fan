"""Plug-in: the daily level from known-in-advance drivers (PAR-830).

``predict_daily`` = naive level (baseline 5a: median of the last four same-weekday
daily P90s in the 56 days before the origin) × exp(ŷ), where ŷ is a gradient-
boosted model of ``log(true P90 / naive)`` on the drivers of
``mlbench/drivers.py`` that are known at the origin — weekday, season, lead,
public and school holidays incl. neighbour regions (ml-service OR semantics),
bridge days, published opening hours vs the reference days, ticketed events /
extra hours, season start/end — schedule drivers only where the schedule row was
written before the origin (``drivers.add_asof``). Weather is NOT used: the export only has actuals
(ORACLE), and an oracle input must not reach the hand-over table.

The runner composes the level with H5 into ``driver_level_x_h5`` at every slot
lead, so the driver model is scored on UC2/UC3 slots, UC4 ranking and D6/D7
exactly like ``lvlh5_naive`` / ``lvlh5_tft``. ``predict`` itself returns no
slots — the level is the whole model.

Training happens inside ``predict_daily`` every ``refit_every_days``: ride-day
P90s from the truth strictly before the origin (``origin.history``), one row per
ride-day × training lead with the reference cut at ``date − lead``, so the
training rows obey the same information cut as the test rows.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

from .. import drivers as D
from .base import Model, Origin

TRAIN_LEADS = [1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90]
GROUPS = ["cal", "hol", "sched"]


class DriverLevel(Model):
    name = "driver_level"
    provides_quantiles = False
    intraday = False
    max_lead_days = 90
    refit_every_days = 28
    train_days = 1                  # fit() is unused; training reads the history view in DuckDB

    window_days = 56
    min_ride_day_slots = 8
    min_train_rows = 20000
    max_train_rows = 300_000
    seed = 830

    def __init__(self) -> None:
        self._model = None
        self._fit_date = None
        self._static_con = None

    # ------------------------------------------------------------------ helpers
    def _export_dir(self, con) -> Path | None:
        env = os.environ.get("MLBENCH_EXPORT")
        if env:
            return Path(env)
        try:
            rows = con.execute("SELECT path FROM duckdb_databases() WHERE database_name = 'b'").fetchall()
        except Exception:  # noqa: BLE001
            rows = []
        return Path(rows[0][0]).parent if rows and rows[0][0] else None

    def _static(self, con) -> None:
        """Park-day drivers (``pdf``) once per connection."""
        if self._static_con is con:
            return
        exp = self._export_dir(con)
        sched = exp / "raw" / "schedule.csv.gz" if exp else None
        con.execute(f"CREATE OR REPLACE TEMP TABLE ev AS "
                    f"{D.event_sql(str(sched) if sched and sched.exists() else None)}")
        D.park_day_features(con)
        self._static_con = con

    def _rdh(self, origin: Origin, days: int) -> None:
        con = origin.history.con
        con.execute(f"""CREATE OR REPLACE TEMP TABLE rdh AS
            SELECT attraction_id AS aid, park_id, date, quantile_cont(y, 0.9) AS p90
            FROM ({origin.history.sql(days)}) GROUP BY ALL
            HAVING count(*) >= {self.min_ride_day_slots}""")

    def _fit(self, origin: Origin) -> None:
        from ..driver_analysis import fit_model

        con = origin.history.con
        c = origin.date
        self._rdh(origin, 3650)
        d0 = con.execute("SELECT min(date) FROM rdh").fetchone()[0]
        if d0 is None:
            return
        leads = ",".join(str(v) for v in TRAIN_LEADS)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE q AS
            SELECT aid, park_id, date - L AS origin, date, L FROM rdh, (SELECT unnest([{leads}]) AS L)
            WHERE date - L >= DATE '{d0}' + {self.window_days} AND date < DATE '{c}'""")
        tr = D.add_asof(con.execute(D.driver_rows_sql(self.window_days)).df())
        if len(tr) < self.min_train_rows:
            return
        if len(tr) > self.max_train_rows:
            tr = tr.sample(self.max_train_rows, random_state=self.seed)
        self._model = fit_model("hgb", D.features(GROUPS), tr, self.seed)
        self._fit_date = c

    # ------------------------------------------------------------------ interface
    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(columns=["attraction_id", "slot_start_utc", "q50", "q80", "q95"])

    def predict_daily(self, origin: Origin, horizon_days: pd.DataFrame,
                      known_future_covariates: pd.DataFrame) -> pd.DataFrame | None:
        con = origin.history.con
        self._static(con)
        c = origin.date
        if self._fit_date is None or (c - self._fit_date).days >= self.refit_every_days:
            self._fit(origin)
        if self._model is None or horizon_days.empty:
            return None
        self._rdh(origin, self.window_days)
        q = horizon_days.rename(columns={"attraction_id": "aid", "lead_days": "L"})[
            ["aid", "park_id", "date", "L"]].copy()
        q["date"] = pd.to_datetime(q["date"]).dt.date
        q["origin"] = c
        con.register("q_in", q)
        con.execute("""CREATE OR REPLACE TEMP TABLE q AS
            SELECT aid, park_id, CAST(origin AS DATE) origin, CAST(date AS DATE) date, CAST(L AS INTEGER) L
            FROM q_in""")
        con.unregister("q_in")
        rows = D.add_asof(con.execute(D.driver_rows_sql(self.window_days, with_truth=False)).df())
        if rows.empty:
            return None
        X = rows.copy()
        X["L"] = X["L"].clip(upper=max(TRAIN_LEADS))   # no training rows beyond 90 d
        yhat = self._model.predict(X)
        return pd.DataFrame({"attraction_id": rows["aid"], "date": rows["date"],
                             "level": rows["ref"].to_numpy() * np.exp(yhat)})
