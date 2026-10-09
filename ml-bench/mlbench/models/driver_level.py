"""Plug-in: the daily level from known-in-advance drivers (PAR-830).

level = naive (baseline 5a: median of the last four same-weekday daily P90s in the
56 days before the origin) × exp(ŷ), where ŷ is a gradient-boosted model of
``log(true P90 / naive)`` on the drivers of ``mlbench/drivers.py``: weekday, lead,
region, the reference level, public and school holidays incl. neighbour regions
(ml-service OR semantics), bridge days, day before/after a holiday, and the
opening hours of the target day against the reference days.

Only what the runner hands over is used (no connection, no files):

* ride-day P90s and the past opening windows come from ``origin.history`` (truth
  before the origin), accumulated across origins;
* holidays and the target day's window come from ``known_future_covariates``
  (the window AS KNOWN AT THE ORIGIN — published if written before it, else
  projected by the runner), also accumulated, so past days keep the holiday flags
  they were handed when they were still in the future;
* no weather (ORACLE only), no ticketed events / season start (not in the
  covariates; neither was significant in the offline analysis).

Training rows are the past ride-days × leads 1–90 with their reference cut at
``date − lead``; their opening hours are the actual ones (a past day's window is
known). At prediction an unpublished window is the runner's projection, so its
hours move the level only as far as the projection differs from the reference.

``predict`` returns the naive level × H5 curve of ``LevelH5Example`` scaled by
exp(ŷ) (first hour unscaled, like baseline 5); ``predict_daily`` returns the level,
which the runner also composes into ``driver_level_x_h5``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .. import drivers as D
from .base import Origin
from .example_level_h5 import LevelH5Example

TRAIN_LEADS = [1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90]
FEATURES = [f for f in D.features(["cal", "hol", "sched"])
            if f not in ("is_headliner", "since_season_start_a", "to_season_end_a",
                         "ticketed_a", "extra_a", "ref_ticketed")]
COV_KEEP = ["park_id", "date", "dow", "is_holiday_primary", "is_school_holiday_primary",
            "is_holiday_neighbor_1", "is_holiday_neighbor_2", "is_holiday_neighbor_3",
            "neighbor_school_holiday_count", "holiday_count_total", "is_school_holiday_any",
            "open_local", "close_local"]


class DriverLevel(LevelH5Example):
    name = "driver_level"
    provides_quantiles = False
    intraday = False
    max_lead_days = 90
    refit_every_days = 28

    window_days = 56
    min_ride_day_slots = 8
    min_train_rows = 500             # below this, no model (and no forecast) yet
    settled_rows = 20000             # below this, refit at every origin instead of every 28 days
    max_train_rows = 300_000
    seed = 830

    def __init__(self) -> None:
        import duckdb

        self._db = duckdb.connect()
        self._db.execute("SET threads=2")
        self._db.execute("SET TimeZone='UTC'")
        self._rd = pd.DataFrame(columns=["aid", "park_id", "date", "p90"])
        self._win = pd.DataFrame(columns=["park_id", "date", "open_local", "close_local"])
        self._cov = pd.DataFrame(columns=COV_KEEP)
        self._seen = None            # last origin whose history is in the caches
        self._model = None
        self._fit_date = None
        self._fit_rows = 0
        self._memo: dict = {}

    # ------------------------------------------------------------------ caches
    def _advance(self, origin: Origin, cov: pd.DataFrame) -> None:
        c = origin.date
        if self._seen == c:
            return
        days = 400 if self._seen is None else (c - self._seen).days + 2
        h = origin.history.df(days)
        if len(h):
            h = h.assign(date=pd.to_datetime(h["date"]).dt.date)
            h = h[h["date"] < c]
            g = h.groupby(["attraction_id", "park_id", "date"])["y"]
            rd = g.quantile(0.9).rename("p90").to_frame().join(g.size().rename("n")).reset_index()
            rd = rd[rd["n"] >= self.min_ride_day_slots].rename(columns={"attraction_id": "aid"})
            self._rd = (pd.concat([self._rd, rd[["aid", "park_id", "date", "p90"]]])
                        .drop_duplicates(["aid", "date"], keep="last"))
            # the window of a past service day, read off its truth slots
            w = (h.assign(o=h["ws"] - h["ko"], x=h["ws"] + h["kc"] + 1)
                 .groupby(["park_id", "date"])[["o", "x"]].median().reset_index())
            base = pd.to_datetime(w["date"])
            w["open_local"] = base + pd.to_timedelta(w["o"] * 15, unit="m")
            w["close_local"] = base + pd.to_timedelta(w["x"] * 15, unit="m")
            self._win = (pd.concat([self._win, w[["park_id", "date", "open_local", "close_local"]]])
                         .drop_duplicates(["park_id", "date"], keep="last"))
        if len(cov):
            cv = cov[[k for k in COV_KEEP if k in cov.columns]].copy()
            cv["date"] = pd.to_datetime(cv["date"]).dt.date
            self._cov = pd.concat([self._cov, cv]).drop_duplicates(["park_id", "date"], keep="last")
        self._parks = origin.origin_utc[["park_id", "timezone"]].rename(columns={"park_id": "id"})
        self._seen = c
        self._memo = {}

    def _tables(self, c) -> None:
        """Register rdh / rides / parks / park_day_cov / windows / ev and build ``pdf``."""
        db = self._db
        cov = self._cov.copy()
        for col in ("temp_max", "temp_min", "precip_sum", "wind_max"):
            cov[col] = np.nan
        past = self._win[self._win["date"] < c]
        fut = cov.loc[cov["date"] >= c, ["park_id", "date", "open_local", "close_local"]].dropna()
        win = pd.concat([past, fut]).drop_duplicates(["park_id", "date"], keep="last")
        win["n_windows"] = 1
        rides = self._rd[["aid", "park_id"]].drop_duplicates("aid").assign(is_headliner=False)
        for name, df in (("rdh_df", self._rd), ("rides_df", rides), ("parks_df", self._parks),
                         ("cov_df", cov.drop(columns=["open_local", "close_local"])), ("win_df", win)):
            db.register(name, df)
        db.execute("""CREATE OR REPLACE TABLE rdh AS
                      SELECT aid, park_id, CAST(date AS DATE) date, CAST(p90 AS DOUBLE) p90 FROM rdh_df""")
        db.execute("CREATE OR REPLACE TABLE rides AS SELECT * FROM rides_df")
        db.execute("CREATE OR REPLACE TABLE parks AS SELECT * FROM parks_df")
        db.execute("""CREATE OR REPLACE TABLE park_day_cov AS
                      SELECT * EXCLUDE (date), CAST(date AS DATE) date FROM cov_df""")
        db.execute("""CREATE OR REPLACE TABLE windows AS
                      SELECT park_id, CAST(date AS DATE) date, CAST(open_local AS TIMESTAMP) open_local,
                             CAST(close_local AS TIMESTAMP) close_local, n_windows FROM win_df""")
        # every window counts as known here: the past is observed, the future is the
        # runner's as-known-at-origin window (published or projected)
        db.execute("""CREATE OR REPLACE TABLE ev AS SELECT park_id, date, NULL::INTEGER ticketed,
                      NULL::INTEGER extra, 0::BIGINT win_upd_us, NULL::BIGINT ev_upd_us FROM windows""")
        D.park_day_features(db)

    def _fit(self, c) -> None:
        from ..driver_analysis import fit_model

        db = self._db
        if self._rd.empty:
            return
        self._tables(c)
        d0 = min(self._rd["date"])
        leads = ",".join(str(v) for v in TRAIN_LEADS)
        db.execute(f"""CREATE OR REPLACE TABLE q AS
            SELECT aid, park_id, date - L AS origin, date, L FROM rdh, (SELECT unnest([{leads}]) AS L)
            WHERE date - L >= DATE '{d0}' + {self.window_days} AND date < DATE '{c}'""")
        n = db.execute("SELECT count(*) FROM q").fetchone()[0]
        if n > 2 * self.max_train_rows:      # keep memory flat: deterministic hash sample
            keep = max(1, int(1000 * 2 * self.max_train_rows / n))
            db.execute(f"DELETE FROM q WHERE hash(aid, date, L) % 1000 >= {keep}")
        tr = D.add_asof(db.execute(D.driver_rows_sql(self.window_days)).df())
        if len(tr) < self.min_train_rows:
            return
        if len(tr) > self.max_train_rows:
            tr = tr.sample(self.max_train_rows, random_state=self.seed)
        self._model = fit_model("hgb", FEATURES, tr, self.seed)
        self._fit_date, self._fit_rows = c, len(tr)

    def _yhat(self, origin: Origin, days: pd.DataFrame, cov: pd.DataFrame) -> pd.DataFrame:
        """ref and ŷ per requested (ride, date)."""
        c = origin.date
        self._advance(origin, cov)
        if (self._fit_date is None or self._fit_rows < self.settled_rows
                or (c - self._fit_date).days >= self.refit_every_days):
            if self._fit_date != c:
                self._fit(c)
        if self._model is None or days.empty:
            return pd.DataFrame(columns=["aid", "date", "ref", "yhat"])
        q = days.rename(columns={"attraction_id": "aid", "lead_days": "L"})[["aid", "park_id", "date", "L"]].copy()
        q["date"] = pd.to_datetime(q["date"]).dt.date
        q = q.drop_duplicates(["aid", "date"])
        todo = q[[k not in self._memo for k in zip(q["aid"], q["date"])]]
        if len(todo):
            self._tables(c)
            todo = todo.assign(origin=c)
            self._db.register("q_in", todo)
            self._db.execute("""CREATE OR REPLACE TABLE q AS
                SELECT aid, park_id, CAST(origin AS DATE) origin, CAST(date AS DATE) date,
                       CAST(L AS INTEGER) L FROM q_in""")
            rows = D.add_asof(self._db.execute(D.driver_rows_sql(self.window_days, with_truth=False)).df())
            if len(rows):
                rows["L"] = rows["L"].clip(upper=max(TRAIN_LEADS))   # no training rows beyond 90 d
                yh = self._model.predict(rows)
                for a, d, r, y in zip(rows["aid"], pd.to_datetime(rows["date"]).dt.date, rows["ref"], yh):
                    self._memo[(a, d)] = (float(r), float(y))
            for k in zip(todo["aid"], todo["date"]):
                self._memo.setdefault(k, None)
        out = [(a, d, *self._memo[(a, d)]) for a, d in zip(q["aid"], q["date"]) if self._memo.get((a, d))]
        return pd.DataFrame(out, columns=["aid", "date", "ref", "yhat"])

    # ------------------------------------------------------------------ interface
    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        days = horizon_slots[["attraction_id", "park_id", "date", "lead_days"]].drop_duplicates(
            ["attraction_id", "date"])
        y = self._yhat(origin, days, known_future_covariates)
        base = super().predict(origin, horizon_slots, known_future_covariates)
        if base.empty or y.empty:
            return base.iloc[:0]
        g = horizon_slots[["attraction_id", "slot_start_utc", "date", "ko"]].copy()
        g["date"] = pd.to_datetime(g["date"]).dt.date
        m = base.merge(g, on=["attraction_id", "slot_start_utc"]).merge(
            y.rename(columns={"aid": "attraction_id"}), on=["attraction_id", "date"])
        first = m["ko"].between(0, 3)
        m["q50"] = np.where(first, m["q50"], m["q50"] * np.exp(m["yhat"]))
        return m[["attraction_id", "slot_start_utc", "q50"]]

    def predict_daily(self, origin: Origin, horizon_days: pd.DataFrame,
                      known_future_covariates: pd.DataFrame) -> pd.DataFrame | None:
        y = self._yhat(origin, horizon_days, known_future_covariates)
        if y.empty:
            return None
        return pd.DataFrame({"attraction_id": y["aid"], "date": y["date"],
                             "level": y["ref"] * np.exp(y["yhat"])})
