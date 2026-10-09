"""The model plug-in interface (BENCH-SPEC "Model interface").

A model is a class with ``fit`` and ``predict``; the runner owns the data split,
the information cut and all scoring. A model never sees a truth value whose slot
ends after its origin.

    fit(train_panel, cutoff)
    predict(origin, horizon_slots, known_future_covariates)
        -> DataFrame[attraction_id, slot_start_utc, q50, q80, q95]

Optional, for the crowd calendar (UC4) and long leads:

    predict_daily(origin, horizon_days, known_future_covariates)
        -> DataFrame[attraction_id, date, level]       # level = the ride-day's P90

When a model implements ``predict_daily`` the runner also scores
``<name>_x_h5``: its daily level composed with the H5 profile (the same ratio
scaling as baseline 5), at every slot lead — so a daily model gets a 15-min
curve at long leads without producing slots itself.

Register a model in ``mlbench/models/__init__.py`` (``REGISTRY``) or pass
``--model package.module:ClassName`` to ``mlbench run``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import pandas as pd


@dataclass
class Origin:
    """One forecast origin.

    ``date`` is the park-local origin date; ``origin_utc`` differs per park
    (06:00 park-local, or the intraday hour). ``kind`` is ``daily`` (06:00,
    UC2/UC3/UC4) or ``intraday`` (every 2 h, UC1/UC2).
    """

    date: Any                       # datetime.date
    kind: str                       # 'daily' | 'intraday'
    hour_local: int
    origin_utc: pd.DataFrame        # park_id, origin_utc (tz-aware UTC)
    history: Any                    # HistoryView — truth strictly before each park's origin_utc


class HistoryView:
    """Lazy access to the truth panel strictly before the origin.

    ``df(days=56)`` returns ``attraction_id, park_id, date, slot_start_utc,
    slot_local, ws, ko, kc, y`` for the last ``days`` service days (all slots end
    before the park's origin). ``sql(days)`` returns the same as a DuckDB SQL
    string for models that prefer to aggregate in DuckDB via ``con``.
    """

    def __init__(self, con, origin_table: str, origin_date):
        self.con = con
        self.origin_table = origin_table
        self.origin_date = origin_date

    def sql(self, days: int = 56) -> str:
        return f"""
            SELECT t.aid AS attraction_id, t.park_id, t.date, t.slot_utc AS slot_start_utc,
                   t.slot_local, t.ws, t.ko, t.kc, t.y
            FROM truth t JOIN {self.origin_table} o ON o.park_id = t.park_id
            WHERE t.date >= DATE '{self.origin_date}' - {int(days)}
              AND t.date < DATE '{self.origin_date}'
              AND t.slot_utc + INTERVAL 15 MINUTE <= o.origin_utc"""

    def df(self, days: int = 56) -> pd.DataFrame:
        return self.con.execute(self.sql(days)).df()


class Model:
    """Base class. Subclasses set ``name`` and override ``predict``."""

    name: ClassVar[str] = "model"
    #: True if predict() returns q80/q95 (scored for D8 calibration)
    provides_quantiles: ClassVar[bool] = False
    #: True to also be called at the intraday origins (UC1 / UC2 d0 / D1 / D2)
    intraday: ClassVar[bool] = False
    #: largest slot lead (days) the model produces; longer leads are not requested
    max_lead_days: ClassVar[int] = 90
    #: refit cadence in days (None = fit once at the first origin)
    refit_every_days: ClassVar[int | None] = 28
    #: days of truth handed to fit() (None = all history before the cutoff)
    train_days: ClassVar[int | None] = None

    def fit(self, train_panel: pd.DataFrame, cutoff: pd.Timestamp) -> None:  # noqa: B027
        """Train on ``train_panel`` (truth slots that end before ``cutoff``)."""

    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        """Return one row per requested slot you can forecast.

        ``horizon_slots``: attraction_id, park_id, date, slot_start_utc, slot_local,
        ws, ko, kc, lead_days — every 15-min slot inside the PUBLISHED window of each
        target day, for every ride with truth in the 56 days before the origin
        (not only the slots that later turn out to be operating).
        ``known_future_covariates``: one row per (park_id, date) — see
        ``park_day_cov`` in mlbench/build.py; weather columns are ORACLE actuals.
        """
        raise NotImplementedError

    def predict_daily(self, origin: Origin, horizon_days: pd.DataFrame,
                      known_future_covariates: pd.DataFrame) -> pd.DataFrame | None:
        """Optional daily levels. ``horizon_days``: attraction_id, park_id, date, lead_days."""
        return None
