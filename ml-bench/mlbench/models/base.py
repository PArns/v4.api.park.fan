"""The model plug-in interface (BENCH-SPEC "Model interface").

A model is a class with ``fit`` and ``predict``; the runner owns the data split,
the information cut and all scoring. **A model never receives a database
connection**, and no connection is reachable from any object it is handed — only
DataFrames the runner has already cut at the origin — so a plug-in cannot read
the future even by accident:

    fit(train_panel, cutoff)
    predict(origin, horizon_slots, known_future_covariates)
        -> DataFrame[attraction_id, slot_start_utc, q50, q80, q95]

Optional, for the crowd calendar (UC4) and long leads:

    predict_daily(origin, horizon_days, known_future_covariates)
        -> DataFrame[attraction_id, date, level]       # level = the ride-day's P90

When a model implements ``predict_daily`` the runner also scores
``<name>_x_h5``: its daily level composed with the H5 profile (the same ratio
scaling as baseline 5), at every slot lead.

What the runner hands over, and what it guarantees:

* ``origin.history.df(days)`` / ``train_panel``: truth slots that END before the
  park's origin (``slot_start_utc + 15 min <= origin_utc``), nothing else. The
  history frame is materialised up front, so ``days`` is capped at
  ``BenchConfig.history_days`` (56); ask ``fit`` for longer history.
* ``horizon_slots``: every 15-min slot of each target day's opening window AS
  KNOWN AT THE ORIGIN — the published window if its schedule row was written
  before the origin, else the window projected from the last 56 days
  (``schedule_known_at_origin`` in the covariates says which). Rows the model
  returns for slots outside this grid are dropped.
* ``known_future_covariates``: one row per (park_id, date) — weekday, holiday
  flags (ml-service ``holiday_features.py``), the window above. Weather is
  stripped unless the model sets ``uses_oracle_weather = True``: no forecast
  archive exists, so the only weather is the ACTUAL weather of the target day
  (an oracle). An opting-in model is scored under ``<name>_owx`` so its rows can
  never be mistaken for an honest forecast.

``tests/test_runner.py::test_information_cut_plugins`` poisons every input after
the origin and asserts that no registered model's forecast moves — keep it green
for any model you add.

Register a model in ``mlbench/models/__init__.py`` (``REGISTRY``) or pass
``--model package.module:ClassName`` to ``mlbench run``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import pandas as pd


class HistoryView:
    """Truth strictly before the origin, as a DataFrame.

    ``df(days=56)``: ``attraction_id, park_id, date, slot_start_utc, slot_local, ws,
    ko, kc, y`` for the last ``days`` service days; every slot ends before its
    park's origin.

    The frame is **materialised by the runner before the model is called**. It used
    to be a lazy closure over the DuckDB connection, which meant the live connection
    — and through it every table at every date — was reachable as
    ``origin.history._fetch.__closure__[0].cell_contents``. Any closure or bound
    method leaks the same way, so the view holds data and nothing else; the
    guarantee in this module's docstring is now enforceable and
    ``test_plugins_get_no_connection`` walks the object graph to check it.

    ``days`` beyond what the runner materialised (``BenchConfig.history_days``, 56 by
    default — BENCH-SPEC's profile window) raises instead of silently returning a
    short frame. For long training history use ``fit(train_panel, cutoff)``, which
    gets ``train_days`` of truth.
    """

    def __init__(self, frame: pd.DataFrame, origin_date: Any = None, max_days: int = 56):
        self._frame = frame.reset_index(drop=True)
        self._origin_date = origin_date
        self._max_days = int(max_days)

    @property
    def max_days(self) -> int:
        return self._max_days

    def df(self, days: int = 56) -> pd.DataFrame:
        days = int(days)
        if days > self._max_days:
            raise ValueError(
                f"HistoryView holds {self._max_days} days (BenchConfig.history_days); asked for "
                f"{days}. Raise --history-days, or use fit(train_panel, cutoff) for long history.")
        f = self._frame
        if self._origin_date is None or f.empty:
            return f.copy()
        lo = pd.Timestamp(self._origin_date).date() - pd.Timedelta(days=days)
        return f[pd.to_datetime(f["date"]).dt.date >= lo].reset_index(drop=True)


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
    origin_utc: pd.DataFrame        # park_id, timezone, origin_utc (tz-aware UTC)
    history: HistoryView


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
    train_days: ClassVar[int | None] = 365
    #: opt in to ORACLE weather (actuals); the model is then scored as <name>_owx
    uses_oracle_weather: ClassVar[bool] = False

    @classmethod
    def scored_name(cls) -> str:
        return f"{cls.name}_owx" if cls.uses_oracle_weather else cls.name

    def fit(self, train_panel: pd.DataFrame, cutoff: pd.Timestamp) -> None:  # noqa: B027
        """Train on ``train_panel`` (truth slots that end before ``cutoff``). Only
        called when a subclass overrides it."""

    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        """Return one row per requested slot you can forecast.

        ``horizon_slots``: attraction_id, park_id, date, slot_start_utc, slot_local,
        ws, ko, kc, lead_days (see the module docstring for what the grid is).
        """
        raise NotImplementedError

    def predict_daily(self, origin: Origin, horizon_days: pd.DataFrame,
                      known_future_covariates: pd.DataFrame) -> pd.DataFrame | None:
        """Optional daily levels. ``horizon_days``: attraction_id, park_id, date, lead_days."""
        return None
