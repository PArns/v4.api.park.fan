"""The SERVED arm (PAR-1043): what production actually showed, replayed from the forward
archive (``forecast_archive_curves``, docs/ml/forward-archive.md) instead of re-implemented.

``prod_served`` (baselines.py) reconstructs the planner's composed tier and only exists from
d3; PCN + the persistence blend are not in the bench at all. This arm reads the archived
curves themselves (``mlbench export`` table ``archive_curves`` -> ``build`` ->
``parquet/served_curves.parquet``), so a candidate paired against it (``run --pair-with
served`` + ``report --vs served``) is compared with the product, not with a naive.

Surfaces, per bench origin kind:

* daily origin (06:00 park-local, the slot table, UC2 d0 / UC3 / D3): ``plan_day`` (2), the
  planner — its 06:00 capture (kind 1) for d0–d7 and its 07:00 long-lead capture (kind 3)
  for d10–d90. Hours are served as a step, so a 60-min value covers its four quarters.
* intraday origins (the intraday table, UC1 / UC2-intraday): ``park_hourly`` (1), the
  served 15-min curve (CatBoost, PCN + blend, Chronos-2 where it is served).
  ``served_incumbent`` prefers surface 8 (what park_hourly would show without Chronos-2,
  archived only while ``SERVE_CHRONOS_INTRADAY`` is on) at the same capture.

The nearest-origin rule and the information cut. A capture runs within the hour of its
NOMINAL origin (the archive job fires at :05 UTC and reads each park's clock), so the curve
read at 10:07 is the 10:00 origin's. For every (ride, target day) the arm takes the archived
row with the LATEST nominal origin that is not after the bench origin, at most ``MAX_AGE``
older (one intraday capture cadence: a bench origin with no capture of its own, 08:00 or
20:00, is served the 06:00 / 18:00 curve; nothing staler — production would have shown a
fresher curve that the archive does not hold). Kind 3 counts as the 06:00 origin it extends.
Because the capture itself happens up to an hour after the nominal origin, only slots that
START at or after the capture instant are answered: no slot the served curve could have
partly observed is ever scored. What remains is a head start of minutes (the served curve
knew the live readings up to its capture), which favours SERVED — a candidate's win is, if
anything, understated.

Known biases, documented rather than corrected: ``park_hourly`` waits are archived ROUNDED
(``roundServedWait``, 5-minute steps) while every bench candidate is scored raw, which costs
the served arm a little MAE (the archive's own live board rounds both arms); ``plan_day`` is
archived at 1-minute precision.

The arm never reads truth, so the runner's poison test (``test_information_cut_plugins``)
cannot see it; ``tests/test_served.py`` covers its own cut. It holds its own DuckDB
connection to the archive file only; nothing the runner hands over is touched.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import ClassVar

import pandas as pd

from .base import Model, Origin

PLAN_DAY, PARK_HOURLY, PARK_HOURLY_INCUMBENT = 2, 1, 8
KIND_DAILY, KIND_INTRADAY, KIND_LONG = 1, 2, 3
#: oldest nominal origin still taken as "what was served at the bench origin"
MAX_AGE_MIN = 120
#: a capture belongs to the nominal origin of the hour it ran in
CAPTURE_WINDOW_MIN = 60


def curves_path() -> Path:
    """``$MLBENCH_SERVED_CURVES``, else ``$MLBENCH_EXPORT/parquet/served_curves.parquet``
    (``mlbench run`` sets ``MLBENCH_EXPORT`` to its ``--export``)."""
    if os.environ.get("MLBENCH_SERVED_CURVES"):
        return Path(os.environ["MLBENCH_SERVED_CURVES"])
    if os.environ.get("MLBENCH_EXPORT"):
        return Path(os.environ["MLBENCH_EXPORT"]) / "parquet" / "served_curves.parquet"
    raise FileNotFoundError("served arm: set MLBENCH_SERVED_CURVES or MLBENCH_EXPORT")


def served_sql(path: str, kinds: tuple[int, ...], surfaces: tuple[int, ...]) -> str:
    """Archived served value per requested slot. ``o``: park_id, origin_utc (the bench
    origin); ``g``: attraction_id, slot_start_utc. ``surfaces`` in order of preference
    within one capture."""
    prio = " ".join(f"WHEN {s} THEN {i}" for i, s in enumerate(surfaces))
    return f"""
    WITH c AS (
      SELECT cv.aid, cv.target_date, cv.surface, cv.origin_utc, cv.slot_start_utc,
             cv.slot_minutes, cv.waits, o.origin_utc AS bench_utc,
             cv.origin_utc - CASE WHEN cv.origin_kind = {KIND_LONG} THEN INTERVAL 1 HOUR
                                  ELSE INTERVAL 0 HOUR END AS nominal_utc
      FROM read_parquet('{path}') cv JOIN o ON o.park_id = cv.park_id
      WHERE cv.surface IN ({', '.join(map(str, surfaces))})
        AND cv.origin_kind IN ({', '.join(map(str, kinds))})
        AND cv.origin_utc >= (SELECT min(origin_utc) FROM o) - INTERVAL {MAX_AGE_MIN} MINUTE
        AND cv.origin_utc < (SELECT max(origin_utc) FROM o) + INTERVAL {CAPTURE_WINDOW_MIN + 60} MINUTE),
    pick AS (
      SELECT * FROM c
      WHERE nominal_utc < bench_utc + INTERVAL {CAPTURE_WINDOW_MIN} MINUTE
        AND nominal_utc >= bench_utc - INTERVAL {MAX_AGE_MIN} MINUTE
      QUALIFY row_number() OVER (PARTITION BY aid, target_date
                                 ORDER BY nominal_utc DESC, CASE surface {prio} END) = 1)
    SELECT g.attraction_id, g.slot_start_utc,
           CAST(p.waits[1 + date_diff('minute', p.slot_start_utc, g.slot_start_utc)
                            // p.slot_minutes] AS DOUBLE) AS q50
    FROM g JOIN pick p ON p.aid = g.attraction_id
     AND g.slot_start_utc >= p.slot_start_utc
     AND g.slot_start_utc >= p.origin_utc
     AND date_diff('minute', p.slot_start_utc, g.slot_start_utc) < p.slot_minutes * len(p.waits)
    WHERE q50 IS NOT NULL"""


class Served(Model):
    name: ClassVar[str] = "served"
    intraday: ClassVar[bool] = True
    max_lead_days: ClassVar[int] = 90
    refit_every_days: ClassVar[int | None] = None
    train_days: ClassVar[int | None] = None
    daily_surfaces: ClassVar[tuple[int, ...]] = (PLAN_DAY,)
    intraday_surfaces: ClassVar[tuple[int, ...]] = (PARK_HOURLY,)

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path) if path else None
        self._con = None

    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        import duckdb

        path = self._path or curves_path()
        if not path.exists():
            # a missing archive must never read as "served had no value"
            raise FileNotFoundError(f"served arm: {path} not found — export `archive_curves` "
                                    "and re-run `mlbench build`")
        if self._con is None:
            self._con = duckdb.connect()
            self._con.execute("SET TimeZone='UTC'")
        con = self._con
        daily = origin.kind == "daily"
        kinds = (KIND_DAILY, KIND_LONG) if daily else (KIND_DAILY, KIND_INTRADAY)
        surfaces = self.daily_surfaces if daily else self.intraday_surfaces
        con.register("o", origin.origin_utc[["park_id", "origin_utc"]])
        con.register("g", horizon_slots[["attraction_id", "slot_start_utc"]])
        try:
            return con.execute(served_sql(str(path), kinds, surfaces)).df()
        finally:
            con.unregister("o")
            con.unregister("g")


class ServedIncumbent(Served):
    """``served`` with the intraday incumbent (surface 8) preferred where it was archived:
    the reference a served Chronos-2 is compared against, so it is not paired with itself."""

    name: ClassVar[str] = "served_incumbent"
    intraday_surfaces: ClassVar[tuple[int, ...]] = (PARK_HOURLY_INCUMBENT, PARK_HOURLY)
