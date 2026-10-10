"""Level drivers (PAR-830): how much of a day's level is explainable in advance?

The question is about the DAILY LEVEL only. For a ride-day ``d`` forecast from an
origin ``c = d - L`` (06:00 park-local, BENCH-SPEC) the reference is the naive
level of baseline 5a — the median of the last four same-weekday daily P90s inside
the 56-day window before ``c`` (at least two). The target is

    y = log(true daily P90 / naive reference)

and every driver is something a forecaster knows at ``c``. This module builds that
table (one row per ride-day × lead) in DuckDB SQL and is shared by the offline
analysis (``python -m mlbench drivers``) and by the harness plug-in
``models/driver_level.py`` — the plug-in builds its training rows and its
prediction rows with exactly the same SQL, so what the analysis measures is what
the harness scores.

Inputs (DuckDB tables/views on ``con``):

``rdh``          aid, park_id, date, p90 — history ride-day levels (truth only)
``q``            aid, park_id, origin, date, L — the rows to build
``park_day_cov`` the export's known-future covariates (holidays via ml-service
                 ``holiday_features.py``, OR semantics; weather = ORACLE actuals)
``windows``      published OPERATING windows (latest version: ``published_final``)
``ev``           park_id, date, ticketed, extra — TICKETED_EVENT / EXTRA_HOURS
                 schedule entries (only synced since September 2026; see notes)
``parks``, ``rides``

Driver groups (``GROUPS``) — the unit of the "what is worth adding" ranking:

``cal``    weekday, lead, region, headliner, the reference itself (log level,
           how many weeks it rests on, its age)
``hol``    public / school holidays, own region and neighbour regions, bridge days,
           day before / after a holiday, and how many reference days were holidays
``sched``  published opening hours (length, open/close hour, vs the reference
           days), ticketed events, extra hours, days since the season opened /
           until it closes, days since the park last opened — AS KNOWN AT THE
           ORIGIN (``add_asof``); ``sched_final`` is the same as finally published
``wx``     daily weather (ORACLE actuals — no forecast archive exists) and its
           difference to the reference days
"""

from __future__ import annotations

from .config import REGION_SQL

# A season break: no published OPERATING window for more than this many days.
SEASON_GAP_DAYS = 7
CAP_DAYS = 60

# Driver columns that come from the TARGET day's published schedule. Their as-of
# variant (``<col>_a``, ``add_asof``) is NULL when the schedule row was last written
# after the origin: the export keeps only the latest version of a day, and its
# ``updated_us`` is the only hint of when it became known. A re-sync also rewrites
# ``updated_us``, so the mask is conservative (it may hide a window that was known).
WINDOW_COLS = ["hours", "open_h", "close_h", "d_hours", "r_hours", "d_close", "d_open",
               "since_season_start", "to_season_end", "since_prev_open"]
EVENT_COLS = ["ticketed", "extra"]

GROUPS: dict[str, list[str]] = {
    # no month / day-of-year: with < 1 year of history a month the model has never
    # seen is out of distribution (measured: it made the calendar-only model WORSE
    # than naive); season enters through the reference and the holidays instead
    "cal": ["L", "dow", "region", "is_headliner", "log_ref", "n_ref", "ref_age"],
    "hol": ["hol", "school", "hol_nb", "school_nb_cnt", "school_any", "hol_cnt", "bridge",
            "hol_prev", "hol_next", "ref_hol", "ref_school", "d_hol", "d_school", "d_school_any"],
    # as known at the origin (the honest set)
    "sched": [f"{c}_a" for c in WINDOW_COLS + EVENT_COLS] + ["ref_hours", "ref_ticketed"],
    # as finally published (upper bound: assumes every schedule was known at any lead)
    "sched_final": WINDOW_COLS + EVENT_COLS + ["ref_hours", "ref_ticketed"],
    "wx": ["temp_max", "precip", "wind_max", "d_temp", "d_precip"],
}
CATEGORICAL = ["dow", "month", "region"]


def event_sql(schedule_csv: str | None) -> str:
    """``ev`` per park-day from the raw schedule export: ticketed event / extra hours
    flags and when the OPERATING window and the event rows were last written
    (``win_upd_us`` / ``ev_upd_us``). Empty when the file is not available — then
    every schedule driver counts as unknown at the origin."""
    if not schedule_csv:
        return ("SELECT NULL::VARCHAR park_id, NULL::DATE date, 0 ticketed, 0 extra, "
                "NULL::BIGINT win_upd_us, NULL::BIGINT ev_upd_us WHERE false")
    return f"""
        SELECT park_id, CAST(date AS DATE) AS date,
               max(CASE WHEN schedule_type = 'TICKETED_EVENT' THEN 1 ELSE 0 END) AS ticketed,
               max(CASE WHEN schedule_type = 'EXTRA_HOURS' THEN 1 ELSE 0 END) AS extra,
               max(updated_us) FILTER (WHERE schedule_type = 'OPERATING') AS win_upd_us,
               max(updated_us) FILTER (WHERE schedule_type IN ('TICKETED_EVENT', 'EXTRA_HOURS'))
                 AS ev_upd_us
        FROM read_csv('{schedule_csv}', header=true, auto_detect=true)
        WHERE schedule_type IN ('OPERATING', 'TICKETED_EVENT', 'EXTRA_HOURS')
        GROUP BY ALL"""


def park_day_features(con) -> None:
    """``pdf``: one row per park-day with every driver that does not depend on the origin."""
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE pdf AS
    WITH w AS (
      SELECT park_id, date, open_local, close_local, n_windows,
             date - lag(date) OVER (PARTITION BY park_id ORDER BY date) AS gap_prev,
             lead(date) OVER (PARTITION BY park_id ORDER BY date) - date AS gap_next,
             max(date) OVER (PARTITION BY park_id) AS last_published
      FROM windows),
    s AS (
      SELECT *, sum(CASE WHEN gap_prev IS NULL OR gap_prev > {SEASON_GAP_DAYS} THEN 1 ELSE 0 END)
                  OVER (PARTITION BY park_id ORDER BY date) AS season_id
      FROM w),
    s2 AS (
      SELECT *, min(date) OVER (PARTITION BY park_id, season_id) AS season_start,
             max(date) OVER (PARTITION BY park_id, season_id) AS season_end,
             bool_or(gap_prev IS NULL) OVER (PARTITION BY park_id, season_id) AS first_season
      FROM s),
    c AS (
      SELECT cv.park_id, cv.date, cv.dow, month(cv.date) AS month,
             sin(2 * pi() * dayofyear(cv.date) / 365.25) AS doy_sin,
             cos(2 * pi() * dayofyear(cv.date) / 365.25) AS doy_cos,
             cv.is_holiday_primary AS hol, cv.is_school_holiday_primary AS school,
             greatest(cv.is_holiday_neighbor_1, cv.is_holiday_neighbor_2,
                      cv.is_holiday_neighbor_3) AS hol_nb,
             cv.neighbor_school_holiday_count AS school_nb_cnt,
             cv.is_school_holiday_any AS school_any, cv.holiday_count_total AS hol_cnt,
             lag(cv.is_holiday_primary) OVER (PARTITION BY cv.park_id ORDER BY cv.date) AS hol_prev,
             lead(cv.is_holiday_primary) OVER (PARTITION BY cv.park_id ORDER BY cv.date) AS hol_next,
             cv.temp_max, cv.precip_sum AS precip, cv.wind_max
      FROM park_day_cov cv)
    SELECT c.*,
           CAST((c.dow = 1 AND c.hol_next = 1 AND c.hol = 0)
                OR (c.dow = 5 AND c.hol_prev = 1 AND c.hol = 0) AS INTEGER) AS bridge,
           s2.date IS NOT NULL AS has_window,
           date_diff('minute', s2.open_local, s2.close_local) / 60.0 AS hours,
           hour(s2.open_local) + minute(s2.open_local) / 60.0 AS open_h,
           date_diff('minute', CAST(c.date AS TIMESTAMP), s2.close_local) / 60.0 AS close_h,
           s2.n_windows,
           coalesce(ev.ticketed, 0) AS ticketed, coalesce(ev.extra, 0) AS extra,
           ev.win_upd_us, ev.ev_upd_us,
           -- the first season in the data starts where our recording starts: unknown
           CASE WHEN NOT s2.first_season THEN least(c.date - s2.season_start, {CAP_DAYS}) END
             AS since_season_start,
           -- a season that ends on the last published day has no known end yet
           CASE WHEN s2.season_end < s2.last_published THEN least(s2.season_end - c.date, {CAP_DAYS}) END
             AS to_season_end,
           least(s2.gap_prev, {CAP_DAYS}) AS since_prev_open
    FROM c
    LEFT JOIN s2 ON s2.park_id = c.park_id AND s2.date = c.date
    LEFT JOIN ev ON ev.park_id = c.park_id AND ev.date = c.date""")


def driver_rows_sql(window_days: int = 56, weeks: int = 4, min_weeks: int = 2,
                    with_truth: bool = True) -> str:
    """One row per ``q`` row that has a naive reference, with every driver and,
    when ``with_truth``, the true P90 and the target ``y``."""
    truth_cols = ", t.p90 AS true_p90, ln(t.p90 / r.ref) AS y" if with_truth else ""
    truth_join = "JOIN rdh t ON t.aid = q.aid AND t.date = q.date" if with_truth else ""
    return f"""
    WITH refc AS (
      SELECT q.aid, q.date, q.L, h.date AS rdate, h.p90,
             row_number() OVER (PARTITION BY q.aid, q.date, q.L ORDER BY h.date DESC) AS rn
      FROM q JOIN rdh h ON h.aid = q.aid AND dayofweek(h.date) = dayofweek(q.date)
           AND h.date >= q.origin - {int(window_days)} AND h.date < q.origin),
    r AS (
      SELECT f.aid, f.date, f.L, median(f.p90) AS ref, count(*) AS n_ref,
             min(f.date - f.rdate) AS ref_age,
             avg(p.hol) AS ref_hol, avg(p.school) AS ref_school, avg(p.school_any) AS ref_school_any,
             avg(p.hours) AS ref_hours, avg(p.close_h) AS ref_close, avg(p.open_h) AS ref_open,
             avg(p.ticketed) AS ref_ticketed, avg(p.temp_max) AS ref_temp, avg(p.precip) AS ref_precip
      FROM refc f JOIN q USING (aid, date, L)
      LEFT JOIN pdf p ON p.park_id = q.park_id AND p.date = f.rdate
      WHERE f.rn <= {int(weeks)}
      GROUP BY ALL HAVING count(*) >= {int(min_weeks)})
    SELECT q.aid, q.park_id, q.origin, q.date, q.L, r.ref, ln(r.ref) AS log_ref, r.n_ref, r.ref_age,
           {REGION_SQL} AS region, CAST(rd.is_headliner AS INTEGER) AS is_headliner,
           x.dow, x.month, x.doy_sin, x.doy_cos,
           x.hol, x.school, x.hol_nb, x.school_nb_cnt, x.school_any, x.hol_cnt, x.bridge,
           x.hol_prev, x.hol_next, r.ref_hol, r.ref_school,
           x.hol - r.ref_hol AS d_hol, x.school - r.ref_school AS d_school,
           x.school_any - r.ref_school_any AS d_school_any,
           x.hours, x.open_h, x.close_h, x.n_windows, r.ref_hours,
           x.hours - r.ref_hours AS d_hours, x.hours / nullif(r.ref_hours, 0) AS r_hours,
           x.close_h - r.ref_close AS d_close, x.open_h - r.ref_open AS d_open,
           x.ticketed, x.extra, r.ref_ticketed, x.since_season_start, x.to_season_end,
           x.since_prev_open, x.win_upd_us, x.ev_upd_us,
           -- the 06:00 origin, taken as 06:00 UTC (conservative within the park offset)
           epoch_us(CAST(q.origin AS TIMESTAMP) + INTERVAL 6 HOUR) AS origin_us,
           x.temp_max, x.precip, x.wind_max, x.temp_max - r.ref_temp AS d_temp,
           x.precip - r.ref_precip AS d_precip
           {truth_cols}
    FROM q JOIN r USING (aid, date, L)
    {truth_join}
    JOIN rides rd ON rd.aid = q.aid
    JOIN parks p ON p.id = q.park_id
    LEFT JOIN pdf x ON x.park_id = q.park_id AND x.date = q.date"""


def add_asof(df):
    """``<col>_a``: the schedule drivers as known at the origin (NULL otherwise)."""
    import numpy as np

    win = (df["win_upd_us"].notna() & (df["win_upd_us"] < df["origin_us"])).to_numpy()
    ev = (df["ev_upd_us"].isna() | (df["ev_upd_us"] < df["origin_us"])).to_numpy() & win
    for c in WINDOW_COLS:
        df[f"{c}_a"] = np.where(win, df[c].astype("float64"), np.nan)
    for c in EVENT_COLS:
        df[f"{c}_a"] = np.where(ev, df[c].astype("float64"), np.nan)
    df["sched_known"] = win.astype(int)
    return df


def features(groups: list[str]) -> list[str]:
    return [f for g in groups for f in GROUPS[g]]
