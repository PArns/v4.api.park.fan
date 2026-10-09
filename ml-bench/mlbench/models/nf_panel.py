"""The hourly operating-day panel the NeuralForecast plug-ins train on (PAR-829).

Why hourly, and why this grid
-----------------------------
The planner wants d0–d7 at 15 minutes. At 15 minutes on a 24-hour grid that is
8 × 96 = 768 output steps per window, three quarters of them in the night. The
benchmark's own decomposition says the error at d1–d7 is mostly the daily LEVEL
and the sub-hour detail is real only at the day edges (the H5 finding). So the
models forecast the HOURLY mean wait and the plug-in puts the 15-minute shape
back from H5 (see ``neuralforecast_models``): the model owns level and hourly
shape, H5 owns the ramp inside the opening and closing hour.

* One step = one service-day hour, ``ws // 4`` (park-local, counted from the
  service day's midnight, so a window that crosses midnight continues at 24, 25).
* Every service day has the SAME K = 18 steps (hours 08..25): 99.99 % of the
  13.5 M truth slots fall in them (export 2026-10-09; the earliest truth hour is
  08, hours ≥ 26 hold 220 slots). Slots before 08 are counted in 08, after 25 in 25.
* **Closed hours, closed days and missing readings stay on the grid** with
  ``available_mask = 0`` — never dropped. The TFT review found the daily panel
  drops closed days, which silently changes what "7 steps back" means. Here a
  week is always 7 × K steps, for every ride, and the masked steps carry no loss.
* The time index is an integer (day index × K + hour − 8), aligned across all
  rides and parks: series are aligned by service DATE, not by UTC instant, which
  is what a multivariate model (TSMixerx) needs and what a park-local forecast is.

Target: mean of the hour's truth slots (OPERATING, wait ≥ 5, inside the published
window — exactly the harness truth). An hour with at least one truth slot is
available.

Known-future covariates per (park, service day, hour) — all from the export:
opening-relative position (hours since published opening / until published
closing at the hour's midpoint, the share of the hour inside the window, window
length), weekday, day of year, the ml-service holiday flags (own region and
neighbour regions, OR semantics), schedule holiday / bridge-day flags. Weather
(daily actuals, ORACLE) only when asked for.

Static per ride: headliner flag, region, coordinates, and the ride's / park's
median daily P90 over the 56 days before the block's cutoff (information cut).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np

from ..config import region_of

HOUR0 = 8          # first service-day hour on the grid
K = 18             # hours per service day on the grid (08..25)

BASE_FUTR = [
    "hr_pos", "in_win", "day_open", "h_open", "h_close", "win_len",
    "dow_sin", "dow_cos", "is_weekend", "doy_sin", "doy_cos",
    "is_holiday_primary", "is_school_holiday_primary", "is_holiday_neighbor_1",
    "is_holiday_neighbor_2", "is_holiday_neighbor_3", "neighbor_school_holiday_count",
    "is_school_holiday_any", "sched_is_holiday", "sched_is_bridge_day",
]
WEATHER_FUTR = ["wx_temp_max", "wx_precip", "wx_wind", "wx_ok"]
STATIC = ["is_headliner", "reg_eu", "reg_na", "reg_asia", "lat", "lng", "lvl_ride", "lvl_park"]
HOLIDAY_COLS = BASE_FUTR[11:]


@dataclass
class Panel:
    """``temporal`` is [N, T, C] float32 with columns ``cols`` (y first,
    available_mask last); step t is service day ``day0 + t // K``, hour ``HOUR0 + t % K``."""

    aids: list[str]
    parks: list[str]
    groups: list[str]              # region per series (for per-region multivariate fits)
    day0: dt.date
    n_days: int
    temporal: np.ndarray
    cols: list[str]
    futr_cols: list[str]
    static: np.ndarray
    static_cols: list[str]

    @property
    def T(self) -> int:
        return self.n_days * K

    def idx(self, day: dt.date, s: int = 0) -> int:
        return (day - self.day0).days * K + s

    @property
    def mask(self) -> np.ndarray:
        return self.temporal[:, :, -1]


def hour_features(cov, futr_cols: list[str]) -> np.ndarray:
    """[rows, K, F] hour features for park-day rows with ``has_published_window, o_m, c_m``
    (open / close minute from the service day's local midnight), ``dow`` (0 = Sunday),
    ``is_weekend``, ``date``, the holiday flags and (if asked for) the weather columns."""
    n = len(cov)
    dates = cov["date"].to_numpy().astype("datetime64[D]")
    dow = cov["dow"].to_numpy(dtype=float)
    doy = (dates - dates.astype("datetime64[Y]")).astype(int).astype(float)
    has = cov["has_published_window"].fillna(False).to_numpy(dtype=bool)
    o_m = cov["o_m"].to_numpy(dtype=float)
    c_m = cov["c_m"].to_numpy(dtype=float)
    has &= np.isfinite(o_m) & np.isfinite(c_m) & (c_m > o_m)
    day = {
        "day_open": has.astype(float),
        "win_len": np.where(has, (c_m - o_m) / 60.0 / 16.0, 0.0),
        "dow_sin": np.sin(2 * np.pi * dow / 7), "dow_cos": np.cos(2 * np.pi * dow / 7),
        "is_weekend": cov["is_weekend"].fillna(False).to_numpy(dtype=float),
        "doy_sin": np.sin(2 * np.pi * doy / 365.25), "doy_cos": np.cos(2 * np.pi * doy / 365.25),
    }
    for c in HOLIDAY_COLS:
        v = cov[c].astype("float64").fillna(0).to_numpy()
        day[c] = v / 5.0 if c == "neighbor_school_holiday_count" else v
    if "wx_ok" in futr_cols:
        ok = cov["temp_max"].notna().to_numpy()
        day["wx_temp_max"] = np.where(ok, cov["temp_max"].fillna(0).to_numpy(float) / 30.0, 0)
        day["wx_precip"] = np.where(ok, cov["precip_sum"].fillna(0).to_numpy(float) / 20.0, 0)
        day["wx_wind"] = np.where(ok, cov["wind_max"].fillna(0).to_numpy(float) / 50.0, 0)
        day["wx_ok"] = ok.astype(float)
    out = np.zeros((n, K, len(futr_cols)), dtype=np.float32)
    for j in range(K):
        hr = HOUR0 + j
        mid = hr * 60 + 30
        ov = np.clip(np.minimum(c_m, (hr + 1) * 60) - np.maximum(o_m, hr * 60), 0, 60) / 60.0
        f = dict(day)
        f["hr_pos"] = np.full(n, j / (K - 1))
        f["in_win"] = np.where(has, ov, 0.0)
        f["h_open"] = np.where(has, np.clip((mid - o_m) / 60.0, -3, 16) / 10.0, 0.0)
        f["h_close"] = np.where(has, np.clip((c_m - mid) / 60.0, -3, 16) / 10.0, 0.0)
        for fi, name in enumerate(futr_cols):
            out[:, j, fi] = np.nan_to_num(f[name])
    return out


def origin_covariates(con, origins: list[dt.date], n_days: int, origin_hour: int = 6,
                      window_days: int = 56):
    """Park-day covariates for days ``c .. c + n_days - 1`` AS KNOWN AT each origin ``c``
    — the harness's ``pw`` rule (``baselines.target_tables``): the published window if
    its schedule row was last written before the origin (06:00 park-local), else the
    window projected from the last ``window_days`` days (median local opening / closing
    minute per park and weekday type, >= 3 days, else over all days); the schedule's
    holiday / bridge flags only where the schedule was known. Columns as
    ``park_day_cov`` plus ``origin, o_m, c_m, schedule_known``."""
    olist = ",".join(f"DATE '{c}'" for c in origins)
    return con.execute(f"""
        WITH oc AS (SELECT unnest([{olist}]) AS origin),
        o AS (SELECT oc.origin, p.id AS park_id, p.timezone,
                     timezone(p.timezone, CAST(oc.origin AS TIMESTAMP) + INTERVAL {int(origin_hour)} HOUR) AS origin_utc
              FROM oc, parks p),
        d AS (SELECT o.*, CAST(o.origin + L AS DATE) AS date
              FROM o, (SELECT CAST(unnest(range(0, {int(n_days)})) AS INTEGER) AS L)),
        wp AS (SELECT oc.origin, w.park_id,
                      CASE WHEN grouping(we) = 1 THEN 2 ELSE CAST(we AS INTEGER) END AS wk,
                      median(date_diff('minute', CAST(w.date AS TIMESTAMP), w.open_local)) AS om,
                      median(date_diff('minute', CAST(w.date AS TIMESTAMP), w.close_local)) AS cm,
                      count(*) AS n
               FROM oc JOIN (SELECT *, dayofweek(date) IN (0, 6) AS we FROM windows) w
                 ON w.date >= oc.origin - {int(window_days)} AND w.date < oc.origin
               GROUP BY GROUPING SETS ((oc.origin, w.park_id, we), (oc.origin, w.park_id))),
        k AS (SELECT d.*, w.open_local, w.close_local,
                     w.updated_utc IS NOT NULL AND w.updated_utc < d.origin_utc AS known
              FROM d LEFT JOIN windows w ON w.park_id = d.park_id AND w.date = d.date)
        SELECT cov.* EXCLUDE (has_published_window, sched_is_holiday, sched_is_bridge_day,
                              open_local, close_local, open_utc, close_utc),
               k.origin, k.known AS schedule_known,
               CASE WHEN k.known THEN date_diff('minute', CAST(k.date AS TIMESTAMP), k.open_local)
                    ELSE coalesce(p1.om, p2.om) END AS o_m,
               CASE WHEN k.known THEN date_diff('minute', CAST(k.date AS TIMESTAMP), k.close_local)
                    ELSE coalesce(p1.cm, p2.cm) END AS c_m,
               (CASE WHEN k.known THEN k.open_local IS NOT NULL ELSE coalesce(p1.om, p2.om) IS NOT NULL END)
                   AS has_published_window,
               CASE WHEN k.known THEN cov.sched_is_holiday ELSE false END AS sched_is_holiday,
               CASE WHEN k.known THEN cov.sched_is_bridge_day ELSE false END AS sched_is_bridge_day
        FROM k JOIN park_day_cov cov ON cov.park_id = k.park_id AND cov.date = k.date
        LEFT JOIN wp p1 ON p1.origin = k.origin AND p1.park_id = k.park_id
             AND p1.wk = CAST(dayofweek(k.date) IN (0, 6) AS INTEGER) AND p1.n >= 3
        LEFT JOIN wp p2 ON p2.origin = k.origin AND p2.park_id = k.park_id AND p2.wk = 2""").df()


def build_panel(con, day0: dt.date, day_end: dt.date, cutoff: dt.date, weather: bool,
                parks: list[str] | None = None) -> Panel:
    """Panel over service days [day0, day_end). Truth enters only for days < ``truth_end``
    = the day the data ends; nothing here is cut at the origin — the caller's fit /
    predict windows are (see ``nf_precompute``). ``cutoff`` (the block's first origin)
    fixes the static level features."""
    x = con.execute
    pf = ""
    if parks:
        pf = "AND park_id IN (" + ",".join(f"'{p}'" for p in parks) + ")"
    n_days = (day_end - day0).days
    # series: every ride with a truth slot before the cutoff (a ride first seen inside
    # the block has nothing to learn from and nothing to condition on)
    rides = x(f"""
        SELECT r.aid, r.park_id, r.is_headliner, r.lat, r.lng, r.timezone
        FROM rides r WHERE r.aid IN (SELECT DISTINCT aid FROM truth WHERE date < DATE '{cutoff}' {pf})
        ORDER BY r.park_id, r.aid""").df()
    aids = rides["aid"].tolist()
    a_ix = {a: i for i, a in enumerate(aids)}
    park_list = sorted(set(rides["park_id"]))
    p_ix = {p: i for i, p in enumerate(park_list)}
    N, T = len(aids), n_days * K

    futr_cols = BASE_FUTR + (WEATHER_FUTR if weather else [])
    cols = ["y"] + futr_cols + ["available_mask"]
    C = len(cols)
    temporal = np.zeros((N, T, C), dtype=np.float32)

    # ---- target
    h = x(f"""
        SELECT aid, date, least(greatest(ws // 4, {HOUR0}), {HOUR0 + K - 1}) AS hr,
               avg(y) AS y
        FROM truth WHERE date >= DATE '{day0}' AND date < DATE '{day_end}' {pf}
        GROUP BY ALL""").df()
    h = h[h["aid"].isin(a_ix)]
    if len(h):
        si = h["aid"].map(a_ix).to_numpy()
        di = (h["date"].to_numpy().astype("datetime64[D]") - np.datetime64(day0, "D")).astype(int)
        ti = di * K + (h["hr"].to_numpy() - HOUR0)
        temporal[si, ti, 0] = h["y"].to_numpy(dtype=np.float32)
        temporal[si, ti, C - 1] = 1.0

    # ---- park-day covariates expanded to hours (published FINAL windows: used for the
    # past — training windows and the input part of a forecast window; the forecast part
    # is overwritten with the window as known at the origin, see ``origin_covariates``)
    cov = x(f"""
        SELECT c.*, date_diff('minute', CAST(c.date AS TIMESTAMP), c.open_local) AS o_m,
               date_diff('minute', CAST(c.date AS TIMESTAMP), c.close_local) AS c_m
        FROM park_day_cov c
        WHERE c.date >= DATE '{day0}' AND c.date < DATE '{day_end}' {pf.replace('park_id', 'c.park_id')}""").df()
    cov = cov[cov["park_id"].isin(p_ix)]
    pc = np.zeros((len(park_list), n_days, K, len(futr_cols)), dtype=np.float32)
    if len(cov):
        pi = cov["park_id"].map(p_ix).to_numpy()
        di = (cov["date"].to_numpy().astype("datetime64[D]") - np.datetime64(day0, "D")).astype(int)
        pc[pi, di] = hour_features(cov, futr_cols)
    temporal[:, :, 1:1 + len(futr_cols)] = pc.reshape(len(park_list), T, -1)[rides["park_id"].map(p_ix).to_numpy()]
    del pc

    # ---- static (levels cut at the block's first origin)
    lv = x(f"""
        SELECT aid, park_id, median(p90) AS lvl FROM ride_day
        WHERE date < DATE '{cutoff}' AND date >= DATE '{cutoff}' - 56 GROUP BY ALL""").df()
    lvl = dict(zip(lv["aid"], lv["lvl"]))
    plv = lv.groupby("park_id")["lvl"].mean().to_dict()
    regions = [region_of(tz) for tz in rides["timezone"]]
    static = np.zeros((N, len(STATIC)), dtype=np.float32)
    static[:, 0] = rides["is_headliner"].fillna(False).to_numpy(dtype=float)
    static[:, 1] = [r == "EU" for r in regions]
    static[:, 2] = [r == "NA" for r in regions]
    static[:, 3] = [r not in ("EU", "NA") for r in regions]
    static[:, 4] = rides["lat"].fillna(0).to_numpy(float) / 90.0
    static[:, 5] = rides["lng"].fillna(0).to_numpy(float) / 180.0
    static[:, 6] = [np.log1p(lvl.get(a, 0.0) or 0.0) / 5.0 for a in aids]
    static[:, 7] = [np.log1p(plv.get(p, 0.0) or 0.0) / 5.0 for p in rides["park_id"]]
    groups = ["EU" if r == "EU" else "NA" if r == "NA" else "Asia" for r in regions]
    return Panel(aids, rides["park_id"].tolist(), groups, day0, n_days, temporal, cols,
                 futr_cols, static, list(STATIC))
