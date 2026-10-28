"""The daily level from known-in-advance drivers, as a nightly production SHADOW
(PAR-1058; the model is ml-bench's ``driver_level`` plug-in, PAR-830).

level = naive × exp(ŷ): naive is the median of the last four same-weekday daily
P90s in the 56 days before the origin (at least two), ŷ a gradient-boosted model
of log(true P90 / naive) on drivers known at the origin — weekday, lead, region,
the reference itself, public / school holidays incl. neighbour regions (the
ml-service ``holiday_features`` OR semantics), bridge days, the day before / after
a holiday, and the opening hours of the target day against the reference days.
No weather (the bench only had actuals), no events, no season start.

This is a pandas port of ``ml-bench/mlbench/drivers.py`` (``park_day_features``,
``driver_rows_sql``, ``add_asof``) and of the plug-in's fit / predict
(``models/driver_level.py``, ``driver_analysis.fit_model('hgb')``), with the same
features in the same order and the same model parameters; the parity test
(``tests/test_driver_level.py``) runs both on one fixture. What differs is only
where the inputs come from (``run``):

* ride-day P90s are the daily P90 of the raw STANDBY / OPERATING / >= 5 min
  change-log rows per park-local date — the series the served TFT is trained on
  (nf-service ``fetch_daily_peak_panel``), not the bench's 15-min slot truth, so
  the shadow level sits on the served level's scale;
* past opening windows are the published ones, not windows read off truth slots;
* the training-row cap samples rows at random instead of DuckDB's ``hash()``.

Nothing here is served. The nightly job writes the latest levels to
``driver_level_forecasts`` (one row per ride and target date, replaced every
night); the forward archive copies them into ``forecast_archive_curves.
driver_day_peak`` at capture, and the scorer pairs them with the served dayPeak
as model ``driver_level`` (docs/ml/forward-archive/model-comparison.md#the-shadow-levels).
"""

from __future__ import annotations

import datetime as dt
import math
import sys

import numpy as np
import pandas as pd

TRAIN_LEADS = [1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90]
FEATURES = [
    "L", "dow", "region", "log_ref", "n_ref", "ref_age",
    "hol", "school", "hol_nb", "school_nb_cnt", "school_any", "hol_cnt", "bridge",
    "hol_prev", "hol_next", "ref_hol", "ref_school", "d_hol", "d_school", "d_school_any",
    "hours_a", "open_h_a", "close_h_a", "d_hours_a", "r_hours_a", "d_close_a", "d_open_a",
    "since_prev_open_a", "ref_hours",
]
CATEGORICAL = ["dow", "month", "region"]
WINDOW_DAYS = 56
WEEKS = 4
MIN_WEEKS = 2
CAP_DAYS = 60
MIN_TRAIN_ROWS = 500
MAX_TRAIN_ROWS = 300_000
SEED = 830
# Target days origin+1 .. origin+91: a park captured before the night's run in its
# own date (Asia, the Americas) reads lead L from an origin one day older, as L+1.
HORIZON_DAYS = 91
HISTORY_DAYS = 400


def sunday0_dow(dates: pd.Series) -> pd.Series:
    """DuckDB / Postgres ``dayofweek``: 0 = Sunday."""
    return (pd.to_datetime(dates).dt.dayofweek + 1) % 7


def region_of(timezone: str) -> str:
    head = str(timezone).split("/")[0]
    return {"Europe": "EU", "America": "NA", "Asia": "Asia", "Australia": "Asia",
            "Pacific": "Asia"}.get(head, "other")


def _and3(*xs: np.ndarray) -> np.ndarray:
    """SQL three-valued AND over 0/1/NaN arrays."""
    m = np.vstack(xs)
    return np.where((m == 0).any(axis=0), 0.0, np.where(np.isnan(m).any(axis=0), np.nan, 1.0))


def _or3(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    m = np.vstack([a, b])
    return np.where((m == 1).any(axis=0), 1.0, np.where(np.isnan(m).any(axis=0), np.nan, 0.0))


def _eq(a: pd.Series, v: float) -> np.ndarray:
    x = a.to_numpy(dtype="float64")
    return np.where(np.isnan(x), np.nan, (x == v).astype("float64"))


def park_day_features(cov: pd.DataFrame, windows: pd.DataFrame) -> pd.DataFrame:
    """``drivers.park_day_features``: one row per covariate park-day.

    ``cov``: park_id, date, dow (0 = Sunday) and the holiday feature columns, one
    row per calendar day. ``windows``: park_id, date, open_local, close_local
    (naive park-local timestamps), every day that has a window."""
    c = cov.sort_values(["park_id", "date"], kind="stable").reset_index(drop=True)
    out = pd.DataFrame({
        "park_id": c["park_id"], "date": c["date"], "dow": c["dow"],
        "hol": c["is_holiday_primary"], "school": c["is_school_holiday_primary"],
        "hol_nb": c[["is_holiday_neighbor_1", "is_holiday_neighbor_2",
                     "is_holiday_neighbor_3"]].astype("float64").max(axis=1, skipna=True),
        "school_nb_cnt": c["neighbor_school_holiday_count"],
        "school_any": c["is_school_holiday_any"], "hol_cnt": c["holiday_count_total"],
    })
    g = out.groupby("park_id", sort=False)["hol"]
    out["hol_prev"] = g.shift(1)
    out["hol_next"] = g.shift(-1)
    hol = out["hol"].astype("float64")
    out["bridge"] = _or3(
        _and3(_eq(out["dow"], 1), _eq(out["hol_next"], 1), _eq(hol, 0)),
        _and3(_eq(out["dow"], 5), _eq(out["hol_prev"], 1), _eq(hol, 0)),
    )
    w = windows.sort_values(["park_id", "date"], kind="stable").reset_index(drop=True)
    o = pd.to_datetime(w["open_local"]).dt.floor("min")
    x = pd.to_datetime(w["close_local"]).dt.floor("min")
    midnight = pd.to_datetime(w["date"])
    gap = pd.to_datetime(w["date"]).groupby(w["park_id"]).diff().dt.days
    win = pd.DataFrame({
        "park_id": w["park_id"], "date": w["date"],
        "hours": (x - o).dt.total_seconds() / 3600.0,
        "open_h": o.dt.hour + o.dt.minute / 60.0,
        "close_h": (x - midnight).dt.total_seconds() / 3600.0,
        # DuckDB least() skips NULL: the first window of a park reads CAP_DAYS
        "since_prev_open": gap.fillna(CAP_DAYS).clip(upper=CAP_DAYS),
    })
    return out.merge(win, on=["park_id", "date"], how="left")


def driver_rows(q: pd.DataFrame, rdh: pd.DataFrame, pdf: pd.DataFrame, parks: pd.DataFrame,
                with_truth: bool = True) -> pd.DataFrame:
    """``drivers.driver_rows_sql`` + ``add_asof``: one row per ``q`` row (aid,
    park_id, origin, date, L) that has a naive reference. ``rdh``: aid, park_id,
    date, p90. ``parks``: id, timezone."""
    q = q.reset_index(drop=True).assign(qi=lambda d: np.arange(len(d)))
    dates = pd.to_datetime(q["date"])
    origin = pd.to_datetime(q["origin"])
    # the latest same-weekday date before the origin; the 56-day window holds 8
    delta = (origin.dt.dayofweek - dates.dt.dayofweek) % 7
    last = origin - pd.to_timedelta(delta.where(delta > 0, 7), unit="D")
    cand = q[["qi", "aid", "park_id"]].loc[q.index.repeat(8)].reset_index(drop=True)
    k = np.tile(np.arange(8), len(q))
    cand["rdate"] = (last.loc[q.index.repeat(8)].to_numpy()
                     - pd.to_timedelta(7 * k, unit="D").to_numpy())
    cand["rdate"] = pd.to_datetime(cand["rdate"]).dt.date
    h = rdh[["aid", "date", "p90"]].rename(columns={"date": "rdate"})
    f = cand.merge(h, on=["aid", "rdate"], how="inner")
    f = f.sort_values(["qi", "rdate"], ascending=[True, False], kind="stable")
    f = f[f.groupby("qi").cumcount() < WEEKS]
    ref_cols = {"hol": "ref_hol", "school": "ref_school", "school_any": "ref_school_any",
                "hours": "ref_hours", "close_h": "ref_close", "open_h": "ref_open"}
    f = f.merge(pdf[["park_id", "date", *ref_cols]].rename(columns={"date": "rdate", **ref_cols}),
                on=["park_id", "rdate"], how="left")
    for col in ref_cols.values():
        f[col] = f[col].astype("float64")
    agg = f.groupby("qi").agg(ref=("p90", "median"), n_ref=("p90", "size"), last_ref=("rdate", "max"),
                              **{c: (c, "mean") for c in ref_cols.values()})
    agg = agg[agg["n_ref"] >= MIN_WEEKS]
    r = q.merge(agg, left_on="qi", right_index=True, how="inner")
    r["ref_age"] = (pd.to_datetime(r["date"]) - pd.to_datetime(r["last_ref"])).dt.days
    r["log_ref"] = np.log(r["ref"])
    if with_truth:
        r = r.merge(rdh[["aid", "date", "p90"]].rename(columns={"p90": "true_p90"}),
                    on=["aid", "date"], how="inner")
        r["y"] = np.log(r["true_p90"] / r["ref"])
    r = r[r["aid"].isin(rdh["aid"])]
    regions = parks.assign(region=parks["timezone"].map(region_of))[["id", "region"]]
    r = r.merge(regions.rename(columns={"id": "park_id"}), on="park_id", how="inner")
    r = r.merge(pdf.rename(columns={c: f"x_{c}" for c in pdf.columns if c not in ("park_id", "date")}),
                on=["park_id", "date"], how="left")
    out = r.assign(
        dow=r["x_dow"], hol=r["x_hol"], school=r["x_school"], hol_nb=r["x_hol_nb"],
        school_nb_cnt=r["x_school_nb_cnt"], school_any=r["x_school_any"], hol_cnt=r["x_hol_cnt"],
        bridge=r["x_bridge"], hol_prev=r["x_hol_prev"], hol_next=r["x_hol_next"],
    )
    num = lambda c: out[c].astype("float64")
    out["d_hol"] = num("hol") - out["ref_hol"]
    out["d_school"] = num("school") - out["ref_school"]
    out["d_school_any"] = num("school_any") - out["ref_school_any"]
    # add_asof: every window here is known at the origin (published, or projected
    # for a day past the operator's horizon), so the as-of column is the column
    out["hours_a"] = out["x_hours"]
    out["open_h_a"] = out["x_open_h"]
    out["close_h_a"] = out["x_close_h"]
    out["d_hours_a"] = out["x_hours"] - out["ref_hours"]
    out["r_hours_a"] = out["x_hours"] / out["ref_hours"].replace(0, np.nan)
    out["d_close_a"] = out["x_close_h"] - out["ref_close"]
    out["d_open_a"] = out["x_open_h"] - out["ref_open"]
    out["since_prev_open_a"] = out["x_since_prev_open"]
    return out.sort_values("qi", kind="stable").reset_index(drop=True)


def design(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    """``driver_analysis.design``."""
    X = df[feats].copy()
    for c in X.columns:
        if c in CATEGORICAL:
            X[c] = X[c].astype("category")
        else:
            X[c] = pd.to_numeric(X[c], errors="coerce").astype("float64")
    return X


class Fitted:
    def __init__(self, feats: list[str], model, offset: float, rows: int):
        self.feats, self.model, self.offset, self.rows = feats, model, offset, rows

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            return np.full(len(df), self.offset)
        return self.model.predict(design(df, self.feats)) + self.offset


def training_queries(rdh: pd.DataFrame, c: dt.date, seed: int = SEED) -> pd.DataFrame:
    """The plug-in's training ``q``: every past ride-day × TRAIN_LEADS whose origin
    leaves a full reference window after the first recorded day."""
    d0 = min(rdh["date"])
    first = d0 + dt.timedelta(days=WINDOW_DAYS)
    dates = pd.to_datetime(rdh["date"])
    masks = {L: ((dates - pd.Timedelta(days=L)).dt.date >= first) & (rdh["date"] < c)
             for L in TRAIN_LEADS}
    n = sum(int(m.sum()) for m in masks.values())
    keep = min(1.0, 2 * MAX_TRAIN_ROWS / n) if n else 1.0
    parts = []
    for L, m in masks.items():
        p = rdh.loc[m, ["aid", "park_id", "date"]]
        # shortcut: a seeded sample where the plug-in keeps DuckDB hash() % 1000;
        # same purpose (memory), not the same rows. Never taken below 600 k rows.
        if keep < 1.0:
            p = p.sample(frac=keep, random_state=seed + L)
        parts.append(p.assign(L=L, origin=(pd.to_datetime(p["date"]) - pd.Timedelta(days=L)).dt.date))
    return pd.concat(parts, ignore_index=True)[["aid", "park_id", "origin", "date", "L"]]


def fit(rdh: pd.DataFrame, pdf: pd.DataFrame, parks: pd.DataFrame, c: dt.date,
        seed: int = SEED) -> Fitted | None:
    """``DriverLevel._fit`` + ``fit_model('hgb')``."""
    from sklearn.ensemble import HistGradientBoostingRegressor

    if rdh.empty:
        return None
    tr = driver_rows(training_queries(rdh, c, seed), rdh, pdf, parks, with_truth=True)
    if len(tr) < MIN_TRAIN_ROWS:
        return None
    if len(tr) > MAX_TRAIN_ROWS:
        tr = tr.sample(MAX_TRAIN_ROWS, random_state=seed)
    feats = [f for f in FEATURES if tr[f].nunique(dropna=True) > 1]
    y = tr["y"].to_numpy()
    if not feats:
        return Fitted([], None, float(np.median(y)), len(tr))
    m = HistGradientBoostingRegressor(
        loss="absolute_error", max_iter=300, learning_rate=0.08, max_leaf_nodes=31,
        min_samples_leaf=200, l2_regularization=1.0, categorical_features="from_dtype",
        random_state=seed)
    m.fit(design(tr, feats), y)
    return Fitted(feats, m, 0.0, len(tr))


def predict_levels(fitted: Fitted, q: pd.DataFrame, rdh: pd.DataFrame, pdf: pd.DataFrame,
                   parks: pd.DataFrame) -> pd.DataFrame:
    """``DriverLevel._yhat`` + ``predict_daily``: aid, park_id, date, L, level."""
    rows = driver_rows(q.drop_duplicates(["aid", "date"]), rdh, pdf, parks, with_truth=False)
    if rows.empty:
        return pd.DataFrame(columns=["aid", "park_id", "date", "L", "level"])
    lead = rows["L"].copy()
    rows["L"] = rows["L"].clip(upper=max(TRAIN_LEADS))  # no training rows beyond 90 d
    yhat = fitted.predict(rows)
    return pd.DataFrame({"aid": rows["aid"], "park_id": rows["park_id"], "date": rows["date"],
                         "L": lead, "level": rows["ref"].to_numpy() * np.exp(yhat)})


def project_windows(windows: pd.DataFrame, origins: dict, horizon: int = HORIZON_DAYS) -> pd.DataFrame:
    """Every target day without a published window gets the bench runner's
    projection (``baselines.target_tables``): the median local opening / closing
    minute of the park's windows in the 56 days before the origin, per weekday /
    weekend (>= 3 days) else over all days, rounded to the quarter hour."""
    w = windows.copy()
    w["d"] = pd.to_datetime(w["date"])
    w["om"] = (pd.to_datetime(w["open_local"]) - w["d"]).dt.total_seconds() / 60.0
    w["xm"] = (pd.to_datetime(w["close_local"]) - w["d"]).dt.total_seconds() / 60.0
    w["we"] = sunday0_dow(w["date"]).isin([0, 6])
    q15 = lambda v: math.floor(v / 15 + 0.5) * 15
    out = []
    for pid, o in origins.items():
        p = w[(w["park_id"] == pid) & (w["date"] >= o - dt.timedelta(days=WINDOW_DAYS)) & (w["date"] < o)]
        if p.empty:
            continue
        have = set(w.loc[w["park_id"] == pid, "date"])
        for i in range(horizon + 1):
            day = o + dt.timedelta(days=i)
            if day in have:
                continue
            we = (day.weekday() + 1) % 7 in (0, 6)
            s = p[p["we"] == we]
            s = s if len(s) >= 3 else p
            d = pd.Timestamp(day)
            out.append({"park_id": pid, "date": day,
                        "open_local": d + pd.Timedelta(minutes=q15(s["om"].median())),
                        "close_local": d + pd.Timedelta(minutes=q15(s["xm"].median()))})
    cols = ["park_id", "date", "open_local", "close_local"]
    return pd.concat([windows[cols], pd.DataFrame(out, columns=cols)] if out else [windows[cols]],
                     ignore_index=True)


def forecast(rdh: pd.DataFrame, windows: pd.DataFrame, cov: pd.DataFrame, parks: pd.DataFrame,
             origins: dict, horizon: int = HORIZON_DAYS) -> pd.DataFrame:
    """Fit on everything before the latest origin and predict every ride of each
    park for origin+1 .. origin+horizon. ``origins``: park_id -> park-local date."""
    if rdh.empty or not origins:
        return pd.DataFrame(columns=["aid", "park_id", "date", "L", "level"])
    pdf = park_day_features(cov, project_windows(windows, origins, horizon))
    fitted = fit(rdh, pdf, parks, max(origins.values()))
    if fitted is None:
        return pd.DataFrame(columns=["aid", "park_id", "date", "L", "level"])
    rides = rdh[["aid", "park_id"]].drop_duplicates("aid")
    rides = rides[rides["park_id"].isin(origins)]
    q = pd.concat([rides.assign(L=L) for L in range(1, horizon + 1)], ignore_index=True)
    q["origin"] = q["park_id"].map(origins)
    q["date"] = [o + dt.timedelta(days=int(L)) for o, L in zip(q["origin"], q["L"])]
    return predict_levels(fitted, q, rdh, pdf, parks)


# ------------------------------------------------------------------ database job

RIDE_DAYS_SQL = """
    SELECT qd."attractionId"::text AS aid, a."parkId"::text AS park_id,
           DATE(qd.timestamp AT TIME ZONE p.timezone) AS date,
           PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") AS p90
    FROM queue_data qd
    JOIN attractions a ON a.id = qd."attractionId"
    JOIN parks p ON p.id = a."parkId"
    WHERE qd.timestamp >= NOW() - make_interval(days => :days)
      AND DATE(qd.timestamp AT TIME ZONE p.timezone) < DATE(NOW() AT TIME ZONE p.timezone)
      AND NOT COALESCE(qd.is_heartbeat, false)
      AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY' AND qd."waitTime" >= 5
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
"""

WINDOWS_SQL = """
    SELECT s."parkId"::text AS park_id, s.date,
           min(s."openingTime" AT TIME ZONE p.timezone) AS open_local,
           max(s."closingTime" AT TIME ZONE p.timezone) AS close_local
    FROM schedule_entries s JOIN parks p ON p.id = s."parkId"
    WHERE s."attractionId" IS NULL AND s."scheduleType" = 'OPERATING'
      AND s."openingTime" IS NOT NULL AND s."closingTime" IS NOT NULL
      AND s."closingTime" > s."openingTime"
      AND s."closingTime" - s."openingTime" <= interval '24 hours'
      AND s.date BETWEEN :start AND :end
    GROUP BY 1, 2
"""


def build_cov(parks_meta: pd.DataFrame, holidays: pd.DataFrame, start: dt.date, end: dt.date) -> pd.DataFrame:
    """Holiday covariates per park and calendar day, through ml-service's own
    ``holiday_features`` (the bench's source too). A ``SchoolCoverage`` is used
    when this ml-service has one (PAR-1049): school flags past a location's last
    stored school row are then unknown (NaN), not 0."""
    import holiday_features as hf

    index = hf.HolidayIndex(holidays)
    coverage = hf.SchoolCoverage(holidays) if hasattr(hf, "SchoolCoverage") else None
    rows = []
    days = [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]
    for p in parks_meta.itertuples(index=False):
        influences = hf.parse_influencing_regions(p.influencingRegions)
        for d in days:
            kw = {"coverage": coverage} if coverage is not None else {}
            f = hf.park_day_holiday_features(p.country, p.region_code, influences, d, index, **kw)
            f["park_id"], f["date"] = str(p.park_id), d
            rows.append(f)
    cov = pd.DataFrame(rows)
    cov["dow"] = sunday0_dow(cov["date"])
    return cov


def start_single_flight(cmd: list[str], lock_path: str, cwd: str | None = None):
    """Start ``cmd`` detached unless a previous start still runs. Returns the Popen,
    or None when the lock is held. The child inherits the ``flock``ed descriptor and
    holds it until it exits, so the lock spans every uvicorn worker and frees itself
    when the run ends or is OOM-killed — no stale pidfile to clear."""
    import fcntl
    import os
    import subprocess

    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return None
        return subprocess.Popen(cmd, cwd=cwd, pass_fds=(fd,))
    finally:
        os.close(fd)  # the child's copy keeps the lock


def run(now: dt.datetime | None = None) -> int:
    """Read, fit, predict and replace ``driver_level_forecasts``. Returns rows written."""
    from zoneinfo import ZoneInfo

    from sqlalchemy import text
    from threadpoolctl import threadpool_limits

    import db
    import holiday_features as hf

    now = now or dt.datetime.now(dt.timezone.utc)
    with db.engine.connect() as conn, conn.begin():
        conn.execute(text("SET LOCAL statement_timeout = 600000"))
        conn.execute(text("SET LOCAL lock_timeout = 10000"))
        rdh = pd.read_sql(text(RIDE_DAYS_SQL), conn, params={"days": HISTORY_DAYS})
    if rdh.empty:
        print("driver_level: no ride-day history", flush=True)
        return 0
    rdh["date"] = pd.to_datetime(rdh["date"]).dt.date
    rdh["p90"] = rdh["p90"].astype("float64")
    meta = db.fetch_parks_metadata(use_cache=False)
    meta["park_id"] = meta["park_id"].astype(str)
    origins = {pid: now.astimezone(ZoneInfo(tz)).date()
               for pid, tz in zip(meta["park_id"], meta["timezone"]) if tz}
    start = min(rdh["date"]) - dt.timedelta(days=1)
    end = max(origins.values()) + dt.timedelta(days=HORIZON_DAYS + 1)
    with db.engine.connect() as conn, conn.begin():
        conn.execute(text("SET LOCAL statement_timeout = 120000"))
        windows = pd.read_sql(text(WINDOWS_SQL), conn, params={"start": start, "end": end})
    windows["date"] = pd.to_datetime(windows["date"]).dt.date
    countries = set(meta["country"].dropna())
    for raw in meta["influencingRegions"]:
        countries |= {r.get("countryCode") for r in hf.parse_influencing_regions(raw) if isinstance(r, dict)}
    holidays = db.fetch_holidays(sorted(c for c in countries if c),
                                 dt.datetime.combine(start - dt.timedelta(days=7), dt.time()),
                                 dt.datetime.combine(end + dt.timedelta(days=7), dt.time()))
    cov = build_cov(meta, holidays, start, end)
    parks = meta.rename(columns={"park_id": "id"})[["id", "timezone"]]
    with threadpool_limits(2):  # the bench's thread budget; the live service shares the host
        levels = forecast(rdh, windows, cov, parks, origins)
    out = pd.DataFrame({
        "attraction_id": levels["aid"], "target_date": levels["date"], "park_id": levels["park_id"],
        "origin_date": [d - dt.timedelta(days=int(L)) for d, L in zip(levels["date"], levels["L"])],
        "lead_days": levels["L"].astype(int), "level": levels["level"].astype("float64"),
    })
    with db.engine.connect() as conn, conn.begin():
        conn.execute(text("SET LOCAL lock_timeout = 10000"))
        conn.execute(text("DELETE FROM driver_level_forecasts"))
        out.to_sql("driver_level_forecasts", conn, if_exists="append", index=False,
                   method="multi", chunksize=5000)
    print(f"driver_level: {len(out)} ride-day levels for {out['park_id'].nunique()} parks", flush=True)
    return len(out)


if __name__ == "__main__":
    sys.exit(0 if run() >= 0 else 1)
