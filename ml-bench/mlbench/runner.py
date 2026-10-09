"""Rolling-origin backtest (BENCH-SPEC "Protocol").

One origin per park-local day at 06:00 (UC2 d0/d1, UC3, UC4) plus intraday
origins every 2 h (UC1, UC2 d0). For every origin the runner

1. cuts the truth at the origin (``baselines.window_tables``),
2. builds every target slot of the lead grid with all baseline columns
   (``baselines.target_tables``) and asks each plug-in model for the same slots,
3. scores, and writes per-origin aggregates under ``<run>/parts/<table>/``:

   ``slot``      per (lead, park, target day, ex-ante busy): n / Σ|e| / Σe per model,
                 and the PAIRED sums against every reference candidate
   ``rideday``   per (lead, park, day, model, busy): decision metrics built on the
                 ride-day (best time D3, rope drop D5, dayPeak D4/UC3, Spearman UC2)
   ``pairs``     per (lead, park, day, model): headliner dayPeak pairwise ordering (D4)
                 and the across-ride dayPeak Spearman (UC3)
   ``optim``     per (lead, park, day, model): planner-optimiser regret (D4)
   ``intraday``  per (lead bucket, park, day, busy, headliner): UC1 / UC2 d0 / D2
   ``nextbest``  per (park, day, model, peak-lead bucket): D1 suggestion counts
   ``levels``    raw headliner daily levels per (origin, ride, lead) — UC4, D6, D7, D9
   ``openness``  per (lead, park, day, model): forecast present × ride operated (D9)

Nothing is pooled across leads here; the report bootstraps over park-days.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from . import baselines as B
from .config import BenchConfig
from .decisions import execute, optimise, transfer_ceiling
from .models.base import HistoryView, Model, Origin

PROFILE = os.environ.get("MLBENCH_PROFILE") == "1"


# --------------------------------------------------------------------------- setup

def load_tables(con, export: Path, materialize: bool = False) -> None:
    """Attach the export's ``bench.duckdb`` read-only (shared by all shards; blocks are
    paged in on demand) or, with ``materialize``, copy the Parquet tables into memory."""
    x = con.execute
    names = ("parks", "attractions", "windows", "slots", "ride_day", "hour_stats", "tft", "cbd",
             "park_day_cov", "truth")
    db = export / "bench.duckdb"
    if db.exists() and not materialize:
        x(f"ATTACH '{db}' AS b (READ_ONLY)")
        for t in names:
            x(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM b.{t}")
    else:
        pq = export / "parquet"
        for t in names[:-1]:
            x(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM read_parquet('{pq}/{t}.parquet')")
        x(f"""CREATE OR REPLACE TABLE truth AS
            SELECT t.*, CAST(dayofweek(t.date) IN (0, 6) AS INTEGER) AS we, dayofweek(t.date) AS dow
            FROM read_parquet('{pq}/truth.parquet') t""")
    x("""CREATE OR REPLACE TABLE rides AS
        SELECT a.id AS aid, a.park_id, coalesce(a.is_headliner, false) AS is_headliner,
               a.latitude AS lat, a.longitude AS lng, a.land, p.timezone
        FROM attractions a JOIN parks p ON p.id = a.park_id""")


def git_sha() -> str:
    env = os.environ.get("MLBENCH_GIT_SHA")
    if env and env != "unknown":
        return env
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True,
                                       cwd=Path(__file__).parent, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return "unknown"


def code_hash() -> str:
    """sha256 over the mlbench sources (path + bytes), so a run can be tied to code
    even when the SHA is a working tree."""
    import hashlib

    h = hashlib.sha256()
    root = Path(__file__).resolve().parent
    for p in sorted(root.rglob("*.py")):
        h.update(str(p.relative_to(root)).encode())
        h.update(p.read_bytes())
    return h.hexdigest()


def load_model(spec: str) -> Model:
    from .models import REGISTRY

    if spec in REGISTRY:
        return REGISTRY[spec]()
    mod, _, cls = spec.partition(":")
    return getattr(importlib.import_module(mod), cls)()


# --------------------------------------------------------------------------- scoring SQL

def slot_agg_sql(models: list[str], refs: list[str], quantile_models: list[str],
                 table: str, keys: list[str]) -> str:
    cols = ["count(y) AS n_truth"]
    for m in models:
        cols += [f"count({m}) FILTER (WHERE y IS NOT NULL) AS n__{m}",
                 f"sum(abs({m} - y)) AS sae__{m}", f"sum({m} - y) AS se__{m}"]
        for r in refs:
            if r == m:
                continue
            both = f"y IS NOT NULL AND {m} IS NOT NULL AND {r} IS NOT NULL"
            cols += [f"count(*) FILTER (WHERE {both}) AS pn__{m}__{r}",
                     f"sum(abs({m} - y)) FILTER (WHERE {both}) AS psm__{m}__{r}",
                     f"sum(abs({r} - y)) FILTER (WHERE {both}) AS psr__{m}__{r}"]
    for m in quantile_models:
        q80, q95 = ("wt_q80", "wt_q95") if m == "wt_med" else (f"{m}__q80", f"{m}__q95")
        cols += [f"count({q80}) FILTER (WHERE y IS NOT NULL) AS qn__{m}",
                 f"sum(CAST(y <= {q80} AS INTEGER)) AS q80__{m}",
                 f"sum(CAST(y <= {q95} AS INTEGER)) AS q95__{m}"]
    k = ", ".join(keys)
    return f"SELECT {k}, {', '.join(cols)} FROM {table} WHERE y IS NOT NULL GROUP BY {k}"


def rideday_sql(models: list[str], cfg: BenchConfig) -> str:
    """Per (lead, ride-day, model) decision quantities.

    A ride-day only counts for a model when the model covers EVERY truth slot of
    it (``full``): then all models are scored on the same slots, so ride-day
    metrics can be paired ride-day by ride-day (``rdpair``)."""
    cols = ", ".join(models)
    wp, ws_ = cfg.rope_worth_peak, cfg.rope_worth_savings
    return f"""
    WITH base AS (
      SELECT L, aid, park_id, date, ws, ko_t, y, busy, sk, {cols},
             count(*) OVER (PARTITION BY L, aid, date) AS n_truth
      FROM tg WHERE y IS NOT NULL),
    long AS (
      SELECT L, aid, park_id, date, ws, ko_t, y, busy, sk, n_truth, model, pred
      FROM base UNPIVOT (pred FOR model IN ({cols}))),
    r AS (
      SELECT *,
        rank() OVER g_y + (count(*) OVER (PARTITION BY L, aid, date, model, y) - 1) / 2.0 AS ry,
        rank() OVER g_p + (count(*) OVER (PARTITION BY L, aid, date, model, pred) - 1) / 2.0 AS rp,
        row_number() OVER (PARTITION BY L, aid, date, model ORDER BY pred, ws) AS pbest,
        min(y) OVER (PARTITION BY L, aid, date, model) AS ymin,
        first_value(ws) OVER (PARTITION BY L, aid, date, model ORDER BY pred, ws) AS best_ws
      FROM long
      WINDOW g_y AS (PARTITION BY L, aid, date, model ORDER BY y),
             g_p AS (PARTITION BY L, aid, date, model ORDER BY pred)),
    g AS (
      SELECT L, aid, park_id, date, model, any_value(busy) busy, any_value(sk) sk, count(*) n,
        any_value(n_truth) n_truth,
        quantile_cont(y, 0.9) true_p90, quantile_cont(pred, 0.9) pred_p90,
        max(y) ymax, min(y) ymin_, max(pred) pmax,
        CASE WHEN isfinite(corr(ry, rp)) THEN corr(ry, rp) END sp,
        bool_or(pbest <= 2 AND y = ymin) hit2,
        bool_or(y = ymin AND abs(ws - best_ws) <= 2) hit30,
        max(y) FILTER (WHERE pbest = 1) - min(y) regret,
        arg_min(pred, ko_t) p_open, arg_min(y, ko_t) y_open
      FROM r GROUP BY L, aid, park_id, date, model)
    SELECT *, n = n_truth AS fullcov, n >= {cfg.min_ride_day_slots} AS ok,
           n >= {cfg.min_ride_day_slots} AND ymax > ymin_ AS ok_rank,
           pmax >= {wp} AND pmax - p_open >= {ws_} AS worth_p,
           ymax >= {wp} AND ymax - y_open >= {ws_} AS worth_t
    FROM g"""


def rideday_pair_sql(refs: list[str], cfg: BenchConfig) -> str:
    """Ride-day decision metrics, PAIRED: every row compares model ``model`` with
    ``ref`` on the ride-days both cover in full (``ref = model`` gives the model's own
    figure on its own full-coverage ride-days)."""
    wp, ws_ = cfg.rope_worth_peak, cfg.rope_worth_savings
    ref_list = ", ".join(f"'{r}'" for r in refs)
    return f"""
    WITH f0 AS (SELECT * FROM rd WHERE fullcov AND ok),
    hist AS (  -- the production rope-drop rule: one verdict per ride from the window medians
      SELECT f.L, f.aid, f.park_id, f.date, 'prod_ropedrop_hist' AS model, f.busy, f.sk, f.ok_rank,
             NULL::DOUBLE AS true_p90, NULL::DOUBLE AS pred_p90, NULL::DOUBLE AS sp, NULL::BOOLEAN AS hit2,
             NULL::BOOLEAN AS hit30, NULL::DOUBLE AS regret,
             h.busy_peak >= {wp} AND h.busy_peak - h.open_wait >= {ws_} AS worth_p, f.worth_t
      FROM f0 f JOIN ropehist h USING (aid) WHERE f.model = 'wt_med'),
    f AS (SELECT L, aid, park_id, date, model, busy, sk, ok_rank, true_p90, pred_p90, sp, hit2, hit30,
                 regret, worth_p, worth_t FROM f0 UNION ALL SELECT * FROM hist)
    SELECT a.L, a.park_id, a.date, a.model, b.model AS ref, a.busy, a.sk,
      count(*) FILTER (WHERE a.pred_p90 IS NOT NULL AND b.pred_p90 IS NOT NULL) AS dp_n,
      sum(abs(a.pred_p90 - a.true_p90)) FILTER (WHERE b.pred_p90 IS NOT NULL) AS dp_m,
      sum(abs(b.pred_p90 - b.true_p90)) FILTER (WHERE a.pred_p90 IS NOT NULL) AS dp_r,
      count(*) FILTER (WHERE a.ok_rank AND a.hit2 IS NOT NULL AND b.hit2 IS NOT NULL) AS bt_n,
      sum(CAST(a.hit2 AS INTEGER)) FILTER (WHERE a.ok_rank AND b.hit2 IS NOT NULL) AS hit2_m,
      sum(CAST(b.hit2 AS INTEGER)) FILTER (WHERE a.ok_rank AND a.hit2 IS NOT NULL) AS hit2_r,
      sum(CAST(a.hit30 AS INTEGER)) FILTER (WHERE a.ok_rank AND b.hit30 IS NOT NULL) AS hit30_m,
      sum(CAST(b.hit30 AS INTEGER)) FILTER (WHERE a.ok_rank AND a.hit30 IS NOT NULL) AS hit30_r,
      sum(a.regret) FILTER (WHERE a.ok_rank AND b.regret IS NOT NULL) AS regret_m,
      sum(b.regret) FILTER (WHERE a.ok_rank AND a.regret IS NOT NULL) AS regret_r,
      count(*) FILTER (WHERE a.sp IS NOT NULL AND b.sp IS NOT NULL) AS sp_n,
      sum(a.sp) FILTER (WHERE b.sp IS NOT NULL) AS sp_m,
      sum(b.sp) FILTER (WHERE a.sp IS NOT NULL) AS sp_r,
      count(*) FILTER (WHERE a.worth_p IS NOT NULL AND b.worth_p IS NOT NULL) AS w_n,
      sum(CAST(a.worth_p = a.worth_t AS INTEGER)) FILTER (WHERE b.worth_p IS NOT NULL) AS w_m,
      sum(CAST(b.worth_p = b.worth_t AS INTEGER)) FILTER (WHERE a.worth_p IS NOT NULL) AS w_r,
      count(*) FILTER (WHERE a.worth_p AND a.worth_t) AS w_tp,
      count(*) FILTER (WHERE a.worth_p AND NOT a.worth_t) AS w_fp,
      count(*) FILTER (WHERE NOT a.worth_p AND a.worth_t) AS w_fn,
      count(*) FILTER (WHERE NOT a.worth_p AND NOT a.worth_t) AS w_tn
    FROM f a JOIN f b ON b.L = a.L AND b.aid = a.aid AND b.date = a.date
    WHERE b.model IN ({ref_list}) OR b.model = a.model
    GROUP BY ALL"""


def pairs_sql(refs: list[str], cfg: BenchConfig) -> str:
    """D4 headliner dayPeak ordering and UC3 across-ride dayPeak Spearman, paired on
    the same ride pairs / the same set of rides."""
    ref_list = ", ".join(f"'{r}'" for r in refs)
    return f"""
    WITH f AS (SELECT r.* FROM rd r WHERE r.fullcov AND r.ok),
    h AS (SELECT f.* FROM f JOIN rides a USING (aid) WHERE a.is_headliner),
    pm AS (SELECT a.L, a.park_id, a.date, a.model, a.aid AS aa, b.aid AS ab,
                  sign(a.pred_p90 - b.pred_p90) = sign(a.true_p90 - b.true_p90) AS ok
           FROM h a JOIN h b ON a.L = b.L AND a.park_id = b.park_id AND a.date = b.date
                             AND a.model = b.model AND a.aid < b.aid
           WHERE abs(a.true_p90 - b.true_p90) >= 1),
    pw AS (SELECT m.L, m.park_id, m.date, m.model, r.model AS ref, count(*) AS pw_n,
                  sum(CAST(m.ok AS INTEGER)) AS pw_m, sum(CAST(r.ok AS INTEGER)) AS pw_r
           FROM pm m JOIN pm r ON r.L = m.L AND r.date = m.date AND r.aa = m.aa AND r.ab = m.ab
           WHERE r.model IN ({ref_list}) OR r.model = m.model GROUP BY ALL),
    j AS (SELECT a.L, a.park_id, a.date, a.model, b.model AS ref, a.aid, a.true_p90 t,
                 a.pred_p90 pa, b.pred_p90 pb
          FROM f a JOIN f b ON b.L = a.L AND b.aid = a.aid AND b.date = a.date
          WHERE b.model IN ({ref_list}) OR b.model = a.model),
    jr AS (SELECT *, rank() OVER w_t rt, rank() OVER w_a ra, rank() OVER w_b rb FROM j
           WINDOW w_t AS (PARTITION BY L, park_id, date, model, ref ORDER BY t),
                  w_a AS (PARTITION BY L, park_id, date, model, ref ORDER BY pa),
                  w_b AS (PARTITION BY L, park_id, date, model, ref ORDER BY pb)),
    sp AS (SELECT L, park_id, date, model, ref, corr(rt, ra) sa, corr(rt, rb) sb
           FROM jr GROUP BY ALL HAVING count(*) >= 5),
    sp2 AS (SELECT * FROM sp WHERE isfinite(sa) AND isfinite(sb))
    SELECT coalesce(pw.L, sp2.L) AS L, coalesce(pw.park_id, sp2.park_id) AS park_id,
           coalesce(pw.date, sp2.date) AS date, coalesce(pw.model, sp2.model) AS model,
           coalesce(pw.ref, sp2.ref) AS ref, pw.pw_n, pw.pw_m, pw.pw_r,
           CASE WHEN sp2.sa IS NOT NULL THEN 1 END AS dsp_n, sp2.sa AS dsp_m, sp2.sb AS dsp_r
    FROM pw FULL OUTER JOIN sp2 USING (L, park_id, date, model, ref)"""


# D8: production's stated error (src/ml/services/forecast-accuracy.service.ts) is the
# global MAE of TFT's predicted_peak against the day's max hourly P90, by predicted
# band (quiet < 30 <= mid < 60 <= busy) x lead bucket (d1/d3/d7/d14/d30/d60), over
# the 45 days before today, cells with >= 500 comparisons. "stated" rebuilds that
# table as of the origin; "realised" scores the forecasts served at the origin
# (the fresh-TFT as-of read, tft_d) against the same truth.
def _lead_bucket(expr: str) -> str:
    return (f"CASE WHEN {expr} <= 1 THEN 'd1' WHEN {expr} <= 3 THEN 'd3' WHEN {expr} <= 7 THEN 'd7' "
            f"WHEN {expr} <= 14 THEN 'd14' WHEN {expr} <= 30 THEN 'd30' ELSE 'd60' END")


D8_TFT_SQL = """
WITH st AS (
  SELECT CASE WHEN f.peak >= 60 THEN 'busy' WHEN f.peak >= 30 THEN 'mid' ELSE 'quiet' END AS band,
         {bucket_f} AS bucket,
         abs(f.peak - r.peak_h90) AS err
  FROM tft f JOIN ride_day r ON r.aid = f.aid AND r.date = f.target_date
  WHERE f.target_date >= DATE '{c}' - 45 AND f.target_date < DATE '{c}'
    AND f.target_date > f.forecast_date AND f.target_date - f.forecast_date <= 60 AND r.peak_h90 > 0),
stated AS (SELECT 'stated' AS kind, band, bucket, count(*) AS n, sum(err) AS sae FROM st
           GROUP BY ALL HAVING count(*) >= 500),
rl AS (
  SELECT CASE WHEN t.peak >= 60 THEN 'busy' WHEN t.peak >= 30 THEN 'mid' ELSE 'quiet' END AS band,
         {bucket_t} AS bucket,
         abs(t.peak - r.peak_h90) AS err
  FROM tft_d t JOIN ride_day r ON r.aid = t.aid AND r.date = t.target_date
  WHERE t.target_date > DATE '{c}' AND t.target_date - DATE '{c}' <= 60 AND r.peak_h90 > 0),
realised AS (SELECT 'realised' AS kind, band, bucket, count(*) AS n, sum(err) AS sae FROM rl GROUP BY ALL)
SELECT DATE '{c}' AS origin, * FROM stated UNION ALL SELECT DATE '{c}', * FROM realised
"""


# --------------------------------------------------------------------------- runner

class Runner:
    def __init__(self, export: Path, out: Path, cfg: BenchConfig, models: list[Model],
                 shard: tuple[int, int] = (0, 1), origins: tuple[str, str] | None = None,
                 materialize: bool = False):
        from .build import connect

        self.cfg = cfg
        self.export = export
        self.out = out
        self.parts = out / "parts"
        self.models = models
        self.shard = shard
        work = out / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.con = connect(cfg.memory_limit, cfg.threads, temp_dir=str(work / f"tmp{shard[0]}"))
        load_tables(self.con, export, materialize)
        d0, d1 = self.con.execute("SELECT min(date), max(date) FROM truth").fetchone()
        # The last export day is UTC; a park-local day west of UTC is only complete one
        # day earlier, so the last scored day is queue_to - 1 (same for the first day).
        mf = export / "manifest.json"
        if mf.exists():
            m = json.loads(mf.read_text())
            if m.get("queue_to"):
                d1 = min(d1, dt.date.fromisoformat(m["queue_to"]) - dt.timedelta(days=1))
            if m.get("queue_from"):
                d0 = max(d0, dt.date.fromisoformat(m["queue_from"]) + dt.timedelta(days=1))
        self.first_date, self.last_date = d0, d1
        first_origin = d0 + dt.timedelta(days=cfg.window_days)
        if origins:
            o0, o1 = (dt.date.fromisoformat(v) for v in origins)
            first_origin, last_origin = max(first_origin, o0), min(d1, o1)
        else:
            last_origin = d1
        all_origins = [first_origin + dt.timedelta(days=i)
                       for i in range((last_origin - first_origin).days + 1)]
        self.origins = all_origins[shard[0]::shard[1]]
        self._snap_month = None
        self._last_fit: dict[str, dt.date] = {}

    # ------------------------------------------------------------------ helpers
    def write(self, table: str, sql: str, c: dt.date, suffix: str = "") -> None:
        d = self.parts / table
        d.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"COPY ({sql}) TO '{d}/{c.isoformat()}{suffix}.parquet' (FORMAT parquet)")

    def slot_leads(self, c: dt.date) -> list[int]:
        return [L for L in self.cfg.slot_leads if c + dt.timedelta(days=L) <= self.last_date]

    def daily_leads(self, c: dt.date) -> list[int]:
        return [L for L in self.cfg.daily_leads if c + dt.timedelta(days=L) <= self.last_date]

    # ------------------------------------------------------------------ plug-ins
    def grid_sql(self, c: dt.date, leads: list[int], origin_table: str = "o",
                 max_lead: int = 365) -> str:
        """Every 15-min slot of each target day's window AS KNOWN AT THE ORIGIN (``pw``)."""
        lead_list = ",".join(str(L) for L in leads if L <= max_lead) or "NULL"
        return f"""
        WITH days AS (SELECT pw.park_id, pw.date, pw.open_p, pw.close_p FROM pw
                      WHERE pw.open_p IS NOT NULL AND pw.close_p > pw.open_p
                        AND pw.date IN (SELECT DATE '{c}' + unnest([{lead_list}]))),
        g AS (SELECT r.aid AS attraction_id, d.park_id, d.date, d.open_p, d.close_p,
                     unnest(range(d.open_p, d.close_p, INTERVAL 15 MINUTE)) AS slot_start_utc
              FROM days d JOIN rides r ON r.park_id = d.park_id WHERE r.aid IN (SELECT aid FROM rs))
        SELECT g.attraction_id, g.park_id, g.date, g.slot_start_utc,
               timezone(o.timezone, g.slot_start_utc) AS slot_local,
               CAST(date_diff('minute', CAST(g.date AS TIMESTAMP), timezone(o.timezone, g.slot_start_utc)) // 15 AS INTEGER) AS ws,
               CAST(date_diff('minute', g.open_p, g.slot_start_utc) // 15 AS INTEGER) AS ko,
               CAST((date_diff('minute', g.slot_start_utc, g.close_p) - 1) // 15 AS INTEGER) AS kc,
               CAST(g.date - DATE '{c}' AS INTEGER) AS lead_days
        FROM g JOIN {origin_table} o ON o.park_id = g.park_id
        WHERE g.slot_start_utc >= o.origin_utc
        ORDER BY g.attraction_id, g.slot_start_utc"""

    def history_view(self, c: dt.date, origin_table: str) -> HistoryView:
        con = self.con

        def fetch(days: int) -> pd.DataFrame:
            return con.execute(f"""
                SELECT t.aid AS attraction_id, t.park_id, t.date, t.slot_utc AS slot_start_utc,
                       t.slot_local, t.ws, t.ko, t.kc, t.y
                FROM truth t JOIN {origin_table} o ON o.park_id = t.park_id
                WHERE t.date >= DATE '{c}' - {int(days)} AND t.date <= DATE '{c}'
                  AND t.slot_utc + INTERVAL 15 MINUTE <= o.origin_utc
                ORDER BY t.aid, t.slot_utc""").df()

        return HistoryView(fetch)

    def covariates(self, c: dt.date, max_lead: int, oracle_weather: bool) -> pd.DataFrame:
        """Known-future covariates: holidays and weekday as built; the window as known at
        the origin; schedule-derived flags only where the schedule was known; weather
        only for models that opt in to the oracle."""
        weather = ("cov.temp_max, cov.temp_min, cov.precip_sum, cov.wind_max, cov.weather_code, "
                   "cov.weather_source" if oracle_weather else "NULL AS weather_source")
        return self.con.execute(f"""
            SELECT cov.* EXCLUDE (open_local, close_local, open_utc, close_utc, has_published_window,
                                  sched_is_holiday, sched_is_bridge_day, temp_max, temp_min, precip_sum,
                                  wind_max, weather_code, weather_source),
                   pw.sched_known AS schedule_known_at_origin, pw.open_p AS open_utc, pw.close_p AS close_utc,
                   timezone(o.timezone, pw.open_p) AS open_local, timezone(o.timezone, pw.close_p) AS close_local,
                   CASE WHEN pw.sched_known THEN cov.sched_is_holiday END AS sched_is_holiday,
                   CASE WHEN pw.sched_known THEN cov.sched_is_bridge_day END AS sched_is_bridge_day,
                   {weather}
            FROM park_day_cov cov JOIN o ON o.park_id = cov.park_id
            LEFT JOIN pw ON pw.park_id = cov.park_id AND pw.date = cov.date
            WHERE cov.date >= DATE '{c}' AND cov.date <= DATE '{c}' + {int(max_lead)}
            ORDER BY cov.park_id, cov.date""").df()

    def maybe_fit(self, m: Model, c: dt.date) -> None:
        if type(m).fit is Model.fit:
            return                       # nothing to train: do not pull the panel at all
        last = self._last_fit.get(m.name)
        if last is not None and (m.refit_every_days is None or (c - last).days < m.refit_every_days):
            return
        cutoff = self.con.execute("SELECT min(origin_utc) FROM o").fetchone()[0]
        lo = f"AND date >= DATE '{c}' - {int(m.train_days)}" if m.train_days else ""
        panel = self.con.execute(f"""
            SELECT aid AS attraction_id, park_id, date, slot_utc AS slot_start_utc, slot_local,
                   ws, ko, kc, y FROM truth
            WHERE slot_utc + INTERVAL 15 MINUTE <= TIMESTAMPTZ '{cutoff}' {lo}
            ORDER BY aid, slot_utc""").df()
        m.fit(panel, pd.Timestamp(cutoff))
        self._last_fit[m.name] = c

    def plugin_predict(self, m: Model, c: dt.date, leads: list[int], kind: str = "daily",
                       hour: int | None = None, origin_table: str = "o") -> pd.DataFrame:
        """One model's forecasts at one origin, restricted to the grid it was asked for."""
        x = self.con.execute
        hour = self.cfg.origin_hour_local if hour is None else hour
        origin = Origin(c, kind, hour, x(f"SELECT * FROM {origin_table}").df(),
                        self.history_view(c, origin_table))
        grid = x(self.grid_sql(c, leads, origin_table=origin_table, max_lead=m.max_lead_days)).df()
        if grid.empty:
            return pd.DataFrame(columns=["aid", "slot_utc", "q50", "q80", "q95"])
        cov = self.covariates(c, max(leads), m.uses_oracle_weather)
        pred = m.predict(origin, grid, cov)
        if pred is None or len(pred) == 0:
            return pd.DataFrame(columns=["aid", "slot_utc", "q50", "q80", "q95"])
        p = pred.rename(columns={"attraction_id": "aid", "slot_start_utc": "slot_utc"}).copy()
        for q in ("q80", "q95"):
            if q not in p:
                p[q] = np.nan
        keys = grid.rename(columns={"attraction_id": "aid", "slot_start_utc": "slot_utc"})[["aid", "slot_utc"]]
        p["slot_utc"] = pd.to_datetime(p["slot_utc"], utc=True)
        keys["slot_utc"] = pd.to_datetime(keys["slot_utc"], utc=True)
        return keys.merge(p[["aid", "slot_utc", "q50", "q80", "q95"]], on=["aid", "slot_utc"], how="inner")

    def plugin_predict_daily(self, m: Model, c: dt.date) -> pd.DataFrame | None:
        x = self.con.execute
        leads = self.daily_leads(c) or [1]
        origin = Origin(c, "daily", self.cfg.origin_hour_local, x("SELECT * FROM o").df(),
                        self.history_view(c, "o"))
        days = x(f"""SELECT r.aid AS attraction_id, r.park_id, DATE '{c}' + L AS date, L AS lead_days
                     FROM rides r, (SELECT unnest([{','.join(map(str, leads))}]) L)
                     WHERE r.aid IN (SELECT aid FROM rs) ORDER BY 1, 3""").df()
        return m.predict_daily(origin, days, self.covariates(c, max(leads), m.uses_oracle_weather))

    def run_plugins_daily(self, c: dt.date, leads: list[int]) -> tuple[list[str], list[str], list[str]]:
        """Adds plug-in columns to tg and lv. Returns (slot cols, quantile models, level cols)."""
        x = self.con.execute
        slot_cols, qmodels, lvl_cols = [], [], []
        for m in self.models:
            name = m.scored_name()
            self.maybe_fit(m, c)
            p = self.plugin_predict(m, c, leads)
            self.con.register("pm", p)
            x(f"""CREATE OR REPLACE TEMP TABLE tgm AS SELECT t.*, pm.q50 AS {name},
                  pm.q80 AS {name}__q80, pm.q95 AS {name}__q95
                  FROM tg t LEFT JOIN pm ON pm.aid = t.aid AND pm.slot_utc = t.slot_utc""")
            self.con.unregister("pm")
            x("DROP TABLE tg")
            x("ALTER TABLE tgm RENAME TO tg")
            slot_cols.append(name)
            if m.provides_quantiles:
                qmodels.append(name)
            daily = self.plugin_predict_daily(m, c)
            if daily is not None and len(daily):
                col = f"lvl_{name}"
                d = daily.rename(columns={"attraction_id": "aid", "level": "lvl"})
                d["date"] = pd.to_datetime(d["date"]).dt.date
                self.con.register("pd_", d[["aid", "date", "lvl"]])
                x(f"""CREATE OR REPLACE TEMP TABLE lvm AS SELECT l.*, p.lvl AS {col} FROM lv l
                      LEFT JOIN pd_ p ON p.aid = l.aid AND p.date = l.date""")
                x(f"""CREATE OR REPLACE TEMP TABLE tgm AS SELECT t.*, p.lvl AS {col},
                      CASE WHEN t.ko BETWEEN 0 AND 3 AND p.lvl IS NOT NULL THEN t.h5
                           WHEN t.ref_lvl > 0 THEN t.h5 * p.lvl / t.ref_lvl END AS {name}_x_h5
                      FROM tg t LEFT JOIN pd_ p ON p.aid = t.aid AND p.date = t.date""")
                self.con.unregister("pd_")
                x("DROP TABLE lv")
                x("ALTER TABLE lvm RENAME TO lv")
                x("DROP TABLE tg")
                x("ALTER TABLE tgm RENAME TO tg")
                slot_cols.append(f"{name}_x_h5")
                lvl_cols.append(col)
        return slot_cols, qmodels, lvl_cols

    # ------------------------------------------------------------------ main loop
    def run(self) -> None:
        cfg = self.cfg
        x = self.con.execute
        for i, c in enumerate(self.origins):
            t0 = time.monotonic()
            self._t = t0
            self._stages: list[str] = []
            m0 = c.replace(day=1)
            if self._snap_month != m0:
                B.snapshot_tables(self.con, m0.isoformat(), cfg)
                self._snap_month = m0
            B.window_tables(self.con, c.isoformat(), cfg)
            self._tick('window')
            leads = self.slot_leads(c)
            if not leads:
                continue
            B.target_tables(self.con, c.isoformat(), leads, cfg)
            self._tick('targets')
            B.daily_levels(self.con, c.isoformat(), self.daily_leads(c) or [1], cfg)
            self._tick('levels')
            plug_cols, qmodels, lvl_cols = self.run_plugins_daily(c, leads)
            self._tick('plugins')
            models = B.SLOT_MODELS + plug_cols
            scored = models + B.ORACLES
            self.write("slot", slot_agg_sql(scored, B.REF_CANDIDATES, ["wt_med"] + qmodels, "tg",
                                            ["L", "park_id", "date", "busy", "fh", "sk"]), c)
            self._tick('slot')
            x(f"CREATE OR REPLACE TEMP TABLE rd AS {rideday_sql(models, cfg)}")
            self._tick('rd')
            self.write("rideday", rideday_pair_sql(B.REF_CANDIDATES, cfg), c)
            self._tick('rideday')
            self.write("pairs", pairs_sql(B.REF_CANDIDATES, cfg), c)
            self._tick('pairs')
            self.write("openness", self.openness_sql(models), c)
            self._tick('openness')
            self.write("levels", "SELECT * FROM lv", c)
            self.write("d8tft", D8_TFT_SQL.format(
                c=c.isoformat(), bucket_f=_lead_bucket("f.target_date - f.forecast_date"),
                bucket_t=_lead_bucket(f"t.target_date - DATE '{c.isoformat()}'")), c)
            self._tick('lvwrite')
            self.optimiser(c, models)
            self._tick('optim')
            self.intraday(c)
            self._tick('intraday')
            print(f"[shard {self.shard[0]}] origin {c} ({i + 1}/{len(self.origins)}) "
                  f"{time.monotonic() - t0:.1f}s" + (f" | {' '.join(self._stages)}" if PROFILE else ""),
                  flush=True)

    def _tick(self, label: str) -> None:
        now = time.monotonic()
        self._stages.append(f"{label}={now - self._t:.1f}")
        self._t = now

    def openness_sql(self, models: list[str]) -> str:
        has = ", ".join(f"bool_or({m} IS NOT NULL) AS h__{m}" for m in models)
        agg = ", ".join(
            f"count(*) FILTER (WHERE op AND h__{m}) AS op_f__{m}, "
            f"count(*) FILTER (WHERE op AND NOT h__{m}) AS op_nf__{m}, "
            f"count(*) FILTER (WHERE NOT op AND h__{m}) AS cl_f__{m}, "
            f"count(*) FILTER (WHERE NOT op AND NOT h__{m}) AS cl_nf__{m}" for m in models)
        return f"""WITH r AS (SELECT L, park_id, date, aid, bool_or(status = 'OPERATING') op, {has}
                              FROM tg GROUP BY ALL)
                   SELECT L, park_id, date, {agg} FROM r GROUP BY ALL"""

    # ------------------------------------------------------------------ D4
    def optimiser(self, c: dt.date, models: list[str]) -> None:
        cfg = self.cfg
        leads = [L for L in cfg.optimiser_leads if L in self.slot_leads(c)]
        if not leads:
            return
        cols = ", ".join(f"t.{m}" for m in models)
        df = self.con.execute(f"""
            WITH pick AS (
              SELECT r.aid, r.park_id, r.lat, r.lng, r.land,
                     row_number() OVER (PARTITION BY r.park_id
                                        ORDER BY r.is_headliner DESC, d.lvl DESC, r.aid) rk
              FROM rides r JOIN dl d ON d.aid = r.aid AND d.wk = 2)
            SELECT t.L, t.park_id, t.date, t.aid, t.ko_t AS ko, t.y, {cols},
                   p.lat, p.lng, p.land,
                   CAST(date_diff('minute', w.open_utc, w.close_utc) // 15 AS INTEGER) AS nslots
            FROM tg t JOIN pick p ON p.aid = t.aid AND p.rk <= {cfg.optimiser_rides}
            JOIN windows w ON w.park_id = t.park_id AND w.date = t.date
            WHERE t.L IN ({','.join(map(str, leads))})""").df()
        rows = []
        for (L, park, date), g in df.groupby(["L", "park_id", "date"], sort=False):
            aids = sorted(g["aid"].unique())
            if len(aids) < cfg.optimiser_rides:
                continue
            K = int(g["nslots"].iloc[0])
            if K < 8:
                continue
            meta = g.drop_duplicates("aid").set_index("aid")
            info = [{"lat": meta.at[a, "lat"], "lng": meta.at[a, "lng"], "land": meta.at[a, "land"]}
                    for a in aids]
            T = np.array([[0 if i == j else transfer_ceiling(info[i], info[j]) for j in range(len(aids))]
                          for i in range(len(aids))], dtype=float)

            pos = {a: (np.flatnonzero(((g["aid"] == a) & (g["ko"] >= 0) & (g["ko"] < K)).to_numpy()))
                   for a in aids}
            kos = g["ko"].to_numpy()

            def arr(col: str) -> np.ndarray | None:
                vals = g[col].to_numpy(dtype=float)
                out = np.full((len(aids), K), np.nan)
                for i, a in enumerate(aids):
                    p = pos[a]
                    out[i, kos[p]] = vals[p]
                    if np.isfinite(out[i]).sum() < K / 2:
                        return None
                    out[i] = pd.Series(out[i]).ffill().bfill().to_numpy()
                return out

            truth = arr("y")
            if truth is None:
                continue
            best = optimise(truth, T, K, cfg.idle_weight, cfg.optimiser_max_delay_min)
            if best is None:
                continue
            for m in models:
                fc = arr(m)
                if fc is None:
                    rows.append((L, park, date, m, best.cost, None))
                    continue
                plan = optimise(fc, T, K, cfg.idle_weight, cfg.optimiser_max_delay_min)
                cost = execute(plan, truth, T, K, cfg.idle_weight) if plan else math.nan
                rows.append((L, park, date, m, best.cost, cost))
        if rows:
            out = pd.DataFrame(rows, columns=["L", "park_id", "date", "model", "opt_cost", "plan_cost"])
            d = self.parts / "optim"
            d.mkdir(parents=True, exist_ok=True)
            out.to_parquet(d / f"{c.isoformat()}.parquet", index=False)

    # ------------------------------------------------------------------ intraday
    def intraday(self, c: dt.date) -> None:
        cfg = self.cfg
        x = self.con.execute
        if 0 not in self.slot_leads(c):
            return
        plug = [m for m in self.models if m.intraday]
        frames = []
        for h in cfg.intraday_hours_local:
            x(f"CREATE OR REPLACE TEMP TABLE oh AS {B.origin_sql(c.isoformat(), h)}")
            pcols, pjoins = "", ""
            for m in plug:
                name = m.scored_name()
                p = self.plugin_predict(m, c, [0, 1], kind="intraday", hour=h, origin_table="oh")
                self.con.register(f"pi_{name}", p)
                pcols += f", pi_{name}.q50 AS {name}"
                pjoins += (f" LEFT JOIN pi_{name} ON pi_{name}.aid = t.aid"
                           f" AND pi_{name}.slot_utc = t.slot_utc")
            x(f"""CREATE OR REPLACE TEMP TABLE ih_{h} AS
                SELECT {h} AS h, t.aid, t.park_id, t.date, t.slot_utc, t.y, t.busy, a.is_headliner AS hl,
                       CAST(date_diff('minute', oh.origin_utc, t.slot_utc) AS INTEGER) + 15 AS lead_min,
                       CASE WHEN p.status = 'OPERATING' THEN p.wait::DOUBLE END AS persistence,
                       t.snaive7, t.wt_med, t.clim, t.h5, t.lvlh5_naive, t.lvlh5_tft {pcols}
                FROM tg t JOIN oh ON oh.park_id = t.park_id JOIN rides a ON a.aid = t.aid
                LEFT JOIN (SELECT aid, slot_utc, status, wait FROM slots
                           WHERE date BETWEEN DATE '{c}' - 1 AND DATE '{c}') p
                       ON p.aid = t.aid AND p.slot_utc = oh.origin_utc - INTERVAL 15 MINUTE
                {pjoins}
                WHERE t.L = 0 AND t.slot_utc >= oh.origin_utc""")
            for m in plug:
                self.con.unregister(f"pi_{m.scored_name()}")
            frames.append(f"SELECT * FROM ih_{h}")
        x(f"CREATE OR REPLACE TEMP TABLE ih AS {' UNION ALL BY NAME '.join(frames)}")
        for h in cfg.intraday_hours_local:
            x(f"DROP TABLE ih_{h}")
        x("""CREATE OR REPLACE TEMP TABLE ih AS SELECT *,
               CASE WHEN lead_min <= 120 THEN 'm' || lpad(CAST(lead_min AS VARCHAR), 3, '0')
                    WHEN lead_min <= 240 THEN 'h2-4' WHEN lead_min <= 480 THEN 'h4-8' ELSE 'h8+' END AS lead_key
             FROM ih""")
        models = B.INTRADAY_MODELS + [m.scored_name() for m in plug]
        self.write("intraday", slot_agg_sql(models, B.INTRADAY_REFS, [], "ih",
                                            ["lead_key", "park_id", "date", "busy", "hl"]), c)
        # D1 next-best-ride (lib/planner/next-best-ride.ts): suggest a ride when the
        # forecast maximum inside the lookahead is >= live + 10. The candidate set is
        # the same for every model — every (origin, ride) with a live OPERATING wait —
        # and the lookahead is fixed (60 or 120 min), never the model's own peak time.
        thr = cfg.next_best_min_saving
        parts = []
        for look in (60, cfg.next_best_lookahead_min):
            for m in models:
                if m == "persistence":
                    continue
                parts.append(f"""
                  SELECT h, aid, park_id, date, '0-{look}' AS bucket, '{m}' AS model,
                         any_value(persistence) live, max({m}) pred_peak, max(y) true_peak
                  FROM ih WHERE persistence IS NOT NULL AND lead_min - 15 <= {look}
                  GROUP BY h, aid, park_id, date""")
        x(f"CREATE OR REPLACE TEMP TABLE nb AS {' UNION ALL '.join(parts)}")
        self.write("nextbest", f"""
            WITH s AS (SELECT *, coalesce(pred_peak - live >= {thr}, false) AS sug,
                              coalesce(true_peak - live >= {thr}, false) AS opp FROM nb),
            r AS (SELECT *, CASE WHEN sug THEN row_number() OVER (PARTITION BY h, park_id, bucket, model, sug
                                                    ORDER BY pred_peak - live DESC, aid) END AS rk FROM s)
            SELECT park_id, date, model, bucket, count(*) n_cand,
                   count(*) FILTER (WHERE sug) n_sug, count(*) FILTER (WHERE sug AND opp) n_ok,
                   count(*) FILTER (WHERE opp) n_opp,
                   count(*) FILTER (WHERE sug AND rk <= {cfg.next_best_limit}) n_sug3,
                   count(*) FILTER (WHERE sug AND opp AND rk <= {cfg.next_best_limit}) n_ok3
            FROM r GROUP BY ALL""", c)


# --------------------------------------------------------------------------- CLI

def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--export", required=True, help="export dir (with parquet/)")
    p.add_argument("--out", default=None, help="results/<run-id> (default: ml-bench/results/<utc stamp>)")
    p.add_argument("--model", action="append", default=[],
                   help="plug-in model: registry name or package.module:Class (repeatable)")
    p.add_argument("--from", dest="origin_from", default=None)
    p.add_argument("--to", dest="origin_to", default=None)
    p.add_argument("--shard", default="0/1", help="i/n — run every n-th origin starting at i")
    p.add_argument("--memory", default=None)
    p.add_argument("--threads", type=int, default=None)
    p.add_argument("--leads", default=None, help="override slot leads, e.g. 0,1,3,7")
    p.add_argument("--reference", action="store_true",
                   help="a run others compare against: refuse to start without git SHA and image id")


def main(args: argparse.Namespace) -> int:
    cfg = BenchConfig()
    if args.memory:
        cfg.memory_limit = args.memory
    if args.threads:
        cfg.threads = args.threads
    if args.leads:
        cfg.slot_leads = [int(v) for v in args.leads.split(",")]
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%MZ")
    out = Path(args.out or Path(__file__).resolve().parents[1] / "results" / run_id)
    out.mkdir(parents=True, exist_ok=True)
    i, n = (int(v) for v in args.shard.split("/"))
    models = [load_model(s) for s in args.model]
    sha = git_sha()
    image = os.environ.get("MLBENCH_IMAGE_ID", "unknown")
    if args.reference and ("unknown" in (sha, image) or sha.endswith("-dirty")):
        raise SystemExit("--reference needs a known git SHA (--build-arg GIT_SHA) and MLBENCH_IMAGE_ID "
                         f"(-e MLBENCH_IMAGE_ID=$(docker image inspect -f '{{{{.Id}}}}' <tag>)); got {sha}, {image}")
    meta = {"config": json.loads(cfg.to_json()), "models": [m.scored_name() for m in models],
            "git_sha": sha, "code_sha256": code_hash(), "image_id": image, "reference": args.reference,
            "export": str(args.export),
            "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "argv": sys.argv}
    (out / f"run-{i}of{n}.json").write_text(json.dumps(meta, indent=2, default=str))
    origins = (args.origin_from or "1900-01-01", args.origin_to or "2999-12-31")
    r = Runner(Path(args.export), out, cfg, models, (i, n), origins)
    t0 = time.monotonic()
    r.run()
    meta["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    meta["seconds"] = round(time.monotonic() - t0, 1)
    meta["origins"] = [str(c) for c in (r.origins[0], r.origins[-1])] if r.origins else []
    (out / f"run-{i}of{n}.json").write_text(json.dumps(meta, indent=2, default=str))
    return 0
