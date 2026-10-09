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
    names = ("parks", "attractions", "windows", "slots", "ride_day", "tft", "cbd", "park_day_cov", "truth")
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
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True,
                                       cwd=Path(__file__).parent, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return os.environ.get("MLBENCH_GIT_SHA", "unknown")


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
    """Per (lead, ride-day, model) decision quantities, on the slots where both the
    truth and the model exist."""
    cols = ", ".join(models)
    return f"""
    WITH long AS (
      SELECT L, aid, park_id, date, ws, ko, y, busy, model, pred
      FROM (SELECT L, aid, park_id, date, ws, ko, y, busy, {cols} FROM tg WHERE y IS NOT NULL)
      UNPIVOT (pred FOR model IN ({cols}))),
    r AS (
      SELECT *,
        rank() OVER g_y + (count(*) OVER (PARTITION BY L, aid, date, model, y) - 1) / 2.0 AS ry,
        rank() OVER g_p + (count(*) OVER (PARTITION BY L, aid, date, model, pred) - 1) / 2.0 AS rp,
        row_number() OVER (PARTITION BY L, aid, date, model ORDER BY pred, ws) AS pbest,
        min(y) OVER (PARTITION BY L, aid, date, model) AS ymin,
        first_value(ws) OVER (PARTITION BY L, aid, date, model ORDER BY pred, ws) AS best_ws
      FROM long
      WINDOW g_y AS (PARTITION BY L, aid, date, model ORDER BY y),
             g_p AS (PARTITION BY L, aid, date, model ORDER BY pred))
    SELECT L, aid, park_id, date, model, any_value(busy) busy, count(*) n,
      quantile_cont(y, 0.9) true_p90, quantile_cont(pred, 0.9) pred_p90,
      max(y) ymax, min(y) ymin_, max(pred) pmax,
      CASE WHEN isfinite(corr(ry, rp)) THEN corr(ry, rp) END sp,
      bool_or(pbest <= 2 AND y = ymin) hit2,
      bool_or(y = ymin AND abs(ws - best_ws) <= 2) hit30,
      max(y) FILTER (WHERE pbest = 1) - min(y) regret,
      sum(abs(pred - y)) FILTER (WHERE ko < 4) fh_sae, count(*) FILTER (WHERE ko < 4) fh_n,
      arg_min(pred, ko) p_open, arg_min(y, ko) y_open
    FROM r GROUP BY L, aid, park_id, date, model"""


def rideday_agg_sql(cfg: BenchConfig) -> str:
    wp, ws_ = cfg.rope_worth_peak, cfg.rope_worth_savings
    return f"""
    WITH d AS (
      SELECT *, n >= {cfg.min_ride_day_slots} AS ok,
        n >= {cfg.min_ride_day_slots} AND ymax > ymin_ AS ok_rank,
        pmax >= {wp} AND pmax - p_open >= {ws_} AS worth_p,
        ymax >= {wp} AND ymax - y_open >= {ws_} AS worth_t
      FROM rd)
    SELECT L, park_id, date, model, busy,
      count(*) FILTER (WHERE ok) n_rd,
      sum(abs(pred_p90 - true_p90)) FILTER (WHERE ok) dp_sae,
      sum(pred_p90 - true_p90) FILTER (WHERE ok) dp_se,
      count(sp) FILTER (WHERE ok_rank) sp_n, sum(sp) FILTER (WHERE ok_rank) sp_sum,
      count(*) FILTER (WHERE ok_rank) bt_n,
      sum(CAST(hit2 AS INTEGER)) FILTER (WHERE ok_rank) hit2,
      sum(CAST(hit30 AS INTEGER)) FILTER (WHERE ok_rank) hit30,
      sum(regret) FILTER (WHERE ok_rank) regret,
      sum(fh_sae) fh_sae, sum(fh_n) fh_n,
      count(*) FILTER (WHERE ok AND worth_p AND worth_t) w_tp,
      count(*) FILTER (WHERE ok AND worth_p AND NOT worth_t) w_fp,
      count(*) FILTER (WHERE ok AND NOT worth_p AND worth_t) w_fn,
      count(*) FILTER (WHERE ok AND NOT worth_p AND NOT worth_t) w_tn
    FROM d GROUP BY ALL
    UNION ALL
    -- the production rope-drop rule (window medians, one verdict per ride) on the same ride-days
    SELECT d.L, d.park_id, d.date, 'prod_ropedrop_hist', d.busy, count(*), NULL, NULL, NULL, NULL,
      NULL, NULL, NULL, NULL, NULL, NULL,
      count(*) FILTER (WHERE wh AND worth_t), count(*) FILTER (WHERE wh AND NOT worth_t),
      count(*) FILTER (WHERE NOT wh AND worth_t), count(*) FILTER (WHERE NOT wh AND NOT worth_t)
    FROM (SELECT d.*, h.busy_peak >= {wp} AND h.busy_peak - h.open_wait >= {ws_} AS wh
          FROM d JOIN ropehist h USING (aid) WHERE d.ok AND d.model = 'wt_med') d
    GROUP BY ALL"""


PAIRS_SQL = """
    WITH h AS (SELECT r.* FROM rd r JOIN rides a USING (aid)
               WHERE a.is_headliner AND r.n >= {minslots}),
    pw AS (
      SELECT a.L, a.park_id, a.date, a.model,
             count(*) FILTER (WHERE abs(a.true_p90 - b.true_p90) >= 1) pw_n,
             count(*) FILTER (WHERE abs(a.true_p90 - b.true_p90) >= 1
                              AND sign(a.pred_p90 - b.pred_p90) = sign(a.true_p90 - b.true_p90)) pw_ok
      FROM h a JOIN h b ON a.L = b.L AND a.park_id = b.park_id AND a.date = b.date
                         AND a.model = b.model AND a.aid < b.aid
      GROUP BY ALL),
    sp AS (
      SELECT L, park_id, date, model, CASE WHEN isfinite(corr(rt, rp)) THEN corr(rt, rp) END dsp,
             count(*) dsp_n FROM (
        SELECT *, rank() OVER (PARTITION BY L, park_id, date, model ORDER BY true_p90) rt,
                  rank() OVER (PARTITION BY L, park_id, date, model ORDER BY pred_p90) rp
        FROM rd WHERE n >= {minslots}) GROUP BY ALL HAVING count(*) >= 5)
    SELECT coalesce(pw.L, sp.L) L, coalesce(pw.park_id, sp.park_id) park_id,
           coalesce(pw.date, sp.date) date, coalesce(pw.model, sp.model) model,
           pw.pw_n, pw.pw_ok, sp.dsp, sp.dsp_n
    FROM pw FULL OUTER JOIN sp USING (L, park_id, date, model)"""


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
        lead_list = ",".join(str(L) for L in leads if L <= max_lead) or "NULL"
        return f"""
        WITH days AS (SELECT w.park_id, w.date, w.open_utc, w.close_utc
                      FROM windows w WHERE w.date IN (SELECT DATE '{c}' + unnest([{lead_list}]))),
        g AS (SELECT r.aid AS attraction_id, d.park_id, d.date, d.open_utc, d.close_utc,
                     unnest(range(d.open_utc, d.close_utc, INTERVAL 15 MINUTE)) AS slot_start_utc
              FROM days d JOIN rides r ON r.park_id = d.park_id WHERE r.aid IN (SELECT aid FROM rs))
        SELECT g.attraction_id, g.park_id, g.date, g.slot_start_utc,
               timezone(o.timezone, g.slot_start_utc) AS slot_local,
               CAST(date_diff('minute', CAST(g.date AS TIMESTAMP), timezone(o.timezone, g.slot_start_utc)) // 15 AS INTEGER) AS ws,
               CAST(date_diff('minute', g.open_utc, g.slot_start_utc) // 15 AS INTEGER) AS ko,
               CAST((date_diff('minute', g.slot_start_utc, g.close_utc) - 1) // 15 AS INTEGER) AS kc,
               CAST(g.date - DATE '{c}' AS INTEGER) AS lead_days
        FROM g JOIN {origin_table} o ON o.park_id = g.park_id
        WHERE g.slot_start_utc >= o.origin_utc"""

    def maybe_fit(self, m: Model, c: dt.date) -> None:
        last = self._last_fit.get(m.name)
        if last is not None and (m.refit_every_days is None or (c - last).days < m.refit_every_days):
            return
        cutoff = self.con.execute("SELECT min(origin_utc) FROM o").fetchone()[0]
        lo = f"AND date >= DATE '{c}' - {int(m.train_days)}" if m.train_days else ""
        panel = self.con.execute(f"""
            SELECT aid AS attraction_id, park_id, date, slot_utc AS slot_start_utc, slot_local,
                   ws, ko, kc, y FROM truth
            WHERE slot_utc + INTERVAL 15 MINUTE <= TIMESTAMPTZ '{cutoff}' {lo}""").df()
        m.fit(panel, pd.Timestamp(cutoff))
        self._last_fit[m.name] = c

    def run_plugins_daily(self, c: dt.date, leads: list[int]) -> tuple[list[str], list[str], list[str]]:
        """Adds plug-in columns to tg and lv. Returns (slot cols, quantile models, level cols)."""
        x = self.con.execute
        slot_cols, qmodels, lvl_cols = [], [], []
        cov = x(f"""SELECT * FROM park_day_cov WHERE date >= DATE '{c}'
                    AND date <= DATE '{c}' + {max(leads + self.daily_leads(c) + [0])}""").df()
        for m in self.models:
            self.maybe_fit(m, c)
            origin = Origin(c, "daily", self.cfg.origin_hour_local, x("SELECT * FROM o").df(),
                            HistoryView(self.con, "o", c))
            grid = x(self.grid_sql(c, leads, max_lead=m.max_lead_days)).df()
            pred = m.predict(origin, grid, cov) if len(grid) else None
            if pred is not None and len(pred):
                p = pred.rename(columns={"attraction_id": "aid", "slot_start_utc": "slot_utc"})
                for q in ("q80", "q95"):
                    if q not in p:
                        p[q] = np.nan
                self.con.register("pm", p[["aid", "slot_utc", "q50", "q80", "q95"]])
                x(f"""CREATE OR REPLACE TEMP TABLE tgm AS SELECT t.*, pm.q50 AS {m.name},
                      pm.q80 AS {m.name}__q80, pm.q95 AS {m.name}__q95
                      FROM tg t LEFT JOIN pm ON pm.aid = t.aid AND pm.slot_utc = t.slot_utc""")
                self.con.unregister("pm")
            else:
                x(f"""CREATE OR REPLACE TEMP TABLE tgm AS SELECT t.*, NULL::DOUBLE AS {m.name},
                      NULL::DOUBLE AS {m.name}__q80, NULL::DOUBLE AS {m.name}__q95 FROM tg t""")
            x("DROP TABLE tg")
            x("ALTER TABLE tgm RENAME TO tg")
            slot_cols.append(m.name)
            if m.provides_quantiles:
                qmodels.append(m.name)
            days = x(f"""SELECT r.aid AS attraction_id, r.park_id, DATE '{c}' + L AS date, L AS lead_days
                         FROM rides r, (SELECT unnest([{','.join(map(str, self.daily_leads(c) or [1]))}]) L)
                         WHERE r.aid IN (SELECT aid FROM rs)""").df()
            daily = m.predict_daily(origin, days, cov)
            if daily is not None and len(daily):
                col = f"lvl_{m.name}"
                d = daily.rename(columns={"attraction_id": "aid", "level": "lvl"})
                d["date"] = pd.to_datetime(d["date"]).dt.date
                self.con.register("pd_", d[["aid", "date", "lvl"]])
                x(f"""CREATE OR REPLACE TEMP TABLE lvm AS SELECT l.*, p.lvl AS {col} FROM lv l
                      LEFT JOIN pd_ p ON p.aid = l.aid AND p.date = l.date""")
                x(f"""CREATE OR REPLACE TEMP TABLE tgm AS SELECT t.*, p.lvl AS {col},
                      CASE WHEN t.ko < 4 AND p.lvl IS NOT NULL THEN t.h5
                           WHEN t.ref_lvl > 0 THEN t.h5 * p.lvl / t.ref_lvl END AS {m.name}_x_h5
                      FROM tg t LEFT JOIN pd_ p ON p.aid = t.aid AND p.date = t.date""")
                self.con.unregister("pd_")
                x("DROP TABLE lv")
                x("ALTER TABLE lvm RENAME TO lv")
                x("DROP TABLE tg")
                x("ALTER TABLE tgm RENAME TO tg")
                slot_cols.append(f"{m.name}_x_h5")
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
                                            ["L", "park_id", "date", "busy"]), c)
            self._tick('slot')
            x(f"CREATE OR REPLACE TEMP TABLE rd AS {rideday_sql(models, cfg)}")
            self._tick('rd')
            self.write("rideday", rideday_agg_sql(cfg), c)
            self._tick('rideday')
            self.write("pairs", PAIRS_SQL.format(minslots=cfg.min_ride_day_slots), c)
            self._tick('pairs')
            self.write("openness", self.openness_sql(models), c)
            self._tick('openness')
            self.write("levels", "SELECT * FROM lv", c)
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
            SELECT t.L, t.park_id, t.date, t.aid, t.ko, t.y, {cols},
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
                origin = Origin(c, "intraday", h, x("SELECT * FROM oh").df(), HistoryView(self.con, "oh", c))
                grid = x(self.grid_sql(c, [0, 1], origin_table="oh")).df()
                cov = x(f"SELECT * FROM park_day_cov WHERE date BETWEEN DATE '{c}' AND DATE '{c}' + 1").df()
                pred = m.predict(origin, grid, cov) if len(grid) else None
                p = (pred if pred is not None else pd.DataFrame(columns=["attraction_id", "slot_start_utc", "q50"]))
                p = p.rename(columns={"attraction_id": "aid", "slot_start_utc": "slot_utc"})[["aid", "slot_utc", "q50"]]
                self.con.register(f"pi_{m.name}", p)
                pcols += f", pi_{m.name}.q50 AS {m.name}"
                pjoins += (f" LEFT JOIN pi_{m.name} ON pi_{m.name}.aid = t.aid"
                           f" AND pi_{m.name}.slot_utc = t.slot_utc")
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
                self.con.unregister(f"pi_{m.name}")
            frames.append(f"SELECT * FROM ih_{h}")
        x(f"CREATE OR REPLACE TEMP TABLE ih AS {' UNION ALL BY NAME '.join(frames)}")
        for h in cfg.intraday_hours_local:
            x(f"DROP TABLE ih_{h}")
        x("""CREATE OR REPLACE TEMP TABLE ih AS SELECT *,
               CASE WHEN lead_min <= 120 THEN 'm' || lpad(CAST(lead_min AS VARCHAR), 3, '0')
                    WHEN lead_min <= 240 THEN 'h2-4' WHEN lead_min <= 480 THEN 'h4-8' ELSE 'h8+' END AS lead_key
             FROM ih""")
        models = B.INTRADAY_MODELS + [m.name for m in plug]
        self.write("intraday", slot_agg_sql(models, B.INTRADAY_REFS, [], "ih",
                                            ["lead_key", "park_id", "date", "busy", "hl"]), c)
        # D1 next-best-ride: suggestion = forecast max in the next 120 min >= live + 10
        thr, look = cfg.next_best_min_saving, cfg.next_best_lookahead_min
        parts = []
        for m in models:
            if m == "persistence":
                continue
            parts.append(f"""
              SELECT h, aid, park_id, date, '{m}' AS model, any_value(persistence) live,
                     max({m}) pred_peak, arg_max(lead_min, {m}) peak_lead, max(y) true_peak
              FROM ih WHERE persistence IS NOT NULL AND lead_min - 15 <= {look}
              GROUP BY h, aid, park_id, date HAVING count({m}) > 0""")
        x(f"CREATE OR REPLACE TEMP TABLE nb AS {' UNION ALL '.join(parts)}")
        self.write("nextbest", f"""
            WITH s AS (SELECT *, pred_peak - live >= {thr} AS sug,
                              coalesce(true_peak - live >= {thr}, false) AS opp,
                              row_number() OVER (PARTITION BY h, park_id, model
                                                 ORDER BY pred_peak - live DESC, aid) AS rk,
                              CASE WHEN peak_lead - 15 < 60 THEN '0-60' ELSE '60-120' END AS bucket
                       FROM nb)
            SELECT park_id, date, model, bucket, count(*) n_cand,
                   count(*) FILTER (WHERE sug) n_sug, count(*) FILTER (WHERE sug AND opp) n_ok,
                   count(*) FILTER (WHERE opp) n_opp,
                   count(*) FILTER (WHERE sug AND rk <= {cfg.next_best_limit}) n_sug3,
                   count(*) FILTER (WHERE sug AND opp AND rk <= {cfg.next_best_limit}) n_ok3
            FROM s GROUP BY ALL""", c)


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
    meta = {"config": json.loads(cfg.to_json()), "models": [m.name for m in models],
            "git_sha": git_sha(), "export": str(args.export),
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
