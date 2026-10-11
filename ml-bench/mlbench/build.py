"""Turn a raw export (``raw/*.csv.gz``) into the benchmark's Parquet tables.

Truth (BENCH-SPEC "Truth"):

* 15-minute slots on a UTC grid (slot starts at :00/:15/:30/:45; every park
  offset in use is a whole quarter hour, so the grid is also park-local).
* The value of a slot is the STANDBY reading in force at the slot MIDPOINT:
  the newest change-log row with ``ts <= midpoint``, provided it is at most
  3 h old (``queue_data`` writes a row only when a value changes, so a value
  stays in force until the next row — up to the staleness cap).
* A slot belongs to the park's service day whose published OPERATING window
  (``schedule_entries``, park-level) contains the slot start; windows may cross
  midnight. Truth slots additionally need status OPERATING and wait >= 5.

Outputs (``<export>/parquet/``):

``parks``, ``attractions``, ``holidays``, ``weather``, ``tft``, ``cbd``
    the exported tables, typed.
``windows``
    one row per park service day: earliest OPERATING opening, latest closing
    (UTC and park-local), ``n_windows`` and ``schedule_source='published_final'``.
``slots``
    every in-window slot that has a reading of any status (truth + D9 openness):
    ``aid, park_id, date, slot_utc, slot_local, ws, ko, kc, status, wait, age_min``.
    ``ws`` = 15-min index since the service day's local midnight (continues past
    95 on a day that crosses midnight), ``ko`` = slots since published opening,
    ``kc`` = slots before published closing (0 = the last slot).
``truth``
    ``slots`` filtered to OPERATING, wait >= 5.
``ride_day``
    daily level per ride-day: P90 of truth slots (>= 8 slots), plus counts.
``park_day_cov``
    known-future covariates per park service day (weekday, opening hours,
    holiday flags via ml-service ``holiday_features.py``, schedule
    holiday/bridge flags, daily weather actuals flagged ORACLE).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

SLOT_US = 15 * 60 * 1_000_000
HALF_US = SLOT_US // 2
STALE_US = 3 * 3600 * 1_000_000


def connect(memory_limit: str = "6GB", threads: int = 8, temp_dir: str | None = None):
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET TimeZone='UTC'")
    if temp_dir:
        con.execute(f"SET temp_directory='{temp_dir}'")
    return con


def _csv(raw: Path, pattern: str) -> str:
    return f"read_csv('{raw}/{pattern}', header=true, auto_detect=true, union_by_name=true)"


# The expansion of the change log into slot midpoints, in integer microseconds.
# first midpoint >= ts:  ceil((ts - HALF) / SLOT) * SLOT + HALF
# last  midpoint      :  < next row's ts  and  <= ts + 3 h
# Runs in time chunks [lo, hi) over the daily export files. Exact, because a row
# can only be in force inside the chunk if ts >= lo - 3 h, and a row whose next
# row lies beyond hi + 3 h is capped by the 3 h staleness limit anyway — so the
# rows in [lo - 3 h, hi + 3 h) decide every midpoint in [lo, hi).
# ``{hb}`` is either empty or ``AND NOT coalesce(is_heartbeat, false)``. A heartbeat
# row carries the previous status AND wait forward with a fresh ``ts``, so it resets
# the 3 h staleness clock this expansion relies on and a ride that stopped reporting
# keeps producing "truth" with a frozen wait. Dropping them is NOT the default,
# because whether the target is "what the queue did" or "what the app showed" is a
# product decision; `--drop-heartbeats` builds the other target so the two can be
# compared cell for cell (PAR-827 critic B3, BENCH-SPEC "Recorded changes" 6).
EXPAND_CHUNK_SQL = """
WITH q AS (
  SELECT attraction_id AS aid, ts_us, status, wait,
         lead(ts_us) OVER (PARTITION BY attraction_id ORDER BY ts_us) AS nts
  FROM (SELECT * FROM read_csv({files}, header=true,
       columns={{'attraction_id':'VARCHAR','ts_us':'BIGINT','status':'VARCHAR',
                 'wait':'INTEGER','is_heartbeat':'BOOLEAN'}})
        WHERE TRUE {hb})
  WHERE ts_us >= {lo} - {stale} - 1 AND ts_us < {hi} + {stale} + 1
),
r AS (
  SELECT aid, ts_us, status, wait,
         ((ts_us - {half} + {slot} - 1) // {slot}) * {slot} + {half} AS m0,
         least(coalesce(nts, ts_us + {stale} + 1), ts_us + {stale} + 1) AS m_end
  FROM q WHERE ts_us < {hi}
),
m AS (
  SELECT aid, status, wait, ts_us, unnest(range(m0, m_end, {slot})) AS mid_us
  FROM r WHERE m0 < m_end
)
SELECT aid, status, wait, ts_us, mid_us FROM m WHERE mid_us >= {lo} AND mid_us < {hi}
"""
CHUNK_DAYS = 7


def build(raw: Path, out: Path, con, ml_service_dir: Path | None = None,
          drop_heartbeats: bool = False) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    x = con.execute
    stats: dict[str, object] = {}
    t0 = time.monotonic()

    x(f"CREATE OR REPLACE TABLE parks AS SELECT * FROM {_csv(raw, 'parks.csv.gz')}")
    x(f"""CREATE OR REPLACE TABLE attractions AS
        SELECT * EXCLUDE (is_headliner), coalesce(is_headliner, false) AS is_headliner
        FROM {_csv(raw, 'attractions.csv.gz')}""")
    x(f"CREATE OR REPLACE TABLE holidays AS SELECT * FROM {_csv(raw, 'holidays.csv.gz')}")
    x(f"CREATE OR REPLACE TABLE weather AS SELECT * FROM {_csv(raw, 'weather.csv.gz')}")
    x(f"CREATE OR REPLACE TABLE schedule AS SELECT * FROM {_csv(raw, 'schedule.csv.gz')}")
    for name, pat in (("tft", "tft_forecasts_*.csv.gz"), ("cbd", "catboost_daily_forecasts_*.csv.gz")):
        if list(raw.glob(pat)):
            x(f"""CREATE OR REPLACE TABLE {name} AS
                SELECT attraction_id AS aid, CAST(target_date AS DATE) AS target_date,
                       CAST(forecast_date AS DATE) AS forecast_date, predicted_peak::DOUBLE AS peak,
                       model_version, make_timestamptz(created_us) AS created_utc
                FROM {_csv(raw, pat)}""")
        else:
            x(f"""CREATE OR REPLACE TABLE {name} (aid VARCHAR, target_date DATE, forecast_date DATE,
                  peak DOUBLE, model_version VARCHAR, created_utc TIMESTAMPTZ)""")

    # Park service-day windows: earliest OPERATING opening, latest closing.
    # Sanity: closing after opening and at most 24 h later (PAR schedule invariants).
    x("""CREATE OR REPLACE TABLE windows AS
        SELECT s.park_id, CAST(s.date AS DATE) AS date,
               make_timestamptz(min(s.opening_us)) AS open_utc,
               make_timestamptz(max(s.closing_us)) AS close_utc,
               count(*) AS n_windows, 'published_final' AS schedule_source,
               -- the last write of the day's OPERATING rows: a window written after an
               -- origin was not known at that origin (the runner projects it instead)
               make_timestamptz(max(s.updated_us)) AS updated_utc
        FROM schedule s
        WHERE s.schedule_type = 'OPERATING' AND s.opening_us IS NOT NULL AND s.closing_us IS NOT NULL
          AND s.closing_us > s.opening_us AND s.closing_us - s.opening_us <= 86400000000
        GROUP BY ALL""")
    x("""CREATE OR REPLACE TABLE windows AS
        SELECT w.*, timezone(p.timezone, w.open_utc) AS open_local,
               timezone(p.timezone, w.close_utc) AS close_local, p.timezone
        FROM windows w JOIN parks p ON p.id = w.park_id""")
    stats["windows"] = x("SELECT count(*) FROM windows").fetchone()[0]

    hb_filter = "AND NOT coalesce(is_heartbeat, false)" if drop_heartbeats else ""
    stats["drop_heartbeats"] = bool(drop_heartbeats)
    qfiles = sorted(raw.glob("queue_*.csv.gz"))
    days = [dt.date.fromisoformat(p.name[len("queue_"):-len(".csv.gz")]) for p in qfiles]
    by_day = dict(zip(days, qfiles))
    stats["queue_files"] = len(qfiles)
    day_us = 86400 * 1_000_000
    epoch = dt.date(1970, 1, 1)
    start = (min(days) - epoch).days * day_us
    hi_us = ((max(days) - epoch).days + 1) * day_us
    x("""CREATE OR REPLACE TABLE slots (aid VARCHAR, park_id VARCHAR, date DATE, slot_utc TIMESTAMPTZ,
          slot_local TIMESTAMP, ws INTEGER, ko INTEGER, kc INTEGER, status VARCHAR, wait INTEGER,
          age_min DOUBLE)""")
    while start < hi_us:
        end = start + CHUNK_DAYS * day_us
        d0 = epoch + dt.timedelta(days=start // day_us - 1)
        need = [by_day[d0 + dt.timedelta(days=i)] for i in range(CHUNK_DAYS + 2)
                if d0 + dt.timedelta(days=i) in by_day]
        if not need:
            start = end
            continue
        files = "[" + ", ".join(f"'{p}'" for p in need) + "]"
        mids = EXPAND_CHUNK_SQL.format(half=HALF_US, slot=SLOT_US, stale=STALE_US, lo=start, hi=end,
                                       files=files, hb=hb_filter)
        # attach park, local time and the service day: the slot's own local date, or the
        # day before for windows that cross midnight
        x(f"""INSERT INTO slots
            WITH m AS ({mids}),
            m3 AS (
              SELECT m.aid, a.park_id, m.status, m.wait,
                     make_timestamptz(m.mid_us - {HALF_US}) AS slot_utc,
                     (m.mid_us - m.ts_us) / 60000000.0 AS age_min, p.timezone
              FROM m JOIN attractions a ON a.id = m.aid JOIN parks p ON p.id = a.park_id),
            m4 AS (SELECT *, timezone(timezone, slot_utc) AS slot_local FROM m3),
            c AS (
              SELECT m.*, w.date, w.open_utc, w.close_utc
              FROM m4 m JOIN windows w ON w.park_id = m.park_id AND w.date = CAST(m.slot_local AS DATE)
              WHERE m.slot_utc >= w.open_utc AND m.slot_utc < w.close_utc
              UNION ALL
              SELECT m.*, w.date, w.open_utc, w.close_utc
              FROM m4 m JOIN windows w ON w.park_id = m.park_id AND w.date = CAST(m.slot_local AS DATE) - 1
              WHERE m.slot_utc >= w.open_utc AND m.slot_utc < w.close_utc)
            SELECT aid, park_id, date, slot_utc, slot_local,
                   CAST(date_diff('minute', CAST(date AS TIMESTAMP), slot_local) // 15 AS INTEGER) AS ws,
                   CAST(floor(date_diff('minute', open_utc, slot_utc) / 15) AS INTEGER) AS ko,
                   CAST(floor((date_diff('minute', slot_utc, close_utc) - 1) / 15) AS INTEGER) AS kc,
                   status, wait, age_min
            FROM c
            -- overlapping service days (two windows containing one slot): keep the later day
            QUALIFY row_number() OVER (PARTITION BY aid, slot_utc ORDER BY date DESC) = 1""")
        start = end
    x("""CREATE OR REPLACE TABLE truth AS
        SELECT aid, park_id, date, slot_utc, slot_local, ws, ko, kc, wait::DOUBLE AS y
        FROM slots WHERE status = 'OPERATING' AND wait >= 5""")
    # Per ride-day-hour (wall clock) P50/P90 with >= 2 slots: the analogue of
    # queue_data_aggregates' hourly rows (sampleCount >= 2), which production's
    # hourly profile (/stats/hourly) and expectedError are built from.
    x("""CREATE OR REPLACE TABLE hour_stats AS
        SELECT aid, park_id, date, CAST(hour(slot_local) AS INTEGER) AS h,
               quantile_cont(y, 0.5) AS p50, quantile_cont(y, 0.9) AS p90, count(*) AS n
        FROM truth GROUP BY ALL HAVING count(*) >= 2""")
    x("""CREATE OR REPLACE TABLE ride_day AS
        WITH d AS (SELECT aid, park_id, date, quantile_cont(y, 0.9) AS p90, count(*) AS n_slots,
                          min(y) AS y_min, max(y) AS y_max
                   FROM truth GROUP BY ALL HAVING count(*) >= 8),
        hp AS (SELECT aid, date, max(p90) AS peak_h90 FROM hour_stats GROUP BY ALL)
        -- peak_h90 = production's expectedError truth: the day's max hourly P90
        SELECT d.*, hp.peak_h90 FROM d LEFT JOIN hp USING (aid, date)""")
    for t in ("slots", "truth", "ride_day"):
        stats[t] = x(f"SELECT count(*) FROM {t}").fetchone()[0]
    stats["truth_dates"] = [str(v) for v in x("SELECT min(date), max(date) FROM truth").fetchone()]
    stats["truth_rides"] = x("SELECT count(DISTINCT aid) FROM truth").fetchone()[0]
    stats["truth_parks"] = x("SELECT count(DISTINCT park_id) FROM truth").fetchone()[0]

    covariates(con, ml_service_dir)

    for t in ("parks", "attractions", "holidays", "weather", "tft", "cbd", "windows",
              "slots", "truth", "ride_day", "hour_stats", "park_day_cov"):
        order = {"slots": "ORDER BY date, park_id, aid, slot_utc",
                 "truth": "ORDER BY date, park_id, aid, slot_utc"}.get(t, "")
        x(f"COPY (SELECT * FROM {t} {order}) TO '{out}/{t}.parquet' (FORMAT parquet, COMPRESSION zstd)")
    # The same tables as one read-only DuckDB file the runner shards ATTACH: blocks are
    # read on demand through the buffer manager instead of loading ~40 M slots per shard.
    db = out.parent / "bench.duckdb"
    db.unlink(missing_ok=True)
    x(f"ATTACH '{db}' AS b")
    for t in ("parks", "attractions", "windows", "ride_day", "hour_stats", "park_day_cov", "tft", "cbd"):
        x(f"CREATE TABLE b.{t} AS SELECT * FROM {t}")
    x("CREATE TABLE b.slots AS SELECT * FROM slots ORDER BY date, park_id, aid, slot_utc")
    x("""CREATE TABLE b.truth AS SELECT *, CAST(dayofweek(date) IN (0, 6) AS INTEGER) AS we,
           dayofweek(date) AS dow FROM truth ORDER BY date, park_id, aid, slot_utc""")
    x("DETACH b")
    stats["build_seconds"] = round(time.monotonic() - t0, 1)
    (out / "build_stats.json").write_text(json.dumps(stats, indent=2, default=str))
    return stats


def covariates(con, ml_service_dir: Path | None) -> None:
    """Known-future covariates per park service day."""
    import pandas as pd

    hf = import_holiday_features(ml_service_dir)
    parks = con.execute("SELECT id, country_code, region_code, influencing_regions FROM parks").df()
    hol = con.execute("""SELECT country, CAST(date AS DATE) AS date, region,
                                holiday_type, is_nationwide FROM holidays""").df()
    index = hf.HolidayIndex(hol)
    days = con.execute("""
        WITH r AS (SELECT min(date) d0, max(date) + 400 d1 FROM windows WHERE date >= DATE '2025-06-01')
        SELECT p.id AS park_id, CAST(unnest(generate_series(r.d0, r.d1, INTERVAL 1 DAY)) AS DATE) AS date
        FROM parks p, r""").df()
    pmap = parks.set_index("id").to_dict("index")
    rows = []
    for pid, day in zip(days["park_id"], days["date"]):
        p = pmap[pid]
        d = day.date() if hasattr(day, "date") else day
        f = hf.park_day_holiday_features(
            p["country_code"], p["region_code"],
            hf.parse_influencing_regions(p["influencing_regions"]), d, index)
        f["park_id"], f["date"] = pid, d
        rows.append(f)
    hdf = pd.DataFrame(rows)
    con.register("hdf", hdf)
    con.execute("""CREATE OR REPLACE TABLE park_day_cov AS
        SELECT h.park_id, CAST(h.date AS DATE) AS date, dayofweek(CAST(h.date AS DATE)) AS dow,
               dayofweek(CAST(h.date AS DATE)) IN (0, 6) AS is_weekend,
               w.open_local, w.close_local, w.open_utc, w.close_utc,
               w.open_utc IS NOT NULL AS has_published_window,
               h.* EXCLUDE (park_id, date),
               s.is_holiday AS sched_is_holiday, s.is_bridge_day AS sched_is_bridge_day,
               -- the last write of ANY schedule row of the day, which is where the two
               -- flags above come from. The runner masks them with THIS, not with the
               -- OPERATING rows' updated_utc: a day whose OPERATING row was published
               -- before the origin but whose isHoliday/isBridgeDay came from a
               -- non-OPERATING row written after it used to be delivered unmasked
               -- (critic S11).
               s.flags_updated_utc AS sched_flags_updated_utc,
               wx.temp_max, wx.temp_min, wx.precip_sum, wx.wind_max, wx.weather_code,
               CASE WHEN wx.temp_max IS NOT NULL THEN 'ORACLE_actuals' END AS weather_source
        FROM hdf h
        LEFT JOIN windows w ON w.park_id = h.park_id AND w.date = CAST(h.date AS DATE)
        LEFT JOIN (SELECT park_id, CAST(date AS DATE) date, bool_or(is_holiday) is_holiday,
                          bool_or(is_bridge_day) is_bridge_day,
                          make_timestamptz(max(updated_us)) flags_updated_utc
                   FROM schedule GROUP BY ALL) s
               ON s.park_id = h.park_id AND s.date = CAST(h.date AS DATE)
        LEFT JOIN (SELECT park_id, CAST(date AS DATE) date, any_value(temp_max) temp_max,
                          any_value(temp_min) temp_min, any_value(precip_sum) precip_sum,
                          any_value(wind_max) wind_max, any_value(weather_code) weather_code
                   FROM weather WHERE data_type = 'historical' GROUP BY ALL) wx
               ON wx.park_id = h.park_id AND wx.date = CAST(h.date AS DATE)""")
    con.unregister("hdf")


def import_holiday_features(ml_service_dir: Path | None):
    """The ml-service implementation, so the benchmark's holiday flags are exactly
    the ones the production models are trained on (OR-semantics, PAR-816)."""
    import importlib
    import os

    candidates = [ml_service_dir] if ml_service_dir else []
    candidates += [Path(os.environ.get("MLBENCH_ML_SERVICE_DIR", "/opt/ml-service")),
                   Path(__file__).resolve().parents[2] / "ml-service"]
    for c in candidates:
        if c and (c / "holiday_features.py").exists():
            if str(c) not in sys.path:
                sys.path.insert(0, str(c))
            return importlib.import_module("holiday_features")
    raise ImportError("ml-service/holiday_features.py not found; set MLBENCH_ML_SERVICE_DIR")


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--export", required=True, help="export dir containing raw/")
    p.add_argument("--memory", default="6GB")
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--ml-service-dir", default=None)
    p.add_argument("--drop-heartbeats", action="store_true",
                   help="exclude is_heartbeat change-log rows from the truth expansion "
                        "(a heartbeat carries status AND wait forward and resets the 3 h "
                        "staleness clock) — recorded in build_stats.json")


def main(args: argparse.Namespace) -> int:
    exp = Path(args.export)
    con = connect(args.memory, args.threads, temp_dir=str(exp / "tmp"))
    stats = build(exp / "raw", exp / "parquet", con,
                  Path(args.ml_service_dir) if args.ml_service_dir else None,
                  drop_heartbeats=args.drop_heartbeats)
    print(json.dumps(stats, indent=2, default=str))
    return 0
