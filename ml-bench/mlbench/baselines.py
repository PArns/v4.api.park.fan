"""The baselines of BENCH-SPEC, as DuckDB SQL over one origin at a time.

All of them read only the truth strictly before the origin (``hw`` below is
the window of service days ``[c - 56, c)`` whose slots END before the park's
06:00 origin), or a monthly snapshot of the history cut at the first day of
the origin's month (never later than the origin).

Slot-level columns produced for every target slot (``tg``):

``snaive7``      same ride, same local slot, 7 days earlier (only if that day
                 lies before the origin, i.e. lead <= 6 d from the 06:00 origin)
``wt_med``       weekday-type median profile (56 d, weekday/weekend split with
                 >= 3 days per slot, else the all-days profile with >= 4 days),
                 with empirical ``wt_q80`` / ``wt_q95`` (D8)
``clim``         climatology: all history before the month snapshot, same local
                 slot, same weekday-type, same season as the target (>= 4 days),
                 else all seasons pooled. No ride has a previous year yet.
``h5``           H5 hybrid: opening-aligned 15-min profile in the first hour,
                 close-aligned 15-min profile in the last hour, hourly
                 wall-clock median linearly interpolated in between
``lvlh5_naive``  H5 × (level / window median daily P90); level = median of the
                 last 4 same-weekday P90s; first hour (rope-drop ramp) unscaled
``lvlh5_tft``    the same with the TFT forecast made before the origin
``lvlh5_cbd``    the same with the CatBoost-daily forecast made before the origin
``prod_served``  /plan/day composed tier: hourly P50 profile (365 d, >= 10 days
                 per hour, ride >= 20 days) interpolated over the open hours,
                 max-normalised to the day level (TFT, else CatBoost daily),
                 rounded to 5; leads >= 3 d only (d0–d2 serve CatBoost hourly,
                 which is not stored per lead and cannot be reconstructed)
``oracle_level`` H5 scaled with the TRUE daily P90 (decomposition only)
``oracle_shape`` the TRUE shape (y / true P90) times the naive level (decomposition only)
"""

from __future__ import annotations

from .config import BenchConfig, season_sql

SLOT_MODELS = ["snaive7", "wt_med", "clim", "h5", "lvlh5_naive", "lvlh5_tft", "lvlh5_cbd",
               "prod_served"]
ORACLES = ["oracle_level", "oracle_shape"]
REF_CANDIDATES = ["snaive7", "wt_med", "clim"]
INTRADAY_MODELS = ["persistence", "snaive7", "wt_med", "clim", "h5", "lvlh5_naive", "lvlh5_tft"]
INTRADAY_REFS = ["persistence", "snaive7", "wt_med", "clim"]
LEVEL_SOURCES = ["lvl_naive4", "lvl_wt56", "lvl_snaive7", "lvl_clim", "lvl_tft", "lvl_cbd"]

WK = "CASE WHEN grouping(we) = 1 THEN 2 ELSE CAST(we AS INTEGER) END"


def origin_sql(c: str, hour: int) -> str:
    """Per-park origin instant: park-local ``c hour:00`` → UTC (DST-correct)."""
    return f"""
        SELECT p.id AS park_id, p.timezone,
               timezone(p.timezone, CAST(DATE '{c}' AS TIMESTAMP) + INTERVAL {int(hour)} HOUR) AS origin_utc
        FROM parks p"""


def window_tables(con, c: str, cfg: BenchConfig) -> None:
    """hw (window slots), profiles and level statistics at origin date c."""
    x = con.execute
    W = cfg.window_days
    x(f"CREATE OR REPLACE TEMP TABLE o AS {origin_sql(c, cfg.origin_hour_local)}")
    x(f"""CREATE OR REPLACE TEMP TABLE hw AS
        SELECT t.aid, t.park_id, t.date, t.ws, t.ko, t.kc, t.y, t.we, t.dow
        FROM truth t JOIN o ON o.park_id = t.park_id
        WHERE t.date >= DATE '{c}' - {W} AND t.date < DATE '{c}'
          AND t.slot_utc + INTERVAL 15 MINUTE <= o.origin_utc""")
    lo, la = cfg.min_days_daytype, cfg.min_days_all
    keep = f"n >= CASE WHEN wk = 2 THEN {la} ELSE {lo} END"
    x(f"""CREATE OR REPLACE TEMP TABLE p15w AS SELECT * FROM (
        SELECT aid, {WK} AS wk, ws AS idx, median(y) med, quantile_cont(y, 0.8) q80,
               quantile_cont(y, 0.95) q95, count(*) n
        FROM hw GROUP BY GROUPING SETS ((aid, ws, we), (aid, ws))) WHERE {keep}""")
    for name, col in (("p15o", "ko"), ("p15x", "kc")):
        x(f"""CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM (
            SELECT aid, {WK} AS wk, {col} AS idx, median(y) med, count(*) n
            FROM hw GROUP BY GROUPING SETS ((aid, {col}, we), (aid, {col}))) WHERE {keep}""")
    x(f"""CREATE OR REPLACE TEMP TABLE p60 AS SELECT * FROM (
        SELECT aid, {WK} AS wk, ws // 4 AS idx, median(y) med, count(DISTINCT date) n
        FROM hw GROUP BY GROUPING SETS ((aid, ws // 4, we), (aid, ws // 4))) WHERE {keep}""")
    # ex-ante busy flag: ride's q90 of window slots
    x("CREATE OR REPLACE TEMP TABLE rs AS SELECT aid, quantile_cont(y, 0.9) q90 FROM hw GROUP BY aid")
    # window ride-day P90s (same >= 8-slot rule as ride_day)
    x(f"""CREATE OR REPLACE TEMP TABLE wrd AS
        SELECT aid, date, any_value(we) we, any_value(dow) dow, quantile_cont(y, 0.9) p90,
               max(y) peak, arg_min(y, ko) open_wait
        FROM hw GROUP BY aid, date HAVING count(*) >= {cfg.min_ride_day_slots}""")
    x(f"""CREATE OR REPLACE TEMP TABLE dl AS
        SELECT aid, {WK} AS wk, median(p90) lvl, count(*) nd
        FROM wrd GROUP BY GROUPING SETS ((aid, we), (aid))""")
    x(f"""CREATE OR REPLACE TEMP TABLE n4 AS
        SELECT aid, dow, median(p90) lvl, count(*) nd FROM (
          SELECT *, row_number() OVER (PARTITION BY aid, dow ORDER BY date DESC) rn FROM wrd)
        WHERE rn <= {cfg.naive_level_weeks} GROUP BY aid, dow HAVING count(*) >= {cfg.naive_level_min}""")
    # production rope-drop rule on the window (median day peak and median opening wait)
    x("""CREATE OR REPLACE TEMP TABLE ropehist AS
        SELECT aid, round(median(peak)) busy_peak, round(median(open_wait)) open_wait
        FROM wrd GROUP BY aid""")


def snapshot_tables(con, m0: str, cfg: BenchConfig) -> None:
    """Month snapshot (history strictly before m0 = first day of the origin's month)."""
    x = con.execute
    la = cfg.min_days_all
    x(f"""CREATE OR REPLACE VIEW snap_hist AS
        SELECT aid, park_id, date, ws, y, we, {season_sql('date')} AS season
        FROM truth WHERE date < DATE '{m0}'""")
    x(f"""CREATE OR REPLACE TABLE clim AS SELECT * FROM (
        SELECT aid, coalesce(season, 'all') season, CAST(we AS INTEGER) wk, ws idx,
               median(y) med, count(*) n
        FROM snap_hist GROUP BY GROUPING SETS ((aid, season, we, ws), (aid, we, ws)))
        WHERE n >= {la}""")
    x(f"""CREATE OR REPLACE TABLE clim_lvl AS SELECT * FROM (
        SELECT aid, coalesce(season, 'all') season, CAST(we AS INTEGER) wk, median(p90) lvl, count(*) n
        FROM (SELECT r.aid, r.p90, dayofweek(r.date) IN (0, 6) we, {season_sql('r.date')} season
              FROM ride_day r WHERE r.date < DATE '{m0}')
        GROUP BY GROUPING SETS ((aid, season, we), (aid, we))) WHERE n >= {la}""")
    # production hourly P50 profile: median of ride-day-hour means
    x(f"""CREATE OR REPLACE TABLE prodprof AS
        WITH hm AS (SELECT aid, date, ws // 4 h, avg(y) m FROM snap_hist
                    WHERE date >= DATE '{m0}' - {cfg.prod_profile_days} GROUP BY ALL),
        rd AS (SELECT aid FROM hm GROUP BY aid HAVING count(DISTINCT date) >= {cfg.prod_min_ride_days})
        SELECT hm.aid, h, median(m) v FROM hm JOIN rd USING (aid)
        GROUP BY ALL HAVING count(*) >= {cfg.prod_min_hour_days}""")
    # typical-day peak per park: median over operating days of the headliner-mean P90
    x(f"""CREATE OR REPLACE TABLE typpeak AS
        SELECT park_id, median(lvl) typical_peak, count(*) ndays FROM (
          SELECT r.park_id, r.date, avg(r.p90) lvl FROM ride_day r JOIN rides a ON a.aid = r.aid
          WHERE a.is_headliner AND r.date < DATE '{m0}' GROUP BY ALL)
        GROUP BY park_id HAVING count(*) >= {cfg.typical_peak_min_days}""")


def asof_levels_sql(table: str, c: str, max_lead: int) -> str:
    """Latest daily-level forecast created before each park's origin, per (ride, target)."""
    return f"""
        SELECT f.aid, f.target_date, arg_max(f.peak, f.created_utc) AS peak
        FROM {table} f JOIN rides r ON r.aid = f.aid JOIN o ON o.park_id = r.park_id
        WHERE f.target_date >= DATE '{c}' AND f.target_date <= DATE '{c}' + {int(max_lead)}
          AND f.created_utc < o.origin_utc
        GROUP BY ALL"""


def target_tables(con, c: str, leads: list[int], cfg: BenchConfig) -> None:
    """tg: every in-window status slot of the target days c+L with all baseline columns."""
    x = con.execute
    lead_list = ",".join(str(int(v)) for v in leads)
    maxl = max(leads)
    x(f"CREATE OR REPLACE TEMP TABLE tft_o AS {asof_levels_sql('tft', c, maxl)}")
    x(f"CREATE OR REPLACE TEMP TABLE cbd_o AS {asof_levels_sql('cbd', c, maxl)}")
    x(f"""CREATE OR REPLACE TEMP TABLE tg0 AS
        SELECT s.aid, s.park_id, s.date, s.slot_utc, s.ws, s.ko, s.kc, s.status,
               CASE WHEN s.status = 'OPERATING' AND s.wait >= 5 THEN s.wait::DOUBLE END AS y,
               CAST(s.date - DATE '{c}' AS INTEGER) AS L,
               CAST(dayofweek(s.date) IN (0, 6) AS INTEGER) AS we, dayofweek(s.date) AS dow,
               {season_sql('s.date')} AS season
        FROM slots s JOIN o ON o.park_id = s.park_id
        WHERE s.date IN (SELECT DATE '{c}' + unnest([{lead_list}]))
          AND s.slot_utc >= o.origin_utc
          AND s.aid IN (SELECT aid FROM rs)""")
    # profile lookups: day-type first, all-days fallback (tables are pre-filtered on n)
    x("""CREATE OR REPLACE TEMP TABLE tg1 AS
        SELECT t.*,
          coalesce(a1.med, a2.med) wt_med, coalesce(a1.q80, a2.q80) wt_q80, coalesce(a1.q95, a2.q95) wt_q95,
          coalesce(o1.med, o2.med) p_open, coalesce(x1.med, x2.med) p_close,
          coalesce(h1.med, h2.med) h0, coalesce(hm1.med, hm2.med) hm, coalesce(hp1.med, hp2.med) hp,
          coalesce(c1.med, c2.med) clim,
          coalesce(d1.lvl, d2.lvl) ref_lvl, n4.lvl lvl_naive4, rs.q90 >= {busy} AS busy,
          tf.peak lvl_tft, cb.peak lvl_cbd, rd.p90 true_p90
        FROM tg0 t
        LEFT JOIN p15w a1 ON a1.aid = t.aid AND a1.wk = t.we AND a1.idx = t.ws
        LEFT JOIN p15w a2 ON a2.aid = t.aid AND a2.wk = 2 AND a2.idx = t.ws
        LEFT JOIN p15o o1 ON o1.aid = t.aid AND o1.wk = t.we AND o1.idx = t.ko
        LEFT JOIN p15o o2 ON o2.aid = t.aid AND o2.wk = 2 AND o2.idx = t.ko
        LEFT JOIN p15x x1 ON x1.aid = t.aid AND x1.wk = t.we AND x1.idx = t.kc
        LEFT JOIN p15x x2 ON x2.aid = t.aid AND x2.wk = 2 AND x2.idx = t.kc
        LEFT JOIN p60 h1 ON h1.aid = t.aid AND h1.wk = t.we AND h1.idx = t.ws // 4
        LEFT JOIN p60 h2 ON h2.aid = t.aid AND h2.wk = 2 AND h2.idx = t.ws // 4
        LEFT JOIN p60 hm1 ON hm1.aid = t.aid AND hm1.wk = t.we AND hm1.idx = t.ws // 4 - 1
        LEFT JOIN p60 hm2 ON hm2.aid = t.aid AND hm2.wk = 2 AND hm2.idx = t.ws // 4 - 1
        LEFT JOIN p60 hp1 ON hp1.aid = t.aid AND hp1.wk = t.we AND hp1.idx = t.ws // 4 + 1
        LEFT JOIN p60 hp2 ON hp2.aid = t.aid AND hp2.wk = 2 AND hp2.idx = t.ws // 4 + 1
        LEFT JOIN clim c1 ON c1.aid = t.aid AND c1.season = t.season AND c1.wk = t.we AND c1.idx = t.ws
        LEFT JOIN clim c2 ON c2.aid = t.aid AND c2.season = 'all' AND c2.wk = t.we AND c2.idx = t.ws
        LEFT JOIN dl d1 ON d1.aid = t.aid AND d1.wk = t.we AND d1.nd >= {lo}
        LEFT JOIN dl d2 ON d2.aid = t.aid AND d2.wk = 2 AND d2.nd >= {la}
        LEFT JOIN n4 ON n4.aid = t.aid AND n4.dow = t.dow
        LEFT JOIN rs ON rs.aid = t.aid
        LEFT JOIN tft_o tf ON tf.aid = t.aid AND tf.target_date = t.date
        LEFT JOIN cbd_o cb ON cb.aid = t.aid AND cb.target_date = t.date
        LEFT JOIN ride_day rd ON rd.aid = t.aid AND rd.date = t.date""".format(
        busy=cfg.busy_q90_min, lo=cfg.min_days_daytype, la=cfg.min_days_all))
    # snaive7: the same local slot 7 days earlier, only if that day is before the origin
    x(f"""CREATE OR REPLACE TEMP TABLE tg2 AS
        SELECT t.*, CASE WHEN t.date - 7 < DATE '{c}' THEN s7.y END AS snaive7,
          -- hourly median linearly interpolated between hour centres (H5 middle part)
          CASE WHEN t.h0 IS NULL THEN NULL
               WHEN (t.ws % 4) * 15 + 7.5 < 30
                 THEN t.h0 + ((30 - ((t.ws % 4) * 15 + 7.5)) / 60.0) * (coalesce(t.hm, t.h0) - t.h0)
               ELSE t.h0 + ((((t.ws % 4) * 15 + 7.5) - 30) / 60.0) * (coalesce(t.hp, t.h0) - t.h0)
          END AS h_lin
        FROM tg1 t LEFT JOIN (SELECT aid, date, ws, y FROM truth
                              WHERE date >= DATE '{c}' - 7 AND date < DATE '{c}') s7
               ON s7.aid = t.aid AND s7.date = t.date - 7 AND s7.ws = t.ws""")
    x("""CREATE OR REPLACE TEMP TABLE tg3 AS
        SELECT t.*,
          coalesce(CASE WHEN t.ko < 4 THEN t.p_open WHEN t.kc < 4 THEN t.p_close END, t.h_lin, t.wt_med) AS h5
        FROM tg2 t""")
    x("""CREATE OR REPLACE TEMP TABLE tg AS
        SELECT t.*,
          CASE WHEN t.ko < 4 THEN t.h5 WHEN t.ref_lvl > 0 THEN t.h5 * t.lvl_naive4 / t.ref_lvl END AS lvlh5_naive,
          CASE WHEN t.ko < 4 AND t.lvl_tft IS NOT NULL THEN t.h5
               WHEN t.ref_lvl > 0 THEN t.h5 * t.lvl_tft / t.ref_lvl END AS lvlh5_tft,
          CASE WHEN t.ko < 4 AND t.lvl_cbd IS NOT NULL THEN t.h5
               WHEN t.ref_lvl > 0 THEN t.h5 * t.lvl_cbd / t.ref_lvl END AS lvlh5_cbd,
          CASE WHEN t.ko < 4 AND t.true_p90 IS NOT NULL THEN t.h5
               WHEN t.ref_lvl > 0 THEN t.h5 * t.true_p90 / t.ref_lvl END AS oracle_level,
          CASE WHEN t.true_p90 > 0 THEN t.y / t.true_p90 * t.lvl_naive4 END AS oracle_shape
        FROM tg3 t""")
    prod_served(con, cfg)
    for t in ("tg0", "tg1", "tg2", "tg3"):
        x(f"DROP TABLE IF EXISTS {t}")


def prod_served(con, cfg: BenchConfig) -> None:
    """composeDayCurve (src/common/utils/day-shape.util.ts) over the target days."""
    x = con.execute
    # open hours per (ride, target day): openHour .. unfolded closeHour, inclusive
    x(f"""CREATE OR REPLACE TEMP TABLE ph AS
        WITH rdays AS (
          SELECT DISTINCT t.aid, t.park_id, t.date, coalesce(t.lvl_tft, t.lvl_cbd) AS level
          FROM tg t WHERE t.L >= {cfg.prod_min_lead} AND coalesce(t.lvl_tft, t.lvl_cbd) IS NOT NULL
            AND t.aid IN (SELECT aid FROM prodprof)),
        hrs AS (
          SELECT r.*, unnest(range(hour(w.open_local),
                 CASE WHEN CAST(w.close_local AS DATE) > w.date THEN hour(w.close_local) + 24
                      ELSE hour(w.close_local) END + 1)) AS h
          FROM rdays r JOIN windows w ON w.park_id = r.park_id AND w.date = r.date),
        lo AS (SELECT h.*, p.h AS ph_lo, p.v AS pv_lo FROM hrs h ASOF LEFT JOIN prodprof p
               ON p.aid = h.aid AND p.h <= h.h),
        hi AS (SELECT l.*, p.h AS ph_hi, p.v AS pv_hi FROM lo l ASOF LEFT JOIN
               (SELECT aid, -h AS nh, h, v FROM prodprof) p ON p.aid = l.aid AND p.nh <= -l.h),
        iv AS (SELECT *, CASE WHEN ph_lo IS NULL THEN pv_hi WHEN ph_hi IS NULL THEN pv_lo
                              WHEN ph_hi = ph_lo THEN pv_lo
                              ELSE pv_lo + (pv_hi - pv_lo) * (h - ph_lo) / (ph_hi - ph_lo) END AS v
               FROM hi)
        SELECT aid, date, h, v, level, max(v) OVER (PARTITION BY aid, date) AS peak FROM iv""")
    x("""CREATE OR REPLACE TEMP TABLE tgp AS
        SELECT t.*, CASE WHEN ph.peak > 0 THEN (CASE WHEN ph.v * ph.level / ph.peak < 2.5 THEN 0
                                              ELSE floor((ph.v * ph.level / ph.peak + 2.5) / 5) * 5 END)
                         WHEN ph.peak IS NOT NULL THEN 0 END AS prod_served
        FROM tg t LEFT JOIN ph ON ph.aid = t.aid AND ph.date = t.date AND ph.h = t.ws // 4""")
    x("DROP TABLE tg")
    x("ALTER TABLE tgp RENAME TO tg")


def daily_levels(con, c: str, leads: list[int], cfg: BenchConfig) -> None:
    """lv: one row per (headliner, target day c+L) with every level source and the truth."""
    x = con.execute
    lead_list = ",".join(str(int(v)) for v in leads)
    maxl = max(leads)
    x(f"CREATE OR REPLACE TEMP TABLE tft_d AS {asof_levels_sql('tft', c, maxl)}")
    x(f"CREATE OR REPLACE TEMP TABLE cbd_d AS {asof_levels_sql('cbd', c, maxl)}")
    x(f"""CREATE OR REPLACE TEMP TABLE lv AS
        WITH tgt AS (
          SELECT r.aid, r.park_id, L, DATE '{c}' + L AS date
          FROM rides r, (SELECT unnest([{lead_list}]) AS L)
          WHERE r.is_headliner AND r.aid IN (SELECT aid FROM rs))
        SELECT DATE '{c}' AS origin, t.aid, t.park_id, t.L, t.date,
               dayofweek(t.date) IN (0, 6) AS we, rd.p90 AS true_p90,
               n4.lvl AS lvl_naive4, coalesce(d1.lvl, d2.lvl) AS lvl_wt56,
               CASE WHEN t.L <= 6 THEN s7.p90 END AS lvl_snaive7,
               coalesce(c1.lvl, c2.lvl) AS lvl_clim, tf.peak AS lvl_tft, cb.peak AS lvl_cbd,
               cov.has_published_window
        FROM tgt t
        LEFT JOIN ride_day rd ON rd.aid = t.aid AND rd.date = t.date
        LEFT JOIN n4 ON n4.aid = t.aid AND n4.dow = dayofweek(t.date)
        LEFT JOIN dl d1 ON d1.aid = t.aid AND d1.wk = CAST(dayofweek(t.date) IN (0, 6) AS INTEGER)
             AND d1.nd >= {cfg.min_days_daytype}
        LEFT JOIN dl d2 ON d2.aid = t.aid AND d2.wk = 2 AND d2.nd >= {cfg.min_days_all}
        LEFT JOIN ride_day s7 ON s7.aid = t.aid AND s7.date = t.date - 7
        LEFT JOIN clim_lvl c1 ON c1.aid = t.aid AND c1.season = {season_sql('t.date')}
             AND c1.wk = CAST(dayofweek(t.date) IN (0, 6) AS INTEGER)
        LEFT JOIN clim_lvl c2 ON c2.aid = t.aid AND c2.season = 'all'
             AND c2.wk = CAST(dayofweek(t.date) IN (0, 6) AS INTEGER)
        LEFT JOIN tft_d tf ON tf.aid = t.aid AND tf.target_date = t.date
        LEFT JOIN cbd_d cb ON cb.aid = t.aid AND cb.target_date = t.date
        LEFT JOIN park_day_cov cov ON cov.park_id = t.park_id AND cov.date = t.date""")
