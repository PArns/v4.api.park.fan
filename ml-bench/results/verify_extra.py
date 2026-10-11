import duckdb
c = duckdb.connect()
c.execute("SET memory_limit='1600MB'"); c.execute("SET threads=2")
A = "/data/exports/20261009/parquet/ride_day.parquet"
B = "/data/exports/20261010-nohb/parquet/ride_day.parquet"
q = f"""
WITH a AS (SELECT aid, date, p90 FROM read_parquet('{A}')
           WHERE date BETWEEN DATE '2026-09-01' AND DATE '2026-10-07' AND p90 IS NOT NULL),
     b AS (SELECT aid, date, p90 FROM read_parquet('{B}')
           WHERE date BETWEEN DATE '2026-09-01' AND DATE '2026-10-07' AND p90 IS NOT NULL)
SELECT count(*) shared_ride_days,
       avg(a.p90) mean_p90_with_hb, avg(b.p90) mean_p90_without_hb,
       avg(b.p90) - avg(a.p90) delta,
       sum(CASE WHEN b.p90 > a.p90 THEN 1 ELSE 0 END) higher_without,
       sum(CASE WHEN b.p90 < a.p90 THEN 1 ELSE 0 END) lower_without,
       sum(CASE WHEN b.p90 = a.p90 THEN 1 ELSE 0 END) unchanged
FROM a JOIN b USING (aid, date)"""
print("RIDE-DAY P90 — heartbeats kept vs dropped, 2026-09-01..10-07")
for row in c.execute(q).fetchall():
    print("  shared_ride_days   ", row[0])
    print("  mean P90 with hb   ", f"{row[1]:.4f}")
    print("  mean P90 without   ", f"{row[2]:.4f}")
    print("  delta (without-with)", f"{row[3]:+.4f}")
    print("  ride-days higher without / lower / unchanged:", row[4], "/", row[5], "/", row[6])
