# Database Health Runbook

Quick reference for checking DB performance, bloat, and index health.

**Connect:**
```bash
sshpass -p 'REDACTED' ssh <user>@<dockerhost> \
  "docker exec postgres-\$(docker ps --format '{{.Names}}' | grep postgres) \
   psql -U parkfan -d parkfan -c \"<QUERY>\""
```

Or interactively:
```bash
sshpass -p 'REDACTED' ssh <user>@<dockerhost>
docker exec -it $(docker ps --format '{{.Names}}' | grep postgres) psql -U parkfan -d parkfan
```

---

## 1. Table Sizes & Bloat

```sql
SELECT
  relname AS table_name,
  pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
  pg_size_pretty(pg_relation_size(relid)) AS table_size,
  n_live_tup AS rows,
  n_dead_tup,
  CASE WHEN n_live_tup > 0
    THEN ROUND(n_dead_tup::numeric / n_live_tup * 100, 1)
    ELSE 0 END AS dead_pct,
  seq_scan,
  idx_scan,
  last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 20;
```

**What to look for:**
- `dead_pct > 10%` → run `VACUUM ANALYZE <table>`
- `total_size` unexpectedly large → check if cleanup jobs are running (see §4)
- `seq_scan` very high on large tables → missing index (see §2)

**Known hot tables (2026-04-06 baseline):**

| Table | Size | Rows | Notes |
|---|---|---|---|
| `ml_prediction_request_log` | ~707 MB | ~1.9M | Cleaned daily, 30-day retention |
| `wait_time_predictions` | ~386 MB | ~1M | 16% dead tuples normal after bulk deletes |
| `prediction_accuracy` | ~233 MB | ~400K | Heavy CTE queries expected |
| `holidays` | ~166 MB | ~220K | Heavily indexed, fine |

---

## 2. Unused Indexes (Wasted Space)

```sql
SELECT
  relname AS table_name,
  indexrelname AS index_name,
  pg_size_pretty(pg_relation_size(indexrelid)) AS wasted_size,
  idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
  AND idx_scan = 0
  AND indexrelname NOT LIKE '%_pkey'
ORDER BY pg_relation_size(indexrelid) DESC;
```

**Known unused indexes (do NOT drop without checking):**

| Index | Table | Size | Why unused |
|---|---|---|---|
| `idx_*_name_trgm` | parks, attractions, shows, restaurants | ~2 MB total | Trigram search — not triggered by current queries |
| `idx_schedule_operating_times` | schedule_entries | 456 kB | Only used by warmupUpcomingParks (hourly) |
| `IDX_2af5d518...` (modelVersion) | wait_time_predictions | 24 MB | Rarely queried by model version |

**Stats reset date:** `SELECT pg_stat_reset();` — only run if you need fresh counters (resets all stats).

---

## 3. Seq Scan Analysis

```sql
SELECT
  relname AS table_name,
  seq_scan,
  seq_tup_read,
  idx_scan,
  CASE WHEN idx_scan > 0
    THEN ROUND(seq_scan::numeric / idx_scan * 100, 1)
    ELSE 9999 END AS seq_pct,
  n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND seq_scan > 100
ORDER BY seq_tup_read DESC
LIMIT 15;
```

**Expected high seq_scan tables (not problems):**
- `parks` (157 rows), `destinations` (96 rows), `ml_models` (9 rows) — tiny tables, Postgres always seq scans these
- `attractions` (5598 rows) — fits in memory, Postgres prefers seq scan for many query patterns

**Investigate if:**
- A large table (>10K rows) has `seq_pct > 5%` — means many queries aren't using indexes
- `seq_tup_read` suddenly jumps on `prediction_accuracy` or `wait_time_predictions`

---

## 4. Cleanup Job Status

Check if the daily cleanup ran recently:

```sql
-- ml_prediction_request_log: should have max ~30 days of data
SELECT
  MIN(created_at) AS oldest,
  MAX(created_at) AS newest,
  COUNT(*) AS total_rows,
  pg_size_pretty(pg_relation_size('ml_prediction_request_log')) AS size
FROM ml_prediction_request_log;

-- wait_time_predictions: should have max ~90 days (hourly) + ~60 days (daily)
SELECT
  prediction_type,
  MIN("predictedTime") AS oldest,
  MAX("predictedTime") AS newest,
  COUNT(*) AS count
FROM wait_time_predictions
GROUP BY prediction_type;
```

**If ml_prediction_request_log is older than 30 days**, the `ml-monitoring / cleanup` BullMQ job isn't running.
Check Bull Board: `http://<host>/admin/queues` → ml-monitoring → cleanup.

---

## 5. Dead Tuple Cleanup

```sql
-- Tables needing VACUUM
SELECT relname, n_dead_tup, n_live_tup,
  ROUND(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 1) AS dead_pct,
  last_autovacuum, last_vacuum
FROM pg_stat_user_tables
WHERE schemaname = 'public' AND n_dead_tup > 5000
ORDER BY n_dead_tup DESC;
```

Manual vacuum if autovacuum isn't keeping up:
```sql
VACUUM ANALYZE wait_time_predictions;
VACUUM ANALYZE prediction_accuracy;
```

---

## 6. Slow Query Logging

TypeORM logs queries taking **>500ms** automatically (configured in `typeorm.config.ts`).
Look for them in API container logs:

```bash
sshpass -p 'REDACTED' ssh <user>@<dockerhost> \
  "docker logs <api-container> 2>&1 | grep 'query.*[0-9]\{4,\}ms'"
```

To enable PostgreSQL-side slow query logging (requires PG restart — avoid in prod):
```sql
-- Check current setting
SHOW log_min_duration_statement;
-- Enable (500ms threshold):
ALTER SYSTEM SET log_min_duration_statement = 500;
SELECT pg_reload_conf();
```

**`pg_stat_statements`** is NOT enabled (requires restart + `shared_preload_libraries` change).
TypeORM's `maxQueryExecutionTime: 500` in `typeorm.config.ts` covers the same use case without DB changes.

---

## 7. Index Health on Key Tables

```sql
SELECT
  t.relname AS table_name,
  i.relname AS index_name,
  array_agg(a.attname ORDER BY x.n) AS columns,
  pg_size_pretty(pg_relation_size(i.oid)) AS size,
  s.idx_scan
FROM pg_class t
JOIN pg_index ix ON t.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, n) ON true
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
WHERE t.relname IN (
  'wait_time_predictions',
  'prediction_accuracy',
  'schedule_entries',
  'attractions'
)
GROUP BY t.relname, i.relname, i.oid, s.idx_scan
ORDER BY t.relname, s.idx_scan DESC NULLS LAST;
```

---

## 8. Quick OOM / Restart Check

```bash
# Restart count
docker inspect <api-container> --format '{{.RestartCount}} restarts'

# OOM crashes in current container logs
docker logs <api-container> 2>&1 | grep "FATAL ERROR\|heap out of memory"

# Full crash context (what was running before each OOM)
docker logs <api-container> 2>&1 | grep -B 20 'FATAL ERROR' | grep -E '(LOG|WARN|Processor|warmup)'
```

**Node.js heap:** `--max-old-space-size=6144` (6 GB) — set in Dockerfile CMD.
**Previously crashed at:** 4 GB limit (`NODE_OPTIONS=--max-old-space-size=4096` in Coolify env).

---

## 9. Container Overview

```bash
sshpass -p 'REDACTED' ssh <user>@<dockerhost> \
  "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'"
```

| Service | Port | Notes |
|---|---|---|
| API (NestJS) | 3000 | `api-m08og...` |
| ML Service (Python) | 8000 (internal) | `ml-service-m08og...` |
| PostgreSQL (TimescaleDB) | 5432 (internal) | `postgres-m08og...` |
| Redis | 6379 (internal) | `redis-m08og...` |
| Bull Board | (via proxy) | `bull-board-m08og...` |
