# Database health runbook

Copy-paste checks for the production database: how to connect safely, table and index health, dead
tuples, the slow-query logs, dropping an index without blocking, and OOM checks. Open it when the database
or the API is slow. Two incidents have their own pages: [TimescaleDB lock queues](timescaledb-lock-queues.md)
(jobs waiting on `ACCESS EXCLUSIVE`) and [nightly DB maintenance](nightly-db-maintenance.md) (the VACUUM,
REINDEX and retention jobs and the windows around the 06:00 training).

---

## 0. Time limits on ad-hoc SQL

Connect **only** through the wrapper, on the host, from the repository:

```bash
scripts/prod-psql.sh -c '<QUERY>'     # or < query.sql, or no arguments for interactive
```

It finds the postgres container (the name carries a Coolify deploy id; it refuses to guess between two
matches) and sets three limits on that session via `PGOPTIONS`:

| Setting                               | Wrapper default | Server default | Stops                                                      | Override                          |
| ------------------------------------- | --------------- | -------------- | ---------------------------------------------------------- | --------------------------------- |
| `statement_timeout`                   | `5min`          | off            | one statement running forever                              | `PARKFAN_PSQL_STATEMENT_TIMEOUT`  |
| `idle_in_transaction_session_timeout` | `1min`          | off            | a session parked between `BEGIN` and `COMMIT`              | `PARKFAN_PSQL_IDLE_TX_TIMEOUT`    |
| `lock_timeout`                        | `10s`           | off            | a writing session queueing on a lock and collecting waiters | `PARKFAN_PSQL_LOCK_TIMEOUT`      |

`PARKFAN_PSQL_CONTAINER`, `PARKFAN_PSQL_USER` and `PARKFAN_PSQL_DB` (both `parkfan`) pick the target.

**Why.** On 2026-09-28 a research `SELECT` started through `docker exec … psql` stayed open after the
process that launched it had exited. The `queue_data` compression policy queued for a strong lock behind
its snapshot, every reader queued behind the policy, the TypeORM pool hit its 15-second acquire timeout,
and the whole public API, `/v1/health` included, answered 500 for 32 minutes. The query was fine; it had
no deadline.

**Why 5 minutes.** Over the 117 days since the last `pg_stat_statements` reset (2026-06-03), 6 of 4,962
recorded statements ever exceeded one minute and 2 exceeded five, both background jobs.

**Why per session and not on the role.** The application pool (103 connections at the time) and the
TimescaleDB background workers connect as the same `parkfan` role, so `ALTER ROLE` or `ALTER DATABASE`
would cap them too. `idle_in_transaction_session_timeout` is a separate gap: `statement_timeout` never
fires while no statement runs. An idle session outside a transaction holds nothing, so there is no
`idle_session_timeout`.

---

## 1. Table sizes and bloat

```sql
SELECT relname AS table_name,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
       n_live_tup AS rows, n_dead_tup,
       CASE WHEN n_live_tup > 0 THEN ROUND(n_dead_tup::numeric / n_live_tup * 100, 1) ELSE 0 END AS dead_pct,
       seq_scan, idx_scan, last_autovacuum
FROM pg_stat_user_tables WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(relid) DESC LIMIT 20;
```

`dead_pct > 10` → VACUUM (§5). An unexpectedly large table → check its retention job ([nightly DB
maintenance](nightly-db-maintenance.md)). A high `seq_scan` on a large table → a missing index (§3).

## 2. Unused indexes

```sql
SELECT relname AS table_name, indexrelname AS index_name,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size, idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'public' AND idx_scan = 0 AND indexrelname NOT LIKE '%_pkey'
ORDER BY pg_relation_size(indexrelid) DESC;
```

Before dropping one, check what declares it. The `idx_*_name_trgm` trigram indexes are created by
`SearchService.initializeFuzzySearchIndices` (`synchronize: false` in the entities) and serve the Postgres
search fallback; `idx_schedule_operating_times` is declared on `ScheduleEntry`. `SELECT pg_stat_reset();`
resets every counter, so only run it when fresh counters are worth losing the history.

### 2a. Dropping a redundant index without blocking (PAR-872)

Production runs `synchronize`, so **an `@Index` removed from an entity is dropped at the next boot with a
plain, blocking `DROP INDEX`**. On a hypertable that takes `ACCESS EXCLUSIVE` on the parent and every chunk
at once (21 chunks → 42 locks in one transaction, TimescaleDB 2.24 / PG 18). Always:

1. drop the index by hand, without blocking, **before** the deploy;
2. ship the entity change in the same release, so `synchronize` finds nothing left.

`DROP INDEX CONCURRENTLY` on a hypertable's index fails while chunk indexes exist ("does not support
dropping multiple objects"), but works on each chunk index, compressed chunks included, and then on the
parent. New chunks do not get the index back. Run outside 01:00–09:30 UTC, outside :00–:12 of the hour,
and away from the table's compression job
(`SELECT job_id, next_start FROM timescaledb_information.job_stats WHERE hypertable_name = '…';`).

```bash
# lock_timeout 2 s: a statement that cannot get its lock gives up instead of queueing readers; rerun it.
PARKFAN_PSQL_LOCK_TIMEOUT=2s scripts/prod-psql.sh <<'SQL'
SELECT format('DROP INDEX CONCURRENTLY IF EXISTS %I.%I;', n.nspname, c.relname)
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind = 'i' AND n.nspname = '_timescaledb_internal'
   AND c.relname LIKE '%chunk_<INDEX_NAME>'
\gexec
-- then the parent; if a chunk appeared in between this fails with "multiple objects": run the block again
DROP INDEX CONCURRENTLY IF EXISTS public."<INDEX_NAME>";
SQL
```

On a plain table one `DROP INDEX CONCURRENTLY IF EXISTS public."<name>";` is enough.

Dropped this way in PAR-872 (identical key columns, no predicate; the kept one is constraint-backed or
covering): `queue_data`'s `IDX_aa19b21038bdfe8d295a5f2f2c` (prefix of `idx_queue_data_covering`, 297 MB),
`weather_data`'s `idx_weather_1` (same as the primary key), and the non-unique twins of `UNIQUE`
constraints on `destinations` (slug, externalId), `restaurants` and `shows` (externalId).
`test/e2e/db-cleanup-writes.e2e-spec.ts` fails if `synchronize` would build any of them again.

## 3. Sequential scans

```sql
SELECT relname, seq_scan, seq_tup_read, idx_scan,
       CASE WHEN idx_scan > 0 THEN ROUND(seq_scan::numeric / idx_scan * 100, 1) ELSE 9999 END AS seq_pct,
       n_live_tup AS rows
FROM pg_stat_user_tables WHERE schemaname = 'public' AND seq_scan > 100
ORDER BY seq_tup_read DESC LIMIT 15;
```

Small tables (`parks`, `destinations`, `ml_models`, and `attractions` at a few thousand rows) are always
seq-scanned and fine. Investigate a table over 10k rows with `seq_pct > 5`, or a sudden jump in
`seq_tup_read` on `prediction_accuracy` or `wait_time_predictions`.

## 4. Retention jobs ran?

```sql
-- wait_time_predictions: chunks past 90 days (createdAt) are dropped by predictions/cleanup-old at 03:30 UTC.
SELECT "predictionType", MIN("createdAt") AS oldest_created, MIN("predictedTime") AS oldest_predicted, COUNT(*)
FROM wait_time_predictions GROUP BY "predictionType";
```

A failed nightly job shows in Bull Board (port 3001, see [deployment](../deployment/coolify.md#containers))
under its queue. The jobs and their windows: [nightly DB maintenance](nightly-db-maintenance.md).

## 5. Dead tuples

```sql
SELECT relname, n_dead_tup, n_live_tup,
       ROUND(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 1) AS dead_pct, last_autovacuum, last_vacuum
FROM pg_stat_user_tables WHERE schemaname = 'public' AND n_dead_tup > 5000
ORDER BY n_dead_tup DESC;
```

A manual `VACUUM ANALYZE <table>` uses `maintenance_work_mem` (2 GB here), not `autovacuum_work_mem`: run
`SET maintenance_work_mem = '256MB';` first, and never between 05:50 and 07:30 UTC
([why](nightly-db-maintenance.md#the-0615-oom-of-2026-10-12)). Leave hypertables to autovacuum.

## 6. Slow-query logs

Two logs, both at 500 ms. **Neither alone sees everything.**

**6a. Application log** (TypeORM `maxQueryExecutionTime: 500`, `src/config/typeorm.config.ts`): JSON lines
(`timestamp`, `durationMs`, `query`, `parameters`) in `/data/parkfan/logs/slow-queries.YYYY-MM-DD.log` on
the host, kept 7 days. It sees **only the NestJS API's queries**: the Python services use their own
SQLAlchemy engines, so their queries can never appear there at any duration (an empty result for an
ml-service query is blindness, not health).

```bash
tail -f /data/parkfan/logs/slow-queries.$(date -u +%Y-%m-%d).log
# Top 20 patterns by total time from the last 1000 entries:
tail -1000 /data/parkfan/logs/slow-queries.$(date -u +%Y-%m-%d).log | python3 -c '
import json, sys, collections
g = collections.defaultdict(list)
for line in sys.stdin:
    try: q = json.loads(line); g[q["query"][:120]].append(q["durationMs"])
    except Exception: pass
for k, v in sorted(g.items(), key=lambda kv: -sum(kv[1]))[:20]:
    print(f"{int(sum(v)/1000)}s total | {len(v)}x | {int(sum(v)/len(v))}ms avg | {max(v)}ms max\n  {k[:110]}\n")'
```

**6b. PostgreSQL log**: every client, Python services and `psql` included.
`log_min_duration_statement = 500` was set with `ALTER SYSTEM` on 2026-04-06 (in `postgresql.auto.conf`).
Since the 2026-10 incidents each service also sets `application_name` (`parkfan-api`,
`parkfan-ml-service`, `parkfan-nf-service`, `parkfan-pcn-service`, `parkfan-db-maintenance`), and
`log_lock_waits=on` logs every lock wait over 1 s with both pids.

```bash
docker logs "$(docker ps --format '{{.Names}}' | grep postgres | grep -v coolify)" 2>&1 | grep -A1 'duration:' | tail -100
```

Change the threshold without a restart: `ALTER SYSTEM SET log_min_duration_statement = 1000; SELECT
pg_reload_conf();` (`-1` disables it).

## 7. Indexes on the key tables

```sql
SELECT t.relname AS table_name, i.relname AS index_name,
       array_agg(a.attname ORDER BY x.n) AS columns,
       pg_size_pretty(pg_relation_size(i.oid)) AS size, s.idx_scan
FROM pg_class t
JOIN pg_index ix ON t.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, n) ON true
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
WHERE t.relname IN ('wait_time_predictions', 'prediction_accuracy', 'schedule_entries', 'attractions')
GROUP BY t.relname, i.relname, i.oid, s.idx_scan
ORDER BY t.relname, s.idx_scan DESC NULLS LAST;
```

## 8. API restarts and out-of-memory

```bash
docker inspect <api-container> --format '{{.RestartCount}} restarts'
docker logs <api-container> 2>&1 | grep -B 20 'FATAL ERROR\|heap out of memory' | grep -E '(LOG|WARN|Processor|warmup)'
```

The API's heap limit is set twice: the Dockerfile `CMD` passes `--max-old-space-size=6144`, and the
production compose sets `NODE_OPTIONS=--max-old-space-size=4096`. Node applies command-line flags after
`NODE_OPTIONS`, so the running API should have 6 GB. For a host-level kill, `journalctl -k` holds the
kernel's OOM report with every process group's RSS.

## 9. Containers

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'
```

Services and ports: [deployment](../deployment/coolify.md#containers).
