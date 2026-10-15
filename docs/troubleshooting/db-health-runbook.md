# Database Health Runbook

Quick reference for checking DB performance, bloat, and index health.

**Connect — always through the wrapper:**
```bash
ssh <user>@<dockerhost>
cd <repo> && scripts/prod-psql.sh -c '<QUERY>'   # or with no arguments, interactively
```

`scripts/prod-psql.sh` resolves the container (its name carries a Coolify deploy
id and changes with every deploy) and sets the three limits below on that session
via `PGOPTIONS`. **Do not call `docker exec … psql` by hand.** Ad-hoc SQL against
this database has no deadline of its own, and a session without one is how the
public API went down for 32 minutes — see "Why the limits exist" below.

---

## 0. Time limits on ad-hoc SQL

| Setting | Wrapper default | Server default | What it stops |
|---|---|---|---|
| `statement_timeout` | `5min` | `0` (off) | one statement running forever |
| `idle_in_transaction_session_timeout` | `1min` | `0` (off) | a session parked between `BEGIN` and `COMMIT` |
| `lock_timeout` | `10s` | `0` (off) | a writing session queueing on a lock and collecting waiters |

Each is overridable per invocation when a query legitimately needs longer:

```bash
PARKFAN_PSQL_STATEMENT_TIMEOUT=15min scripts/prod-psql.sh -c '<QUERY>'
```

| Variable | Overrides |
|---|---|
| `PARKFAN_PSQL_STATEMENT_TIMEOUT` | `statement_timeout` |
| `PARKFAN_PSQL_IDLE_TX_TIMEOUT` | `idle_in_transaction_session_timeout` |
| `PARKFAN_PSQL_LOCK_TIMEOUT` | `lock_timeout` |
| `PARKFAN_PSQL_CONTAINER` | the container, when the lookup finds more than one |
| `PARKFAN_PSQL_USER` / `PARKFAN_PSQL_DB` | role and database, both `parkfan` by default |

**Why `5min` and not less.** Measured over the 117 days since the last
`pg_stat_statements` reset (2026-06-03): of 4962 recorded statements, 6 ever
exceeded one minute and 2 ever exceeded five, and both of those two are
background jobs rather than research queries (the TimescaleDB compression policy,
and one `WITH park_tz` analytics query at 685 s). The limit costs nothing that is
run by hand.

**Why the limits exist.** On 2026-09-28 a research `SELECT` started through
`docker exec … psql` stayed open after the runner process that launched it had
exited. The compression policy on `queue_data` then asked for a strong lock,
queued behind that snapshot, and every subsequent reader queued behind the
policy: 90 queries piled up, the TypeORM pool ran into its 15-second acquire
timeout, and the whole public API — `/v1/health` included — answered HTTP 500 for
about 32 minutes. The query itself was fine. It simply had no deadline.

**Why this is not set on the role or the database.** The application pool
connects as the *same* `parkfan` role as an ad-hoc session (103 pool connections
against 1 `psql` at the time of measurement), and so does the TimescaleDB
background worker. `ALTER ROLE parkfan SET statement_timeout` or
`ALTER DATABASE parkfan SET …` would therefore cap application queries and
maintenance jobs too. The pool has its own bound — `connectionTimeoutMillis:
15000` in `src/config/typeorm.config.ts` — and giving it a query-runtime limit is
a separate decision, not this one. Keeping the guard per session is what makes it
apply to exactly the sessions that lack one.

`idle_in_transaction_session_timeout` is a **separate** gap, not a duplicate of
`statement_timeout`: the latter never fires while no statement is running, so a
session sitting between `BEGIN` and `COMMIT` would hold its snapshot and its
locks indefinitely. An idle session *outside* a transaction holds neither, which
is why there is no `idle_session_timeout` here.

---

## 0b. TimescaleDB jobs and lock queues (2026-10-07 … 09)

Ad-hoc sessions were one way in. The TimescaleDB background jobs are the other:
two of them asked for an `ACCESS EXCLUSIVE` lock and waited for it, and every
reader that arrived afterwards queued behind the waiting request — Postgres
grants locks in arrival order, so a pending `ACCESS EXCLUSIVE` blocks even
`SELECT`s that do not conflict with whatever the holder has. The API pool (80
connections, `connectionTimeoutMillis` 15 s) filled with queued readers and
every request after that failed with `timeout exceeded when trying to connect`.

### What happened

| When (UTC) | Job | Lock it waited for | Effect |
|---|---|---|---|
| 10-07 15:59–20:28, 10-08 21:20–10-09 01:49 | 1007, retention on `wait_time_predictions` | `ACCESS EXCLUSIVE` on **`attractions`** | 20 five-minute stalls; in each, every query touching `attractions` waited until the job gave up |
| 10-09 02:12–07:31 | 1000, compression on `queue_data` | `ACCESS EXCLUSIVE` on the 30-day-old chunk (`_hyper_1_2162_chunk`, 2026-09-08) | API down for 5 h 16 min, until the 07:24 deploy restarted Postgres |

**Why a retention job locks `attractions`.** `drop_chunks` begins with
`lock_referenced_tables()`: an `ACCESS EXCLUSIVE` lock on every table the
hypertable has a foreign key to (a guard against TimescaleDB issue #865), and
`wait_time_predictions` → `attractions` is such a key. It takes this lock on
every run, **before** looking for a chunk to drop — on 10-08 there was nothing to
drop at all and it still stalled for ten hours. The policy's `max_runtime` was
5 min and `retry_period` 5 min, so it gave up and came back roughly hourly.

**Why compression locks out readers.** `compress_chunk` ends by truncating the
uncompressed rows, which needs `ACCESS EXCLUSIVE` on the chunk. Under the default
`timescaledb.compress_truncate_behaviour = truncate_only` it waits for that lock
with no deadline (the compression policies run with `max_runtime` 0). The 02:12
run of job 1000 was still waiting when the deploy stopped Postgres; the job
history shows it as a crash at 07:31:34 and the chunk was compressed by the
retry at 08:46.

**The evidence.**

- `slow-queries.*.log`: every one of the 20 retention attempts that failed while
  the API was still answering is followed by a burst of 8–73 queries over 60 s
  finishing **in the same second the attempt ended** (10-07 16:04:25, 16:15:02,
  …, 10-09 01:49:44) — 655 queries in all, up to 300 s long, and all 655 name
  `attractions`. That is the signature of a lock queue draining.
- From 02:15:03 on 10-09 the slow-query log holds **no** entry until 07:31:47:
  nothing that started finished. Queries do not stall that way on their own.
- `_timescaledb_catalog.chunk.creation_time` of the compressed chunks: the daily
  `queue_data` compression ran at 02:12 every day from 09-29 to 10-08, and on
  10-09 at 08:46 — after the restart.
- Reproduced on `timescale/timescaledb:2.24.0-pg18`: with one session holding a
  plain `SELECT` transaction on `attractions`, `drop_chunks` on a hypertable with
  **no** chunk to drop waits on `AccessExclusiveLock` on `attractions`, and a
  third session's `SELECT count(*) FROM attractions` hangs behind it. The same
  for `compress_chunk` and a reader of the chunk. With
  `compress_truncate_behaviour = truncate_or_delete` set on the database,
  `compress_chunk` finished in 5 s with the reader still open, and readers were
  never blocked; a background job confirmed the database-level value reaches
  TimescaleDB workers.

**What is not known: who held the lock.** Each stall needs a session that kept a
lock on `attractions` (and, on 10-09, on a 30-day-old `queue_data` chunk) for
hours — from at most 10-07 15:59 until the 20:54 deploy, and from at most 10-08
21:20 until the 10-09 07:24 deploy. Both ended with a Postgres restart. It was
not a statement that finished (nothing in the slow-query log or in
`pg_stat_statements` ran that long), so it was either idle in a transaction or
never completed. `log_lock_waits` was off, the container's log went with the
container, and every client connects as `parkfan` with no `application_name`,
so `pg_stat_activity` could not have named it either.

**The fix does not depend on finding it.** The two job-side changes below close
both outage paths whoever holds the lock and for however long: the chunk drop
waits at most 2 s per attempt, and compression no longer waits in the lock queue
at all. The connection-side changes do something narrower. `application_name`
and `log_lock_waits` make the next holder nameable, and
`idle_in_transaction_session_timeout` bounds a holder only while it is idle in a
transaction. A statement that is still running is not bounded: no application
pool sets `statement_timeout`, and `pg_stat_statements` has the park-open-window
`WITH park_tz AS …` query at a mean of 167 s and a maximum of 685 s, and
`INSERT INTO queue_data_aggregates` at up to 284 s.

### What changed

| Change | Where | Why |
|---|---|---|
| The retention policy is removed at boot; the nightly `cleanup-old` job (03:30) drops chunks past 90 days itself | `TimescaleInitService.setupRetentionPolicies`, `MLService.dropExpiredPredictionChunks` | It calls `show_chunks` first, which locks nothing, so `attractions` is untouched on days nothing is due. When something is, `drop_chunks` runs under a transaction-local `lock_timeout` of 2 s, up to 4 attempts 30 s apart. If all four time out it warns and the next night tries again; once a chunk is more than 90 + 14 days old and still there, it logs at **error** level, because by then the misses are a pattern |
| `timescaledb.compress_truncate_behaviour = truncate_or_delete` on the database | `TimescaleInitService.setupCompressTruncateBehaviour` | Compression tries the truncate lock without queueing for 5 s, then deletes the rows instead. Readers never wait. The cost is dead tuples for autovacuum |
| `application_name` per service (`parkfan-api`, `parkfan-ml-service`, `parkfan-pcn-service`, `parkfan-nf-service`, `parkfan-shape-service`) | `typeorm.config.ts`, each service's `db.py` | So `pg_stat_activity` and lock-wait log lines say whose session it is |
| `idle_in_transaction_session_timeout` = 10 min on application connections | same | A session left **idle** in a transaction is ended after 10 minutes. A running statement is not affected (see the follow-up below). Per connection, so `scripts/prod-psql.sh` and the TimescaleDB workers keep their own settings |
| `log_lock_waits=on` | `docker-compose.production.yml` | Any lock wait over `deadlock_timeout` (1 s) is logged with the holding and waiting pids |

### Follow-up, not done here

A `statement_timeout` for the application pools. It is the only thing that
would bound a long-running statement as a lock holder, but it would also cut off
the legitimate long ones above (685 s, 284 s), so the value has to come from
measuring those first. Until then a running statement can still hold a lock for
as long as it runs. Since the jobs no longer queue, that delays the drop or the
compression and no longer stalls the readers.

### When a stall looks like this again

```sql
-- Who waits for what, and who holds it.
SELECT w.pid AS waiting_pid, w.application_name AS waiting_app, w.wait_event_type,
       pg_blocking_pids(w.pid) AS blocked_by, left(w.query, 80) AS waiting_query
FROM pg_stat_activity w
WHERE cardinality(pg_blocking_pids(w.pid)) > 0;

SELECT pid, application_name, client_addr, state, now() - xact_start AS xact_age,
       left(query, 120) AS query
FROM pg_stat_activity
WHERE xact_start < now() - interval '5 minutes'
ORDER BY xact_start;

-- Job runs that failed or crashed (successes are not logged by default).
SELECT job_id, succeeded, start_time, finish_time, sqlerrcode, err_message
FROM timescaledb_information.job_history
ORDER BY finish_time DESC LIMIT 20;
```

The tell in the logs is the same as on 2026-09-28: blocked queries all end in
the same second, and that second is a job's `finish_time` (or its `next_start`
in `timescaledb_information.jobs`).

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


### 2a. Dropping a redundant index without blocking (PAR-872)

Production runs TypeORM `synchronize`, so **an `@Index` removed from an entity is
dropped at the next boot with a plain, blocking `DROP INDEX`**. On a hypertable that
one statement takes `ACCESS EXCLUSIVE` on the parent and on every chunk at once, and
holds them all until it commits — measured on TimescaleDB 2.24 / PG 18: 21 chunks gave
42 `AccessExclusiveLock`s in one transaction. The order is therefore always:

1. drop the index by hand, without blocking, **before** the deploy;
2. deploy the entity change in the same release, so `synchronize` finds nothing left.

`DROP INDEX CONCURRENTLY` on the hypertable's own index fails while chunk indexes exist
(`DROP INDEX CONCURRENTLY does not support dropping multiple objects`), but it works on
each **chunk** index, compressed chunks included, and once those are gone it works on
the parent too. Verified on `timescale/timescaledb:2.24.0-pg18`; new chunks created
afterwards do not get the index back, and `decompress_chunk` still works.

Each `CONCURRENTLY` statement runs in its own transaction, waits for transactions
already using the table, and is bounded by `lock_timeout`. Run outside 01:00–09:30 UTC,
outside :00–:12 of the hour, and away from the compression policy on that table
(`SELECT job_id, next_start FROM timescaledb_information.job_stats WHERE hypertable_name = '…';`).

```bash
# On the host, from the repository. lock_timeout 2 s: a statement that cannot get its
# lock gives up instead of queueing readers behind it; just run the same command again.
PARKFAN_PSQL_LOCK_TIMEOUT=2s scripts/prod-psql.sh <<'SQL'
-- one chunk index at a time, generated from the catalogue
SELECT format('DROP INDEX CONCURRENTLY IF EXISTS %I.%I;', n.nspname, c.relname)
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind = 'i' AND n.nspname = '_timescaledb_internal'
   AND c.relname LIKE '%chunk_IDX_aa19b21038bdfe8d295a5f2f2c'
\gexec
-- then the parent; if a chunk was created in between this fails with the
-- "multiple objects" error — run the whole block again
DROP INDEX CONCURRENTLY IF EXISTS public."IDX_aa19b21038bdfe8d295a5f2f2c";
SQL
```

Same block with `idx_weather_1` for `weather_data`. On plain tables a single
`DROP INDEX CONCURRENTLY IF EXISTS public."<name>";` is enough.

**Dropped this way in PAR-872** (all verified read-only first — identical key columns,
no predicate, and the one kept is the constraint-backed or the covering one):

| table | dropped | kept | size | why it was redundant |
| --- | --- | --- | --- | --- |
| `queue_data` (hypertable, 293 chunks) | `IDX_aa19b21038bdfe8d295a5f2f2c` `(attractionId, timestamp)` | `idx_queue_data_covering` `(attractionId, timestamp, status, queueType)` | 297 MB | a leading prefix of the covering index |
| `weather_data` (hypertable, 45 chunks) | `idx_weather_1` `(parkId, date)` | `weather_data_pkey` | 1.5 MB | the primary key, column for column |
| `destinations` | `IDX_30864aeaf404f4a6d3f816bd8c` `(slug)`, `IDX_97c24ff3f37564f943658aba19` `(externalId)` | `UQ_30864aeaf404f4a6d3f816bd8c5`, `UQ_97c24ff3f37564f943658aba19e` | 48 kB | non-unique twin of a `UNIQUE` constraint |
| `restaurants` | `IDX_728d45b746ca82dca8f04cfd9b` `(externalId)` | `UQ_728d45b746ca82dca8f04cfd9be` | 304 kB | same |
| `shows` | `IDX_a97b856e5ea567f8127754ff14` `(externalId)` | `UQ_a97b856e5ea567f8127754ff140` | 312 kB | same |

The E2E spec `test/e2e/db-cleanup-writes.e2e-spec.ts` fails if `synchronize` would
build any of them again.

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

### 4a. `predictions/cleanup-old` and compressed chunks (PAR-900, 2026-10)

From 2026-10-10 the nightly `predictions/cleanup-old` failed with
`duplicate key value violates unique constraint "1229_1462_wait_time_predictions_pkey"`.
Its daily step deleted on `predictedTime < now-90d`, which is not the partition
key (`createdAt`), so the DELETE planned every chunk and TimescaleDB decompressed
the matching batches of the compressed ones. Decompression writes the rows back
through the chunk's primary key, which a compressed chunk never enforced: chunks
1229 and 1324 (July) held 1051 and 1267 duplicate keys. It failed as soon as the
cutoff reached a chunk's oldest `predictedTime` (the first `orderby` column, so
batches carry its min/max) and stopped once the chunk was dropped.

Since PAR-900 the daily step deletes only from chunks after the newest compressed
one, and each step of the job runs on its own. To check that the DELETE still
skips compressed chunks (plan only, nothing runs):

```sql
SELECT max(range_end) FROM timescaledb_information.chunks
 WHERE hypertable_name = 'wait_time_predictions' AND is_compressed;
-- put that value in as a literal, as the job does:
EXPLAIN (COSTS OFF)
WITH del AS (DELETE FROM wait_time_predictions
              WHERE "predictionType" = 'daily'
                AND "predictedTime" < now() - interval '90 days'
                AND "createdAt" >= '<boundary>'
              RETURNING 1)
SELECT count(*) FROM del;
```

The `Delete on _hyper_16_…_chunk` lines must name only uncompressed chunks. On
2026-10-15 that was 3 of 14 chunks, against all 14 (with Seq Scans over the 11
compressed ones) for the old predicate. The duplicates themselves were left
alone: removing them means decompressing the chunks, and the chunk drop removes
them anyway.

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

A manual VACUUM uses `maintenance_work_mem` (2 GB here), not `autovacuum_work_mem`.
Run `SET maintenance_work_mem = '256MB';` first, and never between 05:50 and 07:30 UTC
(§5a).

### 5a. The nightly VACUUM window and the 06:15 OOM (PAR-868, 2026-10-12)

**What happened.** At 06:15:55 UTC on 2026-10-12 the kernel OOM-killed the CatBoost
training (`ml-training/train-model`, cron 06:00 UTC, 35–70 min, an ml-service
subprocess at 6.4 GB RSS when killed and ~13 GB at peak) on the shared 29 GB host.
Trivial queries took 40 s in the seconds before. No new model was registered;
`v20261011_0600` stayed active.

**What caused it — corrected (PAR-872).** This section first blamed memory pressure
with autovacuum as a likely part of it. The kernel's OOM dump (`journalctl -k`,
`global_oom`) says otherwise:

| process group | count | anonymous RSS |
| --- | --- | --- |
| `postgres` (all backends, autovacuum included) | 75 | **0.51 GB** |
| Node.js, uid `arns` — the local Linear runner | 28 | **15.29 GB** (~550 MB each) |
| `python3.13` — the CatBoost subprocess, the victim | 1 | 6.12 GB |

The 28 Node processes were the local Linear runner's 06:00 run
(`park-worker-local.timer`, a user unit on the host): its log shows `pnpm test` at
06:14:21 — jest, one worker per core at ~550 MB each, load average 66 — and then
`pnpm test:e2e`. The OOM came 94 s after `pnpm test` started, at the training's peak.
Postgres' private memory was half a gigabyte; autovacuum is not needed to explain
the kill. The VACUUM window and the autovacuum memory cap below stay, as hygiene,
but they would not have prevented it.

**Changed on the host on 2026-10-12, evening (not in this repository):** the runner
timer moved out of the training window to 09:45, 13:45, 17:45 and 21:45 UTC, and the
unit runs under `MemoryMax=18G`. Check with
`systemctl --user list-timers park-worker-local.timer` on the host.

**Changed on 2026-10-12 (ALTER SYSTEM + reload, no restart):**
`autovacuum_work_mem = 256MB`. Check with
`SELECT setting, source FROM pg_settings WHERE name = 'autovacuum_work_mem';`
(`262144` kB, `configuration file`).

**Changed in code:** the `db-maintenance` queue's `vacuum-churn-tables` job
(`src/database/maintenance/`) runs `VACUUM (ANALYZE)` at **04:50 UTC** on the
high-churn tables, one at a time, so autovacuum has nothing large left to do when
the training starts:

| table | live | dead | autovacuums | size |
| --- | --- | --- | --- | --- |
| `pcn_forecasts` | 122.1 M | 9.46 M | 143 | 28 GB |
| `prediction_accuracy` | 16.2 M | 1.11 M | 5 320 | 6.3 GB |
| `shape_forecasts` (retired with PAR-870, not in the job) | 102.6 M | 2.40 M | 119 | 19 GB |
| `tft_forecasts` | 24.4 M | 0.49 M | 37 | 3.4 GB |
| `holidays` | 0.20 M | 34.8 k | 192 | 164 MB |

(`pg_stat_user_tables` on 2026-10-12; stats never reset.) Rules: its own connection
outside the pool (`application_name = parkfan-db-maintenance`), `lock_timeout = 5s`,
`maintenance_work_mem = 256MB`, a per-table `statement_timeout` capped to end by
05:58, a table with a vacuum already in `pg_stat_progress_vacuum` is skipped, no table
starts at or after 05:50, no `VACUUM FULL`, and the job never fails. Each table logs
`🧽 <table>: VACUUM (ANALYZE) in N s — dead tuples X → Y`.

Hypertables are left to autovacuum on purpose: their dead rows sit in a few small
uncompressed chunks, and a manual VACUUM does not yield to a conflicting lock the
way autovacuum does — a compression policy landing on a chunk mid-VACUUM would queue
behind it (§0b). The policies' 12-hour schedules drift, so their current times are
no guarantee.

**Two lock holders found on the way:**
- `pcn-service` ran `CREATE INDEX IF NOT EXISTS` on `pcn_forecasts` inside every
  15-minute write and every hourly prune. That statement takes a SHARE lock before it
  finds the index exists, which conflicts with VACUUM: it cancelled every
  autovacuum of the table that ran longer than a forecast tick, and against the
  manual VACUUM it would have queued every writer for the whole run. The DDL now runs
  once per process.
- `prediction-accuracy/compare-accuracy` (every 15 min) ran the retention DELETEs
  on every tick — 96 times a day, including the `DELETE FROM prediction_accuracy
  WHERE target_time < $1 AND comparison_status = $2` (3.4 s) logged at 06:15:17 on
  2026-10-12. The daily `cleanup-old` job at 04:00 applies the same windows; the
  per-tick copy is gone.

**Applied on 2026-10-12, evening** (ALTER SYSTEM + `pg_reload_conf()`; on PG18 all of
these take effect on reload, `autovacuum_max_workers` included because it stays under
`autovacuum_worker_slots` = 16):

| setting | before | now | why |
| --- | --- | --- | --- |
| `max_wal_size` | 1 GB | 4 GB | half of all checkpoints were *requested* (forced by WAL volume), 28 536 against 28 667 timed |
| `autovacuum_max_workers` | 10 | 4 | with the churn tables done at 04:50 four are enough; caps autovacuum memory at 4 × 256 MB |
| `log_autovacuum_min_duration` | 10 min | 30 s | the 06:15 question could not be answered from the log |
| `log_temp_files` | off | 100 MB | 1.9 TB of temp files since May with no trace of who wrote them |
| `track_io_timing` | off | on | I/O time per statement in `pg_stat_statements` |

Check with
`SELECT name, setting, source FROM pg_settings WHERE name IN ('max_wal_size','autovacuum_max_workers','log_autovacuum_min_duration','log_temp_files','track_io_timing');`
— all `configuration file`, none `pending_restart`.

**Anti-wraparound vacuum pre-empted, same evening.** The database's
`age(datfrozenxid)` stood at 197.0 M against `autovacuum_freeze_max_age` = 200 M, held
by never-vacuumed old hypertable chunks, compressed chunks and their TOAST tables. At
~1.4 M XIDs a day the forced "to prevent wraparound" autovacuums would have started
around 2026-10-14 across hundreds of relations at once — they ignore the 04:50 window
and cannot be cancelled by a lock conflict. A targeted `VACUUM (FREEZE, ANALYZE)` of the
51 relations with `age(relfrozenxid)` > 150 M, run by hand in a quiet window, took the
database to **148.8 M**. Watch it with
`SELECT age(datfrozenxid) FROM pg_database WHERE datname = current_database();`
and repeat the same targeted freeze before it nears 200 M again (about five weeks).

**Is the window holding?** After 06:00, nothing large should be autovacuuming:

```sql
SELECT relname, last_vacuum, last_autovacuum, n_dead_tup
FROM pg_stat_user_tables
WHERE relname IN ('pcn_forecasts','prediction_accuracy','tft_forecasts','holidays')
ORDER BY relname;
```

`last_vacuum` should read today, between 04:50 and 05:58 UTC. A missing table means it
was skipped — the API log says why (`skipped_in_progress`, `skipped_window_closed`,
or a `failed` with the error).

---

## 6. Slow Query Logging

Two independent slow query logs are active. Both use a **500ms threshold**.

---

### 6a. Application-level log (NestJS → JSON file)

Catches all queries from the NestJS API. Written to a mounted volume as JSON, one entry per line.

**Files:** `/data/parkfan/logs/slow-queries.YYYY-MM-DD.log` on <dockerhost> (daily, 7-day retention)

```bash
# Live tail (today)
ssh <user>@<dockerhost> 'tail -f /data/parkfan/logs/slow-queries.$(date -u +%Y-%m-%d).log'

# List available days
ssh <user>@<dockerhost> 'ls /data/parkfan/logs/slow-queries.*.log 2>/dev/null'

# Top 20 slowest query patterns (last 1000 entries from today, ranked by total time)
ssh <user>@<dockerhost> 'tail -1000 /data/parkfan/logs/slow-queries.$(date -u +%Y-%m-%d).log | python3 -c "
import json, sys
from collections import defaultdict
queries = []
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try: queries.append(json.loads(line))
    except: pass
groups = defaultdict(list)
for q in queries:
    groups[q[\"query\"][:120]].append(q[\"durationMs\"])
results = [(k, max(v), len(v), int(sum(v)/len(v)), int(sum(v)/1000)) for k, v in groups.items()]
results.sort(key=lambda x: -x[4])
for key, mx, cnt, avg, total_s in results[:20]:
    print(str(total_s)+\"s total | \"+str(cnt)+\"x | \"+str(avg)+\"ms avg | \"+str(mx)+\"ms max\")
    print(\"  \"+key[:110])
    print()
"'
```

**JSON fields:** `timestamp`, `durationMs`, `query`, `parameters`

---

### 6b. PostgreSQL-level log (catches ML service + direct connections)

Catches queries from all clients: NestJS, Python ML service, psql, etc.
Enabled 2026-04-06 via `ALTER SYSTEM` (survives container restarts, stored in `postgresql.auto.conf`).

**Current setting:**
```sql
SHOW log_min_duration_statement;  -- 500ms
```

**Read the log:**
```bash
# All PG slow queries (last 200 lines)
ssh <user>@<dockerhost> '
PG=$(docker ps --format "{{.Names}}" | grep postgres)
docker logs "$PG" 2>&1 | grep "duration:" | tail -50'

# Only show duration + query (strip noise)
ssh <user>@<dockerhost> '
PG=$(docker ps --format "{{.Names}}" | grep postgres)
docker logs "$PG" 2>&1 | grep -A1 "duration:" | grep -v "^--$" | tail -100'
```

**PG log format:**
```
2026-04-06 08:00:38.251 UTC [4145] parkfan@parkfan LOG:  duration: 1234.567 ms  statement: SELECT ...
```

**Change threshold without restart:**
```sql
ALTER SYSTEM SET log_min_duration_statement = 1000;  -- raise to 1s to reduce noise
SELECT pg_reload_conf();

-- Disable entirely:
ALTER SYSTEM SET log_min_duration_statement = -1;
SELECT pg_reload_conf();
```

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
ssh <user>@<dockerhost> \
  "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'"
```

| Service | Port | Notes |
|---|---|---|
| API (NestJS) | 3000 | `api-m08og...` |
| ML Service (Python) | 8000 (internal) | `ml-service-m08og...` |
| PostgreSQL (TimescaleDB) | 5432 (internal) | `postgres-m08og...` |
| Redis | 6379 (internal) | `redis-m08og...` |
| Bull Board | (via proxy) | `bull-board-m08og...` |
