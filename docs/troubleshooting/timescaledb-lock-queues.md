# TimescaleDB jobs and lock queues

Why a TimescaleDB background job asking for an `ACCESS EXCLUSIVE` lock can take the whole API down, what
was changed so no job queues for that lock any more, and how to diagnose a stall that looks like it. Open it before adding a retention or compression policy, a foreign
key to a hypertable, or when every query suddenly waits. Ad-hoc sessions are the other way in:
[runbook §0](db-health-runbook.md#0-time-limits-on-ad-hoc-sql).

## The mechanism

Postgres grants locks in arrival order, so a **pending** `ACCESS EXCLUSIVE` request blocks every later
`SELECT` on that table, even ones that do not conflict with whatever the current holder has. The API pool
(80 connections, `connectionTimeoutMillis` 15 s) then fills with queued readers and every request fails
with `timeout exceeded when trying to connect`.

- **Retention locks the tables a hypertable references.** `drop_chunks` starts with
  `lock_referenced_tables()`: `ACCESS EXCLUSIVE` on every table the hypertable has a foreign key to
  (`wait_time_predictions` → `attractions`), on every run, **before** looking for a chunk to drop.
- **Compression locks the chunk.** `compress_chunk` ends by truncating the uncompressed rows, which needs
  `ACCESS EXCLUSIVE` on the chunk; under the default `compress_truncate_behaviour = truncate_only` it waits
  with no deadline (compression policies run with `max_runtime` 0).

Both were reproduced on `timescale/timescaledb:2.24.0-pg18` with one session holding a plain `SELECT`
transaction.

## What changed

Both paths stalled production in October 2026: the retention on `wait_time_predictions` queued for
`attractions` twenty times, and a compression on `queue_data` queued on a chunk and took the API down
for 5 h 16 min until Postgres restarted. The holder was never identified, so the fix does not depend on
finding one.

| Change                                                                                     | Where                                                                                          | Effect                                                                                                         |
| ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| No retention policy; the nightly `predictions/cleanup-old` (03:30 UTC) drops chunks past 90 days | `TimescaleInitService.setupRetentionPolicies` removes it at boot, `MLService.dropExpiredPredictionChunks` | `show_chunks` first (locks nothing), so `attractions` is untouched when nothing is due; `drop_chunks` under a transaction-local `lock_timeout` of 2 s, up to 4 attempts 30 s apart; an error-level log once a chunk is 14 days overdue |
| `timescaledb.compress_truncate_behaviour = truncate_or_delete` on the database             | `TimescaleInitService.setupCompressTruncateBehaviour`                                          | Compression tries the truncate lock for 5 s, then deletes the rows instead; readers never wait (cost: dead tuples for autovacuum) |
| `application_name` per service                                                              | `typeorm.config.ts`, each service's `db.py`                                                    | `pg_stat_activity` and lock-wait log lines name the session                                                   |
| `idle_in_transaction_session_timeout` = 10 min on application connections                  | same                                                                                           | Ends a session left idle in a transaction; a running statement is not affected                                |
| `log_lock_waits=on`                                                                         | `docker-compose.production.yml`                                                                | Every wait over `deadlock_timeout` (1 s) is logged with holder and waiter pids                                |

**Rule for new code:** no TimescaleDB job may queue for an `ACCESS EXCLUSIVE` lock. A new retention goes
through a job with a `lock_timeout`, not `add_retention_policy`; a new foreign key to a hypertable makes
every drop lock its target. `src/database/hypertables.ts` points here.

**Not done:** a `statement_timeout` for the application pools. It is the only bound on a long-running
statement as a lock holder, but legitimate statements run long (the park-open-window `WITH park_tz` query
up to 685 s, `INSERT INTO queue_data_aggregates` up to 284 s), so the value needs those measured first.
Since the jobs no longer queue, a long holder only delays a drop or a compression.

## When a stall looks like this again

```sql
-- Who waits, and on whom.
SELECT w.pid, w.application_name, w.wait_event_type, pg_blocking_pids(w.pid) AS blocked_by,
       left(w.query, 80) AS waiting_query
FROM pg_stat_activity w WHERE cardinality(pg_blocking_pids(w.pid)) > 0;

-- Old transactions.
SELECT pid, application_name, client_addr, state, now() - xact_start AS xact_age, left(query, 120)
FROM pg_stat_activity WHERE xact_start < now() - interval '5 minutes' ORDER BY xact_start;

-- Failed or crashed job runs (successes are not logged by default).
SELECT job_id, succeeded, start_time, finish_time, sqlerrcode, err_message
FROM timescaledb_information.job_history ORDER BY finish_time DESC LIMIT 20;
```

The tell: blocked queries all finish in the same second, and that second is a job's `finish_time` (or its
`next_start` in `timescaledb_information.jobs`).
