# Nightly database maintenance

The jobs that keep the production database in shape around the 06:00 UTC CatBoost training: the 04:50
VACUUM window, the TFT retention, the weekly REINDEX of `pcn_forecasts_pkey`, the prediction cleanup that
must not touch compressed chunks, and the host settings applied along the way. Open it before moving a
nightly job, adding a large table, or deploying near these windows. Code: `src/database/maintenance/`
(`vacuum-plan.ts`, `db-vacuum.service.ts`), queue `db-maintenance`.

## The windows (UTC)

| Time            | What                                                                 |
| --------------- | -------------------------------------------------------------------- |
| 03:30           | `predictions/cleanup-old`                                            |
| 04:35–04:48     | `db-maintenance/prune-tft-forecasts` (no batch starts outside it)    |
| 04:50–05:58     | `db-maintenance/vacuum-churn-tables`                                 |
| 05:50–07:30     | Nothing large: CatBoost training (`ml-training/train-model`, 06:00, 35–70 min) |
| Sunday 19:10–20:03 | `db-maintenance/reindex-pcn-forecasts-pkey`; no deploy until 20:05 |

## The 06:15 OOM of 2026-10-12

At 06:15:55 the kernel killed the CatBoost training subprocess (6.4 GB RSS then, ~13 GB at peak) on the
29 GB host; `v20261011_0600` stayed the active model. The kernel's dump (`journalctl -k`) showed what held
the memory: 28 Node processes of the host's local Linear runner (`pnpm test`, one jest worker per core)
with 15.29 GB, the training with 6.12 GB, and all of Postgres, autovacuum included, with 0.51 GB. The
runner had started its 06:00 run 94 s before.

Changed on the host the same evening (not in this repository): the runner timer
(`park-worker-local.timer`, a user unit) moved to 09:45, 13:45, 17:45 and 21:45 UTC under
`MemoryMax=18G` (check with `systemctl --user list-timers park-worker-local.timer`). The VACUUM window
below stays as hygiene; it would not have prevented this kill. Container limits against the host:
[container memory limits](../deployment/container-memory-limits.md).

## The VACUUM window (`vacuum-churn-tables`, 04:50)

`VACUUM (ANALYZE)` on the high-churn plain tables, one at a time, so autovacuum has nothing large left when
the training starts: `pcn_forecasts`, `prediction_accuracy`, `tft_forecasts`, `holidays` (the list is
`vacuum-plan.ts`). Rules: its own connection outside the pool (`application_name =
parkfan-db-maintenance`), `lock_timeout = 5s`, `maintenance_work_mem = 256MB`, a per-table
`statement_timeout` capped to end by 05:58, a table already in `pg_stat_progress_vacuum` is skipped, nothing
starts at or after 05:50, no `VACUUM FULL`, and the job never fails. Each table logs
`🧽 <table>: VACUUM (ANALYZE) in N s — dead tuples X → Y`.

Hypertables are left to autovacuum: their dead rows sit in a few small uncompressed chunks, and a manual
VACUUM does not yield to a conflicting lock the way autovacuum does, so a compression job landing on a chunk
mid-VACUUM would queue behind it ([lock queues](timescaledb-lock-queues.md)).

Two lock holders found on the way, both fixed: pcn-service ran `CREATE INDEX IF NOT EXISTS` (a SHARE lock,
taken before it finds the index exists) on every 15-minute write and cancelled every long autovacuum of
`pcn_forecasts`; it now runs once per process. And `prediction-accuracy/compare-accuracy` ran its retention
DELETEs every 15 minutes; only the daily `prediction-accuracy/cleanup-old` (04:00) does now.

Is the window holding? After 06:00:

```sql
SELECT relname, last_vacuum, last_autovacuum, n_dead_tup FROM pg_stat_user_tables
WHERE relname IN ('pcn_forecasts', 'prediction_accuracy', 'tft_forecasts', 'holidays') ORDER BY relname;
```

`last_vacuum` should be today between 04:50 and 05:58. A missing table was skipped; the API log says why
(`skipped_in_progress`, `skipped_window_closed`, or `failed` with the error).

### Server settings applied on 2026-10-12

`ALTER SYSTEM` + `pg_reload_conf()`, no restart (on PG 18 all of these take effect on reload):

| Setting                       | Before | Now    | Why                                                                                 |
| ----------------------------- | ------ | ------ | ----------------------------------------------------------------------------------- |
| `autovacuum_work_mem`         | —      | 256 MB | caps autovacuum memory at 4 × 256 MB                                                |
| `autovacuum_max_workers`      | 10     | 4      | enough with the churn tables done at 04:50 (stays under `autovacuum_worker_slots` 16) |
| `max_wal_size`                | 1 GB   | 4 GB   | half of all checkpoints were forced by WAL volume                                   |
| `log_autovacuum_min_duration` | 10 min | 30 s   | the 06:15 question could not be answered from the log                               |
| `log_temp_files`              | off    | 100 MB | 1.9 TB of temp files since May with no trace of who wrote them                      |
| `track_io_timing`             | off    | on     | I/O time per statement in `pg_stat_statements`                                      |

Check: `SELECT name, setting, source FROM pg_settings WHERE name IN ('autovacuum_work_mem',
'max_wal_size', 'autovacuum_max_workers', 'log_autovacuum_min_duration', 'log_temp_files',
'track_io_timing');` — all `configuration file`, none `pending_restart`.

### Transaction-ID wraparound

On 2026-10-12 `age(datfrozenxid)` stood at 197.0 M against `autovacuum_freeze_max_age` 200 M, held by
never-vacuumed old chunks, compressed chunks and their TOAST tables. Forced anti-wraparound autovacuums
ignore the 04:50 window and cannot be cancelled by a lock conflict. A hand-run `VACUUM (FREEZE, ANALYZE)` of
the 51 relations with `age(relfrozenxid)` > 150 M in a quiet window brought it to 148.8 M. At ~1.4 M XIDs a
day, **repeat the targeted freeze about every five weeks**, before it nears 200 M:

```sql
SELECT age(datfrozenxid) FROM pg_database WHERE datname = current_database();
```

## TFT retention

`db-maintenance/prune-tft-forecasts`, cron `35 4 * * *`, deletes rows with `forecast_date` older than 120
days from `tft_forecasts`, then `tft_forecasts_shadow` (PAR-1041) and `tft_forecasts_shadow_drivers`
(PAR-1057). The shadows are created by nf-service on its first shadow write; until then `to_regclass` is
null and they are skipped. 120 days is the oldest origin any reader asks for (the forward-archive shadow
scoring at 14 + 90 + 3 = 107 days, the planner accuracy profile at 45 + 60 = 105) plus margin; the reader
table is in `vacuum-plan.ts`.

Batches of 200k rows, each its own autocommitted `DELETE … WHERE ctid = ANY(ARRAY(SELECT ctid … LIMIT n))`
(plain tables only), at most 40 a night for all three tables together (served table first),
`statement_timeout` 5 min per batch and capped to end by 04:50, `lock_timeout` 5 s. `forecast_date` has
no index, so a batch is a sequential scan that stops once it has its rows, and each batch rescans past the
rows earlier batches deleted (dead until the 04:50 VACUUM). When the window closes the log says
`not finished, N rows left` and the next night continues. Log line: `🗑️  TFT forecasts older than 120
days: …`.

The first run deletes TFT's oldest forward record (on a run dated 2026-10-25, `forecast_date` before
2026-06-27, ~5–6 M rows). The plan was to export those rows to the NAS before the deploy that ships the job
(whether that ran is not recorded in the repository): `COPY (SELECT *
FROM tft_forecasts WHERE forecast_date < DATE '<cutoff>') TO STDOUT` through `scripts/prod-psql.sh`
(`PARKFAN_PSQL_STATEMENT_TIMEOUT=20min`) into `parkfan/tft-forecasts-archive/` on the NAS, mounted with the
[backup](../deployment/backup.md) credentials, with a row-count check against the file.

## Weekly REINDEX of the PCN primary key

On 2026-10-24 `pcn_forecasts_pkey` was 10.1 GB against a 5.2 GB heap: the hourly 4-day prune (PAR-873)
deletes at one end of the key space while the writer inserts at the other, and VACUUM marks the emptied
pages reusable but never returns them.

`db-maintenance/reindex-pcn-forecasts-pkey`, cron `10 19 * * 0` (Sunday 19:10 UTC), runs `REINDEX INDEX
CONCURRENTLY pcn_forecasts_pkey` on the maintenance connection (PAR-1054).

- **Slot**: outside the VACUUM and training windows, after the :05 forward-archive capture, on the
  evening with no nightly batch. It starts only between 19:10 and 19:30; a late Bull delivery logs
  `skipped_window_closed`.
- **Timeouts**: `statement_timeout` 50 min, capped to end by 20:03, before the 20:05 capture.
  `lock_timeout` 60 s, longer than the VACUUM's on purpose: `REINDEX CONCURRENTLY` waits twice for every
  open transaction on the table, and at 5 s one slow PCN upsert would throw away a finished build. Its own
  lock (SHARE UPDATE EXCLUSIVE) conflicts with no read or write.
- **Skipped (`skipped_busy`)** while an index build is in `pg_stat_progress_create_index`, while
  `pcn_forecasts` is being vacuumed, or while another `parkfan-db-maintenance` session is active.
- **Leftovers**: a failed build leaves an invalid `pcn_forecasts_pkey_ccnew` / `_ccold` that every write
  still maintains; the job drops invalid `pcn_forecasts_pkey_cc*` before it starts and after a failure.
- **Only on a plain table.** `pcn_forecasts` is not in `HYPERTABLES` (a spec asserts it); TimescaleDB does
  not support `REINDEX … CONCURRENTLY` on a hypertable. Making it one means rewriting this job and
  pcn-service's ctid prune.
- **Do not deploy on Sunday 19:10–20:05.** pcn-service's once-per-process `CREATE INDEX IF NOT EXISTS` takes
  a SHARE lock that conflicts with the REINDEX, and the PCN writers queue behind it until the build ends.

```sql
SELECT pg_size_pretty(pg_relation_size('pcn_forecasts_pkey')) AS pkey,
       pg_size_pretty(pg_relation_size('pcn_forecasts')) AS heap;
-- Must be empty (an invalid copy left behind):
SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE i.indrelid = 'pcn_forecasts'::regclass AND NOT i.indisvalid;
```

Log line: `🧽 REINDEX pcn_forecasts_pkey in N s — X GB → Y GB`.

## `predictions/cleanup-old` and compressed chunks (PAR-900)

From 2026-10-10 the nightly cleanup failed with `duplicate key value violates unique constraint
"…_wait_time_predictions_pkey"`. Its daily step deleted on `predictedTime < now − 90 days`, which is not the
partition key (`createdAt`), so the DELETE planned every chunk and TimescaleDB decompressed matching
batches of compressed ones, writing the rows back through a primary key a compressed chunk had never
enforced (two July chunks held 1,051 and 1,267 duplicate keys).

Since then the daily step deletes only from chunks newer than the newest compressed one (a `createdAt`
bound rendered as a constant, so the planner leaves compressed chunks out of the plan), and each of the
job's three steps runs on its own. Check that the plan names only uncompressed chunks:

```sql
SELECT max(range_end) FROM timescaledb_information.chunks
WHERE hypertable_name = 'wait_time_predictions' AND is_compressed;
EXPLAIN (COSTS OFF)
WITH del AS (DELETE FROM wait_time_predictions
             WHERE "predictionType" = 'daily' AND "predictedTime" < now() - interval '90 days'
               AND "createdAt" >= '<that value>' RETURNING 1)
SELECT count(*) FROM del;
```

The duplicates were left alone: removing them means decompressing the chunks, and the 90-day chunk drop
removes them anyway.
