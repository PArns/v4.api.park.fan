# Recalculating stored aggregates after a logic change

What to rerun when a calculation changes (a threshold such as `MIN_WAIT_TIME_THRESHOLD`, a percentile, a
filter): the daily park statistics, the baselines, and the caches that hold the old results. Open it
after merging such a change.

## Daily park statistics (`park_daily_stats`)

Per park and park-local day: P50, P90, maximum wait. The `stats` queue writes them:

| Job                        | When (UTC)        | Does                                         |
| -------------------------- | ----------------- | -------------------------------------------- |
| `update-today-stats`       | hourly at :00     | today, every park                            |
| `finalize-yesterday-stats` | 01:00             | yesterday, every park                        |
| `backfill-stats`           | by hand           | `{ days = 30, parkId?, startDate? }`: `days` days back from `startDate` (default today), one park or all |

Stored days do not change on their own, so a logic change needs a `backfill-stats` run over the period it
affects. `pnpm job:stats-backfill -- --days=365 --parkId=<uuid> --startDate=YYYY-MM-DD` enqueues it, but
its script (`scripts/trigger-stats-backfill.ts`) is local-only and not in the repository
([scripts](../development/scripts.md)); without it, add the job to the `stats` queue from Bull Board.

## Baselines

P50, P90 and the typical-day-peak are recomputed from 548 days of raw data every night (parks 03:00,
rides 04:00 UTC), so a logic change reaches every baseline within a day. To apply it now, enqueue
`calculate-park-baselines` (and `calculate-attraction-baselines`) on the `p50-baseline` queue from Bull
Board ([crowd levels: jobs](crowd-levels.md#jobs)).

## Caches

Old results stay in Redis until their TTL ends: `calendar:month:*` (assembled months),
`analytics:crowdlevel:*` (a day's crowd level), `park:integrated:*` (full park responses) and others.
`POST /v1/admin/flush-cache` (owner role) deletes every park cache family in
`PARK_CACHE_FLUSH_PATTERNS` (`src/admin/flush-cache-patterns.ts`) and leaves Bull's keys, admin sessions
and state such as `downtime:*` and `ml:hourly-fresh:*` alone. `POST /v1/admin/cache/reset?confirm=true`
does the same and then queues the full rebuild (holidays, parks, children, live data).

## Order

1. Change the code (queries and constants).
2. Run `backfill-stats` for the affected period, so the calendar history uses the new logic.
3. Let the baselines recompute (nightly or by hand), so live values and baselines agree again.
4. Flush the caches.
