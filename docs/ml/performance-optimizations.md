# ml-service query performance

How the CatBoost serving path keeps its database reads cheap — the uuid-cast rule that applies to
every service, the in-process caches, the recent-history reads — and the measurements behind
them. Open it before adding a query to `ml-service`, `nf-service` or `pcn-service`, or any uuid
filter in the API.

## Never cast an indexed uuid column; cast the parameter (PAR-871)

**Rule.** A filter on a uuid column compares uuid to uuid. The cast goes on the bound value, never
on the column:

```sql
-- WRONG: the column is hidden from its index
WHERE qd."attractionId"::text = ANY(:attraction_ids)
WHERE "parkId"::text = :park_id

-- RIGHT
WHERE qd."attractionId" = ANY(CAST(:attraction_ids AS uuid[]))   -- SQLAlchemy
WHERE "parkId" = CAST(:park_id AS uuid)
WHERE a."parkId" = $1::uuid      /  = ANY($1::uuid[])             -- TypeORM / pg
```

A cast on the column turns the predicate into an expression no index covers. On `queue_data` it
is worse than a missed index: compressed chunks are filtered on their segment-by column, and with
a text expression TimescaleDB decompresses every chunk in the time window instead. The two serving
reads in `predict.py` (`fetch_recent_rolling_stats`, `fetch_recent_wait_times`, ~90,000 calls a day
each) carried this cast and were ~80 % of all database time on 2026-10-12. An expression index
does not help (segment-by filtering on compressed chunks does not use it); fixing the predicate
does.

**The one trap.** Casting the parameter raises `22P02` on a value that is not a uuid, which fails
the whole query, where the text comparison just matched nothing. So ids are filtered before they
are bound: `uuid_params.uuid_list` / `uuid_or_none` (one identical copy in each of `ml-service`,
`pcn-service`, `nf-service`) and `isCanonicalUuid` in `src/common/utils/uuid-param.util.ts`. Both
keep only the lowercase hyphenated form, which is exactly what `uuid::text` prints, so they drop
only values the old comparison could never have matched. A dropped value is logged once per
process. Where an empty id list means "all parks" (`fetch_attraction_meta` in pcn/nf), the
`WHERE` follows the input list, not the filtered one, so a list of bad ids still matches nothing.

Output casts (`SELECT qd."attractionId"::text AS "attractionId"`) are fine: they run after the
filter. Columns that really are text (`queue_data_aggregates."attractionId"`) are compared as text.

**Measured on production, Phantasialand (40 rides), warm cache, same rows:**

| Query | Before (`::text`) | After (`uuid[]`) |
|---|---|---|
| rolling stats, 90 d | 264,781 buffers, 289 ms exec + 85 ms plan, 61 seq scans on chunks | 28,195 buffers, 93 ms + 18 ms, 0 seq scans |
| recent waits, 32 d | 198,643 buffers, 165 ms + 35 ms | 50,520 buffers, 45 ms + 5 ms |
| historical occupancy (`db.py`) | 42,318 buffers, 62 ms + 459 ms plan | 30,315 buffers, 33 ms + 116 ms plan |
| `weather_data` averages | 1,611 buffers, 5 ms + 32 ms plan | 121 buffers, 1.5 ms + 2 ms |
| `schedule_entries` | 698 buffers | 4 buffers |

Old and new SQL return identical rows (`EXCEPT ALL` both ways = 0) for Phantasialand, Europa-Park
and Magic Kingdom on all five queries. `ml-service/tests/test_uuid_params.py` (and the copies in
`pcn-service` and `nf-service/tests`) fail on any `::text =`, `::text IN` or `::text <>` in the
service's query files.

## Recent history: two reads, both shaped like training

- **`fetch_recent_wait_times`** — one row per (ride, park-local date, hour) over
  `RECENT_LOOKUP_DAYS` (32), for the date/hour lag lookups.
- **`fetch_recent_rolling_stats`** — one row per ride: `rolling_avg_7d/_weekday/_weekend/_28d/_90d`
  and the matching stds, computed in SQL over the same time-based, left-closed windows training
  uses (one median per hour, park-local weekday split, sample std).

Their predecessor read 730 days with nine ROWS-based window functions and took `.iloc[-1]` under
`ORDER BY date DESC` — the **oldest** row — so every served rolling feature was the value from
the start of the two-year window (Taron, 2026-10-01: `rolling_avg_7d` 54.75 served against 28.8
real), and that query alone was ~2/3 of all Postgres execution time.

## In-process caches

| Cache | Where | TTL |
|---|---|---|
| holidays | `db.py` | 1 h |
| parks metadata | `db.py` | 5 min |
| schedules | `db.py` | 5 min |
| historical park occupancy (weekday × hour) | `db.py` | 1 h, expired entries evicted |
| weather climatology | `predict.py` | 1 h |
| recent wait times, recent rolling stats | `predict.py` | 15 min; the end time is floored to the 15-min window, so one key and one SQL bound per window; expired entries evicted on write |

Every cache hands out `.copy()`, so prediction code cannot mutate a cached frame. TTLs are
time-based, not event-based: a holiday or a weather average changes rarely, a schedule a little
more often. The holidays, schedules and weather caches check the TTL on read but never evict, and
their keys carry date ranges and park sets; when no cache evicted, the 2 gunicorn workers grew to
~15 GiB over ~25 h. `gunicorn.conf.py` recycles a
worker after `ML_MAX_REQUESTS` (1000, jitter 200) requests, which caps that growth; bounding the
remaining caches is the real fix.

## Connection pool

`ml-service/db.py`: `pool_size` 20, `max_overflow` 10, `pool_timeout` 30 s, `pool_pre_ping`.
