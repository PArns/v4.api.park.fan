# ml-service query performance

How the CatBoost serving path keeps its database reads cheap — the uuid-cast fix, the in-process
caches, the recent-history reads — and the measurements behind them. Open it before adding a query
to `ml-service`, `nf-service` or `pcn-service`.

## The uuid-cast fix, measured (PAR-871)

The rule is [cast the parameter, never the uuid column](../rules/uuid-casts.md). Here it was the two
serving reads in `predict.py` (`fetch_recent_rolling_stats`, `fetch_recent_wait_times`, ~90,000
calls a day each). A value the uuid filter drops is logged once per process. Where an empty id list
means "all parks" (`fetch_attraction_meta` in pcn/nf), the `WHERE` follows the input list, not the
filtered one, so a list of bad ids still matches nothing.

**Measured on production, Phantasialand (40 rides), warm cache, same rows:**

| Query | Before (`::text`) | After (`uuid[]`) |
|---|---|---|
| rolling stats, 90 d | 264,781 buffers, 289 ms exec + 85 ms plan, 61 seq scans on chunks | 28,195 buffers, 93 ms + 18 ms, 0 seq scans |
| recent waits, 32 d | 198,643 buffers, 165 ms + 35 ms | 50,520 buffers, 45 ms + 5 ms |
| historical occupancy (`db.py`) | 42,318 buffers, 62 ms + 459 ms plan | 30,315 buffers, 33 ms + 116 ms plan |
| `weather_data` averages | 1,611 buffers, 5 ms + 32 ms plan | 121 buffers, 1.5 ms + 2 ms |
| `schedule_entries` | 698 buffers | 4 buffers |

Old and new SQL return identical rows (`EXCEPT ALL` both ways = 0) for Phantasialand, Europa-Park
and Magic Kingdom on all five queries.

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
remaining caches is the real fix (PAR-1149).

## Connection pool

`ml-service/db.py`: `pool_size` 20, `max_overflow` 10, `pool_timeout` 30 s, `pool_pre_ping`.
