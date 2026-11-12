# Caching strategy (Redis)

Which responses and computations are cached in Redis, for how long, how the caches are warmed, and how
a change reaches the frontend through three layers of cache. Open this before adding a cache key, a TTL
or a job that changes what a page shows. Key builders live in `CacheKeys`
(`src/common/cache/cache-keys.ts`); shared TTLs in `src/common/cache/cache-ttl.ts`. The frontend's side:
its `docs/architecture/caching-strategy.md`.

## Keys and TTLs

| Key | TTL | Notes |
| --- | --- | --- |
| `park:integrated:{parkId}` | OPERATING: to the next 5-min boundary (+5 s); CLOSED: until 5 min before the next opening, max 6 h ±5 min; no next opening: 30 min | `calculateDynamicTTL`. Cold misses and stale-while-revalidate refreshes run through one in-process `SingleFlight` per park, and the refresh skips the cache read |
| `attraction:integrated:{attractionId}:{days}` | to the next 5-min boundary (+5 s) | `days` = history length asked for (1–365, default 30); evict all variants with `attractionIntegratedPattern` |
| `attraction:history:{attractionId}:…` | 5 min with today in range, else 24 h | Attraction wait-time history |
| `calendar:month:{parkId}:{YYYY-MM}:{includeHourly}` | 15 min current month, 30 min other months; warmed months 13 h, warmed current month 2 h | Only full months are stored; any range is sliced from months, and on a partial hit only the missing months are built. Warmup re-`expire`s them (`CALENDAR_WARMUP_MONTH_TTL`, `CALENDAR_WARMUP_CURRENT_MONTH_TTL`) |
| `best-days:{parkId}` | 13 h (`CALENDAR_WARMUP_MONTH_TTL`, the warmed months it is built from) | `/best-days` snapshot ([frontend contract](../frontend/best-days-endpoint.md)) |
| `trip-regions:calendar:v1:{region}:{from}:{to}`, `trip-regions:plan:v1:{region}:{query}` | 1 h (`TRIP_REGION_RESPONSE_TTL`) | Whole keyed answers of the [trip regions](../frontend/trip-regions.md); the plan by its normalized query, so parameters in another order hit the same entry |
| `trip-regions:last-year:v1:{parkId}:{YYYY-MM}` | 7 days plus up to a day of spread (`TRIP_LAST_YEAR_TTL`) | A passed month's measured crowd level and headliner day peak per day, for the `last_year` days; written by the calendar warmup or on a miss, and not evicted by a schedule sync |
| `trip-regions:forecast-until:v1:{parkId}` | 13 h | The last day a region park's warm months carry a forecast (`meta.forecastUntil`) |
| `ml:park:{parkId}:{type}:{date}` | hourly 30 min, daily 13 h | Same single-flight guard as the park payload |
| `ml:tft-daily:{parkId}:{days}:{today}` | 13 h | Evicted (`ml:tft-daily:*`) when `train-nf` completes (PAR-1053); caches built on it still run out on their own TTLs |
| `ml:hourly-fresh:{attractionId}` | 30 min | Freshness marker of the diff-written hourly predictions ([schema](../database/schema.md#write-path-par-874)) |
| `park:statistics:{parkId}`, `park:occupancy:{parkId}` | 5 min | Occupancy is ratio-vs-P50 ([crowd levels](../rules/crowd-levels.md)) |
| `analytics:crowdlevel:park:{parkId}:{date}` | 30 min today, 6 h past | Daily crowd level; the calendar reads all past days with one `MGET` |
| `park:p50:{id}`, `park:p90:{id}`, `attraction:p50:{id}`, `attraction:p90:{id}`, `park:typicalpeak:{id}`, `attraction:typicalpeak:{id}` | per key: [crowd levels](../analytics/crowd-levels.md#baselines-and-where-they-live) | Mirrors of the baseline tables ([schema](../database/schema.md#precomputed-tables)) |
| `count:queue_data` | 6 h (other `count:*` 1 h) | Exact global row total; `approximate_row_count` overstated it 28× on compressed chunks |
| `health:count:queue_data_24h`, `health:count:wait_time_predictions_24h` | 10 min | `/health` counts, shared across instances (PAR-872); a failed count is never cached. Liveness is `/health/ready` |
| `analytics:headliners:{parkId}` | 6 h | `getHeadlinerAttractions`, read-through |
| `park:opdaterange:{parkId}` | 1 h | `ParksService.getOperatingDateRange` (MIN/MAX OPERATING date), read on every calendar build and gap fill |
| `holiday:year:{country}:{year}` | 24 h, plus a 10-min in-process memo | `HolidaysService.getHolidays` caches whole years, so any range is served from them |
| `search:index:v1:*`, `search:fuzzy:v2:{type}:{limit}:{q}` | 2 h, 5 min | [Search](search.md) |
| `weather:*` | per surface | [Weather](weather.md#call-cadence-per-surface) |
| `popularity:parks`, `popularity:attractions` | sorted sets, decayed daily | `ZINCRBY` from `PopularityInterceptor` on successful GETs; feeds warmup priority |
| `site-analytics:*` | buffers `buf:*` none (parked ones 7 days); a visit until the flush ends it, at most 40 min after its last hit, and its place in `visits:open` at most 2 h; caps to the end of the UTC day + 1 min; day visitors, also by country, device, browser, OS and channel, to its end + 1 h; guard sets and drop counts to the end of the next day; realtime and live view minutes 35 min | State, not cache, and never flushed: the [site statistic](../frontend/site-analytics.md#privacy)'s counts until the flusher writes them, its visits, caps, guards, HyperLogLogs and the live view's minutes. Keys standing for a reader are HMACs under the UTC day's key |

Everything park-scoped is evicted together by `invalidateParkCaches(redis, parkId, attractionIds)`
(`src/common/cache/park-cache-invalidation.ts`), including the discovery geo structure, the `/nearby`
coordinate index and the attraction sitemap. A change to what a flat cached list contains (the sitemap,
the geo structure) needs a version bump in `CacheKeys`.

## Publishing order

The order in which a write lets go of the three caches in front of it (here, Cloudflare, the
frontend's data cache) is the rule [publish a write outside-in](../rules/publish-outside-in.md). The
park open/close webhook below applies it every five minutes.

### Park opening and closing (`ParkStatusRevalidationService`)

Two blocks of the frontend's 24-hour park snapshot are scoped to the day: today's showtimes, and the
CLOSED status this API gives every show and restaurant while the park is closed. The snapshot says
whatever was true when it was written, overnight in practice: on 2026-09-01 every park served
yesterday's showtimes under "no performances today". A frontend clock cannot fix that across every
timezone, so `warmupOperatingParks()`, which recomputes every park's status every five minutes, posts
**one** webhook with only the parks that flipped, tagged `park:<continent>/<country>/<city>/<slug>` (the
geo path, because slugs repeat: `disneyland-park` is Anaheim and Paris). The tag comes from
`parkCacheTag` (`src/common/revalidation/park-status-transitions.ts`), a hand-written twin of park.fan's
`parkCacheTag` in `lib/api/park-live-projection.ts`: change both or neither.

- It fires **after** the warmup loop, which is what refreshed `park:integrated`.
- Every transition is sent **twice**, the second after `CDN_SETTLE_MS`, scheduled in Redis
  (`revalidate:park-status:pending`) and picked up by whichever five-minute cycle finds it due.
- The previous statuses live in `revalidate:park-status`; an absent or unreadable snapshot counts as a
  first run and reports nothing, or every restart would post all parks.
- **While a park is OPERATING** its tag goes out again about every 15 minutes (PAR-1103), in the
  default stale-while-revalidate profile, so a crawled park page shows waits at most ~15 minutes plus
  the edge window old instead of up to a day. The last send per tag lives in
  `revalidate:park-status:refreshed`; a park seen open for the first time is recorded, not sent, and a
  closed park drops out, so closed parks cost no extra writes. Gated, like every ping, by
  `REVALIDATE_SECRET`.

## Cache warmup

`CacheWarmupService` (`src/queues/services/cache-warmup.service.ts`) runs one park at a time to avoid
connection contention, in priority order: popular OPERATING parks, other OPERATING parks (both forced
to refresh), popular CLOSED parks (top 50 by `PopularityService`), then the rest only where expired.

| Trigger | What is warmed |
| --- | --- |
| `live-cache-warmup`, after every wait-times run | parks, top 1000 attractions (`warmupTopAttractions`: user traffic plus the rides with the most `queue_data` rows in 7 days), occupancy, geo discovery, global and park statistics ([job queues](job-queues.md#the-wait-times-run-and-its-warmup)) |
| `generate-hourly` predictions | parks opening in the next 12 h (`warmupUpcomingParks`) |
| `warmup-calendar-daily`, twice a day ([schedule](job-queues.md#schedule-utc)) | calendar months −1…+3 (the `none` variant the frontend reads) for every park, after evicting the daily ML and month caches, and the `/best-days` snapshot; the morning run follows the CatBoost training so the calendar serves fresh predictions without the ~15 s cold path. A trip region's parks get −1…+6, their last forecast day, and the last-year snapshots of −6…−2 where missing ([below](#what-the-trip-regions-add)) |

### What the trip regions add

The [trip-region calendar](../frontend/trip-regions.md) serves a year ahead: the forecast for as far
as it reaches, the same weekday a year back past it. So its 16 parks (`regions.ts`, matched by their
geographic path in `regionOfPark`) are warmed further than the rest. Estimated from the code paths,
not measured; read the real figure off the warmup's own line (`✅ Calendar warmup complete: … in …s`)
on the first runs after the deploy.

- **Months +4…+6: 48 more month builds per run** (3 × 16) on top of about 1,000 (5 months × ~200
  parks), about 5 %. A future month that far out costs no ML call (the park's daily serving
  predictions are cached by the run's first month) and no `queue_data` scan: about eight indexed
  reads (schedule, weather, measured-operation verdicts, maintenance windows, headliner names) and
  cached lookups, so some 400 light queries a run.
- **Last-year snapshots −6…−2: 80 past-month builds when all are missing**, which is the first run
  after the deploy. Each is about 31 `calculateCrowdLevelForDate` scans over a day of `queue_data`
  (their own cache lives 6 h, so a warmup finds them cold) plus one `getHeadlinerDailyPeaks` over the
  month: about 2,560 day scans in that one run. After it a snapshot lives a week, spread by up to a
  day, so about 12 month builds a day, plus 16 when a month turns. A schedule sync does not evict
  them, unlike the month caches it evicts twice a day, which is why they are a key of their own.
- **Redis:** a region calendar entry is about 300 bytes per park and day (16 parks × 31 days ≈
  150 KB for a month); entries live an hour, one per window asked for.

## Database cache tables

The precomputed tables (baselines, hourly history, daily stats, aggregates) are described with their
writers in [Schema → precomputed tables](../database/schema.md#precomputed-tables).
