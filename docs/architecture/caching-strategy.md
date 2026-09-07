# Redis Caching Strategy

## Overview

Redis is used aggressively to cache expensive computations and ensure low-latency API responses. The strategy combines time-based expiration with an intelligent, popularity-aware background warmup system.

## Key Patterns

### 1. Integrated Responses

Full JSON responses for "Integrated" DTOs (aggregating weather, schedule, attractions) are cached.

- **Keys**:
  - `park:integrated:{parkId}` (TTL: 5 min)
  - `attraction:integrated:{attractionId}` (TTL: 5 min)
- **Invalidation**:
  - Time-based (TTL)
  - Event-based (via `CacheInvalidationService` when significant data changes)

> **Publishing order — evict here BEFORE telling the frontend.** There are three
> caches in front of a write, and they have to be dealt with outside-in:
> `park:integrated:{parkId}` here (up to 6 h for a closed park, see
> `calculateDynamicTTL`), the Cloudflare edge copy (`max-age=300` +
> `stale-while-revalidate=600`, and **nothing in this service can purge it** —
> no Cloudflare API integration exists), and finally the frontend's own data
> cache, which pins a park snapshot for **24 h**.
>
> The frontend refetches the instant `RevalidationService.revalidateTags` tells
> it to. Calling that first therefore does not publish a write — it makes the
> frontend read the pre-write payload from Redis or the edge and freeze it for
> a day. Any job that writes park or attraction data must call
> `invalidateParkCaches()` first, revalidate second, and — because the edge
> cannot be purged — revalidate once more after the edge window has passed.
> `CuratedDataProcessor.publish()` is the reference implementation
> (`CDN_SETTLE_MS`).

#### Park opening and closing (`ParkStatusRevalidationService`)

The frontend pins a park's structure snapshot for 24 h, and two blocks in it are scoped to the
day rather than the week: a show's showtimes are dated to today, and this API reports every show
and restaurant as CLOSED for exactly as long as the park itself is. So the frontend's copy says
whatever was true at the hour that cache entry happened to be written — overnight, in practice.
On 2026-09-01 every park on park.fan served yesterday's showtimes under "no performances today"
while this API answered `OPERATING` with today's times.

Nothing on the frontend can fix that on a clock: opening times are per park and per day across
every timezone in the catalogue, and a sweep would drop 213 entries to catch the handful that
moved. `warmupOperatingParks()` already recomputes every park's status every five minutes, so the
transition is observed there for free and posted as **one** webhook carrying only the parks that
flipped — `park:<continent>/<country>/<city>/<slug>`, the geo path because slugs are unique per
destination and not globally (`disneyland-park` is Anaheim and Paris).

Two details are load-bearing:

- **It fires after the warmup loop, not before.** That loop is what refreshed
  `park:integrated:{parkId}`; revalidating first would send the frontend straight back into the
  pre-opening copy — the same outside-in rule as above.
- **Every transition is sent twice**, the second time after `CDN_SETTLE_MS`, because the edge copy
  still cannot be purged from here. Without it the frontend's re-fetch can land on a copy cached
  minutes before the gates opened and pin _that_ for a day, which is worse than the miss it was
  meant to fix. The repeat is scheduled in Redis (`revalidate:park-status:pending`) rather than on
  a queue: the method runs every five minutes anyway, so the cycle that finds an entry due sends
  it along with whatever else changed.

The previous cycle's statuses live in `revalidate:park-status`. An absent or unreadable snapshot
counts as the first run and reports nothing — reading it as "all 213 parks just changed" would
post the whole catalogue on every restart.

### 2. Analytics & Statistics

Heavy analytical queries are cached with varying TTLs based on data volatility.

- **Keys**:
  - `park:statistics:{parkId}` (TTL: 5 min) — Aggregated wait times, active attraction counts.
  - `park:occupancy:{parkId}` (TTL: 5 min) — Current crowd level % calculation (ratio-vs-P50).
  - `analytics:crowdlevel:park:{parkId}:{date}` (TTL: 30 min for today, 6 h for past) — Daily crowd level and peak load.
  - `park:p50:{parkId}` (TTL: 24 h) — Park P50 baseline from headliners (table: `park_p50_baselines`). JSON `{p50, confidence}`. **Primary** baseline for live occupancy (ratio-vs-P50).
  - `park:p90:{parkId}` (TTL: 24 h) — Park P90 baseline from headliners (table: `park_p90_baselines`). JSON `{p90, confidence}`. Computed for free; no longer the crowd-level reference.
  - `attraction:p50:{attractionId}` (TTL: 24 h) — Per-attraction P50 baseline (table: `attraction_p50_baselines`). **Primary** for the live park load (`Σ latest ÷ Σ P50` across headliners).
  - `attraction:p90:{attractionId}` (TTL: 24 h) — Per-attraction P90 baseline (table: `attraction_p90_baselines`). Computed for free; no longer the crowd-level reference.

> **Orphaned keys** — the previous P90 sliding-window precompute used `analytics:percentile:sliding:park:{parkId}` and `analytics:percentile:sliding:attraction:{attractionId}` to cache its 548-day live aggregation. Both the precompute job and the live-aggregation method have been removed; nothing writes or reads these keys any more. Existing entries TTL out within 24 h of deploy.

### 3. Calendar Monthly Cache

The calendar endpoint uses a per-month cache to handle various date ranges efficiently.

- **Keys**: `calendar:month:{parkId}:YYYY-MM:{includeHourly}`
- **TTL**: 5 min for the current month, 30 min for future months (allows for updated weather and ML predictions).

### 4. Popularity Tracking

Real-time user traffic is tracked using Redis Sorted Sets (`ZINCRBY`).

- **Keys**:
  - `popularity:parks`: Park hit counts (UUID).
  - `popularity:attractions`: Attraction hit counts (UUID).
- **Update**: Triggered by `PopularityInterceptor` on successful GET requests.

## Cache Warmup

**Service**: `CacheWarmupService` (`src/queues/services/cache-warmup.service.ts`).  
Warmup is invoked **after** data-sync jobs (e.g. wait-times, predictions).

### Priority Warmup Logic

Warmup tasks are executed **sequentially** to prevent database connection contention. Parks are sorted by priority:

1.  **Priority 1: OPERATING** parks (forced refresh).
2.  **Priority 2: HOT** parks (top 50 by user traffic).
3.  **Priority 3: All others** (warmed only if expired).

### When Warmup Runs

| Trigger                                | When                                 | What gets warmed                                                                                                                       |
| -------------------------------------- | ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| **Wait-times sync** (every 5 min)      | After `WaitTimesProcessor`           | Parks (Operating + Popular), top 1000 attractions (User hits + Data density), occupancy, geo discovery, global stats, park statistics. |
| **Hourly predictions**                 | After `PredictionGeneratorProcessor` | Parks opening in next 12h.                                                                                                             |
| **warmup-calendar-daily** (daily, 5am) | Cron on `park-metadata`              | **Calendar** for **all parks** (-1 month to +3 months).                                                                                |

### Attraction Warmup Strategy

The `warmupTopAttractions(limit=1000)` method combines two signals:

- **User Traffic**: Top attractions currently being visited by users (from `PopularityService`).
- **Data Density**: Attractions with the most frequent queue data updates in the last 7 days (Database proxy for activity).

## DB Cache Tables (persistent pre-computed data)

| Table                       | Written by                                 | Used for                                                                                                                                    |
| --------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `park_p50_baselines`        | P50 baseline cron (daily 03:00)            | **Primary** baseline for live occupancy (ratio-vs-P50); also holds `typicalDayPeak`, the **primary** calendar baseline.                     |
| `park_p90_baselines`        | Same cron, populated alongside P50         | Computed for free alongside P50; no longer the crowd-level baseline.                                                                        |
| `attraction_p50_baselines`  | P50 baseline cron (daily 04:00)            | **Primary** per-attraction baseline for live ratios.                                                                                        |
| `attraction_p90_baselines`  | Same cron, populated alongside P50         | Computed for free; no longer the per-attraction crowd-level baseline.                                                                       |
| `attraction_hourly_history` | Hourly-history cron (daily 04:30)          | Per-day per-attraction 15-min-slot P90/avg/sampleCount rollup; read by the attraction history endpoint for past days (today is still live). |
| `park_daily_stats`          | Stats cron (hourly today, daily yesterday) | Park statistics (p50/p90/max per day).                                                                                                      |
| `queue_data_aggregates`     | Queue-percentile cron (daily 02:00)        | Hourly wait-time aggregates (P25/P50/P75/P90/P95/P99) per attraction.                                                                       |
