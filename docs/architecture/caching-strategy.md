# Redis Caching Strategy

## Overview

Redis is used aggressively to cache expensive computations and ensure low-latency API responses.

## Key Patterns

### 1. Integrated Responses
Full JSON responses for "Integrated" DTOs (aggregating weather, schedule, attractions) are cached.
- **Keys**: 
  - `park:integrated:{parkId}` (TTL: 5 min)
  - `attraction:integrated:{attractionId}` (TTL: 5 min)
- **Invalidation**: 
  - Time-based (TTL)
  - Event-based (via `CacheInvalidationService` when significant data changes)

### 2. Analytics & Statistics
Heavy analytical queries are cached with varying TTLs based on data volatility.
- **Keys**:
  - `park:statistics:{parkId}` (TTL: 5 min) - Aggregated wait times, active attraction counts.
  - `park:occupancy:{parkId}` (TTL: 5 min) - Current crowd level % calculation.
  - `park:p50:{parkId}` (TTL: 24h) - Park P50 baseline from headliners (table: `park_p50_baselines`).
  - `attraction:p50:{attractionId}` (TTL: 24h) - Attraction P50 baseline (table: `attraction_p50_baselines`).
  - `analytics:percentile:sliding:park:{parkId}` (TTL: 24h) - 548-day sliding P90/P50 for park (fallback).
  - `analytics:percentile:sliding:attraction:{attractionId}` (TTL: 24h) - 548-day sliding P90/P50 for attraction; shared by `get90thPercentileWithConfidence` and `getBatchAttractionP90s`.

### 3. Background Job Data
Shared state for background processors.
- **Keys**:
  - `downtime:current:{attractionId}` - Timestamp when an attraction went down (used to calc downtime duration).
  - `park:operating_hours:{parkId}` - Cached schedule for quick lookup.

## Caching Service (`src/common/cache/cache.service.ts`)

We use a standard NestJS service wrapping `ioredis`.
- **Method**: `getOrSet(key, ttl, fetcher)`
- **Pattern**: "Stale-While-Revalidate" is NOT currently implemented; we use hard expiration.

## DB Cache Tables (persistent pre-computed data)

Pre-computed values are stored in DB and optionally mirrored in Redis for fast reads.

| Table | Written by | Used for |
|-------|------------|----------|
| `park_p50_baselines` | P50 baseline job (daily) | Park occupancy/crowd level baseline (headliner P50). Redis: `park:p50:{parkId}`. |
| `attraction_p50_baselines` | P50 baseline job (daily) | Attraction crowd level baseline. Redis: `attraction:p50:{attractionId}`. |
| `park_daily_stats` | Stats job (hourly today, daily yesterday) | Park statistics (p90/max today), calendar P90. |
| `queue_data_aggregates` | Queue-percentile job (daily) | Hourly P25/P50/P75/P90 per attraction; used by `getParkPercentilesToday`, `getAttractionPercentilesToday`. |

Sliding-window percentiles (548-day) are not stored in DB; they are computed and cached in Redis only (`analytics:percentile:sliding:*`).

## Cache Warmup

**Service**: `CacheWarmupService` (`src/queues/services/cache-warmup.service.ts`).  
Warmup is **not** a separate BullMQ processor; it is invoked **after** data-sync jobs (e.g. wait-times, predictions) from the respective processors.

### When Warmup Runs

| Trigger | When | What gets warmed |
| --- | --- | --- |
| **Wait-times sync** (every 5 min) | After `WaitTimesProcessor` finishes | All parks (park integrated only), top 100 attractions, park occupancy; then async: discovery geo, global stats, park statistics (OPERATING parks). **Calendar** is **not** warmed here. |
| **Hourly predictions** | After `PredictionGeneratorProcessor` | Parks opening in next 12h (park integrated only). |
| **warmup-calendar-daily** (once per day, 5am) | Cron on `park-metadata` queue | **Calendar** for **all parks** (current month + next month, park timezone). |

### What Gets Warmed (per park, every 5 min)

When a park is warmed via `warmupParkCache(parkId, force)`:

1. **Park integrated response** — `park:integrated:{parkId}` (weather, schedule, attractions, live data). **Calendar is not warmed** in this flow.

**Skip logic**: If park integrated cache is already fresh (TTL > 2 min), the whole park warmup is skipped unless `force === true`. OPERATING parks are warmed with `force = true`; CLOSED parks with `force = false`.

### Calendar warmup (once per day)

Job **`warmup-calendar-daily`** runs daily at **5:00** (cron on `park-metadata` queue). It calls `warmupCalendarForAllParks()` and warms **per-month** keys `calendar:month:{parkId}:YYYY-MM:today+tomorrow` for **current month + next month** (park timezone) for **all parks**. So the calendar endpoint is fast after the first request of the day without warming every 5 min.

### Other Warmup Methods

- **Discovery**: `warmupDiscovery()` — geo structure and live stats (`/discovery/geo`).
- **Attractions**: `warmupTopAttractions(limit)` — top N attractions by queue-data frequency.
- **Occupancy**: `warmupParkOccupancy(parkIds)` — `park:occupancy:{parkId}` for given parks.
- **Statistics**: `warmupParkStatistics(parkIds)` — `park:statistics:{parkId}` for OPERATING parks.
- **Global stats**: `warmupGlobalStats()` — expensive global analytics query.

### Redis Keys Touched by Warmup

- `park:integrated:{parkId}`
- `calendar:month:{parkId}:YYYY-MM:today+tomorrow` (current + next month, per-month cache)
- `attraction:integrated:{attractionId}`
- `park:occupancy:{parkId}`
- `park:statistics:{parkId}`
- Discovery/structure caches (see DiscoveryService)

This keeps the first user request for parks and calendar fast (cache hit).
