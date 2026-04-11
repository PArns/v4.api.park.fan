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

### 2. Analytics & Statistics
Heavy analytical queries are cached with varying TTLs based on data volatility.
- **Keys**:
  - `park:statistics:{parkId}` (TTL: 5 min) - Aggregated wait times, active attraction counts.
  - `park:occupancy:{parkId}` (TTL: 5 min) - Current crowd level % calculation.
  - `analytics:crowdlevel:park:{parkId}:{date}` (TTL: 30m for today, 6h for past) - Daily crowd level and peak load.
  - `park:p50:{parkId}` (TTL: 24h) - Park P50 baseline from headliners (table: `park_p50_baselines`).
  - `attraction:p50:{attractionId}` (TTL: 24h) - Attraction P50 baseline (table: `attraction_p50_baselines`).
  - `analytics:percentile:sliding:park:{parkId}` (TTL: 24h) - 548-day sliding P90/P50 for park (fallback).
  - `analytics:percentile:sliding:attraction:{attractionId}` (TTL: 24h) - 548-day sliding P90/P50 for attraction.

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

| Trigger | When | What gets warmed |
| --- | --- | --- |
| **Wait-times sync** (every 5 min) | After `WaitTimesProcessor` | Parks (Operating + Popular), top 1000 attractions (User hits + Data density), occupancy, geo discovery, global stats, park statistics. |
| **Hourly predictions** | After `PredictionGeneratorProcessor` | Parks opening in next 12h. |
| **warmup-calendar-daily** (daily, 5am) | Cron on `park-metadata` | **Calendar** for **all parks** (-1 month to +3 months). |

### Attraction Warmup Strategy
The `warmupTopAttractions(limit=1000)` method combines two signals:
*   **User Traffic**: Top attractions currently being visited by users (from `PopularityService`).
*   **Data Density**: Attractions with the most frequent queue data updates in the last 7 days (Database proxy for activity).

## DB Cache Tables (persistent pre-computed data)

| Table | Written by | Used for |
|-------|------------|----------|
| `park_p50_baselines` | P50 baseline job (daily) | Park occupancy/crowd level baseline (headliner P50). |
| `attraction_p50_baselines` | P50 baseline job (daily) | Attraction crowd level baseline. |
| `park_daily_stats` | Stats job | Park statistics (p50/p90/max today/yesterday). |
| `queue_data_aggregates` | Queue-percentile job | Hourly wait-time aggregates. |
