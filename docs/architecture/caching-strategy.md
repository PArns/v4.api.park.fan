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
  - `park:statistics:{parkId}` (TTL: 5 min) — Aggregated wait times, active attraction counts.
  - `park:occupancy:{parkId}` (TTL: 5 min) — Current crowd level % calculation (peak-vs-peak).
  - `analytics:crowdlevel:park:{parkId}:{date}` (TTL: 30 min for today, 6 h for past) — Daily crowd level and peak load.
  - `park:p50:{parkId}` (TTL: 24 h) — Park P50 baseline from headliners (table: `park_p50_baselines`). JSON `{p50, confidence}`.
  - `park:p90:{parkId}` (TTL: 24 h) — Park P90 baseline from headliners (table: `park_p90_baselines`). JSON `{p90, confidence}`. **Primary** baseline for crowd-level reading; P50 is the fallback.
  - `attraction:p50:{attractionId}` (TTL: 24 h) — Per-attraction P50 baseline (table: `attraction_p50_baselines`).
  - `attraction:p90:{attractionId}` (TTL: 24 h) — Per-attraction P90 baseline (table: `attraction_p90_baselines`). **Primary** baseline; P50 is the fallback.

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
| `park_p50_baselines` | P50 baseline cron (daily 03:00) | Fallback for park crowd-level baseline + avg-shaped surfaces. |
| `park_p90_baselines` | Same cron, populated alongside P50 | **Primary** park crowd-level baseline (peak-vs-peak). |
| `attraction_p50_baselines` | P50 baseline cron (daily 04:00) | Fallback per-attraction baseline. |
| `attraction_p90_baselines` | Same cron, populated alongside P50 | **Primary** per-attraction baseline. |
| `attraction_hourly_history` | Hourly-history cron (daily 04:30) | Per-day per-attraction 15-min-slot P90/avg/sampleCount rollup; read by the attraction history endpoint for past days (today is still live). |
| `park_daily_stats` | Stats cron (hourly today, daily yesterday) | Park statistics (p50/p90/max per day). |
| `queue_data_aggregates` | Queue-percentile cron (daily 02:00) | Hourly wait-time aggregates (P25/P50/P75/P90/P95/P99) per attraction. |
