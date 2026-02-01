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

A background processor `CacheWarmupProcessor` runs every 5 minutes (via BullMQ) to pre-calculate and cache:
- Park Statistics
- Park Occupancy
- Integrated Park Responses

This ensures that the first user request is always fast (hitting Redis).
