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
  - `park:p50:{parkId}` (TTL: 24h) - Statistical baseline (changes daily).
  - `analytics:attraction:{id}:p90` (TTL: 24h) - Historical P90 baseline.

### 3. Background Job Data
Shared state for background processors.
- **Keys**:
  - `downtime:current:{attractionId}` - Timestamp when an attraction went down (used to calc downtime duration).
  - `park:operating_hours:{parkId}` - Cached schedule for quick lookup.

## Caching Service (`src/common/cache/cache.service.ts`)

We use a standard NestJS service wrapping `ioredis`.
- **Method**: `getOrSet(key, ttl, fetcher)`
- **Pattern**: "Stale-While-Revalidate" is NOT currently implemented; we use hard expiration.

## Cache Warmup

A background processor `CacheWarmupProcessor` runs every 5 minutes (via BullMQ) to pre-calculate and cache:
- Park Statistics
- Park Occupancy
- Integrated Park Responses

This ensures that the first user request is always fast (hitting Redis).
