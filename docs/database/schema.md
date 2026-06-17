# Database Schema

## Overview

The database uses PostgreSQL with TimescaleDB extension for efficient time-series data storage. TypeORM is used as the ORM.

## Key Entities

### 1. Parks (`parks`)
Represents a theme park.
- `id`: UUID
- `name`: String
- `timezone`: Timezone string (e.g., 'Europe/Berlin')
- `externalId`: ID from upstream source (e.g., ThemeParks.wiki)
- `location`: Geometry point
- `slug`: URL-friendly identifier

### 2. Attractions (`attractions`)
Represents a ride or show within a park.
- `id`: UUID
- `parkId`: FK to Park
- `name`: String
- `externalId`: Upstream ID
- `status`: `OPERATING`, `CLOSED`, `DOWN`, `REFURBISHMENT`
- `type`: `RIDE`, `SHOW`, `RESTAURANT`

### 3. Queue Data (`queue_data`)
Stores raw wait time observations. This is a hypertable (TimescaleDB).
- `attractionId`: FK to Attraction
- `timestamp`: Time of observation
- `waitTime`: Minutes (integer)
- `status`: `OPERATING`, `CLOSED`, etc.
- `source`: Source of data (e.g., 'themeparks-wiki')

### 4. Wait Time Predictions (`wait_time_predictions`)
Stores ML-generated predictions.
- `attractionId`: FK to Attraction
- `timestamp`: Target time for prediction
- `predictedWaitTime`: Predicted minutes
- `confidence`: 0-1 score

### 5. Park Schedules (`schedule_entries`)
Operating hours for parks.
- `parkId`: FK to Park
- `date`: Date of schedule
- `visitingHours`: Open/Close times
- `type`: `OPERATING`, `CLOSED`, `Ticketed Event`

## Relationships

- **One-to-Many**: Park -> Attractions
- **One-to-Many**: Park -> Schedules
- **One-to-Many**: Attraction -> QueueData
- **One-to-Many**: Attraction -> Predictions

## Cache & Precomputed Tables

Precomputed analytics are stored in dedicated tables and optionally mirrored in Redis. See [Caching Strategy](../architecture/caching-strategy.md) for Redis keys and TTLs.

| Table | Purpose |
|-------|---------|
| `park_p50_baselines` | Park P50 baseline (headliner attractions only). **Primary** input for live occupancy / ratio-vs-P50; also stores `typicalDayPeak` (the **primary** calendar baseline). |
| `park_p90_baselines` | Park P90 (peak) baseline. Computed for free alongside P50; **not** the crowd-level reference any more. |
| `attraction_p50_baselines` | Per-attraction P50 baseline. **Primary** for live per-headliner ratios. |
| `attraction_p90_baselines` | Per-attraction P90 (peak) baseline. Computed for free; **not** the crowd-level reference (feeds the per-ride daily P90 the calendar averages). |
| `headliner_attractions` | Which attractions were selected as headliners per park (for baseline calculation). Also stores per-headliner `p50Wait548d` / `p90Wait548d` so park baselines are avg-of-per-headliner without a re-scan. |
| `attraction_hourly_history` | Per-day per-attraction 15-min-slot P90/avg/sampleCount rollup (JSONB `slots` array). Read by the attraction history endpoint for past days; today is computed live against `queue_data`. |
| `park_daily_stats` | Daily stats per park (P50/P90/max wait). Updated hourly for today, daily for yesterday. |
| `queue_data_aggregates` | Precomputed hourly percentiles (P25/P50/P75/P90/P95/P99) per attraction. Used by ML feature engineering. |

The P50/P90 baseline pair is recalculated nightly by the same cron (3 AM parks, 4 AM attractions) — PostgreSQL produces both percentiles in a single PERCENTILE_CONT sort. The 548-day **live** sliding-window calculation that used to back this on every cache miss was removed; missing rows mean "wait for the next cron run" rather than "scan half a billion data points now".

## Extensions

- **TimescaleDB**: Used for `queue_data` and potentially `wait_time_predictions` to handle high-volume time-series data efficiently.
- **PostGIS**: Used for location-based queries (nearby parks).
