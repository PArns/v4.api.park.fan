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
| `park_p50_baselines` | Park P50 baseline (headliner attractions only); used for park occupancy/crowd level. |
| `attraction_p50_baselines` | Per-attraction P50 baseline; used for ride crowd level. |
| `headliner_attractions` | Which attractions were selected as headliners per park (for baseline calculation). |
| `park_daily_stats` | Daily stats per park (P90, max wait, etc.); updated hourly for today, daily for yesterday. |
| `queue_data_aggregates` | Precomputed hourly percentiles (P25/P50/P75/P90) per attraction for fast lookups. |

Sliding-window percentiles (548-day) are not stored in DB; they are computed and cached in Redis only.

## Extensions

- **TimescaleDB**: Used for `queue_data` and potentially `wait_time_predictions` to handle high-volume time-series data efficiently.
- **PostGIS**: Used for location-based queries (nearby parks).
