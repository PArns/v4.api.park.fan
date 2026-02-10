# Database Indexes

Overview of indexes and which queries they cover. Entities define indexes via TypeORM `@Index()`; with `synchronize: true` they are created on app startup.

## queue_data

| Index | Columns | Partial WHERE | Usage |
|-------|---------|----------------|--------|
| (attractionId, timestamp) | Base | – | findCurrentStatusByAttraction, general time-series |
| (attractionId, queueType, timestamp) | DISTINCT ON | – | findCurrentStatusByPark, findCurrentStatusByAttraction |
| **idx_queue_data_operating** | (attractionId, timestamp) | status = 'OPERATING' | Analytics, OPERATING filter |
| **idx_queue_data_ml_recent** | (attractionId, timestamp) | status = 'OPERATING' AND queueType = 'STANDBY' | **ML fetch_recent_wait_times** (predict.py) |
| (attractionId, queueType, status, timestamp) | Full filter | – | Training, History (STANDBY + OPERATING) |
| (queueType, status, timestamp) | – | – | ML Training (fetch_training_data) |
| idx_queue_data_down | (attractionId, timestamp) | status = 'DOWN' | Down count |
| (timestamp) | – | – | Cleanup, global time range |

**Note:** TimescaleDB hypertable is partitioned by `timestamp`; indexes apply per chunk.

## schedule_entries

| Index | Columns | Usage |
|-------|---------|--------|
| (parkId, date, scheduleType) | Range parkId + date | getSchedule, getUpcomingSchedule; **ML fetch_schedule_entries_for_prediction** (parkId + date BETWEEN) |
| (parkId, scheduleType, openingTime) | “open now” status | getBatchParkStatusFromDb (openingTime ≤ now, closingTime > now) |
| (date) | – | Calendar / time span |
| (attractionId, date) | Attraction schedule | Attraction-specific entries |

## holidays

| Index | Columns | Usage |
|-------|---------|--------|
| (country, date) | – | **ML fetch_holidays** (country = ANY, date BETWEEN) |
| (country, region, date) | – | Regional holidays |

## weather_data

| Index | Columns | Usage |
|-------|---------|--------|
| (parkId, date) | Entity: `@Index(["park", "date"])` → FK parkId | **ML predict.py** (parkId + EXTRACT(MONTH FROM date), date range); WeatherService |

## queue_data_aggregates

| Index | Columns | Usage |
|-------|---------|--------|
| (attractionId, hour) | – | Percentile lookups, ML percentile_features |
| (parkId, hour) | – | Park-wide aggregate queries |
| (hour) | – | Time range |

## Other tables

- **parks, attractions, shows, restaurants**: e.g. slug/name (including trigram for search), parkId.
- **park_daily_stats**: (parkId, date) unique.
- **park_p50_baselines, attraction_p50_baselines**: confidence, calculatedAt, parkId depending on query.

## Creating indexes (after entity change)

- **Development:** TypeORM `synchronize: true` creates new indexes on app startup.
- **Production:** Create a migration or run `CREATE INDEX ...` manually (e.g. derived from the entity). Partial index for `idx_queue_data_ml_recent`:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS "IDX_..." ON queue_data ("attractionId", timestamp)
WHERE "status" = 'OPERATING' AND "queueType" = 'STANDBY';
```

(Exact name depends on TypeORM naming; with synchronize it is generated automatically.)
