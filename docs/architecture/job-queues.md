# Background Jobs & Queues

## Infrastructure
- **Framework**: BullMQ (Redis-based)
- **Dashboard**: Available at `/admin/queues` (protected)
- **Processor Location**: `src/queues/processors/*.processor.ts`

## Queue Responsibilities

The application relies heavily on background processing for data consistency and analytics.

### Data Synchronization (The "Heartbeat")
| Processor | Schedule | Purpose |
| --- | --- | --- |
| `WaitTimesProcessor` | Every 5 min | Fetches live wait times from external APIs (Themeparks, Queue-Times, Wartezeiten) and pushes to DB/Redis. |
| `WeatherProcessor` | Hourly | Syncs current weather for all parks. |
| `HolidaysProcessor` | Daily | Syncs public/school holidays for next 365 days. |
| `ParkMetadataProcessor` | Daily 03:00 (`sync-all-parks`), Daily 15:00 (`sync-schedules-only`) | Syncs static data (geo, images) and **opening hours** for parks. Job names must be `sync-all-parks` / `sync-schedules-only` (not `fetch-all-parks`). See [Schedule Sync & Calendar](schedule-sync-and-calendar.md). |
| `ChildrenMetadataProcessor` | Daily 04:00 | Combined sync of attractions / shows / restaurants metadata (replaced the deprecated per-entity-type metadata jobs). |

### Analytics & ML
| Processor | Schedule | Purpose |
| --- | --- | --- |
| `P50BaselineProcessor` | Daily — parks 03:00, attractions 04:00 | Recalculates **both** the P50 (median) and P90 (peak) 548-day baselines per park and per attraction. PostgreSQL computes both percentiles in a single PERCENTILE_CONT sort, so adding P90 was free on top of the existing P50 scan. Writes `park_p50_baselines`, `park_p90_baselines`, `attraction_p50_baselines`, `attraction_p90_baselines` plus the matching Redis cache keys (`park:p50:{id}`, `park:p90:{id}`, `attraction:p50:{id}`, `attraction:p90:{id}`, 24 h TTL). See [Crowd Levels](../analytics/crowd-levels.md). |
| `AttractionHourlyHistoryProcessor` | Daily 04:30 | Pre-aggregates yesterday's per-attraction 15-min-slot P90 / avg / sampleCount breakdown into `attraction_hourly_history`. The history endpoint reads past days from this table (one indexed SELECT) and only computes today's slots live. One GROUP BY query per park, idempotent. |
| `QueuePercentileProcessor` | Daily 02:00 | Pre-computes hourly percentiles (P25/P50/P75/P90/P95/P99) for the `queue_data_aggregates` table — fast lookups for ML feature engineering and analytics. |
| `StatsProcessor` | Hourly (today's stats), Daily 01:00 (yesterday) | Aggregates `queue_data` into `park_daily_stats`. |
| `PredictionGeneratorProcessor` | Every 15 min | Generates ML predictions for the next 12h. |
| `MlTrainingProcessor` | Daily 06:00 | Retrains the ML model on recent data. Labels now use the P90-based crowd-level definition; models recalibrate within ~1 cycle after the refactor. |
| `PredictionAccuracyProcessor` | Every 15 min | Compares past predictions vs. actuals to score model performance. |

### Helper & Cleanups
| Processor | Purpose |
| --- | --- |
| `CacheWarmupProcessor` | Pre-fills Redis caches after data syncs to ensure low latency. |
| `EntityMappingsProcessor` | Maps external IDs (e.g., from different APIs) to our internal UUIDs. |
| `GeoipUpdateProcessor` | Refreshes GeoLite2-City data every 48 hours. |

## Flow Example: Wait Time Sync
1.  **Trigger**: CRON triggers `sync-wait-times` job (every 5 min).
2.  **WaitTimesProcessor**:
    *   Fetches data via `MultiSourceOrchestrator`.
    *   Normalizes status / wait time across sources.
    *   Saves to Postgres (`queue_data`).
    *   Updates Redis real-time keys.

## Recently Retired Jobs

The following queues used to exist and have been removed — referenced here so a contributor reading old commits or runbooks can map them to their replacements:

| Removed | Replacement | Notes |
| --- | --- | --- |
| `OccupancyCalculationProcessor` (`precompute-p90-sliding-window`) | `P50BaselineProcessor` | The precompute job wrote a Redis cache (`analytics:percentile:sliding:*`) that nobody reads any more — the P90 baseline lives in `park_p90_baselines` / `attraction_p90_baselines` now. The orphaned Redis keys TTL out within 24 h of deploy. |
| `AttractionsMetadataProcessor`, `ShowsMetadataProcessor`, `RestaurantsMetadataProcessor` | `ChildrenMetadataProcessor` (Phase 6.2) | Combined sync replaced the per-entity-type processors. The old queue names were kept as stubs for several releases; this branch finally deletes them. |
