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
| `WaitTimesProcessor` | Every 5 min | Fetches live wait times from external APIs (Themeparks, etc.) and pushes to DB/Redis. |
| `WeatherProcessor` | Hourly | Syncs current weather for all parks. |
| `HolidaysProcessor` | Daily | Syncs public/school holidays for next 365 days. |
| `ParkMetadataProcessor` | Daily/Weekly | Syncs static data (geo, images) for parks. |

### Analytics & ML
| Processor | Schedule | Purpose |
| --- | --- | --- |
| `OccupancyCalculationProcessor` | Every 5 min | Calculates current crowd levels based on live data. |
| `P50BaselineProcessor` | Daily (3 AM) | Recalculates historical P50 baselines for next day. |
| `QueuePercentileProcessor` | Daily | Pre-computes hourly percentiles (P50/P90) for fast lookups. |
| `PredictionGeneratorProcessor` | Hourly | Generates ML predictions for next 12h. |
| `ModelTrainingProcessor` | Weekly | Retrains the ML model on recent data. |

### Helper & Cleanups
| Processor | Purpose |
| --- | --- |
| `CacheWarmupProcessor` | Pre-fills Redis caches after data syncs to ensure low latency. |
| `EntityMappingsProcessor` | Maps external IDs (e.g., from different APIs) to our internal UUIDs. |
| `PredictionAccuracyProcessor` | Compares past predictions vs actuals to score model performance. |

## Flow Example: Wait Time Sync
1.  **Trigger**: CRON triggers `sync-wait-times` job.
2.  **WaitTimesProcessor**:
    *   Fetches data via `ExternalApisService`.
    *   Normalizes data (status, wait time).
    *   Saves to Postgres (`queue_data`).
    *   Updates Redis real-time keys.
    *   *Triggers* `OccupancyCalculationProcessor` (Event-driven).
