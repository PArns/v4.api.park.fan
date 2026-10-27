# Database schema

What lives in PostgreSQL + TimescaleDB, where each table's definition comes from, and the tables whose
write path or read rules are not obvious from the entity. Open this before querying or adding a table.
How the schema reaches production: [the entities are the schema](../rules/the-entities-are-the-schema.md).

## Where the schema is defined

- **Entities** (`src/**/entities/*.entity.ts`, 64 of them) — TypeORM `synchronize` creates and alters
  their tables at boot. Column names are camelCase unless the entity gives a `name:` (`"parkId"`,
  `"attractionId"`, `"queueType"` must be quoted in SQL; newer columns are snake_case, e.g.
  `open_with_park`, `retired_at`).
- **Hypertables** — `HYPERTABLES` in `src/database/hypertables.ts`, applied by `TimescaleInitService`:

  | Table | Time column | Chunk | Compressed after |
  | --- | --- | --- | --- |
  | `queue_data` | `timestamp` | 1 day | 30 days (no `segmentby`) |
  | `forecast_data` | `createdAt` | 1 day | 7 days |
  | `weather_data` | `date` | 7 days | 60 days |
  | `show_live_data`, `restaurant_live_data` | `timestamp` | 1 day | 30 days |
  | `queue_data_aggregates` | `hour` | 7 days | 30 days |
  | `wait_time_predictions` | `createdAt` | 7 days | 14 days, `segmentby attractionId` |

- **Tables of the Python services** (`pcn_forecasts`, `tft_forecasts`, `tft_forecasts_shadow`,
  `tft_forecasts_shadow_drivers`) have no entity; their DDL is in `pcn-service/db.py` / `nf-service/db.py`.
- No PostGIS: coordinates are `latitude` / `longitude` decimals (Postgres returns them as strings).

## Core tables

| Table | What it holds | Notes |
| --- | --- | --- |
| `parks` | one row per park | `slug` is unique per destination, not globally (`disneyland-park` is Anaheim and Paris) — group by `id`. `timezone` is read everywhere ([park timezone](../rules/park-timezone.md)). Source ids `wikiEntityId`, `queueTimesEntityId`, `wartezeitenEntityId`; `primaryDataSource`; `influencingRegions`; curated columns ([curation](../admin/curation.md)) |
| `attractions` | one row per ride | `externalId` unique globally, `slug` unique per park ([identity](../architecture/attraction-status/identity.md)). Synced and curated columns side by side; `is_seasonal`, `season_months`, `season_out_since` ([seasonality](../architecture/attraction-status/seasonality.md)); `retired_at` / `retired_reason` / `retired_kind`, `absent_since`, `last_listed_at` ([reclassification](../architecture/attraction-status/reclassification-and-absence.md)) |
| `shows`, `restaurants` | the other children of a park | `externalId` unique per table; `retired_at` / `retired_reason` |
| `queue_data` | every wait-time reading | Written on change plus an hourly heartbeat ([ingestion](../architecture/data-ingestion.md)); `data_source` (`system-reconciliation` / `system-heartbeat` are ours), `is_heartbeat`, `lastUpdated`, `queueType`, `status`. Read observations through `observedReadingsSql()` |
| `schedule_entries` | opening hours per park-day (and per ride) | `scheduleType`, `openingTime`, `closingTime`, holiday flags; one day-status row per day plus event rows ([schedule sync](../architecture/schedule-sync-and-calendar.md)) |
| `holidays` | public, school and `working_day` rows | keyed by `externalId` (contains the date) ([holiday sources](../architecture/holiday-sources-outside-europe.md)) |
| `weather_data` | daily weather per park | PK `(parkId, date)`, stored at noon UTC ([weather](../architecture/weather.md)) |
| `external_entity_mapping` | `<source>:<externalId>` → internal row | how live data finds a row a second source also reports |
| `attraction_review_marks`, `attraction_slug_aliases`, `park_slug_aliases` | human verdicts; old slugs that redirect | [review marks](../architecture/attraction-status/review-marks.md) |
| `admin_users`, `admin_audit_log` | admin accounts and every admin write | [authentication](../admin/authentication.md) |

## Precomputed tables

Written by the nightly jobs ([job queues](../architecture/job-queues.md)), read instead of scanning
`queue_data`, several mirrored in Redis ([caching](../architecture/caching-strategy.md)):

| Table | Written by | Used for |
| --- | --- | --- |
| `park_p50_baselines` | `p50-baseline`, 03:00 | park P50 (live occupancy) and `typicalDayPeak` (calendar baseline) ([crowd levels](../rules/crowd-levels.md)) |
| `park_p90_baselines` | same run | computed alongside; not a crowd-level reference |
| `attraction_p50_baselines`, `attraction_p90_baselines` | `p50-baseline`, 04:00 | per-ride live ratio (P50); the per-ride daily P90 the calendar averages |
| `headliner_attractions` | `p50-baseline` | headliners per park, with per-ride 548-day `p50Wait548d` / `p90Wait548d`, so park baselines need no re-scan |
| `attraction_hourly_history` | 04:30 | per-day, per-ride 15-min slots (P90 / avg / samples, JSONB) and `downCount`; past days of the history endpoint |
| `attraction_day_operating` | `detect-seasonal` (`refreshOperatingDayRollup`) | which days a ride reported OPERATING |
| `park_day_operations` | 04:45 | measured-operation verdicts ([calendar rules §1.5](../architecture/calendar-schedule-and-ml-rules.md#15-a-past-closed-day-the-measurement-overrules-par-697)) |
| `park_daily_stats` | `stats`, hourly / 01:00 | park P50 / P90 / max per day |
| `queue_data_aggregates` | `analytics`, 02:00 | hourly P25…P99 per ride, for ML features and `getTypicalStatsForHour` |

Both columns of `attraction_hourly_history` are built from observations only (`observedReadingsSql`):
`slots` since PAR-729, `downCount` since PAR-897. Rows written before those deploys were not recomputed,
so for older days up to 45 % of a park's slots come from carried heartbeats and 1.64 % of rows overstate
`downCount` by a median of 2 h. A `backfill-attraction-hourly-history` run over those days removes that
slot coverage rather than repairing it.

The 548-day **live** P90 sliding window that used to back the baselines on every cache miss is gone; a
missing baseline row means "wait for the next run".

## Forecast tables

| Table | Notes |
| --- | --- |
| `wait_time_predictions` | CatBoost's hourly (15-min slots) and daily predictions; write path below |
| `tft_forecasts`, `tft_forecasts_shadow`, `tft_forecasts_shadow_drivers` | nf-service's daily peak per (attraction, target_date, forecast_date), every origin. Plain tables; retention 120 days by `forecast_date` (`db-maintenance/prune-tft-forecasts`, 04:35 UTC, 200 k-row batches; the oldest origin a reader asks for is 107 days, readers listed in `src/database/maintenance/vacuum-plan.ts`) |
| `pcn_forecasts` | pcn-service's 15-min intraday forecast; retention 4 days by `created_at` in pcn-service (PAR-873); primary key rebuilt weekly ([nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md)) |
| `weather_forecast_issues`, `schedule_day_versions` | append-only as-of logs, write-only for now ([covariate source](../architecture/covariate-source.md) §3.2). No FK, no hypertable; a park merge moves them (`PARK_DEPENDENCIES`) |

### `wait_time_predictions`

Primary key `(attractionId, predictionType, createdAt, predictedTime)`: the natural key plus the
partition column, which TimescaleDB requires in every unique index (the live order on production,
verified 2026-10-12; `timescale-init.service.ts` derives it from the running schema). `predictedTime` is
the slot (15 min for `hourly`, a day for `daily`); `predictedWaitTime`, `confidence`,
`uncertaintyMinutes`, `crowdLevel`, `status`, `baseline` and `modelVersion` are the served answer.

### Write path (PAR-874)

`generate-hourly` runs every 15 minutes over a 48-hour horizon. It used to delete every future hourly row
of a park and insert the whole horizon again — ~25 M inserts and as many deletes a day, 94 % of the rows
deleted again, and chunk primary keys at 15–25× their natural size (8.5 GB instead of ~0.3 GB per 7-day
chunk). It now **diff-writes** (`MLService.storePredictions(…, { replaceHourlyForPark })`,
`src/ml/utils/prediction-diff-write.util.ts`):

1. read the park's stored hourly rows with `predictedTime >= now` and `createdAt >= now - 50 h`;
2. compare each slot's newest row with the new answer on every served column — an unchanged slot is
   **not written**;
3. insert changed and new slots, then delete superseded rows and every stored future slot the run no
   longer predicts. Insert first, so a reader never sees a hole (it takes the newest row per slot).

A slot already under way is outside the comparison; the run's row for it is appended, so the day curve
keeps the row written before the slot began. `ON CONFLICT … DO UPDATE` is impossible: the slot cannot be
a unique key, since every unique index must contain `createdAt`.

**For readers:** a row's `createdAt` is when its *value* was written, not when it was last confirmed.
Freshness moved to Redis: every run sets `ml:hourly-fresh:{attractionId}` (30 min), and
`getStoredPredictions` / `getBatchStoredPredictions` serve a ride's hourly rows when the marker exists or
a row is younger than 30 minutes (so hourly serving depends on Redis). They read the 50-hour window and
return the newest row per slot. Readers of the newest row per slot (`prediction-deviation`, the PCN
scorer, the nf-service backtests, the daily CatBoost snapshot) are unaffected. `stats/day`'s
`first_predicted` takes the *oldest* row and now reaches further back: an unchanged slot keeps the row of
the run that last changed it. Measured 2026-10-12: 59–80 % of slots unchanged per run (88–96 % on
`predictedWaitTime` alone, most other changes `confidence` moving ~0.7 points); the run after the 06:00
retrain rewrites everything (`modelVersion`).

`deduplicatePredictions` remains for `daily` only; its `createdAt` window is per type
(`DEDUPE_CREATED_AFTER_HOURS`: 72 h hourly, 8 days daily) so its delete stays on the newest chunks.
Retention: [nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md#predictionscleanup-old-and-compressed-chunks-par-900).
