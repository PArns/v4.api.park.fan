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
Stores CatBoost's raw predictions. Hypertable on `createdAt` (7-day chunks,
compressed after 14 days, segment by `attractionId`).
- Primary key `(attractionId, predictionType, createdAt, predictedTime)` — the
  natural key plus the partition column, which TimescaleDB requires in every
  unique index. That is the live column order on production (verified against
  `pg_index` on 2026-10-12); `timescale-init.service.ts` derives the key from the
  running schema rather than declaring it, so this is the order queries can rely
  on for an index prefix.
- `predictedTime`: the slot being predicted (15-minute slots for `hourly`,
  despite the name; days for `daily`)
- `predictedWaitTime`, `confidence`, `uncertaintyMinutes`, `crowdLevel`,
  `status`, `baseline`, `modelVersion`: the answer a reader is served

#### Write path (PAR-874)

`generate-hourly` runs every 15 minutes over a 48-hour horizon. It used to
delete every future hourly row of a park (`deduplicatePredictions`) and insert
the whole horizon again: ~25 M inserts and as many deletes a day, 94 % of the
rows written were deleted again, and because a B-tree page is only reused once
it is completely empty, the chunk primary keys grew to 15–25× their natural
size (8.5 GB instead of ~0.3 GB for one 7-day chunk; DB audit 2026-10-12).

It now **diff-writes** (`MLService.storePredictions(…, { replaceHourlyForPark })`,
`src/ml/utils/prediction-diff-write.util.ts`):

1. read the park's stored hourly rows with `predictedTime >= now` and
   `createdAt >= now - 50 h` (no future slot can be held by an older row);
2. compare each slot's newest stored row with the new answer on every served
   column — a slot that did not change is **not written at all**;
3. insert the changed and new slots, then delete the superseded rows of changed
   slots and every stored future slot the run no longer predicts. Insert-first
   means a reader never sees a hole; for a moment a changed slot holds two rows
   and every reader takes the newest.

A slot already under way (`predictedTime < now`) is outside the comparison, as
it was outside the old delete: the run's row for it is appended, so the
stats/day curve keeps the row written before the slot began.

Why not `ON CONFLICT … DO UPDATE`: the slot `(attractionId, predictionType,
predictedTime)` cannot be a unique key on this hypertable, since every unique
index must contain `createdAt` and no two runs share one.

**Consequences for readers.** A row's `createdAt` is now the time its *value*
was written, not the time it was last confirmed. Freshness therefore moved off
the row: every run sets a Redis marker `ml:hourly-fresh:{attractionId}` with a
30-minute TTL, and `getStoredPredictions` / `getBatchStoredPredictions` serve a
ride's hourly rows when the marker exists or one of its rows is younger than 30
minutes (the old rule, which carries the deploy window itself, and after that
any ride with a changed slot in the last 30 minutes — a ride whose curve sits
unchanged writes no row, so hourly serving does depend on Redis now). Their
`createdAt` bound is the 50-hour horizon window, and they return one row per
slot, the newest. The history readers that take the *newest* row per slot
(`prediction-deviation`, the PCN scorer, the nf-service backtests, and the
`daily`-only CatBoost snapshot) are unaffected: the surviving value per slot is
the same as before. One reader takes the *oldest* row per slot — `stats/day`'s
`first_predicted`, "what we said first about this slot". It still answers that,
and now reaches further back: before, a past slot's oldest surviving row came
from the last run before the slot began, because every earlier run's row had
been deleted while the slot was still in the future; now a slot whose value sat
unchanged keeps the row of the run that last changed it, which can be hours
earlier. `last_predicted` is unchanged.

Measured on production (2026-10-12, two consecutive evening runs, ~90–105 k
future slots each): 59 % and 80 % of the slots were unchanged.
`predictedWaitTime` alone was unchanged in 88–96 %; most of the remaining
changes are `confidence` moving by ~0.7 points. One run a day (after the 06:00
retrain changes `modelVersion`) rewrites everything.

`deduplicatePredictions` remains for `daily` (nightly) and is no longer called
for hourly at all. Its `createdAt` window is derived per type — 72 h for hourly,
8 days for daily (`DEDUPE_CREATED_AFTER_HOURS`, PAR-874) — down from a flat 13
days, so the delete stays on the newest chunk or two.

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
| `attraction_p50_baselines` | Per-attraction P50 baseline. **Primary** for the live park load (`Σ latest ÷ Σ P50` across headliners). |
| `attraction_p90_baselines` | Per-attraction P90 (peak) baseline. Computed for free; **not** the crowd-level reference (feeds the per-ride daily P90 the calendar averages). |
| `headliner_attractions` | Which attractions were selected as headliners per park (for baseline calculation). Also stores per-headliner `p50Wait548d` / `p90Wait548d` so park baselines are avg-of-per-headliner without a re-scan. |
| `attraction_hourly_history` | Per-day per-attraction 15-min-slot P90/avg/sampleCount rollup (JSONB `slots` array). Read by the attraction history endpoint for past days; today is computed live against `queue_data`. |
| `park_daily_stats` | Daily stats per park (P50/P90/max wait). Updated hourly for today, daily for yesterday. |
| `queue_data_aggregates` | Precomputed hourly percentiles (P25/P50/P75/P90/P95/P99) per attraction. Used by ML feature engineering. |

The P50/P90 baseline pair is recalculated nightly by the same cron (3 AM parks, 4 AM attractions) — PostgreSQL produces both percentiles in a single PERCENTILE_CONT sort. The 548-day **live** sliding-window calculation that used to back this on every cache miss was removed; missing rows mean "wait for the next cron run" rather than "scan half a billion data points now".

## Extensions

- **TimescaleDB**: Used for `queue_data` and potentially `wait_time_predictions` to handle high-volume time-series data efficiently.
- **PostGIS**: Used for location-based queries (nearby parks).
