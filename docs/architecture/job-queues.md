# Background jobs and queues

Every scheduled job, its queue and its time, plus how failed jobs are kept and reported. Open this to
find what runs when, or before adding a queue or a cron.

- **Library:** Bull 4 through `@nestjs/bull` (not BullMQ). Keys live under `<BULL_PREFIX>:<queue>:*`
  (default prefix `parkfan`) in the same Redis as the caches, so a cache flush must never match that
  shape (`flush-cache-patterns.ts`).
- **Queues:** `BULL_QUEUE_REGISTRATIONS` (`src/queues/queue-registrations.ts`), with longer
  `lockDuration` for the long batch queues. Processors: `src/queues/processors/*.processor.ts`.
- **Schedules:** `QueueSchedulerService.registerScheduledJobs`
  (`src/queues/services/queue-scheduler.service.ts`) is the source of truth. Crons run in UTC (the
  container has no `TZ`; a few also pass `tz: "UTC"`).
- **Self-healing repeat chains.** The registration runs at boot and again every hour, and
  `hasRepeatableJob` decides per entry whether to keep it: an entry whose cron differs from the code's,
  whose next run is more than 2 min overdue, or whose materialised next job has lost its name
  (`materialisedRepeatJobId`) is removed and registered afresh. A chain broken in Redis therefore heals
  within the hour instead of at the next deploy (PAR-626; Redis now runs `volatile-lru`, which never
  evicts the TTL-less Bull keys, PAR-633).
- **Boot.** `QueueBootstrapService` queues the initial syncs only when their data is missing or stale
  (no parks → `fetch-all-children`; no `queue_data` row in the last 5 min → `fetch-wait-times`), so a
  restart does not re-run the whole pipeline;
  `SKIP_QUEUE_BOOTSTRAP=true` turns it off for scripts.
- **Dashboard:** the `bull-board` container (port 3001) in the Compose files.

## Schedule (UTC)

| Queue / job | When | What |
| --- | --- | --- |
| `wait-times` / `fetch-wait-times` | every 5 min | Live waits from every source ([ingestion](data-ingestion.md)), reverse reconciliation, hourly heartbeats; enqueues `live-cache-warmup` |
| `live-cache-warmup` | after each wait-times run | Park, top-1000 attraction and occupancy caches ([caching](caching-strategy.md#cache-warmup)) |
| `park-metadata` / `sync-all-parks` | 03:00 | Park metadata, geocoding, schedules for every wiki park; chains `children-metadata` (no cron of its own since PAR-875) |
| `park-metadata` / `sync-schedules-only` | 15:00 | Schedules only ([schedule sync](schedule-sync-and-calendar.md)); `sync-park-schedule` and `fill-all-gaps` run on demand |
| `park-metadata` / `warmup-calendar-daily` | 08:00, 20:00 | Calendar months and the `/best-days` snapshot ([caching](caching-strategy.md#cache-warmup)) |
| `park-metadata` / `decay-popularity` | 00:00 | Decays the popularity sorted sets |
| `children-metadata` / `sync-attraction-details` | Sun 05:30 | Attraction detail sync |
| `six-flags-heights` / `sync-heights` | Mon 03:00 | Ride heights the wiki lacks |
| `weather` / `fetch-weather` | 00:00, 12:00 (full); 07:00, 19:00 (current) | [Weather](weather.md) |
| `weather-warnings` / `sync-warnings` | every 15 min | Severe-weather warnings |
| `weather-historical` / `mark-historical` | 05:00 | Marks past weather rows `historical` |
| `holidays` / `fetch-holidays` | 02:00 on the 1st of each month | Public and school holidays ([holiday sources](holiday-sources-outside-europe.md)); queues `fill-all-gaps` after |
| `wartezeiten-schedule` / `fetch-opening-times` | 06:00 | Opening times for wartezeiten parks without a wiki schedule |
| `analytics` / `calculate-percentiles` | 02:00 | `queue_data_aggregates` (P25…P99 per hour) |
| `analytics` / `detect-seasonal` | 02:30 | [Seasonality](attraction-status/seasonality.md) and the [maze rule](attraction-status/maze-rule.md) |
| `analytics` / `monitor-data-quality` | 06:45 | Silenced clusters, silent parks, failing jobs (below) |
| `stats` / `update-today-stats`, `finalize-yesterday-stats` | hourly; 01:00 | `park_daily_stats` |
| `stats` / `plan-day-coverage` | 09:00 | `/plan/day` coverage measurement |
| `p50-baseline` / `calculate-park-baselines`, `calculate-attraction-baselines` | 03:00; 04:00 | P50 + P90 (548 days) and typical-day-peak ([crowd levels](../analytics/crowd-levels.md)) |
| `attraction-hourly-history` / `calculate-yesterday-hourly-history` | 04:30 | `attraction_hourly_history` 15-min rollup |
| `park-day-operation` / `calculate-yesterday-park-day-operation` | 04:45 | Measured-operation verdicts ([calendar rules §1.5](calendar-schedule-and-ml-rules.md#15-a-past-closed-day-the-measurement-overrules-par-697)) |
| `downtime` / `reconstruct-downtime` | 05:00 | [Ride downtime](../analytics/ride-downtime/README.md) |
| `show-patterns` / `rebuild-patterns` | 04:15 | Projected showtimes for planned days |
| `rope-drop` / `calculate-rope-drop`; `typical-waits` / `calculate-typical-waits` | 05:15; 05:30 | Per-ride rope-drop and P50/P90 stats |
| `predictions` / `generate-hourly` | every 15 min | CatBoost 15-min slots over the next 48 h, diff-written ([schema](../database/schema.md#write-path-par-874)) |
| `predictions` / `generate-daily` | 01:00 | CatBoost daily predictions |
| `predictions` / `score-lead-snapshots`, `rebuild-accuracy-profile`, `cleanup-old` | 03:00, 03:10, 03:30 | Lead-time accuracy; prediction retention ([nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md#predictionscleanup-old-and-compressed-chunks-par-900)) |
| `prediction-accuracy` / `compare-accuracy`, `aggregate-stats` | every 15 min; hourly | Scores past predictions |
| `prediction-accuracy` / `cleanup-old`, `calibrate-bands` | 04:00; 04:20 | Retention; band calibration |
| `ml-training` / `train-model`, `cleanup-models` | 06:00; 07:00 | CatBoost training (35–70 min, ~13 GB peak) |
| `nf-training` / `train-nf` | 03:00 | TFT training, writes `tft_forecasts`; evicts `ml:tft-daily:*` when done |
| `pcn-shadow` / `train-pcn`, `forecast-pcn`, `score-pcn` | 08:30; every 15 min; hourly | PCN training, the intraday forecast (served when `SERVE_PCN_INTRADAY` is set) and its scoring; "shadow" is the queue's historical name |
| `forecast-archive` / `capture`, `driver-level` | hourly :05; 01:30 | [Forward archive](../ml/forward-archive/README.md) |
| `forecast-archive-score` / `score` | 12:00 | Scores the archive (own queue, so it never delays a capture) |
| `ml-monitoring` / `detect-feature-drift`, `check-alerts`, `detect-anomalies`, `cleanup` | 02:00; hourly; 03:00; 04:00 | Model monitoring |
| `push-notifications` / `send-due-notifications`, `sweep-expired-follows` | every 5 min; 04:20 | Trip, ride-alert and show-follow pushes |
| `trips` / `sweep-expired` | 04:45 | Deletes expired stored plans |
| `site-analytics` / `flush`, `snapshot-visitors`, `drop-old-chunks` | every 5 min from :01; 00:13; 01:47 | The [site statistic](../frontend/site-analytics.md#storage-insert-only-no-dead-tuples): finished five-minute windows from Redis into its hypertables, the last two days' visitors, chunks past 400 days |
| `manual-metadata` / `audit-ride-profile-terms` | 06:30 | Checks every glossary term id in `attraction_ride_profiles` against the frontend's list. The queue keeps its old name because Bull keys repeatable jobs by queue name; it is `CuratedDataProcessor` |
| `db-maintenance` / `prune-tft-forecasts`, `vacuum-churn-tables`, `reindex-pcn-forecasts-pkey` | 04:35; 04:50; Sun 19:10 | TFT retention, VACUUM before training, PCN index rebuild ([nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md); plan in `src/database/maintenance/vacuum-plan.ts`) |
| `geoip-update` / `update-geolite2-city` | 00:00 every second day | [GeoIP](location-resolution.md) |

On demand only: `entity-mappings` (`sync-park-mappings`), `park-enrichment` (`enrich-all`),
`ride-stats` (`import-stats`, Wikidata), `manual-metadata` (`publish-ride-profiles`),
`children-metadata` (`fetch-all-children`). Nothing large belongs in 05:50–07:30 while CatBoost
trains on the shared host ([nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md#the-windows-utc)).

### The wait-times run and its warmup

The post-sync warmup has its own queue since PAR-822: inside the `wait-times` job it took ~210 s of a
~263 s run, and because `wait-times` runs one job at a time, a run past five minutes delayed the next
fetch. A warmup that finds a newer request waiting returns at once, so a backlog collapses to the
newest. Before each park batch it waits until no `wait-times` job is active (a park whose schedule
reads CLOSED is re-checked against the live sources in `buildIntegratedResponse`), so warmup and sync
never fetch at the same time. Both log `⏱️ … phases {json}`; the wait-times line carries `prepareMs`,
`parksMs`, `heartbeatsMs` and per-park sums (`mappingsSumMs`, `fetch.<source>.SumMs`, `writeSumMs`)
that overlap because five parks run in parallel.

## Failed jobs: retention and monitoring

- **Retention** is `defaultJobOptions` in `src/queues/queues.module.ts`: the last 100 completed and
  500 failed jobs per queue. Nothing else deletes job history (until PAR-821 every boot cleaned six
  queues' failures).
- **The nightly sweep** (`monitor-data-quality`, `DataQualityMonitorService.findFailingJobs`, also on
  `GET /v1/admin/data-quality`) reads `<prefix>:<queue>:failed` for every registered queue.
  `MONITORED_QUEUES` derives from `BULL_QUEUE_REGISTRATIONS`; a queue opts out only through
  `UNMONITORED_QUEUES` with a reason, and the spec fails for one in neither.
- **Window:** a failure is reported until the job's next scheduled run. The nightly sweep starts from
  its previous run (`data-quality:failing-jobs:last-sweep`; 26 h when missing, always 26 h for the
  admin page); a slower cron reaches back to its previous fire minus 2 h, read from the repeat ZSET
  (`failureWindowStarts`), so a weekly failure stays visible until it runs again.
- **A later success clears it** when the `completed` set holds a run of that name finished after the
  newest failure. Limits: only 100 completed runs are kept, and the match is by job name, so an
  intermittent failure of a five-minute job that has since succeeded is not reported.
- **Blind spot:** a job added with `removeOnFail: true` leaves no failed entry (the term audit and the
  sweep itself, and the delayed `revalidate-parks` that the ride-stats and curated-data processors and
  `AdminCurationService` enqueue).
