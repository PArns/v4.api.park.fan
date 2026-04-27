# Changelog

Notable changes to the Park Fan API. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Version and date align with releases or significant doc/code milestones.

---

## [Unreleased]

### Added

- **Sparklines on `/v1/analytics/realtime`** (`analytics.service.ts`, `global-stats.dto.ts`): `longestWaitRide` and `shortestWaitRide` now include a `sparkline` field — an array of `{ timestamp, waitTime }` pairs covering today's operating window. Each ride uses its own park's timezone and schedule opening time as the window start (identical to the park controller), so rides from e.g. Tokyo and Orlando both show the correct local-day history.

- **`getAttractionSparklinesBatch`** (`analytics.service.ts`): New helper on `AnalyticsService` for fetching sparklines when attractions may span multiple parks. Groups by `parkId`, calls `getEffectiveStartTime` once per park, batches `getBatchAttractionWaitTimeHistory` per group, and merges results into a single `Map<attractionId, SparklinePoint[]>`. Use this for any multi-park context (global stats, recommendations, …); use `getBatchAttractionWaitTimeHistory` directly when you already hold a shared `startTime` for a single park. See [Sparklines](analytics/sparklines.md).

### Added
- **`crowdLevel` on `ParkReference.analytics.statistics`** (`discovery.service.ts`, `geo-structure.dto.ts`): The `/v1/discovery/geo`, `/v1/discovery/continents`, `/v1/discovery/continents/:continent`, and `/v1/discovery/continents/:continent/:country` endpoints now expose the park's current live crowd level inside `analytics.statistics` alongside `avgWaitTime`. Previously the only source of live crowd level on these endpoints was `currentLoad.crowdLevel`, which could be `null` even when wait-time statistics were available — causing the frontend's Popular Parks section to render wait times without a crowd badge. `analytics.statistics.crowdLevel` is now co-present with `avgWaitTime` whenever the park has a valid P50 baseline. The other discovery routes already expose live crowd level via their existing shapes: `/v1/discovery/:continent/:country` (`ParkResponseDto.analytics.statistics.crowdLevel`) and `/v1/discovery/nearby` (`analytics.crowdLevel` per park/ride).

### Changed
- **Shared crowd-level utility** (`common/utils/crowd-level.util.ts`): Extracted the P50-relative occupancy → CrowdLevel threshold ladder (very_low/low/moderate/high/very_high/extreme) into a single reusable function. `AnalyticsService.determineCrowdLevel` now delegates to it (all ~20 existing call sites unchanged), and `DiscoveryService.hydrateStructure` uses it directly. Thresholds now exist in exactly one place.

### Added 
- **Smart Gaps: Historical Hour Reconstruction** (`docs/analytics/smart-gaps.md`): Automatically reconstructs park opening/closing hours for past days using a 15-minute sliding window and 10% attraction activity threshold (rides with waitTime >= 5 min only). Includes rounding to nearest full hour and strict exclusion of service points (bars, snacks) via name-based blacklist. 
- **`isEstimated` flag for Calendar API**: New per-day flag in `CalendarDay` to signal reconstructed historical data. 
- **`hasOperatingSchedule` flag for Parks API**: New per-park flag to signal if a park provides an official API calendar (true) or relies on inference/estimates (false). Added to all park-related DTOs and Nearby responses. 
- **Automated Seasonal Detection**: Logic to identify "Seasonal Parks" (winter gaps > 21 days) to suppress crowd predictions during off-season while allowing them for year-round parks with UNKNOWN schedule. 

### Changed 
- **Optimized Seasonal Check**: Accelerated `isParkSeasonal` query by 120x (from 72ms to 0.6ms) using SQL Window Functions (`LEAD`). 
- **ML Feature Context Alignment**: ML service now receives real-time reconstructed opening hours instead of static 9/10 AM fallbacks, improving prediction accuracy for "No-Schedule" parks. 
- **Batch Processing for DTO Enrichment**: Introduced `getBatchHasOperatingSchedule` to prevent N+1 queries when listing parks. 


### Added

- **Training roadmap doc** (`docs/ml/training-roadmap.md`): Tracks known ML issues, data quality analysis, and next steps for training improvements including UNKNOWN park inclusion strategy.
- **Reverse reconciliation for stale attractions** (`wait-times.processor.ts`, docs: `docs/architecture/reverse-reconciliation.md`): Attractions that disappear from every upstream source (ThemeParks.wiki, Queue-Times, Wartezeiten) for >24h are now auto-closed. A Redis `attraction:last-seen:{id}` key is touched only by real source sightings (never by the heartbeat). After each park's 5-minute sync the processor diffs seen vs. known attractions and writes a `status=CLOSED` `queue_data` entry for any attraction stale for >24h. Grace period of 24h protects newly created rides from premature closure, and the safety guard `seenAttractionIds.size > 0` prevents mass-close during upstream outages. The hourly heartbeat now also skips stale attractions instead of preserving their last `OPERATING` status. Fixes Movie Park Germany's Halloween mazes (e.g. *A Quiet Place*) showing "open, 0 min" year-round.
- **`POST /admin/detect-seasonal`** (`admin.controller.ts`, `admin.module.ts`): Manual trigger for the `detect-seasonal` analytics job (normally daily at 2:30 am). Intended to re-evaluate seasonal flags after deploying the reverse-reconciliation fix so newly `CLOSED` attractions get `isSeasonal=true` + `seasonMonths` populated without waiting for the cron.

### Fixed

- **Stale "open with 0 min" status for disappeared attractions** (`wait-times.processor.ts`): Previously `writeHourlyHeartbeats` re-stamped `lastUpdated=now` with the previous `status` every hour for any attraction missing from the feed, so seasonal Halloween mazes and silently-removed rides remained `OPERATING` forever. The heartbeat now reads `attraction:last-seen:{id}` and skips attractions not seen in any source for >24h; the reverse-reconciliation step has already written `CLOSED` for them. Root cause was the missing counter-signal: no upstream source ever reports "this attraction no longer exists".
- **P50 baselines missing for UNKNOWN parks** (`analytics.service.ts`): `identifyHeadliners` and `calculateAttractionP50` both filtered `scheduleType = 'OPERATING'`. Parks with UNKNOWN schedule entries (USJ, Universal Studios, Warner Bros Movie World, Blackpool etc.) had no headliners identified → no P50 baseline → `getCurrentOccupancy` returned hardcoded 100 → `park_occupancy_pct = 100` flat for all UNKNOWN parks at inference (feature useless for 22+ parks). Fixed: changed both queries to `IN ('OPERATING', 'UNKNOWN')`. Safe because both queries already filter `qd.status = 'OPERATING'` AND `qd.waitTime >= 10` — truly closed parks produce 0 qualifying rows regardless of schedule type.

---

### ML: ride-based park open/closed detection

- **ML: ride-based park open/closed detection** (`parks.service.ts`): `getBatchParkStatus` and `isParkOperatingToday` now derive open/closed status from live ride data when no confirmed schedule exists. Threshold: ≥3 attractions with recent data AND ≥25% reporting `waitTime ≥ 5 min`. Window: 2h for real-time status, park-local today for daily planning. Parks with explicit `CLOSED` schedule today are excluded from the heuristic.
- **ML: `parkLiveStatus` feature context** (`ml.service.ts`, `predict.py`, `feature-context.type.ts`): NestJS now passes `featureContext.parkLiveStatus` to the Python ML service. In `predict.py`, UNKNOWN-schedule rows are corrected to `is_park_open=1` when the park is confirmed OPERATING via ride data. Explicit CLOSED entries are never overridden. Fixes predictions for parks like Six Flags, Universal, and other parks that report UNKNOWN schedule but are genuinely open.
- **ML dashboard: model metrics history endpoint** (`GET /v1/ml/models/metrics-history?limit=50`): Returns MAE, RMSE, MAPE, R² per trained model ordered oldest→newest for sparkline charts. See integration guide for frontend.

### Fixed

- **UNKNOWN schedule parks excluded from prediction generation** (`parks.service.ts` `isParkOperatingToday`): Parks with `scheduleType=UNKNOWN` (e.g. Six Flags, Universal Hollywood, 66 parks affected) were treated the same as CLOSED — no predictions generated. Fixed: UNKNOWN falls through to ride-data check; if no data, defaults to `true` (conservative).
- **`getBatchParkStatus` heuristic over-filtered** (`parks.service.ts`): Previous filter excluded parks that ever had any OPERATING schedule entry, making the heuristic dead code for most UNKNOWN parks. New filter: only exclude parks with explicit CLOSED schedule today (park-local timezone via `AT TIME ZONE` join). Threshold raised from `waitTime > 0` (any single ride) to ≥25% with `waitTime ≥ 5`.
- **`CURRENT_DATE` UTC vs. park-local** (`parks.service.ts`): CLOSED-schedule exclusion query used `date = CURRENT_DATE` (UTC), which could match wrong date for UTC+ parks at night. Fixed with `(CURRENT_TIMESTAMP AT TIME ZONE p.timezone)::date` via JOIN on parks.
- **Daily predictions: `parkLiveStatus` always "CLOSED" at night** (`prediction-generator.processor.ts`): Daily prediction generator called `getBatchParkStatus` at runtime (e.g. 02:00 UTC), getting `"CLOSED"` for parks outside operating hours → UNKNOWN override never fired. Fixed: parks that pass `isParkOperatingToday` now receive `liveStatus="OPERATING"` explicitly.
- **ML training UNKNOWN filter reverted** (`ml-service/db.py`): Including UNKNOWN-schedule parks in training data caused MAE to jump from ~5.9 → 14.4 min and R² to drop from 0.86 → 0.37. Root cause: UNKNOWN days include closed parks still sending 5-min sentinel values — the filter can't distinguish real operating data from sentinel data. Reverted to `scheduleType = 'OPERATING'` only. The training/inference asymmetry for UNKNOWN parks is accepted; `parkLiveStatus` correctly handles them at inference time without needing training examples.
- **`park_has_operating` UUID type mismatch** (`ml-service/predict.py`): Dict key built from `schedules_df["parkId"]` could be a UUID object while `row["parkId"]` was a string → silent dict miss → UNKNOWN override never fired. Fixed: `astype(str)` on groupby key + `str(row["parkId"])` at lookup.
- **Dead code in `features.py`** (`ml-service/features.py`): `parkLiveStatus` override block in `add_park_schedule_features` was unreachable (only called during training where `feature_context=None`). Removed; the authoritative override is in `predict.py`.

### Weather forecast in integrated park response

### Fixed (weather)

- **Weather DATE timezone off-by-one** (`weather.service.ts`): Two bugs caused non-UTC parks to show wrong weather. (1) Save used `fromZonedTime(midnight, tz)` → east-of-UTC parks (e.g. `Europe/Berlin`) stored dates shifted -1 day (March 31 saved as March 30). (2) Query used `DATE(weather.date AT TIME ZONE :tz)` — PostgreSQL casts DATE to midnight-UTC timestamptz first, then shifts back to local time, which for west-of-UTC parks (e.g. `America/New_York`) moves today's date to yesterday → `current` always null. Fixed: save uses noon-UTC (`new Date(\`${date}T12:00:00Z\`)`), query uses direct date-string comparison (`weather.date >= :start`).
- **Weather empty for US parks** (root cause above): Parks like "Universal's Epic Universe" returned `weather: { current: null, forecast: [] }`. The park has coordinates and Open-Meteo data; the off-by-one query excluded today's DB record. (`park-integration.service.ts`, `park-with-attractions.dto.ts`): The integrated park endpoint now returns `weather.forecast` (next 6 days) in addition to `weather.current`. Previously `getCurrentAndForecast()` fetched 16 days from DB but only `current` was mapped into the response. The API now exposes today + 6 forecast days (7 total).
- **Weather architecture doc** (`docs/architecture/weather.md`): Documents Open-Meteo sync strategy, storage schema, BullMQ jobs, timezone handling, DATE timezone bug pattern, and why parks may have empty weather (missing lat/lng coordinates).
- **Weather cache TTL extended** (`weather.service.ts`): Increased from 30 minutes to 2 hours. Weather data changes at most twice a day (sync at 00:00 and 12:00 UTC); frequent cache misses caused unnecessary DB load.

### Fixed

- **P50/headliner: `waitTime >= 10` filter** (`analytics.service.ts`, `calendar.service.ts`, `stats.service.ts`, `attraction-integration.service.ts`): All historical wait-time aggregations (headliner identification, P50 baseline calculation, weekday averages, percentiles, longest waits) used `waitTime > 0`, while the real-time path used `minWaitTime=5`. Queue-Times API reports `waitTime=1` as a walk-on/no-queue placeholder (common for water parks, e.g. Rulantica slides). This caused ~40–65% of water-park samples to be 1-minute placeholders, depressing P50 baselines and causing "Extreme" crowd level while individual rides showed normal waits. Fixed by aligning all historical queries to `waitTime >= 10`. The existence check `hasQueueDataInWindow` is intentionally kept at `> 0`.
- **P50/headliner: schedule-based closed-day exclusion** (`analytics.service.ts`, `calendar.service.ts`): Seasonal parks (Kennywood, Canada's Wonderland) accumulate queue data during off-season months. Without filtering, closed-day data drags P50 baselines down (e.g., Kennywood: 31 raw data days → 7 OPERATING days). Fixed by adding a `LEFT JOIN schedule_entries` (park-level, `attractionId IS NULL`) to all historical queries, using `DATE(qd.timestamp AT TIME ZONE <park_tz>)` for correct local-date matching. Days with no schedule entry are included; days with `OPERATING` are included; any other type is excluded.
- **ML training: same `>= 5` and schedule filters** (`ml-service/db.py`): Training data extraction used `waitTime >= 0` and had no schedule filter. Now applies `waitTime >= 10` and the same schedule JOIN (with `JOIN parks p` for timezone). Requires retraining to take effect.
- **ML training: `fetch_recent_wait_times` `>= 5` filter** (`ml-service/predict.py`): Inference recent-wait lookup also aligned to `waitTime >= 10` + schedule JOIN.
- **ML: historical occupancy DOW×hour timezone bug** (`ml-service/db.py`): `fetch_historical_park_occupancy` built the (DOW, hour) occupancy profile using `EXTRACT(DOW/HOUR FROM qd.timestamp)` (UTC), but inference looked up with local park time → systematic 1–2 hour shift for all non-UTC parks. Fixed by joining `parks` and using `AT TIME ZONE p.timezone` in the GROUP BY. Since `queue_data` has no `parkId`, the join path is `queue_data → attractions → parks`.

### Changed

- **DB indexes: remove unused** (`ml-prediction-request-log.entity.ts`, `park-p50-baseline.entity.ts`, `attraction-p50-baseline.entity.ts`, `attraction.entity.ts`, `park.entity.ts`, `ml-model.entity.ts`): Removed ~182 MB of unused indexes from `ml_prediction_request_log` (6 indexes with 0–2 scans) and 6 further duplicate/zero-scan indexes across other entities. TypeORM `synchronize: true` creates new indexes but does not drop removed ones; `scripts/drop-unused-indexes.sql` must be run once on production.
- **DB index: new partial index for schedule JOIN** (`schedule-entry.entity.ts`): Added `idx_schedule_park_date_no_attraction` — partial index on `(parkId, date) WHERE "attractionId" IS NULL`. Covers the `schedule_entries` lookup in all analytics and ML historical queries without touching attraction-level schedule rows.

- **ML: 5-minute prediction bug** (`model.py` `predict_with_uncertainty`): `virtual_ensembles_predict` was called with `prediction_type="TotalUncertainty"`, which returns uncertainty scalars `[knowledge_unc, data_unc]` (shape `(n, 2)`), not per-ensemble predictions. `np.mean(axis=1)` averaged the two ~2.77 values → `round_to_nearest_5` → **5 min** for all predictions. Fixed by switching to `prediction_type="VirtEnsembles"` (shape `(n, 10, 1)`), squeezing to `(n, 10)`, and taking `median ± std` instead of `p5/p95` (more stable at n=10).
- **ML: NoneType crash in `fetch_holidays`** (`db.py`): `sorted(country_codes)` failed when the list contained `None` (parks with missing country metadata). Fixed by filtering: `country_codes = [c for c in country_codes if c is not None]`.
- **ML: Weekend underprediction** (`features.py`, `predict.py`, `config.py`): `volatility_7d` dominated feature importance at 32.91% while `is_weekend` was 0.01% and `avg_wait_last_1h` was 0.00%. The model could not distinguish weekday vs weekend crowd levels. Fixed by:
  - Splitting `volatility_7d` into `volatility_weekday` + `volatility_weekend` in training pipeline (`calculate_trend_volatility`)
  - Adding `rolling_avg_weekday` + `rolling_avg_weekend` via SQL window functions in `fetch_recent_wait_times`
  - Adding `avg_wait_same_dow_4w` (mean of last 4 same-day-of-week observations) for a stable historical reference
  - Lowering `VOLATILITY_CAP_STD_MINUTES` from 40 → 15 to reduce volatility dominance
  - All new features propagated to inference in `predict.py`
  - Detailed analysis: [Prediction Quality Issues](ml/prediction-quality-issues.md)

- **ML: Flat future predictions / hour importance 0.84%** (`features.py`, `db.py`, `predict.py`): `park_occupancy_pct` (15% importance) was broadcast from the current real-time value to ALL prediction rows — including rows 24h or 14 days in the future — causing flat, hour-invariant predictions. Fixed in two stages:
  - **Inference fix** (`db.py`, `features.py`): `fetch_historical_park_occupancy()` computes expected park occupancy by (DOW, hour) over the last 8 weeks (via `attractions` JOIN, since `queue_data` has no `parkId`). `add_park_occupancy_feature` now applies real-time occupancy only to rows within ±2h of base_time; future rows use the DOW×hour historical profile.
  - **Training fix** (`features.py`, `config.py`): Occupancy Dropout — 30% of training rows have their actual `park_occupancy_pct` replaced with the DOW×hour mean from the same park's training data (`OCCUPANCY_DROPOUT_RATE=0.30`). This teaches the model to rely on `hour`/`day_of_week` when occupancy is approximate, closing the gap for future predictions.

- **Schedule date-shift bug** (`saveScheduleData`): ThemeParks.wiki returns dates as date-only strings (`"YYYY-MM-DD"`). These were passed to `new Date()`, producing midnight UTC, which `formatInParkTimezone` then shifted back by one day for parks west of UTC (e.g. a park with `date:"2026-03-02"` was stored as `2026-03-01` in America/New_York). Fix: detect date-only strings via regex and use them directly without timezone conversion. Full ISO timestamps (from wartezeiten/queue-times processors) still go through `formatInParkTimezone`. (Bug: today's schedule entry stored under yesterday's DB date; opening hours were 1–2 days off in live DB for US parks.)
- **Holiday date range in `saveScheduleData`**: Date range for holiday pre-fetch was built from `new Date(e.date)` (midnight UTC), causing `formatInParkTimezone` to shift the range back 1 day for US parks. Fixed: use noon-UTC timestamps (`${dateStr}T12:00:00Z`) consistent with the rest of `saveScheduleData`.
- **Weather service date filter** (`weather.service.ts`): `allWeather.find()` and `.filter()` used `formatInParkTimezone(new Date(w.date), tz)` on a TypeORM DATE column (midnight UTC). For US parks this shifts midnight UTC to the previous calendar day, causing today's weather entry to be lost (not matched as "current" and excluded from "forecast"). Fixed: extract date string via `w.date.toISOString().split("T")[0]`, which is always correct because midnight UTC IS the calendar date stored in the DB.
- **Schedule response missing today's entry** (`buildIntegratedResponse`): Added filter `date >= todayInParkTz` to trim past entries (DB query fetches from -2 days), and a synthetic OPERATING entry for today if the park is operating but its schedule row is missing.
- **`peakHour` timezone ambiguity** (`analytics.service.ts`): Changed from returning `"HH:mm"` (plain string, interpreted as UTC by frontend) to a full ISO-8601 datetime with timezone offset (`"2026-03-02T11:00:00-05:00"`), eliminating frontend UTC misinterpretation.
- **Cache invalidation on INSERT** (`saveScheduleData`): `invalidateScheduleCache` was only called after UPDATE, not after INSERT. New entries would remain stale for up to 1 hour. Fixed: call `invalidateScheduleCache` after INSERT too.

### Changed

- **Calendar API:** UNKNOWN→OPERATING upgrade only for parks **without** OPERATING entries in `schedule_entries`. Parks with schedule integration keep UNKNOWN for days without schedule (DB-check via `hasOperatingSchedule`). Fixes Phantasialand Jan 26–31 incorrectly showing OPERATING.
- **Gap-fill** (`fillScheduleGaps`): Look-back added. Range: (today - 182 days) through (today + 182 days). Past gaps (e.g. winter closure Jan–Mar) are re-evaluated when new OPERATING (e.g. March 28) arrives, so UNKNOWN→CLOSED is promoted correctly.

### Performance

#### Schedule Sync Optimizations (NestJS)
- **Schedule sync (`saveScheduleData`)**: Batch DELETE operations for cleanup placeholders (UNKNOWN/CLOSED removed when API provides real data) reduced from ~300 individual queries to **3 batch queries** (99% reduction). Code deduplication: normalize scheduleType once instead of 3× redundant iterations.
- **Gap-fill (`fillScheduleGaps`)**: Batch INSERT/UPDATE operations for gap-filled entries and status changes reduced from ~364 individual queries to **~5 batch queries** (98.6% reduction). All iterations collect entries/updates in-memory, then execute bulk operations using `createQueryBuilder().insert()` and `whereInIds()`.
- **Duplicate cleanup (`cleanupDuplicateScheduleEntries`)**: SQL window functions and CTEs replace N+1 queries; same-type and cross-type duplicate detection reduced from ~160 queries to **2 queries** (98.8% reduction). Uses PostgreSQL `ROW_NUMBER()` OVER (PARTITION BY) for efficient deduplication.
- **Per-park cleanup**: New `cleanupDuplicateScheduleEntriesForPark()` method called before gap-fill to prevent duplicates from parallel schedule syncs (runs targeted cleanup for single park instead of waiting for daily global cleanup).
- **Operating date range extraction**: New `getOperatingDateRange()` helper extracts min/max OPERATING date logic into reusable function (used by gap-fill classification and calendar fallback).

**Schedule sync impact**: Typical schedule sync reduced from ~924 database queries to ~12 queries (**98.7% reduction**), estimated duration improvement from ~92 seconds to ~1.2 seconds.

#### ML Service Optimizations (Python) – 2026-02-15
- **Database query caching**: Added in-memory caching for holidays (1h TTL), schedules (5min TTL), recent wait times (2min TTL), and weather historical data (1h TTL). Reduces repeated queries for unchanged data.
- **Query optimization with window functions**: `fetch_recent_wait_times` now pre-computes `rolling_avg_7d` and `rolling_std_7d` using PostgreSQL window functions instead of Python aggregation. Reduces data transfer and eliminates expensive Python loops.
- **Holiday lookup vectorization**: Replaced loop over 1000+ prediction rows with pandas `.map()` operations. Pre-processes park metadata once instead of per-row. Eliminates JSON parsing in loop.
- **Historical features optimization**: Uses pre-computed rolling averages and standard deviations directly from database instead of Python calculations.

**ML service impact**:
- First request (cold cache): **40-50% faster**
- Cached requests (warm cache): **70-85% faster**
- Daily predictions (365 days): up to **90% faster**
- Database query reduction for repeated requests: **80-90%** fewer queries

**Documentation**: [ML Performance Optimizations](ml/performance-optimizations.md)

---

## [4.6.2] – 2026-02-08

### Changed

- **Schedule sync / Gap-fill**
  - **Doc:** "When gap-fill runs (DB updates are automatic)" in [Schedule Sync & Calendar](architecture/schedule-sync-and-calendar.md): gap-fill runs after every schedule sync (sync-all-parks, sync-schedules-only, sync-park-schedule); optional job `fill-all-gaps` for all parks. No one-off DB correction needed when using park-timezone range.
  - **lookAheadDays:** Default increased from 90 to **120 days** so the DB is filled further ahead (typical 4‑month planning).
- **Calendar warmup:** Range extended from "current + 2 months" to **-1 to +3 months** (last month through 3 months ahead, park timezone) so the typical user range (recap + planning) is cache-hot after daily warmup.

---

## [4.6.1] – 2026-02-08

### Added

- **Calendar, Schedule & ML rules doc** (`docs/architecture/calendar-schedule-and-ml-rules.md`): Single source of truth for status (OPERATING/CLOSED/UNKNOWN), crowd level, schedule sync, next schedule, and ML alignment.
- **Frontend doc** (`docs/frontend/calendar-schedule-status.md`): How to display calendar status (UNKNOWN vs CLOSED) in the UI. Linked from CLAUDE.md.
- **Changelog** (`docs/changelog.md`): This file; linked from CLAUDE.md.
- **Timezone Audit** (`docs/development/timezone-audit.md`): Audit of all time operations against park timezone. Linked from CLAUDE.md.

### Changed

- **Calendar API**
  - Status is derived only from schedule (and the rule "past/today + crowd level = OPEN"); crowd level no longer overrides status.
  - Past and today: if no schedule but we have a (non-closed) crowd level → treat as OPERATING so we can show data.
  - Future: use schedule (OPEN/CLOSED/UNKNOWN); future days without schedule stay UNKNOWN but get a crowd prediction (ML or fallback "moderate"), not "closed".
- **Schedule sync**
  - `saveScheduleData`: API type "Closed"/"CLOSED" (case-insensitive) is normalised to `ScheduleType.CLOSED` so off-season (e.g. Phantasialand February) is stored when the API provides it. When saving OPERATING, any gap-fill CLOSED for that date is now deleted so the API entry takes precedence.
  - **Gap-fill** (`fillScheduleGaps`): Missing days are now classified as CLOSED or UNKNOWN:
    - **CLOSED** if there is at least one OPERATING day before and one after the gap (strictly between min/max OPERATING dates).
    - **UNKNOWN** if the park has no OPERATING entries, or the gap is before the first OPERATING date (e.g. before we have data), or on/after the last OPERATING date (schedule not yet published).
    - Existing UNKNOWN entries can be updated to CLOSED when re-running gap-fill if they are now "in the middle". OPERATING and API-provided CLOSED are never overwritten.
  - Gap-fill range uses **park timezone** (`getStartOfDayInTimezone`, `addDays`) so the filled range is always "today" through "today + 90" in the park's calendar.
- **Docs**
  - All relevant docs translated to English (frontend calendar status, review, troubleshooting peak hour section, calendar-schedule-and-ml-rules).
  - Schedule sync & calendar doc: new "Gap-fill rules" section; UNKNOWN vs CLOSED and Gaps sections updated.
  - CLAUDE.md: added Frontend section and link to Calendar, Schedule & ML Rules; Critical Rules strengthened (park timezone for all time operations); link to this changelog and Timezone Audit.

### Fixed

- Calendar no longer shows "Öffnungszeiten noch nicht verfügbar" for days that are known to be closed (gap-fill and API CLOSED now set status CLOSED where appropriate).
- When the API provides OPERATING for a date that had a gap-fill CLOSED, the calendar could show CLOSED (because `getSchedule` orders by scheduleType ASC). `saveScheduleData` now deletes any CLOSED row for that date when saving OPERATING.
- **Timezone audit:** All time operations now use park timezone. Fixed: `getUpcomingSchedule` (range in park TZ), `weather.service` fallback + `markPastDataAsHistorical` (per-park), `getBatchParkHours` (per-park today), `getParkPercentilesToday` / `getAttractionPercentilesToday` (startOfDay in park TZ), `tomorrowInParkTz` (getTomorrowDateInTimezone), `isParkCurrentlyOpen` / `isParkOperatingToday` (getCurrentDateInTimezone).

---

## [Older versions]

Older changes were not recorded in this changelog. From this version onward, notable changes will be listed here with version and date.

---

(Compare URLs can be added when using a Git remote, e.g. `[4.6.1]: https://github.com/owner/repo/compare/v4.5.0...v4.6.1`.)
