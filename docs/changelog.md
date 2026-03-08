# Changelog

Notable changes to the Park Fan API. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Version and date align with releases or significant doc/code milestones.

---

## [Unreleased]

### Fixed

- **ML: 5-minute prediction bug** (`model.py` `predict_with_uncertainty`): `virtual_ensembles_predict` was called with `prediction_type="TotalUncertainty"`, which returns uncertainty scalars `[knowledge_unc, data_unc]` (shape `(n, 2)`), not per-ensemble predictions. `np.mean(axis=1)` averaged the two ~2.77 values → `round_to_nearest_5` → **5 min** for all predictions. Fixed by switching to `prediction_type="VirtEnsembles"` (shape `(n, 10, 1)`), squeezing to `(n, 10)`, and taking `median ± std` instead of `p5/p95` (more stable at n=10).
- **ML: NoneType crash in `fetch_holidays`** (`db.py`): `sorted(country_codes)` failed when the list contained `None` (parks with missing country metadata). Fixed by filtering: `country_codes = [c for c in country_codes if c is not None]`.
- **ML: Weekend underprediction** (`features.py`, `predict.py`, `config.py`): `volatility_7d` dominated feature importance at 32.91% while `is_weekend` was 0.01% and `avg_wait_last_1h` was 0.00%. The model could not distinguish weekday vs weekend crowd levels. Fixed by:
  - Splitting `volatility_7d` into `volatility_weekday` + `volatility_weekend` in training pipeline (`calculate_trend_volatility`)
  - Adding `rolling_avg_weekday` + `rolling_avg_weekend` via SQL window functions in `fetch_recent_wait_times`
  - Adding `avg_wait_same_dow_4w` (mean of last 4 same-day-of-week observations) for a stable historical reference
  - Lowering `VOLATILITY_CAP_STD_MINUTES` from 40 → 15 to reduce volatility dominance
  - All new features propagated to inference in `predict.py`
  - Detailed analysis: [Prediction Quality Issues](ml/prediction-quality-issues.md)

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
