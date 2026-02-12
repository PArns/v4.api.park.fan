# Changelog

Notable changes to the Park Fan API. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Version and date align with releases or significant doc/code milestones.

---

## [Unreleased]

### Changed

- **Calendar API:** UNKNOWN→OPERATING upgrade only for parks **without** OPERATING entries in `schedule_entries`. Parks with schedule integration keep UNKNOWN for days without schedule (DB-check via `hasOperatingSchedule`). Fixes Phantasialand Jan 26–31 incorrectly showing OPERATING.
- **Gap-fill** (`fillScheduleGaps`): Look-back added. Range: (today - 182 days) through (today + 182 days). Past gaps (e.g. winter closure Jan–Mar) are re-evaluated when new OPERATING (e.g. March 28) arrives, so UNKNOWN→CLOSED is promoted correctly.

### Performance

- **Schedule sync (`saveScheduleData`)**: Batch DELETE operations for cleanup placeholders (UNKNOWN/CLOSED removed when API provides real data) reduced from ~300 individual queries to **3 batch queries** (99% reduction). Code deduplication: normalize scheduleType once instead of 3× redundant iterations.
- **Gap-fill (`fillScheduleGaps`)**: Batch INSERT/UPDATE operations for gap-filled entries and status changes reduced from ~364 individual queries to **~5 batch queries** (98.6% reduction). All iterations collect entries/updates in-memory, then execute bulk operations using `createQueryBuilder().insert()` and `whereInIds()`.
- **Duplicate cleanup (`cleanupDuplicateScheduleEntries`)**: SQL window functions and CTEs replace N+1 queries; same-type and cross-type duplicate detection reduced from ~160 queries to **2 queries** (98.8% reduction). Uses PostgreSQL `ROW_NUMBER()` OVER (PARTITION BY) for efficient deduplication.
- **Per-park cleanup**: New `cleanupDuplicateScheduleEntriesForPark()` method called before gap-fill to prevent duplicates from parallel schedule syncs (runs targeted cleanup for single park instead of waiting for daily global cleanup).
- **Operating date range extraction**: New `getOperatingDateRange()` helper extracts min/max OPERATING date logic into reusable function (used by gap-fill classification and calendar fallback).

**Overall impact**: Typical schedule sync reduced from ~924 database queries to ~12 queries (**98.7% reduction**), estimated duration improvement from ~92 seconds to ~1.2 seconds.

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
