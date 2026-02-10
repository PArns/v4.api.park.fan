# Changelog

Notable changes to the Park Fan API. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Version and date align with releases or significant doc/code milestones.

---

## [Unreleased]

### Fixed

- **Calendar / Schedule (ThemeParks Wiki):** The Wiki API returns **only OPERATING days**; it does **not** return CLOSED entries for closed days or months (e.g. Phantasialand January 26–31 and entire February come as empty/missing). Two changes fix the calendar:
  1. **saveScheduleData:** When the API does provide CLOSED for a date, any existing OPERATING row for that date is now deleted so the calendar shows CLOSED.
  2. **fillScheduleGaps:** (a) Gaps **after** the last OPERATING date and **before** the next OPERATING date (e.g. winter closure) are now classified as CLOSED, not UNKNOWN. (b) The fill range starts from the day after last OPERATING when that is before today, so closure gaps in the past (e.g. Jan 26–Feb 9) are filled or corrected. (c) When a date in that closure gap already has an OPERATING entry (stale/wrong), gap-fill now replaces it with CLOSED. Phantasialand and similar parks will show the correct closed season after the next schedule sync.
- **Schedule data sources:** Documented in [Schedule Sync & Calendar](architecture/schedule-sync-and-calendar.md): ThemeParks Wiki (no CLOSED), Wartezeiten (CLOSED for today when `opened_today: false`), Wait-Times live (OPERATING only), Queue-Times (no schedule). **Wartezeiten:** When the API returns `opened_today: false`, we now persist CLOSED for today (park timezone) so the calendar shows closed for parks that use Wartezeiten when they are closed.
- **Park schedule endpoint** (`GET .../schedule`): Response is deduplicated to one entry per date (park timezone). When the DB has both OPERATING and CLOSED for the same date, we now expose only CLOSED so the schedule response matches the calendar (closed wins over stale OPERATING).

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

- Calendar no longer shows "Opening hours not yet available" for days that are known to be closed (gap-fill and API CLOSED now set status CLOSED where appropriate).
- When the API provides OPERATING for a date that had a gap-fill CLOSED, the calendar could show CLOSED (because `getSchedule` orders by scheduleType ASC). `saveScheduleData` now deletes any CLOSED row for that date when saving OPERATING.
- **Timezone audit:** All time operations now use park timezone. Fixed: `getUpcomingSchedule` (range in park TZ), `weather.service` fallback + `markPastDataAsHistorical` (per-park), `getBatchParkHours` (per-park today), `getParkPercentilesToday` / `getAttractionPercentilesToday` (startOfDay in park TZ), `tomorrowInParkTz` (getTomorrowDateInTimezone), `isParkCurrentlyOpen` / `isParkOperatingToday` (getCurrentDateInTimezone).

---

## [Older versions]

Older changes were not recorded in this changelog. From this version onward, notable changes will be listed here with version and date.

---

(Compare URLs can be added when using a Git remote, e.g. `[4.6.1]: https://github.com/owner/repo/compare/v4.5.0...v4.6.1`.)
