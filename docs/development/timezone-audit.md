# Timezone Audit – Park timezone in all time operations

> **As of:** 2026-02-08  
> **Rule:** All time operations must respect the park timezone. See [Date & Time Handling](datetime-handling.md).

## Status overview

| File | Status | Priority |
|------|--------|----------|
| `parks.service.ts` getUpcomingSchedule | ✅ FIXED (2026-02-08) | – |
| `parks.service.ts` isParkCurrentlyOpen / isParkOperatingToday | ✅ FIXED (getCurrentDateInTimezone) | – |
| `weather.service.ts` Fallback fetchHourlyForecast | ✅ FIXED (2026-02-08) | – |
| `weather.service.ts` markPastDataAsHistorical | ✅ FIXED (per-park timezone) | – |
| `search.service.ts` getBatchParkHours | ✅ FIXED (2026-02-08) | – |
| `analytics.service.ts` getParkPercentilesToday / getAttractionPercentilesToday | ✅ FIXED (2026-02-08) | – |
| `park-integration.service.ts` tomorrowInParkTz | ✅ FIXED (getTomorrowDateInTimezone) | – |
| `parks.controller.ts` weather | ✅ OK | – |
| `parks.service.ts` getTodaySchedule, getNextSchedule, fillScheduleGaps | ✅ OK | – |
| `calendar.service.ts` | ✅ OK | – |
| `cache-warmup.service.ts` warmupCalendarForPark | ✅ OK | – |

---

## Fixes (2026-02-08)

### 1. `parks.service.ts` – `getUpcomingSchedule` ✅ FIXED

**Problem:** `today` and `endDate` were computed with `new Date()` (server time).

**Fix:** Range in park timezone: `getStartOfDayInTimezone(tz)`, `addDays(startDate, -2)` through `addDays(startDate, days + 1)`.

### 2. `weather.service.ts` – Fallback in `fetchHourlyForecast` ✅ FIXED

**Problem:** `today = new Date()` and `Between(today, next7Days)` used server date.

**Fix:** Load park, `getCurrentDateInTimezone(tz)`, `fromZonedTime`, `addDays(todayStart, 7)`.

### 3. `weather.service.ts` – `markPastDataAsHistorical` ✅ FIXED

**Problem:** `today = new Date()` – global “today” in server time.

**Fix:** Iterate per park, `getCurrentDateInTimezone(park.timezone)` for `todayStr`, update with `date < :todayStr` and `parkId = :parkId`.

### 4. `search.service.ts` – `getBatchParkHours` ✅ FIXED

**Problem:** `Between(todayStart, todayEnd)` with server date for all parks.

**Fix:** Load parks, per park `date = getCurrentDateInTimezone(park.timezone)` (string equality).

### 5. `analytics.service.ts` – `getParkPercentilesToday` / `getAttractionPercentilesToday` ✅ FIXED

**Problem:** `startOfDay = new Date()` – server midnight.

**Fix:** Load park/attraction, `getStartOfDayInTimezone(park.timezone)` or `attraction.park.timezone`.

### 6. `park-integration.service.ts` – `tomorrowInParkTz` ✅ FIXED

**Problem:** `tomorrow = new Date(); tomorrow.setDate(...)` – server tomorrow, not park tomorrow.

**Fix:** `getTomorrowDateInTimezone(park.timezone)`.

### 7. `parks.service.ts` – `isParkCurrentlyOpen` / `isParkOperatingToday` ✅ FIXED

**Status:** Now uses `getCurrentDateInTimezone(park.timezone)` for consistency.

---

## Already correct

- `parks.service.ts`: `getTodaySchedule`, `getNextSchedule`, `fillScheduleGaps`, `getScheduleForDate`
- `calendar.service.ts`: uses `formatInParkTimezone`, `getCurrentDateInTimezone`
- `cache-warmup.service.ts`: `warmupCalendarForPark` uses `getCurrentDateInTimezone(tz)`
- `parks.controller.ts`: weather uses `getCurrentDateInTimezone(park.timezone)`
- `wait-times.processor.ts`: `formatInParkTimezone(new Date(), timezone)` for `todayStr`
- `stats.service.ts`: `formatInParkTimezone` for date comparisons
- `ml.service.ts`: `getCurrentDateInTimezone(park.timezone)` for predictions

---

## Acceptable uses of `new Date()`

These are **OK** because they do not implement park-specific “today” logic:

- **Metadata:** `lastUpdated`, `generatedAt`, `geocodingAttemptedAt` – absolute time
- **Retry delays:** `nextRetryDate = new Date(Date.now() + ...)` – relative time
- **UTC comparisons:** `openingTime`/`closingTime` are UTC; `now >= openingTime` is correct
- **Relative windows:** e.g. `Date.now() - 2 * 60 * 60 * 1000` for “data from the last 2 hours” – absolute clock time
- **Parsing:** `new Date(row.timestamp)` – conversion of DB timestamps
