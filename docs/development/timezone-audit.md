# Timezone Audit – Park-Zeitzone bei allen Zeitoperationen

> **Stand:** 2026-02-08  
> **Regel:** Alle Zeitoperationen müssen die Park-Zeitzone berücksichtigen. Siehe [Date & Time Handling](datetime-handling.md).

## Status-Übersicht

| Datei | Status | Priorität |
|-------|--------|-----------|
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

**Problem:** `today` und `endDate` wurden mit `new Date()` berechnet (Server-Zeit).

**Fix:** Range in Park-Zeitzone: `getStartOfDayInTimezone(tz)`, `addDays(startDate, -2)` bis `addDays(startDate, days + 1)`.

### 2. `weather.service.ts` – Fallback in `fetchHourlyForecast` ✅ FIXED

**Problem:** `today = new Date()` und `Between(today, next7Days)` nutzten Server-Datum.

**Fix:** Park laden, `getCurrentDateInTimezone(tz)`, `fromZonedTime`, `addDays(todayStart, 7)`.

### 3. `weather.service.ts` – `markPastDataAsHistorical` ✅ FIXED

**Problem:** `today = new Date()` – globales „heute“ in Server-Zeit.

**Fix:** Pro Park iterieren, `getCurrentDateInTimezone(park.timezone)` für `todayStr`, Update mit `date < :todayStr` und `parkId = :parkId`.

### 4. `search.service.ts` – `getBatchParkHours` ✅ FIXED

**Problem:** `Between(todayStart, todayEnd)` mit Server-Datum für alle Parks.

**Fix:** Parks laden, pro Park `date = getCurrentDateInTimezone(park.timezone)` (String-Gleichheit).

### 5. `analytics.service.ts` – `getParkPercentilesToday` / `getAttractionPercentilesToday` ✅ FIXED

**Problem:** `startOfDay = new Date()` – Server-Mitternacht.

**Fix:** Park/Attraction laden, `getStartOfDayInTimezone(park.timezone)` bzw. `attraction.park.timezone`.

### 6. `park-integration.service.ts` – `tomorrowInParkTz` ✅ FIXED

**Problem:** `tomorrow = new Date(); tomorrow.setDate(...)` – Server-Morgen, nicht Park-Morgen.

**Fix:** `getTomorrowDateInTimezone(park.timezone)`.

### 7. `parks.service.ts` – `isParkCurrentlyOpen` / `isParkOperatingToday` ✅ FIXED

**Status:** Jetzt mit `getCurrentDateInTimezone(park.timezone)` für Konsistenz.

---

## ✅ Bereits korrekt

- `parks.service.ts`: `getTodaySchedule`, `getNextSchedule`, `fillScheduleGaps`, `getScheduleForDate`
- `calendar.service.ts`: verwendet `formatInParkTimezone`, `getCurrentDateInTimezone`
- `cache-warmup.service.ts`: `warmupCalendarForPark` nutzt `getCurrentDateInTimezone(tz)`
- `parks.controller.ts`: Weather nutzt `getCurrentDateInTimezone(park.timezone)`
- `wait-times.processor.ts`: `formatInParkTimezone(new Date(), timezone)` für `todayStr`
- `stats.service.ts`: `formatInParkTimezone` für Datumsvergleiche
- `ml.service.ts`: `getCurrentDateInTimezone(park.timezone)` für Predictions

---

## Unkritische Verwendungen von `new Date()`

Diese sind **OK**, da sie keine park-spezifische „heute“-Logik haben:

- **Metadata:** `lastUpdated`, `generatedAt`, `geocodingAttemptedAt` – Absolutzeit
- **Retry-Delays:** `nextRetryDate = new Date(Date.now() + ...)` – Relativzeit
- **UTC-Vergleiche:** `openingTime`/`closingTime` sind UTC; `now >= openingTime` ist korrekt
- **Relative Fenster:** z.B. `Date.now() - 2 * 60 * 60 * 1000` für „Daten der letzten 2 Stunden“ – absolute Uhrzeit
- **Parsing:** `new Date(row.timestamp)` – Umwandlung von DB-Timestamps
