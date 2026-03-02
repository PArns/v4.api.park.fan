# Date & Time Handling (The "Park Time" Rule)

## ⚠️ The Golden Rule

**NEVER use `new Date()` directly for business logic.**

In a global application like Park Fan, "today" is different depending on where the park is.
- When it is **10:00 AM** in Berlin (Phantasialand is open), it might be **01:00 AM** in Los Angeles (Disneyland is closed).
- If you use `new Date()`, you get the **server's time** (usually UTC), which will often be wrong for the park you are processing.

## Core Concept: Park Time

**"Park Time"** refers to the date and time *relative to the park's physical location*.

- **Database Storage**: We generally store timestamps in **UTC**.
- **Application Logic**: We strictly convert UTC to **Park Time** before making decisions (e.g., "Is the park open?", "Is this a holiday?").

## Key Utilities

We use `date-fns-tz` for robust timezone handling. All date logic should use our centralized utilities in `src/common/utils/date.util.ts`.

### 1. Getting "Today"

**❌ Wrong:**
```typescript
const today = new Date().toISOString().split('T')[0]; // Uses server time!
```

**✅ Correct:**
```typescript
import { getCurrentDateInTimezone } from 'src/common/utils/date.util';

const parkTimezone = park.timezone; // e.g., "America/New_York"
const today = getCurrentDateInTimezone(parkTimezone); // Returns "2024-05-20"
```

### 2. Getting Current Time (Hour/Minute)

**❌ Wrong:**
```typescript
const hour = new Date().getHours(); // Server hour!
```

**✅ Correct:**
```typescript
import { getCurrentTimeInTimezone } from 'src/common/utils/date.util';

const nowInPark = getCurrentTimeInTimezone(park.timezone);
const hour = nowInPark.getHours(); // 14 (if it's 2 PM in the park)
```

### 3. Formatting for Display or External APIs

When communicating with external APIs or generating daily stats, always ensure the date match the park's day.

```typescript
import { formatInParkTimezone } from 'src/common/utils/date.util';

const dateStr = formatInParkTimezone(someDateObj, park.timezone);
```

## Common Pitfalls

### The "Midnight" Traps

1. **Querying by Date**:
   When you query the database for "all wait times on 2024-05-20", you **cannot** just query `2024-05-20 00:00:00 UTC` to `2024-05-20 23:59:59 UTC`.
   
   You must calculate the UTC start and end time **for that specific timezone**.
   
   ```typescript
   // Helper available in some services
   const start = getStartOfDayInTimezone(park.timezone); 
   // Returns a Date object that is Midnight in local time, converted to UTC.
   ```

2. **Mixing Timezones**:
   Never compare a date from Park A (Europe) with Park B (USA) without normalizing them.

## Testing Timezones

When writing tests, **always mock the system time** or use fixed dates. Do not rely on the test runner's local time.

## 6. Strict Date-Only Handling (Holidays, Schedules & Events)

**Feiertage und Ferien** (holidays, school holidays) sowie **Schedule-Einträge** speichern **nur das Datum** (keine Uhrzeit). In der DB steht ggf. ein DATE oder Timestamp; semantisch bedeuten sie immer **den vollen Kalendertag 00:00–23:59 in der Park-Zeitzone**.

To prevent "Timeshifting" (e.g., a holiday on Dec 25th becoming Dec 24th 23:00 UTC), **Pure Dates** must be handled as strings (`YYYY-MM-DD`).

> [!CAUTION]
> **NEVER** verify a Date-Only value by converting it to a `Date` object without explicit Noon-UTC forcing.

**The Golden Rule for Holidays & Schedules:**
1.  **Storage**: DB stores as `DATE` type (holidays, `schedule_entries.date`).
2.  **Meaning**: The date always means **that calendar day in the park’s timezone** (00:00–23:59 park time).
3.  **"Today"**: Use `getCurrentDateInTimezone(park.timezone)` to get today’s date string; use that for holiday checks and for schedule queries.
4.  **Queries**: Prefer **date-string equality** (`schedule.date = :todayStr`) over timestamp ranges when filtering by a single calendar day, so results do not depend on the DB session timezone.

#### Why?
`new Date("2025-12-25")` defaults to `00:00:00` UTC.
If the browser or server applies a timezone offset (e.g., EST -5), it becomes `Dec 24th, 19:00`. **The day has shifted.**

**Best Practice:**
```typescript
// BAD
const holidayDate = new Date(holiday.date); // Risk of shift

// GOOD
const dateStr = holiday.date; // Use "YYYY-MM-DD" string
const isMatch = dateStr === targetDateStr;
```

**Schedule "today" (timezone-safe):**
```typescript
const todayStr = getCurrentDateInTimezone(park.timezone);
const schedule = await this.getScheduleForDate(parkId, todayStr); // DATE = :todayStr
```

---

## 7. Two Recurring Anti-Patterns That Break US Parks (Read Before Touching Date Code!)

> **Context**: This has been attempted to fix multiple times. Each attempt broke either UTC parks or US parks. These two patterns are the root cause.

### Anti-Pattern A — External API date-only string → `new Date()` → `formatInParkTimezone()`

This is the core bug in `saveScheduleData`. External APIs (ThemeParks.wiki) return dates as date-only strings representing the **park's local calendar day**:
```
{ date: "2026-03-02", openingTime: "2026-03-02T10:00:00-05:00" }
```

**The trap**: `new Date("2026-03-02")` = `2026-03-02T00:00:00.000Z` (midnight UTC per ECMAScript spec). Then `formatInParkTimezone(midnight_UTC, "America/New_York")` = `"2026-03-01"` (7 PM EST = previous day). **The date shifts back 1 day for all parks west of UTC.**

**Why it only affects US parks**: East-of-UTC parks (Europe/Berlin, Asia/Tokyo) are ahead of UTC, so midnight UTC is still the same calendar day in those timezones. UTC parks are unaffected. Only west-of-UTC parks (Americas) are shifted.

**The fix**: detect date-only strings with regex and use them directly:
```typescript
const raw = typeof entry.date === "string" ? entry.date : String(entry.date);
const isDateOnly = /^\d{4}-\d{2}-\d{2}$/.test(raw);
const dateStr = isDateOnly
  ? raw  // ← use as-is; it already represents the park's local calendar day
  : formatInParkTimezone(new Date(raw), park.timezone); // full ISO → convert
```

**Also**: when you need to calculate a date range (min/max) from date-only strings, always use noon UTC to avoid the same shift in `formatInParkTimezone`:
```typescript
const ts = new Date(`${dateStr}T12:00:00Z`).getTime(); // noon UTC = safe for all timezones
```

---

### Anti-Pattern B — TypeORM DATE column → `new Date(entity.date)` → `formatInParkTimezone()`

PostgreSQL `DATE` columns are returned by TypeORM as JavaScript `Date` objects at **midnight UTC** (the pg driver gives "YYYY-MM-DD" string; TypeORM converts with `new Date("YYYY-MM-DD")`). Applying `formatInParkTimezone(midnight_UTC, "America/New_York")` shifts the date back one day for US parks.

**Affected code**: Any place that does `formatInParkTimezone(new Date(entity.date), tz) === todayStr` on a DATE column will silently fail for US parks.

**Example** (weather service):
```typescript
// ❌ BAD — shifts midnight UTC back 1 day for US parks
const weatherDate = new Date(w.date);  // midnight UTC
return formatInParkTimezone(weatherDate, park.timezone) === todayStr; // "2026-03-01" ≠ "2026-03-02"

// ✅ CORRECT — midnight UTC → toISOString → YYYY-MM-DD is always the stored calendar date
const dateStr = w.date instanceof Date ? w.date.toISOString().split("T")[0] : String(w.date);
return dateStr === todayStr; // "2026-03-02" === "2026-03-02" ✓
```

**Why `.toISOString().split("T")[0]` is always correct here**: midnight UTC for `2026-03-02` produces `"2026-03-02T00:00:00.000Z"`. Splitting gives `"2026-03-02"` — exactly the calendar date stored in the DB. This is safe because the DB stores calendar dates *as* midnight UTC, and UTC midnight does not cross a day boundary in UTC.

**Summary table — which approach to use:**

| Source of date | Type at runtime | Correct approach |
|---|---|---|
| External API date-only string (`"2026-03-02"`) | `string` | Use directly (no `new Date()`!) |
| External API ISO datetime (`"2026-03-02T10:00-05:00"`) | `string` | `formatInParkTimezone(new Date(raw), tz)` |
| TypeORM `DATE` column | `Date` (midnight UTC) | `.toISOString().split("T")[0]` |
| TypeORM `TIMESTAMPTZ` column | `Date` (real instant) | `formatInParkTimezone(date, tz)` ✓ |