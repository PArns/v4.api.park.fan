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

