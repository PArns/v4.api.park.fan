# Schedule Sync & Calendar

## Overview

Park opening hours (schedules) come from **ThemeParks Wiki** (and optionally Wartezeiten for fallback). They are stored in `schedule_entries` and used by the **Calendar** and Park Integration endpoints.

## When Schedules Are Synced

| Trigger | Job | When | What |
| --- | --- | --- | --- |
| Cron | `sync-all-parks` | Daily 03:00 | Full park metadata + **schedules for all Wiki parks** (12 months ahead). |
| Cron | `sync-schedules-only` | Daily 15:00 | **Schedules only** for all Wiki parks (no discovery). Ensures new months (e.g. Efteling March) appear same day when the source publishes. |
| On-demand | `sync-park-schedule` | When calendar is requested | If the requested date range has **no or little schedule data** (e.g. user asks for March, we have nothing), a background job is queued for that park only. Rate-limited to **once per 12 hours per park** (Redis key `schedule:refresh:requested:{parkId}`). Triggers only when the requested range ends **14+ days** beyond our last schedule date (less aggressive). |

**Important**: The **daily cron** for park metadata must use the job name **`sync-all-parks`** (not `fetch-all-parks`). The processor only handles `sync-all-parks` and `sync-schedules-only`.

## Source: ThemeParks Wiki

- **Client**: `ThemeParksClient` (`src/external-apis/themeparks/themeparks.client.ts`).
- **Extended fetch**: `getScheduleExtended(entityId, 12)` **always** requests each of the next **12 months** via the month endpoint (`/entity/{id}/schedule/{year}/{month}`). Month is **zero-padded** (e.g. `05` for May) per Wiki API. Optionally merges with generic `/schedule` (~30 days) for the near term.
- Some parks (e.g. Efteling) may return empty/404 for far-future months until the park publishes them; we still **request** every month (01–12 ahead) so data appears as soon as the source adds it. **On-demand** `sync-park-schedule` helps when a user requests a range we don’t have yet.

- **CLOSED not returned:** The Wiki returns **only OPERATING days**. It does **not** return `type: "CLOSED"` for closed days or months (e.g. Phantasialand Jan 26–31 and entire February come as missing or `schedule: []`). Gap-fill therefore treats "after last OPERATING until next OPERATING" as CLOSED so off-season shows correctly.

## Schedule data sources (all)

| Source | Used for schedule? | OPERATING | CLOSED | Notes |
|--------|--------------------|----------|--------|--------|
| **ThemeParks Wiki** (schedule API) | Yes (primary) | Yes, per day/month | **No** – closed days/months are missing or empty | `getScheduleExtended` → `saveScheduleData`. Gap-fill fills closure gaps. |
| **Wartezeiten** (`/v1/openingtimes`) | Yes (fallback when no Wiki today) | Yes, when `opened_today: true` | **Yes, for today** – when `opened_today: false` we persist CLOSED for today (park timezone) | Daily job `fetch-opening-times`. Only today; no historical/future. |
| **Wait-Times processor** (live `operatingHours`) | Yes (fallback when no Wiki today) | Yes, from merged live data | No – only persists when `operatingHours.length > 0` (open windows) | Wiki/Wartezeiten live merge; type from window (typically OPERATING). |
| **Queue-Times** | No | – | – | No operating hours in API; does not feed `schedule_entries`. |

So **CLOSED** can come from: (1) ThemeParks Wiki only if the API ever returns it (currently it does not), (2) Wartezeiten for **today** when the park reports closed, (3) **Gap-fill** for missing days between OPERATING ranges and for “after last OPERATING until next OPERATING”.

## UNKNOWN vs CLOSED

- **`ScheduleType.CLOSED`**: Park is **confirmed** closed: from API (e.g. Wiki) or from **gap-fill** when the day has no schedule but lies **strictly between** two OPERATING days (see Gap-fill rules below).
- **`ScheduleType.UNKNOWN`**: **No schedule data yet** — either before we have any OPERATING data, after the last known OPERATING day, or the park has no OPERATING entries. Calendar shows "Opening hours not yet available".
- **Gap-fill** (`fillScheduleGaps`): For each missing date we create an entry with holiday/bridge metadata. We set **CLOSED** when (1) the date is strictly **between** two OPERATING days, or (2) the date is **after** the last OPERATING but **before** the next OPERATING (e.g. winter closure; Wiki returns no CLOSED). Otherwise **UNKNOWN**. Existing OPERATING entries that fall in such a closure gap are replaced with CLOSED. The fill range can start from the day after last OPERATING when that is before today, so past closure gaps are filled/corrected.
- **Cleanup**: When ThemeParks (or saveScheduleData) provides real data for a date, we **delete** the UNKNOWN entry for that `(parkId, date)`. When the API provides **OPERATING** for a date we also delete any **CLOSED** row for that date (so gap-fill CLOSED is removed and OPERATING wins). Only the API's OPERATING/CLOSED then remains for that date.
- **Calendar API**: Each day has **`status`** (ParkStatus): `OPERATING` | `CLOSED` | `UNKNOWN`. Frontend shows "Closed" for `CLOSED` and "Opening hours not yet available" for `UNKNOWN`.
- **On-demand refresh**: When deciding whether to trigger `sync-park-schedule`, we count **all** schedule types (including UNKNOWN) as “we have data until X”. We only trigger when the requested range extends 14+ days beyond our last schedule date (any type).

## Gap-fill rules (fillScheduleGaps)

### When gap-fill runs (DB updates are automatic)

- **After every schedule sync:** Both `sync-all-parks` (daily 03:00) and `sync-schedules-only` (daily 15:00) call `saveScheduleData` then `fillScheduleGaps(park.id)` for each Wiki park. On-demand `sync-park-schedule` does the same for a single park.
- **Optional job:** `fill-all-gaps` runs `fillAllParksGaps()` (all parks, no API fetch). Used e.g. after holiday data updates. No one-off DB correction is needed when switching to park-timezone range — the next sync fills using the new range.

### Range and classification

**Timezone:** All range and date logic uses **park timezone** (see [Date & Time Handling](../development/datetime-handling.md)). The filled range is "today" through "today + lookAheadDays" in the park’s calendar, not server date.

- **Range**: From **today in park timezone** through today + `lookAheadDays` (default 120), using `getStartOfDayInTimezone(park.timezone)` so the filled range is always in the park’s calendar. Only **missing** dates get an entry.
- **Classification** (no existing entry for that date):
  - **CLOSED**: The park has at least one OPERATING date **before** and one **after** this date (min/max OPERATING over all stored entries). The gap is "in the middle" of known opening — we treat it as a closed day (e.g. mid-week closure).
  - **UNKNOWN**: Otherwise: no OPERATING at all for the park, or this date is **before** the first OPERATING date (e.g. before season or before we stored data), or **on or after** the last OPERATING date (schedule not yet published). We cannot infer closed vs not yet published.
- **Existing UNKNOWN entries**: When re-running gap-fill, if an existing entry is UNKNOWN and is now "in the middle" (OPERATING before and after), we update it to CLOSED. We never overwrite OPERATING or API-provided CLOSED.

## Persistence

- **Service**: `ParksService.saveScheduleData(parkId, scheduleData)`.
- **Behaviour**: Upsert by `(parkId, date, scheduleType)` — insert new, update if times/description/holiday changed, delete `UNKNOWN` placeholders when real data exists.
- **Gaps**: After saving, `fillScheduleGaps(parkId)` fills missing dates (up to `lookAheadDays` ahead, default 120) with CLOSED or UNKNOWN and holiday/bridge metadata; see **Gap-fill rules** above.

## Calendar Endpoint & First-Request Slowness

- **Endpoint**: `GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=`
- **Service**: `CalendarService.buildCalendarResponse()`.

The **first request** for a given park/range can be **slow** if the cache is cold (cache miss; schedule, weather, ML, holidays, etc. fetched in parallel). **After the first request**: responses are cached (5 min if range includes today, 1 h for future-only).

### Optimizations (queries, indexes, cache)

- **Cache**: Only **per-month** keys: `calendar:month:{parkId}:YYYY-MM:{includeHourly}`. Any range (e.g. Feb 1–28, Feb 10–20, Feb 1–Mar 15) is served by loading the relevant months and slicing to the requested range — no full-range key, so overlapping requests (e.g. Feb 1–15 and Feb 10–28) reuse the same February data. When we build a response we cache only **full months** (all days 1st–last of that month) for reuse. TTL: 5 min if month includes today, 1 h for future.
- **Queries**: All range data is fetched in one `Promise.all` (schedule, weather, ML daily, holidays, refurbishments, historical queue data, daily stats). Hourly ML predictions are fetched **once** per calendar build and reused for today+tomorrow (avoids N+1 ML calls). **Weather**: `getWeatherData(parkId, from, to, park.timezone)` is called with timezone so the weather service skips the park lookup. **Queue data**: `findHistoricalDataForDateRange` uses `innerJoin` (no `AndSelect`) so only queue_data rows are loaded, not full attraction entities. **Crowd level**: Redis **MGET** is used once for all historical dates in range (`analytics:crowdlevel:park:{parkId}:{date}`); only cache misses call `calculateCrowdLevelForDate`.
- **Indexes** used by the calendar flow:
  - **schedule_entries**: `(parkId, date, scheduleType)` — range queries `getSchedule(parkId, from, to)` use the leftmost prefix `(parkId, date)`.
  - **weather_data**: `(parkId, date)` for `getWeatherData(parkId, from, to)`.
  - **holidays**: `(country, date)` / `(country, region, date)` for `getHolidays(country, from, to)`.
  - **park_daily_stats**: `(parkId, date)` for `getDailyStats(parkId, from, to)`.
  - **queue_data**: `(attractionId, timestamp)`; calendar uses `findHistoricalDataForDateRange` (join with attractions on `parkId`).
- **Crowd level**: For historical days, `calculateCrowdLevelForDate` is called per day but uses its own Redis cache (`analytics:crowdlevel:...`), so repeated requests hit cache.

### Calendar Warmup

Calendar is warmed **once per day**, not every 5 min with park warmup:

- **Job**: `warmup-calendar-daily` on the `park-metadata` queue.
- **Schedule**: Daily at **5:00** (cron).
- **Effect**: Warms **per-month** keys `calendar:month:{parkId}:YYYY-MM:today+tomorrow` for **-1 to +3 months** (last month through 3 months ahead, park timezone) for **all parks**, covering the typical user range (recap + planning).

So the calendar endpoint is fast after the first request of the day without warming on every wait-times sync. See [Caching Strategy – Cache Warmup](caching-strategy.md#cache-warmup).

## Related

- [Calendar, Schedule & ML Rules](calendar-schedule-and-ml-rules.md) — Status/crowd rules (past vs future, UNKNOWN vs CLOSED), schedule sync, ML alignment.
- [Job Queues & Processors](job-queues.md) — cron and job names.
- [Data Ingestion](data-ingestion.md) — multi-source overview.
- [Caching Strategy](caching-strategy.md) — Redis keys and TTLs.
