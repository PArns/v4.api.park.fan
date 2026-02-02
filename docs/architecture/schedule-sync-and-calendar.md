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
- **Extended fetch**: `getScheduleExtended(entityId, 12)` — generic `/schedule` (≈30 days) plus **month-by-month** for up to **12 months** ahead.
- Some parks (e.g. Efteling) may not expose far-future months until the park publishes them; until then, those months stay empty in our DB and the **on-demand** trigger helps once the source adds data.

## UNKNOWN Handling

- **`ScheduleType.UNKNOWN`**: Used for **holiday/bridge-day placeholders** when we have no real operating hours from the source (e.g. fillScheduleGaps).
- **Creation**: `fillScheduleGaps(parkId)` creates UNKNOWN entries for dates that have no schedule but are holidays/bridge days, so the calendar can still show holiday info.
- **Cleanup**: When ThemeParks (or saveScheduleData) provides real data for a date, we **delete** the UNKNOWN entry for that `(parkId, date)` so only OPERATING/CLOSED remains.
- **Calendar**: UNKNOWN days are shown as **CLOSED** with no operating hours (no times). Only OPERATING entries get opening/closing times.
- **On-demand refresh**: When deciding whether to trigger `sync-park-schedule`, we count **all** schedule types (including UNKNOWN) as “we have data until X”. So we do **not** trigger when the requested range is already covered by UNKNOWN placeholders (e.g. March all UNKNOWN from fillScheduleGaps). We only trigger when the requested range extends 14+ days beyond our last schedule date (any type).

## Persistence

- **Service**: `ParksService.saveScheduleData(parkId, scheduleData)`.
- **Behaviour**: Upsert by `(parkId, date, scheduleType)` — insert new, update if times/description/holiday changed, delete `UNKNOWN` placeholders when real data exists.
- **Gaps**: After saving, `fillScheduleGaps(parkId)` fills holiday/bridge-day metadata for missing dates (up to 90 days ahead).

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
- **Effect**: Warms **per-month** keys `calendar:month:{parkId}:YYYY-MM:today+tomorrow` for **current month + next month** (park timezone) for **all parks**.

So the calendar endpoint is fast after the first request of the day without warming on every wait-times sync. See [Caching Strategy – Cache Warmup](caching-strategy.md#cache-warmup).

## Related

- [Job Queues & Processors](job-queues.md) — cron and job names.
- [Data Ingestion](data-ingestion.md) — multi-source overview.
- [Caching Strategy](caching-strategy.md) — Redis keys and TTLs.
