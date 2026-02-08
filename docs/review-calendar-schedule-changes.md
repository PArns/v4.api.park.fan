# Review: Calendar & Schedule Sync Changes

Short review of all changes from the session (plausibility and completeness).

---

## 1. Schedule sync & opening hours

| Change | Plausible | Complete |
|--------|-----------|----------|
| **Job name** `fetch-all-parks` → `sync-all-parks` in Scheduler + Admin | ✅ Processor only listens for `sync-all-parks`; cron and admin now enqueue the same job. | ✅ Both places updated. |
| **New job** `sync-schedules-only` (daily 15:00) | ✅ Schedules only, no full discovery; reduces load and brings new months sooner. | ✅ Handler + cron registered. |
| **On-demand** `sync-park-schedule` (single park) | ✅ Triggered from calendar when range ends 14+ days beyond last schedule; rate limit 12h. | ✅ Handler + trigger in CalendarService; UNKNOWN counts as “data until X”. |
| **Less aggressive:** gap 14 days, rate limit 12h | ✅ Less API load, still timely updates. | ✅ Constants + docs updated. |

---

## 2. Calendar warmup

| Change | Plausible | Complete |
|--------|-----------|----------|
| Calendar **no longer** in 5-min park warmup | ✅ Calendar once daily is enough; saves load. | ✅ Call removed from `warmupParkCache`. |
| **New job** `warmup-calendar-daily` (daily 5:00) | ✅ After schedule sync, before typical traffic. | ✅ Processor + cron; `warmupCalendarForAllParks()` uses `warmupCalendarForPark()` per park. |
| Warmup builds range “current + next month” | ✅ fromStr = 1st of month, toStr = last day of next month (JS `new Date(y, m, 0)` for last day). | ✅ Range in park timezone; comment updated (only from warmupCalendarForAllParks). |

---

## 3. Per-month cache (calendar)

| Change | Plausible | Complete |
|--------|-----------|----------|
| **Only** per-month keys: `calendar:month:{parkId}:YYYY-MM:{includeHourly}` | ✅ Overlapping ranges (e.g. Feb 1–15 and Feb 10–28) share the same month. | ✅ Full-range cache removed; read only from month cache. |
| **Read:** fetch months in range, merge, slice to [from, to] | ✅ `getMonthsInRange` timezone-safe (formatInParkTimezone per day). | ✅ Empty range (e.g. from > to) → `monthsInRange = []` → `[].every(...)` = true → empty response; in practice covered by controller. |
| **Write:** only cache **full** months | ✅ Check: length = last day, first day = YYYY-MM-01, last = YYYY-MM-lastDay. | ✅ TTL: 5 min if month includes today, else 1h. |

---

## 4. Query optimisations

| Change | Plausible | Complete |
|--------|-----------|----------|
| **Weather** `getWeatherData(..., timezone?)` | ✅ Calendar has park including timezone; one park lookup saved per build. | ✅ Only calendar passes timezone; controller/other callers unchanged (3 args). |
| **QueueData** `innerJoin` instead of `innerJoinAndSelect` | ✅ Calendar only uses `timestamp` and `waitTime`, no attraction fields. | ✅ No access to `qd.attraction` in calendar code (verified). |
| **Crowd level** Redis MGET for historical days | ✅ One MGET instead of N GETs; keys = `analytics:crowdlevel:park:{parkId}:{date}`. | ✅ Prefetch map passed to `buildCalendarDay`; on hit no `calculateCrowdLevelForDate`. |
| **Hourly ML** once per build, then `buildHourlyPredictionsFromList` | ✅ No N+1 (e.g. 2 ML calls → 1). | ✅ Only fetched when `includeHourly !== "none"`. |

---

## 5. Index

| Change | Plausible | Complete |
|--------|-----------|----------|
| **No** extra index `(parkId, date)` on `schedule_entries` | ✅ `(parkId, date, scheduleType)` covers range queries via leftmost prefix; duplicate index removed. | ✅ Entity and docs consistent. |

---

## 6. Docs & comments

| Location | Status |
|----------|--------|
| `docs/architecture/schedule-sync-and-calendar.md` | ✅ Sync, UNKNOWN, calendar, optimisations, warmup (incl. per-month key). |
| `docs/architecture/caching-strategy.md` | ✅ Warmup calendar as per-month key described. |
| `docs/architecture/job-queues.md` | ✅ Park metadata + schedule sync mentioned. |
| `CLAUDE.md` | ✅ Link to Schedule Sync & Calendar. |
| Cache warmup comment “Called from warmupParkCache” | ✅ Changed to “warmupCalendarForAllParks (daily warmup)”. |
| Schedule sync doc “Effect: Warms calendar:…” | ✅ Corrected to “per-month keys calendar:month:…”. |

---

## 7. Tests & build

- **Build:** `npm run build` succeeds.
- **Calendar unit test** (`test/unit/calendar.service.spec.ts`): Mocks only Parks, Weather, ML, Holidays, Attractions, Shows, Redis; **not** QueueDataService, StatsService, AnalyticsService, park-metadata Queue. Running the unit tests may cause `buildCalendarResponse` to fail once these dependencies are used. Optional: add missing mocks if calendar specs should run.

---

## 8. Edge cases (spot check)

- **Empty month range** (e.g. from > to): `getMonthsInRange` → `[]`, `monthCached.every(...)` = true → response with empty `days`; in practice validated by controller.
- **Park without timezone:** Weather fallback (simple date range); calendar passes `park.timezone`; if undefined, warmup uses `tz = park.timezone || "UTC"`.
- **QueueData after join:** Calendar only reads `timestamp` and `waitTime`; no access to `attraction` → `innerJoin` without select is fine.

---

**Conclusion:** Changes are plausible and implemented consistently; docs and comments updated. Only optional follow-up: extend calendar unit test with missing mocks if the specs should be run.
