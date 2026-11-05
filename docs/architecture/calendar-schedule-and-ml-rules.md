# Calendar, schedule and ML rules

How the server decides a calendar day's `status` (OPERATING / CLOSED / UNKNOWN) and `crowdLevel`, when
a measured past overrules the operator, and how the ML services and the API's own hourly gate follow
the same schedule. Open this before changing `CalendarService`, the yearly predictions or the schedule
handling in `ml-service`. What a client may conclude from these fields:
[calendar status for clients](../frontend/calendar-schedule-status.md); how schedules are stored:
[schedule sync](schedule-sync-and-calendar.md).

## 1. The calendar

`GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=` —
`CalendarService.buildCalendarResponse()` → `buildCalendarDay()` (`src/parks/services/calendar.service.ts`).

### 1.1 Status

| Day | Rule |
| --- | --- |
| future | from the schedule's day-status row: OPERATING → `OPERATING`, CLOSED → `CLOSED`, UNKNOWN or none → `UNKNOWN` |
| past and today, park **without** any OPERATING entry (`hasOperatingSchedule` false) | `UNKNOWN` with a crowd level other than `closed` → `OPERATING` (status reconstructed from rides) |
| past and today, park with OPERATING entries | a day without a schedule row stays `UNKNOWN` |
| strictly past, measured activity on a CLOSED day | see §1.5 |

The crowd level never changes the status. UNKNOWN means "opening hours not yet available", CLOSED means
closed.

### 1.2 Crowd level

- OPERATING, and UNKNOWN in the future: `inferredCrowdLevel`. A strictly past day reads its measured
  level; today and future days the headliner forecast (`buildPredictedCrowdLevels`: average predicted
  headliner wait ÷ typical-day-peak). With neither it is **`unknown`** — never one ride's own
  `crowdLevel` and never a placeholder `moderate` (PAR-1028; [crowd-level rule](../rules/crowd-levels.md)).
- CLOSED, or a past day without opening data: `closed`.

### 1.3 Yearly predictions use the same closed rule

`GET /v1/parks/.../predictions/yearly` (`ParkIntegrationService.aggregateDailyPredictions`) asks the
schedule the way the calendar does: a CLOSED entry, or a day with neither OPERATING nor CLOSED that
`isClosedByOperatingRange()` (`src/parks/utils/schedule-closed-day.util.ts`) places strictly between the
first and last OPERATING date, or outside that range for a seasonal park (`isParkSeasonal`), reads
`crowdLevel: "closed"`, `recommendation: "closed"` and no `avgWaitTime`. A day rated `unknown` carries no
`recommendation`. The response ends about 182 days out (the CatBoost daily horizon). Before PAR-410 the
route never read the schedule and recommended 28 of the 49 days `/calendar` called CLOSED at Legoland
Billund.

It also carries `holidays` (PAR-1016): every public, school-break or bridge day in the park's region from
its today through 364 days ahead, with English names (`ParkIntegrationService.getYearlyHolidays()` →
`dayHolidayFlags()`, `src/common/utils/holiday.utils.ts`), as a list of its own because the forecast
skips days and stops at ~182 days while next summer's break is already known. `isLocalHoliday` and
`buildLocalHolidayMap` are the ones `CalendarService` reads. Unreadable holidays or a park without a
country leave `holidays` out; an empty list means none. A row is keyed by the day it names
(`holidayDay()`), never by noon UTC read in the park's zone, which put holidays a day late east of
UTC+12.

### 1.4 Next schedule

The park payload, location/nearby and favorites carry `nextSchedule` from `ParksService.getNextSchedule`
(batch: `getBatchSchedules`): the next **OPERATING** day with opening and closing time, from the park's
tomorrow up to 365 days ahead. CLOSED and UNKNOWN days are skipped. The closed-park cache TTL is derived
from it ([caching](caching-strategy.md)).

### 1.5 A past CLOSED day the measurement overrules (PAR-697)

For a **strictly past** day, measured ride activity outranks the operator's park-level `CLOSED` — in the
calendar (`status: "OPERATING"`, `isEstimated: true`, reconstructed hours) and in the four callers of
the shut-day rule (`src/analytics/closed-park-days.sql.ts`), so they cannot disagree. Today and the
future are unaffected.

The gate is `src/parks/utils/measured-operation.gate.ts`, four conditions over the day's qualifying raw
readings, each with the production day it rejects:

| Condition | Rejects |
| --- | --- |
| ≥ 3 different wait values across the day | a feed reporting one number on every ride (Walibi Holland 2026-09-11) |
| 4–14 h from the first qualifying reading to the last, inclusive | a burst (55 min) and a reading that never stops |
| no qualifying reading 02:00–05:59 park-local | a feed stuck on its last value (352 of 551 candidate days) |
| `timestamp - "lastUpdated"` ≤ 60 min, and `observedReadingsSql` | a frozen upstream and our own bookkeeping (PAR-748); `lastUpdated IS NULL` is rejected too |

**The verdict is materialised**: evaluated in the read path the rule took the ML-accuracy query from
1.4 s to 7.4 s. `park_day_operations` holds one row per (park, park-local day) — verdict, derived hours,
time — and readers do a primary-key lookup (`measuredOperationDayExists`). **A day with no row keeps the
operator's entry**, by construction.

Written by queue `park-day-operation`:

- `calculate-yesterday-park-day-operation`, daily 04:45 UTC, each park's own yesterday — after the 04:30
  `attraction-hourly-history` rollup it reads, before the 05:00 downtime job that reads the same chunks.
- `backfill-park-day-operation` `{ parkId?, fromDate, toDate }` via
  `POST /v1/admin/backfill-park-day-operation` (owner) — the fill after a deploy or a threshold change.
  One month slice per call, the next after the log line `✅ Fill complete`; a Coolify deploy renews the
  Postgres container and ends whatever is writing, so the slice is the damage radius. Rows upsert on
  `(park_id, day)`.

Both judge finished days only (`ParkDayOperationService.computeRange` clamps `toDate` to the park's last
finished local day). Measured 2026-10-07 over 2025-12-24 … 2026-10-07: 6,174 park-level shut days, 1,238
with any activity, **210 opened** across 27 parks (151 of those CLOSED rows came from the gap fill).

## 2. Schedule sync and gap fill

Owned by [schedule sync](schedule-sync-and-calendar.md): the jobs, the day-status arbitration, the
silent-park and withdrawn-day guards, and `fillScheduleGaps` (CLOSED strictly between the first and last
operating day, UNKNOWN outside).

## 3. ML services and the schedule

### 3.1 `ml-service`

| Component | Behaviour |
| --- | --- |
| `schedule_filter.py`, `filter_predictions_by_schedule` | Daily: keeps only days with an OPERATING entry; a park with no OPERATING entry at all keeps everything. Hourly: keeps only times inside opening hours |
| `predict.py` | Skips inference for CLOSED and UNKNOWN rows. `featureContext.parkLiveStatus[parkId] === "OPERATING"` overrides UNKNOWN to `is_park_open = 1` and runs it; an explicit CLOSED is always respected |

`parkLiveStatus` comes from `ParksService.getBatchParkStatus` (`isParkOpen`,
`src/common/utils/status-calculator.util.ts`): an OPERATING window now is open; a park with OPERATING
entries but no window now is closed; only a park with no OPERATING entry falls back to rides — open when
any ride reported OPERATING with a wait above 0 in the last 30 min.

So a future UNKNOWN day gets no daily prediction unless the park is confirmed open, and reads
`crowdLevel: "unknown"` without a headliner forecast.

### 3.2 The API's own hourly gate (PAR-1048)

The service's "no schedule → keep all" let parks without any OPERATING window publish forecasts round the
clock (20 parks / 491 rides held slots at 02:00–04:00 local; Knoebels offered 05:45 as an "optimal"
`bestVisitTimes` slot). `MLService.withinKnownOpeningHours` gates every hourly answer — inside
`buildRawParkPredictions` before the Redis write (so the park payload, favorites, calendar today,
`/plan/day`, the forward archive and stored `wait_time_predictions` inherit it) and on the attraction
detail's live fallback (`getAttractionPredictions`). Per park-local day:

- **an OPERATING window** → slots pass untouched;
- **no window** (UNKNOWN, missing, past the operator's horizon) → only the hours the park's queues were
  measured in survive: `hours` of `getParkHourlyProfile(park, 1, 60, 20)` (an hour needs 10 measured
  days; the same cached profile `/plan/day` uses for `hoursSource: "observed"`). Energylandia, open daily
  with no future schedule, keeps its daytime forecast;
- **a frozen feed** → only profile hours from the first to the last in which some ride's queue moved
  (P90 above P25), `MLService.movingProfileHours`. Queue-Times feeds that never close (Beto Carrero,
  Cinecittà World, Le Parc du Petit Prince) re-stamp their last snapshot all night; on 2026-10-24 Beto
  Carrero kept 10–19, Petit Prince 9–16, Cinecittà none. PAR-759 would remove those night readings at
  the source;
- **no measured hour** → no hourly slot that day;
- **profile unreadable** (DB error, fail-fast window) → slots pass **ungated**: a trimmed answer would be
  cached for 30 min and the generator's replace mode would delete the stored rows.

`bestVisitTimes` is computed from these slots. The diff write removes stored night slots on the next run.

### 3.3 Summary

| Day | Calendar | ML |
| --- | --- | --- |
| CLOSED | `CLOSED`, `crowdLevel: "closed"` | no inference; filtered anyway |
| future UNKNOWN, park confirmed open by rides | `UNKNOWN`, headliner forecast or `unknown` | `parkLiveStatus` override → real prediction |
| future UNKNOWN, not confirmed | `UNKNOWN`, `unknown` | inference skipped |
| past or today without a schedule | with a crowd level → `OPERATING` | independent |
