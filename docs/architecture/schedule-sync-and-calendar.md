# Schedule sync and the calendar

How park opening hours get into `schedule_entries` — the ThemeParks.wiki fetch and its rate limit, the
save, the gap fill, the guards against a silent or withdrawn schedule — and how the calendar endpoint
is cached and warmed. Open this before touching `ParksService.saveScheduleData` / `fillScheduleGaps` or
the schedule jobs. What the calendar then shows per day (status, crowd level, ML alignment) is
[Calendar, schedule and ML rules](calendar-schedule-and-ml-rules.md).

## When schedules are synced

| Job (`park-metadata` queue) | When (UTC) | What |
| --- | --- | --- |
| `sync-all-parks` | 03:00 | Park metadata plus schedules for every wiki park |
| `sync-schedules-only` | 15:00 | Schedules only, so a newly published month appears the same day |
| `sync-park-schedule` | on demand | One park, queued by the calendar (below) |
| `fill-all-gaps` | on demand, after each holiday sync | `fillAllParksGaps()`, no API fetch |

The processor handles `sync-all-parks`, not `fetch-all-parks`; a cron registered under the wrong name
does nothing. Wartezeiten parks get opening times from `wartezeiten-schedule` (06:00), and
`WaitTimesProcessor` can write a single live-data day.

**On-demand refresh.** `CalendarService.requestScheduleRefreshIfNeeded` queues `sync-park-schedule` when
the requested range ends more than `SCHEDULE_REFRESH_GAP_DAYS` (5) after the last OPERATING or CLOSED
entry (UNKNOWN placeholders and event rows do not count, so a range covered only by gap-fill still
triggers), at most once per 12 h per park (`calendar:refresh-check:{parkId}`).

## Source: ThemeParks.wiki

`ThemeParksClient.getScheduleExtended(entityId, 12)` (`src/external-apis/themeparks/themeparks.client.ts`)
asks the month endpoint (`/entity/{id}/schedule/{year}/{month}`, month zero-padded) for the previous
month and each of the next 12, and may merge the generic `/schedule` (~30 days). Far months often answer
empty until the park publishes them; every month is still asked, so data appears as soon as it exists.

### The upstream budget, and why the bulk run is the only job that spends it

Measured 2026-09-26 (PAR-480): **300 requests per fixed 60-second window per sender**, one budget
across all endpoints (`ratelimit-limit: 300`, `ratelimit-policy: 300;w=60`, `ratelimit-remaining`,
`ratelimit-reset`). The API container and the host share one egress IP. What a job costs depends on the
CDN, not its request count:

| endpoint | `s-maxage` | our cadence | origin hits |
| --- | --- | --- | --- |
| `/entity/{id}/live` | 60 s | every 5 min, 195 parks | ~26 per cycle (~87 % Cloudflare hits, kept warm by other users) |
| `/entity/{id}/schedule…` | 3600 s | once a day, 195 × 14 | all 2,730 |

A cache hit carries no `RateLimit-*` header and never reaches the limiter. Rules for any new caller:

- `ThemeParksClient` paces **every** request 250 ms apart through an atomic Redis slot (≤ 241 per 60 s
  under any window alignment). Do not bypass it, and do not assume an endpoint is CDN-warm.
- A cooldown is **waited out**, never thrown past (throwing cost 87 of 200 parks their schedule in one
  run). The client pauses once `ratelimit-remaining` drops to 25 (`remainingFloor`) and ignores those
  headers on a cache hit (a HIT reports numbers from a window long gone).
- A throttled fetch raises `ThemeParksRateLimitError`; an empty list means the source published nothing.
- A caller inside a request path passes `ThemeParksClient.requestPathMaxWaitMs`, so a rate limit is a
  fast failure, not a two-minute sleep on a reader's page.

## Persistence: `saveScheduleData`

- Upsert by `(parkId, date, scheduleType)` over park-level rows (`attractionId IS NULL`). API types are
  normalised (`"Closed"` → `ScheduleType.CLOSED`), so an off-season the source reports is stored as
  CLOSED.
- **One day-status row per day, any number of event rows.** OPERATING, CLOSED and UNKNOWN say what the
  day *is* and are arbitrated against each other: saving OPERATING deletes that date's CLOSED and
  UNKNOWN rows, saving CLOSED deletes its OPERATING row (batched `date IN (...)` deletes). The other
  types (`EXTRA_HOURS`, `TICKETED_EVENT`, `PRIVATE_EVENT`, …) say what *else* happens and stay beside it:
  Rulantica's hotel-guest `EXTRA_HOURS` from 09:00 next to `OPERATING` from 09:30, a Halloween Horror
  Nights evening as `TICKETED_EVENT`. On 2026-10-26, 3,107 park-days at 73 parks held more than one row
  and none held two day-status rows (PAR-1005). Readers pick the day's status by type
  (`pickDayStatusEntry` / `dayStatusRank`, `src/common/utils/schedule.util.ts`), never the first row.
- **Park-level only.** Both `saveScheduleData` and `fillScheduleGaps` read and delete only
  `attractionId IS NULL` rows: a per-ride row is a different statement by a different writer. Without the
  filter the sync handed ride rows the park's hours, and the gap fill rewrote ride schedules by the park's
  operating range. The gap fill also never inserts a day that holds only per-ride rows (that would be a
  confident "Gap-filled" CLOSED built from a lost park row).
- **Version log** (PAR-1061): after the last writer of a sync is done with a park (Wiki save + gap fill,
  the global gap fill, Wartezeiten, the live fallback) `ParksService.recordScheduleVersions(parkId,
  source)` appends changed park-days to `schedule_day_versions`; a withdrawn day becomes a `NONE`
  version. Write-only ([covariate source](covariate-source.md)).

### A silent park does not announce future operating days

While a park's whole feed has been silent for `PARK_FEED_SILENT_DAYS` (30), `saveScheduleData` stores its
**future** OPERATING days as **UNKNOWN** with `openingTime`, `closingTime`, `description` and `purchases`
NULL. The schedule and the live data come from the same upstream, which keeps publishing after it stops
measuring: La Ronde, silent from 2026-06-24, had its schedule name every day to 2027-08-31 OPERATING (31
of 31 January days answered OPERATING with hours and `recommendation: "recommended"`). The
[absent-facts rule](../rules/absent-facts.md) in its own shape.

| row | effect |
| --- | --- |
| future OPERATING, park silent ≥ 30 days | stored UNKNOWN without hours; the date joins the OPERATING delete |
| future OPERATING, feed answering | untouched |
| past OPERATING | untouched — the past-day reconstruction in `CalendarService` reads it |
| CLOSED from the API | untouched |
| future gap-filled CLOSED | demoted to UNKNOWN by the next gap fill (the last OPERATING day moved back) |
| per-ride rows | out of scope |

UNKNOWN, not CLOSED: "no current information" is true, "the park is shut" would be a second unfounded
claim. Holiday flags are kept. The threshold is the same constant `ParkIntegrationService` uses for ride
status ([status regimes](attraction-status/status-regimes.md#5-a-whole-park-gone-silent)). It heals itself:
the first real reading clears the probe (`QueueDataService.hasObservedReadingWithin`, one indexed
`EXISTS`, paid only when the payload has a future operating day) and the next sync writes the days back.
Measured 2026-09-21: 10 parks, 1,254 future OPERATING rows. Recovery needs no dump: revert, deploy, and
the next sync rewrites the rows.

### A day the source withdraws stops being an operating day

`saveScheduleData` deletes future park-level OPERATING rows the payload no longer names — **only inside
the months the fetch reports as answered**. Before, a day the upstream stopped naming was never looked
at: Rulantica's November maintenance closure and 24/25 December stood as 14 OPERATING days for seven
months (2026-09-28: 18 wrong days in 2 of 10 parks, PAR-538).

`getScheduleExtended` reports which months were answered (a flat array loses that, G-103). A month
counts when its own month endpoint answered `200` **with at least one entry**; `[]` (an unpublished far
month), 404/5xx, `ThemeParksRateLimitError` and the generic `/schedule` endpoint never count. A naive
"delete what the payload does not name" would have removed 31 real days in the same measurement.

The DELETE has four filters: `attractionId IS NULL`; `scheduleType = 'OPERATING'` (hand-written CLOSED
corrections such as Phantasialand's `Closed per park announcement (PAR-532)` sit on dates the upstream
no longer names, and there is no provenance column to protect them otherwise); date after the park's
own today; the day not named as operating by the payload (a day the silent-park guard downgraded counts
as named). The end state belongs to `fillScheduleGaps`, run right after: a retracted day between two
operating days becomes CLOSED, one past the last becomes UNKNOWN. Writers that report a single day
(Wartezeiten, the live fallback) pass no coverage and retract nothing.

## Gap fill: `fillScheduleGaps(parkId, lookAheadDays = 182, lookBackDays = 182)`

Runs after every schedule save (both crons and `sync-park-schedule`), over park-local today − 182 to
today + 182 ([park timezone](../rules/park-timezone.md)). Only missing dates get a row, with
holiday/bridge metadata:

- **CLOSED** when the date lies strictly between the park's first and last park-level OPERATING date
  (`isGapClosed`) — a mid-week closure;
- **UNKNOWN** otherwise: no OPERATING at all, before the first, or on/after the last ("opening hours not
  yet available").
- An existing UNKNOWN that is now between two operating days is promoted to CLOSED; a gap-filled CLOSED
  (`description = 'Gap-filled'`, no hours) now after the last operating day is demoted to UNKNOWN. An
  OPERATING row or an API CLOSED row is never overwritten.

It first runs `cleanupDuplicateScheduleEntriesForPark` (same-type and cross-type duplicates, keyed with
`attractionId`; statements in `src/parks/utils/schedule-dedup.sql.ts`), and writes in batches: one
INSERT for all missing dates, one UPDATE each for promotions and demotions, holiday-metadata updates one
by one.

## The calendar endpoint

`GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=`,
`CalendarService.buildCalendarResponse()`.

- **Cache:** per month only, `calendar:month:{parkId}:{YYYY-MM}:{includeHourly}`; TTLs and slicing in
  [caching](caching-strategy.md#keys-and-ttls).
- **One cold build** fetches schedule, weather (`getWeatherData(parkId, from, to, timezone)`), daily ML,
  holidays, refurbishments, historical queue data and daily stats in one `Promise.all`; hourly ML once
  for today and tomorrow; past crowd levels with one `MGET` of `analytics:crowdlevel:park:{id}:{date}`,
  computing only misses. A cold build can take ~15 s, which is why the warmup exists.
- **Warmup:** what `warmup-calendar-daily` builds: [cache warmup](caching-strategy.md#cache-warmup).
- **Indexes it relies on:** `schedule_entries (parkId, date, scheduleType)` and the partial
  `idx_schedule_park_date_no_attraction`, `weather_data (parkId, date)` (its primary key),
  `holidays (country, date)`, `park_daily_stats (parkId, date)`, `queue_data (attractionId, timestamp)`.
