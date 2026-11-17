# Display consistency (PAR-1381)

A park-day is shown in many places, each with its own read path: the crowd calendar's cell and day
dialog, `/best-days`, the trip planner's `/plan/day`, the trip-region calendar, the park payload's live
level, the park lists, Parky's park card and his chat tools, and park.fan's relays, through which a
visitor's page reads the calendar and best-days. The display-consistency check reads one park's days
through every one of those paths at one moment and reports where two of them show different values
although the code makes them the same statistic. Open this page to read a result, or before you add a
surface that shows a park-day's crowd level or wait.

It exists because visitors saw two contradictions in three days: on 2026-11-15 at Europa-Park the today
panel read "Andrang jetzt hoch" and the day dialog "Normal", and on 2026-11-17 at Phantasialand Parky's
tip said "hoher Andrang, lieber nicht" above a day reading "sehr niedrig, 20 Min", the day PAR-1375
changed the park's season level. Both lived in copies this API does not hold: park.fan's relays at
Vercel and Cloudflare and the browser's cache, each filled at its own time (park.fan's
`docs/architecture/caching-strategy.md#one-park-day-on-several-surfaces`). So besides the API's own read
paths the check reads the relays as a visitor's page asks for them ([the relays](#the-relays)).

## What it reads

Every value comes from the method the endpoint or tool itself calls, never from a copy of its logic.
Today, tomorrow and the seven days after by default (`days`, 1–14), park-local.

| Surface                 | Read through                                                                                                                                           | What it shows                                                                                 |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| `calendar`              | `CalendarService.buildCalendarResponse(…, "none")`, the variant the grid, the day dialog, the month summary, `/plan/day` and Parky read                | `crowdLevel`, `headlinerForecast.avgWait` and each listed ride's `waitTime`                   |
| `calendar_so_far`       | the same response                                                                                                                                      | `todayCrowdLevel`, today measured so far                                                      |
| `best_days`             | `BestDaysService.getBestDays`                                                                                                                          | `crowdLevel` of the snapshot, with its `computedAt`                                           |
| `calendar_relay`        | park.fan's `/api/parks/<geo>/calendar?from=&to=`, a whole month as the calendar grid asks for it ([the relays](#the-relays))                           | `crowdLevel`, `headlinerForecast.avgWait`, with the edge copy's `copy`                        |
| `today_relay`           | the same relay with `from` and `to` both today, the day dialog the today panel opens                                                                   | today's `crowdLevel` and `headlinerForecast.avgWait`, with the edge copy's `copy`             |
| `best_days_relay`       | park.fan's `/api/parks/<geo>/best-days`, what „Prognose heute" and the best-days section read                                                          | `crowdLevel`, with the snapshot's `computedAt` and the edge copy's `copy`                     |
| `plan_day`              | `cachedPlanDay`, the endpoint's own 15-minute cache                                                                                                    | `context.crowdLevel`, each headliner's `dayPeak` and their mean                               |
| `trip_region`           | `TripRegionService.calendar`, for a park `regionOfPark` places in a region                                                                             | the cell's `crowdLevel` on a forecast day (`basis` forecast or long_range) and `forecastWait` |
| `trip_region_last_year` | the same                                                                                                                                               | the cell's `crowdLevel` where `basis` is last_year                                            |
| `park_live`             | `ParkIntegrationService.buildIntegratedResponse`, while the payload says OPERATING                                                                     | `analytics.statistics.crowdLevel`, with its `timestamp`                                       |
| `park_lists_live`       | `AnalyticsService.getBatchParkOccupancy` (`park:occupancy:{id}`), what discovery, favourites, nearby and search read, while the payload says OPERATING | `crowdLevel`, with its `updatedAt`                                                            |
| `parky_live`            | Parky's `get_park_live`, run as the chat runs it                                                                                                       | `crowd.level`                                                                                 |
| `parky_today_expected`  | the same answer                                                                                                                                        | `todayExpected.crowdLevel` and `expectedPeakWaitMinutes`                                      |
| `parky_get_days`        | Parky's `get_days`                                                                                                                                     | `crowdLevel`, `expectedPeakWaitMinutes`                                                       |
| `parky_get_park_day`    | Parky's `get_park_day`, which builds the day without the `/plan/day` cache                                                                             | `crowdLevel`                                                                                  |

The reads touch only the caches those endpoints and relays fill on a miss; nothing else is written.
Parky's tools run in English.

Parky's day tip is not read. Its stamp comes from the calendar day, and a tip is kept under the levels
of every day it reads (PAR-1384), so a tip read here can only repeat the calendar read beside it. Where a
tip and its day dialog disagree is in park.fan's copies, and the tip's relay is not probed (below).

## The pairs and their rule

`SURFACE_PAIRS` in `src/monitoring/display-consistency/display-consistency.rules.ts`. A pair is
**must be equal** only where the code makes both sides the same statistic; a difference there is a
mismatch. A **may differ** pair carries two statistics and is reported under `differences`, beside the
mismatches and never counted with them.

| Pair                                                                 | Measure        | Rule       | Why                                                                                                                                                                                            |
| -------------------------------------------------------------------- | -------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `calendar` · `best_days`                                             | crowd level    | equal      | the snapshot is `calendar.days.map(projectDay)`                                                                                                                                                |
| `calendar` · `plan_day`                                              | crowd level    | equal      | `PlanDayService.loadDay` reads the calendar day                                                                                                                                                |
| `calendar` · `trip_region`                                           | crowd level    | equal      | `buildParkDay` copies the calendar day's level on a forecast day                                                                                                                               |
| `calendar` · `parky_get_days`                                        | crowd level    | equal      | `get_days` reads the calendar (`calendarDays`, `compactDay`)                                                                                                                                   |
| `calendar` · `parky_get_park_day`                                    | crowd level    | equal      | `get_park_day` returns `/plan/day`'s context                                                                                                                                                   |
| `plan_day` · `parky_get_park_day`                                    | crowd level    | equal      | both from `buildPlanDay`; they differ only by the `/plan/day` cache                                                                                                                            |
| `calendar` · `parky_today_expected`                                  | crowd level    | equal      | `todayExpected` is today's calendar day (`parkLiveSummary`)                                                                                                                                    |
| `park_live` · `park_lists_live`                                      | crowd level    | equal      | both `calculateParkOccupancy`'s gated `crowdLevel`; the payload through `getParkStatistics`' 5-minute cache, the lists through `park:occupancy:{id}`                                           |
| `park_live` · `parky_live`                                           | crowd level    | equal      | `get_park_live` reads the same payload                                                                                                                                                         |
| `calendar` · `calendar_relay`                                        | crowd level    | equal      | the relay passes `/calendar` on unchanged; a difference is the age of the edge copy it was served from                                                                                         |
| `calendar` · `today_relay`                                           | crowd level    | equal      | the same, from the today panel's own edge copy of today                                                                                                                                       |
| `calendar_relay` · `best_days_relay`                                 | crowd level    | equal      | the grid and its day dialog beside „Prognose heute" on one page, each from its own edge copy of the same days                                                                                  |
| `calendar` · `parky_get_days`, `parky_today_expected`, `trip_region` | headliner wait | equal      | `headlinerForecast.avgWait`, passed on as `expectedPeakWaitMinutes` and `forecastWait`                                                                                                         |
| `calendar` · `calendar_relay`                                        | headliner wait | equal      | the same day, passed on unchanged                                                                                                                                                              |
| `calendar` · `today_relay`                                           | headliner wait | equal      | today, passed on unchanged                                                                                                                                                                     |
| `calendar` · `plan_day`, per headliner                               | ride wait      | equal      | the same served daily row (`getServingDailyPredictions`); compared on the calendar's 5-minute grid, because `dayPeak` is not rounded                                                           |
| `calendar` · `plan_day`                                              | headliner wait | may differ | the calendar averages every headliner with a daily row; the `dayPeak` mean covers only the headliners `/plan/day` draws a curve for, and takes a curve's maximum where a ride has no daily row |
| `calendar` · `park_live`                                             | crowd level    | may differ | the day's forecast against the typical-day peak, and the live level against P50 right now                                                                                                      |
| `calendar` · `calendar_so_far`                                       | crowd level    | may differ | the whole day's forecast, and today's measured day value so far                                                                                                                                |
| `park_live` · `calendar_so_far`                                      | crowd level    | may differ | now against P50, and the day so far against the typical-day peak                                                                                                                               |
| `calendar` · `trip_region_last_year`                                 | crowd level    | may differ | the forecast, and the matching day a year back as measured (PAR-1377)                                                                                                                          |

The region calendar's `headlinerWait` (the forecast times its lead factor) is not read: it differs from
the calendar's wait on every forecast day by design.

**An absent value is never a mismatch.** A surface that shows nothing (null, absent, `unknown`) says
nothing, so a pair with such a side is not compared ([absent facts](../rules/absent-facts.md)). A
`closed` against a level is compared and is a mismatch.

## The relays

A visitor's page never asks this API for a calendar day. It asks park.fan's relays, which pass the
API's answer on and are kept at Vercel until [the forecast ring](../architecture/caching-strategy.md#the-forecast-ring)
and for an hour at Cloudflare. The check asks the same URLs (`display-consistency.relays.ts`), so it is
served the copy a visitor would be:

- **`/api/parks/<geo>/calendar?from=<first>&to=<last>`**, one per month the checked days fall in, first
  to last day: the URL the calendar grid asks for, byte for byte, since the query string is part of the
  edge's cache key.
- **`/api/parks/<geo>/calendar?from=<today>&to=<today>`**, today alone: the URL the today panel asks
  for the day dialog it opens (park.fan's `components/parks/park-today-panel.tsx` through
  `useCalendarData`), an edge copy of its own beside the month's.
- **`/api/parks/<geo>/best-days`**, the URL „Prognose heute", the best-days section and the weekday
  hover read.

A request sends the park page as `Referer`, because the relays answer park.fan's own pages only, and
`User-Agent: api.park.fan-display-consistency`.

**Prerequisites.** Both are settings outside this repo, and without either every run reports each
park's relays as failures, never a clean result:

- **A Cloudflare skip rule** on park.fan for requests with `User-Agent: api.park.fan-display-consistency`
  to `/api/parks/*/calendar` and `/api/parks/*/best-days` from the API server. park.fan's Cloudflare
  challenges clients that are not browsers, as it would Parky's post reads without theirs
  ([assistant](../frontend/assistant.md)); a challenge is read as a failure (`cf-mitigated`).
- **`FRONTEND_URL` set to the relays' origin**, `https://park.fan` in production. The base of every
  relay URL and of the `Referer` is `getFrontendUrl()`: `FRONTEND_URL`, else the origin of
  `REVALIDATE_URL`. A relay answers only a `Referer` of its own origin (`isSameOriginRequest` in
  the frontend) and a 403 otherwise, so a base that redirects there (`http://`, `www.`) sends a
  `Referer` the relay refuses.

How a run reads them:

- **A relay that cannot be read is a failure, never a mismatch:** no answer within 10 seconds, a
  Cloudflare challenge (`cf-mitigated`), a status other than 200, a body that is not JSON with `days`.
- **Each relay value says how old its copy is.** `copy` carries the response's `age`, `date`,
  `cf-cache-status` (`cloudflare`) and `x-vercel-cache` (`vercel`) where it sent them; a best-days value
  also has `asOf`, its snapshot's `computedAt`. A mismatch shows them on the relay's side.
- **Only where this API rings the frontend** (`REVALIDATE_SECRET` set). Elsewhere, in dev and CI, the
  relays carry another API's figures, so none is read and none is listed as failing.
- **What it cannot see:** the copy at another Cloudflare data centre or Vercel region than the ones
  nearest the API server, and the browser's own cache (an hour for a calendar month, 30 minutes for
  best-days).
- **Not read:** Parky's tip relay (`/parky-tip`), because a miss there asks the model for a sentence, a
  cost per park, day and language; and `/calendar/hourly`.

A miss fills the edge copy a visitor would have filled: one function call at Vercel and one read of
this API. Per park the check sends one calendar request per month the days fall in (one, two where they
cross a month's end), one for today alone and one best-days request, three or four at once; parks go
one after the other. A run over N parks sends at most 4N requests to park.fan.

## Reading a mismatch

Each mismatch names the day, the measure, both surfaces with their values, the statistic they share
and, where a surface says when it computed its value, that time (`asOf`). Two values of one statistic
computed at different times are a cache window (PAR-1380 owns those); two computed at the same time are
two computations, and that is a bug in one read path. A relay's side also says how long the edge had
kept its copy (`copy`, [above](#the-relays)). A surface that could not be read is listed under
`failures` and its pairs are left out.

Differences the check does not count, by design:

- **A day past the published schedule.** The region calendar turns the calendar's inferred `CLOSED`
  into `UNKNOWN` ("nobody said so", `buildParkDay`); its level is then `unknown` or last year's.
- **A closed park's live level.** The park payload's closed branch writes `very_low`, the lists and
  Parky `unknown` or nothing; the check reads no live level while the payload is not OPERATING.

## Where it runs

- **On demand:** `GET /v1/admin/display-consistency/check?park=<id|slug|name>&days=9` (viewer) and the
  MCP tool `display_consistency` with `park`.
- **Twice a day:** `analytics` / `check-display-consistency` (one cron, `40 9,23 * * *` UTC).
  - **09:40**, after the 08:00 calendar warmup has published the night's forecasts into the calendar
    and `/best-days` caches and rung the relays: the moment every copy should agree, so a mismatch
    then is a read path or a ring that failed.
  - **23:40**, after midnight in Europe (00:40 CET, 01:40 CEST): park-local today has moved on while
    the relays still serve the copies made after the 20:00 warmup's ring, the evening before.

  It checks the parks with a season of the season level running on their own today
  (`parksWithRunningSeason`, the definition `loadSeasonLevelInputs` reads) and the 25 most read park
  pages of the last seven days (`PageViewsService.adminOverview`), one park after the other. It logs one
  WARN naming the parks with mismatches or unreadable surfaces (a park whose check threw counts as one),
  or saying that no park was checked; the quiet line only when every park was read whole and agrees.
  Counting mismatches alone would let a run in which every read failed pass as clean, since a surface
  that cannot be read compares nothing.

- **Kept:** each park's result under `display-consistency:v1:park:{parkId}` and the run's summary
  (`mismatches`, `failures`, and per park both and a thrown check's `error`) under
  `display-consistency:v1:summary`, two days each; the next run overwrites them.
  `GET /v1/admin/display-consistency`, the MCP tool without `park`, and `displayConsistency` in the
  data-quality report (`GET /v1/admin/data-quality`, `data_quality`) read the last run.

A park costs one calendar read, one best-days read, the park payload, Parky's live and days tools, per
day a cached `/plan/day` plus an uncached `get_park_day` build, three days at a time, and three or four
relay requests.
