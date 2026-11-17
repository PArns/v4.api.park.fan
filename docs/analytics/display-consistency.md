# Display consistency (PAR-1381)

A park-day is shown in many places, each with its own read path: the crowd calendar's cell and day
dialog, `/best-days`, the trip planner's `/plan/day`, the trip-region calendar, the park payload's live
level, the park lists, Parky's park card, his chat tools and his day tip. The display-consistency check
reads one park's days through every one of those paths at one moment and reports where two of them show
different values although the code makes them the same statistic. Open this page to read a result, or
before you add a surface that shows a park-day's crowd level or wait.

It exists because visitors saw two contradictions in three days: on 2026-11-15 at Europa-Park the today
panel read "Andrang jetzt hoch" and the day dialog "Normal", and on 2026-11-17 at Phantasialand Parky's
tip said "hoher Andrang, lieber nicht" above a day reading "sehr niedrig, 20 Min". As the frontend reads
them, the first is two statistics: the live level against P50, and the day so far against the
typical-day peak ([crowd levels](../rules/crowd-levels.md)). The second is one statistic shown twice,
and the tip's 12-hour cache lets a stamp outlive the level it was written from; PAR-1375 changed
Phantasialand's season level that day.

## What it reads

Every value comes from the method the endpoint or tool itself calls, never from a copy of its logic.
Today, tomorrow and the seven days after by default (`days`, 1–14), park-local.

| Surface                 | Read through                                                                                                                                           | What it shows                                                                                                     |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| `calendar`              | `CalendarService.buildCalendarResponse(…, "none")`, the variant the grid, the day dialog, the month summary, `/plan/day` and Parky read                | `crowdLevel`, `headlinerForecast.avgWait` and each listed ride's `waitTime`, the stamp `dayVerdict` gives the day |
| `calendar_predicted`    | the same response                                                                                                                                      | `predictedCrowdLevel`, which the frontend rates the cells on                                                      |
| `calendar_so_far`       | the same response                                                                                                                                      | `todayCrowdLevel`, today measured so far                                                                          |
| `best_days`             | `BestDaysService.getBestDays`                                                                                                                          | `crowdLevel` of the snapshot, with its `computedAt`                                                               |
| `plan_day`              | `cachedPlanDay`, the endpoint's own 15-minute cache                                                                                                    | `context.crowdLevel`, each headliner's `dayPeak` and their mean                                                   |
| `trip_region`           | `TripRegionService.calendar`, for a park `regionOfPark` places in a region                                                                             | the cell's `crowdLevel` on a forecast day (`basis` forecast or long_range) and `forecastWait`                     |
| `trip_region_last_year` | the same                                                                                                                                               | the cell's `crowdLevel` where `basis` is last_year                                                                |
| `park_live`             | `ParkIntegrationService.buildIntegratedResponse`, while the payload says OPERATING                                                                     | `analytics.statistics.crowdLevel`, with its `timestamp`                                                           |
| `park_lists_live`       | `AnalyticsService.getBatchParkOccupancy` (`park:occupancy:{id}`), what discovery, favourites, nearby and search read, while the payload says OPERATING | `crowdLevel`, with its `updatedAt`                                                                                |
| `parky_live`            | Parky's `get_park_live`, run as the chat runs it                                                                                                       | `crowd.level`                                                                                                     |
| `parky_today_expected`  | the same answer                                                                                                                                        | `todayExpected.crowdLevel` and `expectedPeakWaitMinutes`                                                          |
| `parky_get_days`        | Parky's `get_days`                                                                                                                                     | `crowdLevel`, `expectedPeakWaitMinutes`                                                                           |
| `parky_get_park_day`    | Parky's `get_park_day`, which builds the day without the `/plan/day` cache                                                                             | `crowdLevel`                                                                                                      |
| `parky_tip`             | `ParkyTipService.readDay`, the read the tip is written from                                                                                            | the stamp a tip asked for now would get                                                                           |
| `parky_tip_served`      | `ParkyTipService.servedVerdicts`, the 12-hour cache, per locale, without a model call                                                                  | the stamp of each tip served now                                                                                  |

The reads touch only the caches those endpoints fill on a miss; nothing else is written. Parky's tools
run in English, and the tip's cache is read, never filled.

## The pairs and their rule

`SURFACE_PAIRS` in `src/monitoring/display-consistency/display-consistency.rules.ts`. A pair is
**must be equal** only where the code makes both sides the same statistic; a difference there is a
mismatch. A **may differ** pair carries two statistics and is reported under `differences`, beside the
mismatches and never counted with them.

| Pair                                                                 | Measure        | Rule       | Why                                                                                                                                                                                            |
| -------------------------------------------------------------------- | -------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `calendar` · `calendar_predicted`                                    | crowd level    | equal      | `buildCalendarDay` sets `crowdLevel` to the forecast on a day that is not closed; `closed` is not compared, since `predictedCrowdLevel` ignores the status                                     |
| `calendar` · `best_days`                                             | crowd level    | equal      | the snapshot is `calendar.days.map(projectDay)`                                                                                                                                                |
| `calendar` · `plan_day`                                              | crowd level    | equal      | `PlanDayService.loadDay` reads the calendar day                                                                                                                                                |
| `calendar` · `trip_region`                                           | crowd level    | equal      | `buildParkDay` copies the calendar day's level on a forecast day                                                                                                                               |
| `calendar` · `parky_get_days`                                        | crowd level    | equal      | `get_days` reads the calendar (`calendarDays`, `compactDay`)                                                                                                                                   |
| `calendar` · `parky_get_park_day`                                    | crowd level    | equal      | `get_park_day` returns `/plan/day`'s context                                                                                                                                                   |
| `plan_day` · `parky_get_park_day`                                    | crowd level    | equal      | both from `buildPlanDay`; they differ only by the `/plan/day` cache                                                                                                                            |
| `calendar` · `parky_today_expected`                                  | crowd level    | equal      | `todayExpected` is today's calendar day (`parkLiveSummary`)                                                                                                                                    |
| `park_live` · `park_lists_live`                                      | crowd level    | equal      | both `calculateParkOccupancy`'s gated `crowdLevel`; the payload through `getParkStatistics`' 5-minute cache, the lists through `park:occupancy:{id}`                                           |
| `park_live` · `parky_live`                                           | crowd level    | equal      | `get_park_live` reads the same payload                                                                                                                                                         |
| `calendar` · `parky_tip`                                             | stamp          | equal      | the tip's stamp is `dayVerdict` of the calendar day                                                                                                                                            |
| `parky_tip` · `parky_tip_served`                                     | stamp          | equal      | one statistic; a tip is kept under a digest of the levels of every day it reads (PAR-1384), so `servedVerdicts` reads only the tips written for what the calendar shows now                                                                                                                   |
| `calendar` · `parky_get_days`, `parky_today_expected`, `trip_region` | headliner wait | equal      | `headlinerForecast.avgWait`, passed on as `expectedPeakWaitMinutes` and `forecastWait`                                                                                                         |
| `calendar` · `plan_day`, per headliner                               | ride wait      | equal      | the same served daily row (`getServingDailyPredictions`); compared on the calendar's 5-minute grid, because `dayPeak` is not rounded                                                           |
| `calendar` · `plan_day`                                              | headliner wait | may differ | the calendar averages every headliner with a daily row; the `dayPeak` mean covers only the headliners `/plan/day` draws a curve for, and takes a curve's maximum where a ride has no daily row |
| `calendar` · `park_live`                                             | crowd level    | may differ | the day's forecast against the typical-day peak, and the live level against P50 right now                                                                                                      |
| `calendar` · `calendar_so_far`                                       | crowd level    | may differ | the whole day's forecast, and today's measured day value so far                                                                                                                                |
| `park_live` · `calendar_so_far`                                      | crowd level    | may differ | now against P50, and the day so far against the typical-day peak                                                                                                                               |
| `calendar` · `trip_region_last_year`                                 | crowd level    | may differ | the forecast, and the matching day a year back as measured (PAR-1377)                                                                                                                         |

The region calendar's `headlinerWait` (the forecast times its lead factor) is not read: it differs from
the calendar's wait on every forecast day by design.

**An absent value is never a mismatch.** A surface that shows nothing (null, absent, `unknown`) says
nothing, so a pair with such a side is not compared ([absent facts](../rules/absent-facts.md)). A
`closed` against a level is compared and is a mismatch, except on the `predictedCrowdLevel` pair.

## Reading a mismatch

Each mismatch names the day, the measure, both surfaces with their values, the statistic they share
and, where a surface says when it computed its value, that time (`asOf`). Two values of one statistic
computed at different times are a cache window (PAR-1380 owns those); two computed at the same time are
two computations, and that is a bug in one read path. A surface that could not be read is listed under
`failures` and its pairs are left out.

Differences the check does not count, by design:

- **A day past the published schedule.** The region calendar turns the calendar's inferred `CLOSED`
  into `UNKNOWN` ("nobody said so", `buildParkDay`); its level is then `unknown` or last year's.
- **A closed park's live level.** The park payload's closed branch writes `very_low`, the lists and
  Parky `unknown` or nothing; the check reads no live level while the payload is not OPERATING.

## Where it runs

- **On demand:** `GET /v1/admin/display-consistency/check?park=<id|slug|name>&days=9` (viewer) and the
  MCP tool `display_consistency` with `park`.
- **Daily:** `analytics` / `check-display-consistency` at 09:40 UTC, after the 08:00 calendar warmup has
  published the night's forecasts into the calendar and `/best-days` caches. It checks the parks with a
  season of the season level running on their own today (`parksWithRunningSeason`, the definition
  `loadSeasonLevelInputs` reads) and the 25 most read park pages of the last seven days
  (`PageViewsService.adminOverview`), one park after the other, and logs one WARN naming the parks
  with mismatches.
- **Kept:** each park's result under `display-consistency:v1:park:{parkId}` and the run's summary under
  `display-consistency:v1:summary`, two days each. `GET /v1/admin/display-consistency`, the MCP tool
  without `park`, and `displayConsistency` in the data-quality report (`GET /v1/admin/data-quality`,
  `data_quality`) read them.

A park costs one calendar read, one best-days read, the park payload, Parky's live and days tools, a
calendar read per day for the tip, and per day a cached `/plan/day` plus an uncached `get_park_day`
build, three days at a time.
