# `/plan/day`: rides that cannot open, and a day with no rides

Why a ride is missing from `rides[]` (its season or a hand-written works period
excludes the planned day) and why a whole day can come back with `rides: []`
(`ridesUnavailable.reason`), including the job that watches that number. Part of
the [`/plan/day` contract](./README.md).

## A ride out of season is absent, not closed

`rides[]` carries only rides that can open on the day asked about. A seasonal ride
whose season excludes that date is left out entirely — no flat curve, no flag.
Same rule and same three-valued source as the park page's "12 von 45 geöffnet"
counter: `isCurrentlyInSeason` / `attractionIsOutOfSeason`, where `false` closes,
`true` opens and `null` changes nothing (see [seasonality](../seasonality.md)).

- **The month is the planned day's**, not today's.
- **A past date is not filtered.** `observed` reads the rollup, and a row there
  means the ride ran.
- **A ride flagged by the detector without months (`season_out_since`) drops out
  only for today and tomorrow.** That note is a reading of the current state
  (closed on 7 park-open days in a 60-day window, cleared by the next OPERATING
  row) and a three-week refurbishment sets it as well as a winter does; past
  tomorrow only months decide, and a ride with none stays. Far-date plans are
  therefore only cleaned where months exist — the detector writes none before
  `MIN_OBSERVED_DAYS` (330) of history, so on younger parks that needs curated
  `Betriebsmonate` under `/admin/attractions/<id>`.

**A live `OPERATING` row overrules the season**, as on the park page (a
`detect-seasonal` outage has gone unnoticed for 73 days before). It costs 0 or 1
extra query, run inside the existing parallel batch, and is skipped unless it could
change something: past tomorrow, park CLOSED for the day, or nothing excluded by
the season. Only an explicit `OPERATING` reading rescues a ride, so a feed that only
writes CLOSED and reverse-reconciliation's stamps both fail.

- The reading counts back to **the day's own opening and never less than six
  hours** (`LIVE_STATUS_FLOOR_HOURS`); with no published opening the six hours are
  the window. A flat interval would rescue a ride off yesterday evening, local
  midnight off last night's session on a park past midnight.
- **Today and tomorrow lift different things.** The row is today's either way. The
  monthless note blocks both days, so a ride running today is in tomorrow's plan
  too; months do not travel (tomorrow may be the first day outside the season), so
  tomorrow's lookup only asks about rides the note alone blocks, with the six-hour
  floor as its window.

Nothing upstream does this: `MLService.getParkPredictions` keeps rides with an
OPERATING reading in the last 90 days, a question about the past asked for a
future date.

## A ride inside a hand-written works period is absent too

The season is **detected**; a works period is **written** — under
`/admin/attractions/<id>` as `curated_out_of_service_from` / `_to`, park-local,
**both bounds inclusive**, either bound alone allowed. It is the stronger statement
and needs none of the season's caution.

- **Asked about the planned day** (`isCuratedOutOfService(…, dateStr)`), not about
  today, which is the helper's default.
- **Not applied to a past date**, like the season. The outage reconstruction
  (`src/analytics/utils/outage-reconstruction.sql.ts`) does apply it to history,
  but there it suppresses an outage inferred from silence; a rollup row is the
  opposite of silence, and a window covering a day the ride queued wants
  correcting, not hiding.
- **A live reading does not overrule it.** The live exception is the season's
  alone.

The window itself is not repeated here; the client reads `worksPeriod` on the park
and attraction payloads to say why a ride is missing — see
[works-period](../works-period.md).

## A day with no rides says why (`ridesUnavailable`)

Every empty `rides` carries `ridesUnavailable.reason`, and a non-empty one never
does — test the field, not the list length. Without it, "this park publishes no
wait times", "the feed stopped in June", "not enough readings yet" and "something
broke" all looked alike, and a client printed an invented number or an error for a
park that needed neither. Measured 2026-09-14 for 2026-10-14 (lead 30): 19 of 73
open parks (26 %) came back empty, for six different reasons (PAR-194).

Reasons (`PlanDayUnavailableReason`, `src/parks/utils/plan-day-availability.util.ts`):

| reason                 | kind                    | means                                                                                                                                       |
| ---------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `park_closed`          | park/day fact           | the operator says the park is shut that day                                                                                                 |
| `no_rides_on_file`     | park fact               | no ride in the catalogue (Discovery Cove)                                                                                                   |
| `no_wait_time_source`  | park fact, **permanent** | the park publishes wait times nowhere readable — present it as an answer, not a gap ([live-wait-times-availability](../live-wait-times-availability.md)) |
| `rides_cannot_open`    | park fact               | every ride's season or works window excludes the day                                                                                        |
| `never_measured`       | gap                     | rides on file, not one qualifying reading ever                                                                                              |
| `feed_stale`           | **fault**               | readings stopped ≥ 30 days ago (`FEED_STALE_DAYS`) while the park still schedules days; carries `staleDays`                                 |
| `hours_unknown`        | gap                     | neither a published nor an observed window, with both sources answering                                                                     |
| `insufficient_history` | gap                     | readings arrive, no ride reaches the profile's 20-measured-day floor                                                                        |
| `no_hourly_shape`      | gap                     | rides clear that floor but no hour is carried by enough of them to be a column (`visibleHours`) — Knott's Berry Farm                        |
| `no_forecast`          | gap                     | shapes exist, no day level for this date                                                                                                    |
| `no_observations`      | gap                     | a past date whose 15-minute rollup holds nothing for the park                                                                               |
| `data_unavailable`     | **unmeasured**          | a dependency failed, so the cause is unknown; sent `no-store`                                                                               |

**The order is about evidence** (`classifyPlanDayUnavailable`): the park's and the
day's own facts; then the feed; then whether the question could be asked; then what
we have about the rides. Two placements were each bought with a finding:

- `rides_cannot_open` is tested **after** the feed: `season_out_since` is written by
  a detector that reads the feed, so on a park silent since June "out of season" is
  our bookkeeping, not the operator's word ([absent facts](../../rules/absent-facts.md)).
- `data_unavailable` exists because seven inputs (calendar, hourly profile, daily
  forecast, hourly forecast, rollup, live-status lookup, feed recency) each degrade
  to something that reads as an answer. The **live-status** failure is tested
  directly above `rides_cannot_open`, because it is the only one that can empty the
  plannable set; the rest are tested below it.

### The one query this costs

Feed recency is the only fact not already in hand when the list turns out empty, so
it runs only in that branch, in two steps: a 30-day-bounded aggregate settles the
common case (35 ms at Knott's against 185 ms unbounded, 2026-09-14), and only a
silent park pays the full scan. It is memoised per park for five minutes (a failed
statement is not), and it reads `queue_data`, not `queue_data_aggregates` — the
rollup keeps only hours with three readings, so Peppa Pig Theme Park and Aquatica
Orlando had no aggregate row while their feeds delivered thousands of readings.

### The number is watched

The `plan-day-coverage` job (`stats` queue, 09:00 UTC, after the night's
aggregation and the day-level forecasts) writes one `plan_day_coverage` row per open
park and day — planned date, lead, ride count, reason — by calling the real
`buildPlanDay`, never a SQL re-derivation. `isStructuralPlanDayReason`
(`park_closed`, `no_rides_on_file`, `no_wait_time_source`, `rides_cannot_open`)
separates the parks that will never plan from the gaps that close, so the watched
number can reach zero; `isUnknownPlanDayReason` keeps `data_unavailable` out of both
halves. `parksOpen` is counted from the schedule, not from the rows written, so a
systematic outage cannot read as improving coverage.
