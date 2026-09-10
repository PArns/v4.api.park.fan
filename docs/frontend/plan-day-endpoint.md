# One day, ride by ride, hour by hour (`/plan/day`)

`GET /v1/parks/:continent/:country/:city/:parkSlug/plan/day?date=YYYY-MM-DD`

The series a trip planner draws: every ride's expected wait for each open hour
of one date, plus that day's own context (hours, crowd level, weather, holiday
flags). `date` defaults to today in the park's timezone; a malformed one is a
400 rather than a day of nulls that reads like a closed park.

Cached 15 minutes at the edge. There is no origin-side response cache — the
pieces underneath it (calendar month, hourly profile, ML predictions) each have
their own.

---

## 1. `tier` is the whole point

Nothing upstream answers "what will Taron's queue be at 14:00 on 17 October".
Two things exist, at different resolutions and different reaches, so every
response says which one produced it.

| tier | what it is | when |
| --- | --- | --- |
| `observed` | what the queues **actually did**, from the nightly 15-minute rollup | a date in the past |
| `measured` | the model's own hourly prediction | the day carries at least one hour of it — the model reaches **24 hours** ahead |
| `composed` | a day-level prediction scaled by the ride's historical hour shape | everything within the daily horizon |
| `long_range` | the model has produced no day level for this date, so there are **no curves** | past the park's own schedule coverage |

**The tier is derived from the curves that were built, never from the distance.**
That is not a detail: deciding it by distance meant a day whose hourly rows never
arrived — an ML service having a bad minute, a park the run skipped — went out as
composed data under the `measured` label, which is the one failure the whole
design is arranged against.

There is no fixed "daily horizon" constant. `predict.py` walks the park's
schedule, so the daily forecast ends where the operator's published calendar
does: 181 to 362 days across the live parks, averaging 193. A hard-coded 60 was
wrong for every one of them.

## 2. A day is often part measured and part composed

The hourly forecast covers **now → now + 24 h** and not one minute more. So:

- **today** has no measured hours before the current one,
- **tomorrow** has none after it.

Those hours are filled from the composed curve and each one says so:

```jsonc
"hours": [
  { "hour": 9,  "wait": 45 },
  …
  { "hour": 19, "wait": 55, "source": "composed" },
  { "hour": 22, "wait": 50, "source": "composed" }
]
```

`source` is present **only where the hour did not come from the response's
`tier`**, so a fully measured or fully composed day carries none at all.

Before this, the unreached hours were simply missing. Measured against the live
service at 17:15, tomorrow's plan for Disneyland Paris (open 09:00–22:00) stopped
at 17:00 — the evening, where a headliner peaks, silently absent, and `dayPeak`
the maximum of what was left.

## 3. `dayPeak` is the same statistic on every tier

`dayPeak` is the **day's peak wait**: the day-level prediction on a forecast day,
the realised day-P90 on an observed one — the pair the calendar already scores
against each other.

It is deliberately **not** the maximum of `hours`. Those are typical-hour numbers
(a median forecast, a measured mean), and taking their maximum made the field
mean something different on each tier: today read 20 minutes where the same ride
read 42 five days out, and the whole difference was the statistic. This is the
same rule as `claude.md` §3 — *past and future days must carry the same statistic
where they share a response field*.

Consequence for a chart: on the composed tier the curve's maximum equals
`dayPeak` by construction; on the measured and observed tiers `dayPeak` sits at
or above the curve, because a peak is not a typical hour.

## 4. Opening hours can come from the data

`context.openHour` / `closeHour` are the park's own published day, and
`context.hoursSource` says so — `"schedule"`.

Past the operator's publishing horizon there is no schedule to read. Of 177 live
parks with published hours, **91 reach 60 days ahead and 38 reach 120**, so a
summer date asked in January has none, and the endpoint used to answer with an
empty shell for exactly the distance the planner exists for. It now falls back to
the hours this park's queues have actually been measured in over the last year
and labels them `"observed"`.

That window is **narrower than the gates' hours, never wider** — it is where we
have readings, not when the park opens. A caller must present it as approximate
and must not read `status` as a promise that the park is open.

Nothing is invented for a day the operator has **stated** as closed, or for a
park nobody has watched: those still answer `openHour: null` and `rides: []`.

## 5. A day that runs past midnight

A park whose day crosses midnight publishes `closeHour < openHour`. La Ronde is
`10 → 0`, Six Flags Magic Mountain on Halloween `10 → 1`. `context.openHour` and
`context.closeHour` keep the operator's own wall-clock numbers, because that
pair is what renders as "16:00 – 01:00".

**`hours[].hour` does not wrap.** It continues past 23, so one operating day is
always one ascending series and `hours[0]` is always its first hour:

```jsonc
"context": { "openHour": 16, "closeHour": 1 },
"hours": [
  { "hour": 22, "wait": 40 },
  { "hour": 23, "wait": 35 },
  { "hour": 24, "wait": 30 },   // 00:00, the night after `date`
  { "hour": 25, "wait": 20 }    // 01:00, when the park closes
]
```

A caller that walks the window itself must unfold it the same way —
`closeHour < openHour ? closeHour + 24 : closeHour` — and must never test
`hour > closeHour`, which for `openHour: 16, closeHour: 1` is true of **every**
hour of the clock.

### What it used to answer

Nothing. Swept across all 212 parks on three dates, every wrap day came back
`status: OPERATING`, `hoursSource: schedule` and `rides: []`:

| date | wrap days | …with `rides: []` |
| --- | --- | --- |
| 2026-09-05 | 4 | 4 |
| 2026-10-31 | 13 | 13 |
| 2026-12-31 | 5 | 5 |

The control is what made it a defect rather than a property of those parks — the
**same parks** answered normally on a neighbouring day:

| park | 13 Sep | 31 Oct |
| --- | --- | --- |
| Parque Warner Madrid | `12 → 21`, 31 rides | `12 → 0`, **0 rides** |
| Cedar Point | `11 → 20`, 14 rides | `11 → 0`, **0 rides** |
| Kings Dominion | `11 → 20`, 23 rides | `11 → 0`, **0 rides** |

Three parks are wrap **every day** — La Ronde (`10 → 0`), Six Flags Mexico
(`10 → 0`) and Six Flags Qiddiya City (`16 → 0`) — so they had never carried a
single hourly curve.

### Four places had to agree about it

The visible cause was one guard (`closeHour < openHour` → return the day's
context and an empty ride list), but every loop under it would have run zero
times anyway, and two stores key the night by the wrong date:

- the **hourly predictions** are timestamps, so 00:30 of a `16 → 1` day carries
  *tomorrow's* park-local date. Matching on the day's own date dropped precisely
  the hours that make such a day unusual;
- `attraction_hourly_history` is keyed by park-local **date**, so an observed
  night is stored across two rows and is now read back from both — otherwise the
  curve ends at 23:00 and `dayPeak` belongs to whichever half was busier;
- the **historical shape** buckets by wall-clock hour, so its `0` bucket is moved
  to 24 before it is read against the day. Left where it sat, midnight sorted in
  front of the morning and dragged the whole evening down towards it;
- the **observed-hours fallback** (§4) now takes the window as the widest silence
  on the 24-hour clock rather than min-to-max. For an ordinary park those are the
  same answer; for a park measured `[0, 16 … 23]` min-to-max says `0 → 23` and
  draws a queue at 03:00.

The unfolding rule itself lives in exactly one function, `unfoldedCloseHour`
(`common/utils/day-shape.util.ts`). It was written inline twice on the frontend
and the two copies disagreed — the grid drew an axis to 01:00 while the estimator
called every hour of that day out-of-hours, so every ride returned `wait: null`
and the optimiser ordered the day by walking distance while believing it was
minimising queues.

## 6. The rest of the ride object

| field | notes |
| --- | --- |
| `uncertaintyMinutes` | half-width of the model's band, from the same row as `dayPeak`. Absent where the model reports no spread — **not** a band of width zero, and it must not be drawn as one |
| `opensAt` | when the RIDE opens, park-local `HH:mm` — see §9 |
| `expectedError` | how wrong `dayPeak` typically is, in minutes — see §10 |
| `sampleDays` | measured days behind the historical shape. `1` on an observed day, `0` for a ride the shape does not cover |
| `latitude` / `longitude` | **numbers**, not the strings TypeORM returns for a decimal column. A geodesic distance between two of them is a *lower bound* on the walk and nothing more |
| `downYesterday` | the ride was reported **DOWN** at some point in the previous operating day and was **never OPERATING** in any of it. A ride the feed called CLOSED all day is a season or a refurbishment, not a fault, and is not flagged — without the DOWN requirement this put a warning on nine of Phantasialand's winter-only and water attractions every day of the summer. Only asked for today and tomorrow |
| `isHeadliner` | the park's **curated** set, never re-derived from `dayPeak`: a headliner having a quiet Tuesday is still a headliner |

Rides are sorted busiest first (`dayPeak` descending, name as tie-break) on the
forecast tiers, and by name on an observed day.

A ride is **omitted** rather than drawn flat when there is nothing to give it a
shape, and a ride the past-day rollup has no row for is omitted rather than drawn
at zero — absence there means the rollup has not reached that day, which is not
the same statement as an empty queue.

### A ride out of season is absent, not closed

`rides[]` carries only rides that can open on the day being asked about. A
seasonal ride whose season does not cover that date is left out entirely — not
listed with a flat curve, not listed with a flag — because it is not one of the
day's rides. Same rule as the park page's "12 von 45 geöffnet" counter, and the
same three-valued source: `isCurrentlyInSeason` / `attractionIsOutOfSeason`,
where `false` closes, `true` opens, and **`null` — "seasonal, and nothing else
known" — changes nothing**. Most of the catalogue sits on `null`, because the
detector deliberately names no months under `MIN_OBSERVED_DAYS` of history.

Three things about it are easy to get wrong from the outside:

- **The month is the planned day's, not today's.** A ride with months on file is
  judged against December when December is what was asked about, whatever month
  the request arrives in.
- **A past date is not filtered.** `tier: "observed"` is read out of the hourly
  rollup, and a row there means the ride ran — an observation beats a
  description of the past.
- **A ride the detector flagged but gave no months is out on every date**, not
  just today. That branch says "shut now, and we cannot tell you when it runs",
  and there is no month in it to test a December request against. It is a known
  limit: such a ride stays out of future plans until 330 days of history give it
  months, or somebody curates them. Ignoring the branch past tomorrow would put
  most of the catalogue back into every future plan, since the detector names no
  months under `MIN_OBSERVED_DAYS` — so the loss is a minority of rides against
  the bug returning for the majority.

One thing it deliberately does not do: **overrule the season with a live
reading.** The park page does — a live `OPERATING` row means the season on file
is behind the park — and this endpoint reads no live status, so for *today* the
two can disagree about a ride whose season data has gone stale.

Nothing upstream does this. `MLService.getParkPredictions` keeps rides with an
OPERATING reading in the last 90 days, which is a question about the past asked
on behalf of a date in the future: the two windows cannot line up, in either
direction. A summer water ride still carrying August's readings in September was
given a full December forecast, and a ride whose season ended in August stayed
plannable until roughly the end of November.

## 7. `leadTimeMae`

The measured mean absolute error for predictions made this far ahead, in minutes,
from the lead-time archive (`prediction_lead_snapshots`): the nearest sampled
distance **at or below** `leadDays`, over scored headliner days. A 20-day question
is answered by the 14-day bucket, never by the 30-day one — overstating the
distance would understate the error.

Absent until that bucket has enough scored rows, which is the normal state of the
far buckets for their first weeks: the 60-day bucket says nothing until the
archive has been running 60 days. **Absent is the honest answer** — a caller
should widen the band with distance without attaching a figure rather than invent
one.

## 8. Shows

`shows` carries the day's programme, and `source` says who it came from.

**No feed publishes showtimes ahead of the current day.** That is measured, not
assumed. Over the whole retained history of `show_live_data` (snapshots from
2025-12-24 to 2026-09-08, 4,675,700 of them), all 15,185,105 showtime entries
were compared against the park-local date of the snapshot that carried them:

| lead over the snapshot's own day | entries | share |
| --- | --- | --- |
| negative — a day already past | 120,058 | 0.79 % |
| 0 days — the operating day itself | 15,064,895 | 99.21 % |
| +1 day, starting 00:00–05:30 | 109 | 0.0007 % |
| +1 or +2 days, genuine | 43 | 0.0003 % |

The first row is the feed's own litter, and it is why both SQL paths in
`ShowsService` filter on the showtime and not only on the snapshot: the oldest
entry still being served sits **1,396 days** before the snapshot carrying it —
2022, exactly as the code comments say.

The third row is not foresight either. Those are performances past midnight —
Universal's late programme, `The Purge: Dangerous Waters` at 00:45,
`Meet Snowball` at 04:30 — belonging to the operating day that published them,
which only crosses a date boundary because the clock does. Note that we do
**not** unfold them the way §5 unfolds opening hours: `getShowtimesOnDate` and
the pattern rebuild both bucket a showtime by its own park-local calendar date,
so such a performance is attributed to the following day.

That leaves 43 entries — three per million, in two parks, from snapshots taken
between 2025-12-23 and 2025-12-27 park-local, with no successor in the eight
months since. So for any future date the choice is to project or to say nothing,
and saying nothing made `shows` an empty array on every request.

| source | what it is |
| --- | --- |
| `scheduled` | the operator's own times for that day. Today and past days, never a future one |
| `projected` | what the show ran at on the most recent day with the **same weekday**, with `observedOn` and `sampleDays` beside it |

**Why the weekday matters.** Measured at Europa-Park: "Big Moments – The
Celebration-Show" runs 12:30 and 14:30 on a Thursday and 12:30, 14:30 and 17:45
on a Saturday; "Carnival in Venice" runs hourly to 18:00 midweek and to 19:00 on
Saturdays. A weekday-blind projection would either drop the extra performance or
promise it on a Tuesday.

**Why the most recent matching day, not an average over weeks.** A union across
the window merges a summer programme with an autumn one into a day that never
happened. The latest matching day is a day that did.

**Two guards stop a projection becoming a claim.** A show must have been seen on
that weekday more than once — "Crazy Summer with Ross Antony & Paul Reeves" ran
at Europa-Park on exactly one Thursday in July, and projecting it forward would
have put a concert on every remaining Thursday of the year — and it must have
been seen in the last 28 days, measured against **today** rather than against the
date asked about, or every date more than four weeks out would reject itself.

A caller must render `projected` differently from `scheduled`. It is what the
show did, not a promise that it runs; `observedOn` is there so the reader can see
how fresh that evidence is. An empty `shows` means we have never watched this
park's shows — a different statement from "this park has no shows", though see
the coverage note below for the one case where those two blur.

### How far ahead showtimes are genuinely known

**Forward, `scheduled` reaches zero days.** Backward it reaches as far as
`show_live_data` is retained: `getShowtimesOnDate` anchors its snapshot window on
the date being asked about, so a past day is answered from the snapshots taken on
it. Measured at Europa-Park on 2026-09-09 — 2026-09-06 returned 32 `scheduled`
entries, 2026-08-15 returned 34, and 2026-06-01, three months back, still
returned 29.

The one exception runs the other way, and it is the +1-day row above: a snapshot
taken today can carry a past-midnight performance dated tomorrow, so *tomorrow*
can come back with a lone `scheduled` 04:30 entry for a show whose real
programme is not known yet. Because `buildShows` prefers `scheduled` and stops
there, that orphan also suppresses the projection for that one show. 109 entries
in nine months, all at Universal — rare, but not never.

**`projected` has no distance limit at all**, and a caller should know that. The
freshness guard (`MAX_PATTERN_AGE_DAYS`) measures the pattern against *today*
rather than against the target date, and the sighting guard (`MIN_PATTERN_DAYS`)
is a plain count that carries no date at all — so nothing in the path grows
stricter as the question moves further out. Measured against production for
Europa-Park on 2026-09-09, the three future Saturdays returned an identical
answer, with today shown for contrast:

| date asked | entries | source | `observedOn` |
| --- | --- | --- | --- |
| 2026-09-09 — today, a Wednesday | 33 | 32 `scheduled`, 1 `projected` | 2026-09-02 |
| 2026-09-12 — Saturday | 35 | all `projected` | 2026-08-29 / 2026-09-05 |
| 2026-11-14 — Saturday | 35 | all `projected` | 2026-08-29 / 2026-09-05 |
| 2027-03-13 — Saturday | 35 | all `projected` | 2026-08-29 / 2026-09-05 |

A March 2027 answer is therefore neither better nor worse than next Saturday's —
it is the *same* answer, built from a September observation. `observedOn` is the
only thing that says so, which is why it travels with every projected entry and
why a UI that drops it hides the one field carrying the uncertainty. **Distance
from today is not a quality signal here; the age of `observedOn` is.** Past a
season boundary the projection carries a summer programme onto a spring date,
and nothing in the pipeline notices.

**Coverage is the other limit, and it is the bigger one.** Of 128 parks that
have shows in the database, 46 have any weekday pattern at all and 45 can
project today. The remaining 82 return `[]` for every *future* date — but that
array covers two different situations, and the DTO's "we have never watched this
park's shows" is only the first of them:

- 81 of the 82 have never published a single showtime in the whole retained
  history. For those, `[]` is exactly what the DTO says it is.
- Aquatica Orlando published showtimes from 2026-03-07 to 2026-05-03 and has
  been quiet since. It has fallen out of the 56-day `PATTERN_WINDOW_DAYS`, so
  every future date now reads identically to a park nobody ever watched — while
  a date inside that spring window still answers `scheduled` from the snapshots
  taken then.

A seasonal park therefore decays into the "never watched" answer rather than
into a "not running right now" one, and nothing in the response distinguishes
them.

Of 4,715 stored patterns, 4,403 clear `observedDays >= 2` and 4,147 clear both
guards. The two rejections are worth keeping apart: 312 patterns (6.6 %) are
single-sighting events that must never be projected, while 256 (5.4 %) are
ordinary patterns that have simply gone stale — kept because
`PATTERN_WINDOW_DAYS` is 56 days, but no longer projected because
`MAX_PATTERN_AGE_DAYS` is 28.

## 9. A ride opens later than its park

`context.openHour` is the gate. It is not when the rides start, and at some parks
the gap is most of the morning: Phantasialand opens at 09:00 and Taron, F.L.Y.,
Winja's Fear, Winja's Force, Talocan, Crazy Bats, Mystery Castle, River Quest,
Colorado Adventure and seven more run from 10:00 — sixteen rides, an hour behind
the gates.

Each ride carries `opensAt` (park-local `HH:mm`) and **its `hours` already begin
there**, so a caller clamps nothing. The field is there to be shown: somebody
arriving at rope drop needs to know which queue does not exist yet.

Absent means either "opens with the park" or "we cannot tell" — fewer than five
observed openings, or a feed that never reports the transition. Both render the
same way, and `hours` is correct in either case.

**The gap is seasonal, so the lookup is too.** Measured over a year:

| | park opens | Taron | F.L.Y. | Black Mamba |
| --- | --- | --- | --- | --- |
| Apr–Aug | 09:00 | 10:10 | 10:10 | 08:10 |
| January | 11:00 | 11:00 | 11:00 | 11:05 |

In winter, when the park opens at 11:00, **everything opens with the park** —
the delay is not a property of the ride. So neither the absolute time nor the
offset survives the season, and openings are stored keyed by the park's own
opening time that day. A summer median can never cap a winter morning at 10:00,
and a winter one can never cap July at 11:00. A park opening we have never
watched yields no answer rather than the nearest one.

Three things make the detection clean, and each was a wrong answer first:

- **The transition, not the first `OPERATING` row.** `queue_data` is written on
  change plus an hourly heartbeat, so a ride left open overnight would report its
  06:15 heartbeat as an opening. A day counts only when the ride was seen CLOSED
  earlier that day.
- **The park's hour is the floor.** Half the rides report OPERATING *before* the
  gates (08:10 for Black Mamba against a 09:00 opening): the feed carries the
  operator's system state, not whether a visitor can walk up.
- **Floored to the quarter hour.** The raw median is a detection time — a
  five-minute poller plus the feed's own lag puts a 10:00 ride at 10:10.

**There is no `closesAt` and there will not be one.** The feeds do not reliably
flip a ride to CLOSED at the end of the day — rides read OPERATING for hours after
a park shuts — so it would be a guess wearing a timestamp.

## 10. How wrong the day is

Two fields, both measured rather than asserted: `rides[].expectedError` and the
`accuracy` block on the response.

```jsonc
"accuracy": { "basis": "measured", "typicalError": 15.4, "sampleSize": 1140009 },
"rides": [
  { "attractionSlug": "taron", "dayPeak": 60, "expectedError": 23.9 },
  { "attractionSlug": "raik",  "dayPeak": 20, "expectedError": 10.9 }
]
```

**`accuracy.basis` is the planability signal.** `measured` means the forecast at
this distance has been scored against realised days. `unmeasured` means **past 60
days**, where the only model left is CatBoost and nobody has ever scored it there:
`deduplicatePredictions` rewrites a day's prediction until only the last survives,
so of its rows for days that have passed, 72,795 sit at one day out and 179 at
8–30. A day like that should be shown as indicative — never as something to plan a
morning around. `prediction_lead_snapshots` began recording forward on 2026-09-03;
its 60-day bucket reports sixty days after that.

**`expectedError` is per ride because the error has two axes**, and by a lot —
45 days of forecasts scored against the realised day-P90:

| predicted | 1 day | 7 days | 30 days | 60 days |
| --- | --- | --- | --- | --- |
| ≥ 60 min | 21.4 | 22.0 | 23.9 | 25.0 |
| 30–59 min | 12.7 | 13.5 | 15.3 | 16.6 |
| < 30 min | 8.4 | 9.1 | 10.9 | 13.0 |

One figure per day would understate a headliner by ten minutes and overstate a
small ride by four.

**It is a typical miss, not a bound.** Roughly half of days land further out, so
it reads as "give or take" and must not be drawn as a range that contains the
answer.

The band is the **predicted** level, never the realised one. Grouping by the
outcome reproduces regression to the mean and makes a well-calibrated model look
badly biased — the trap that
[long-range-forecasting.md](../ml/long-range-forecasting.md) documents, and the
reason `shape_comparisons`' bias column cannot be read at face value.

The profile is rebuilt nightly at 03:10 rather than configured: these numbers
follow the model, the season and the park set, and a constant would be right on
the day it was written and quietly wrong afterwards.

## Related

- [Ride P50/P90 stats](./ride-typical-waits.md) — the typical/busy peak pair on the attraction detail endpoint
- [The park's day shape](./park-hourly-profile.md) — the historical hour profile this composes with
- [Calendar: status](./calendar-schedule-status.md) — how the day's own context is built
- [Quantile serving & calibration](../ml/quantile-serving-and-calibration.md) — where `uncertaintyMinutes` comes from
