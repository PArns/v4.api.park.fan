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

**No feed publishes showtimes ahead of the current day.** That was checked at the
source, not assumed: ThemeParks.wiki's live response for Europa-Park carries 186
start times for today and, beyond that, only entries it never cleared — some
dated 2022. Across every park in the database, not one holds a park-local
showtime for a future day. So for any planned date the choice is to project or to
say nothing, and saying nothing made `shows` an empty array on every request.

| source | what it is |
| --- | --- |
| `scheduled` | the operator's own times for that day. Today only |
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
park's shows — a different statement from "this park has no shows".

## Related

- [Ride P50/P90 stats](./ride-typical-waits.md) — the typical/busy peak pair on the attraction detail endpoint
- [The park's day shape](./park-hourly-profile.md) — the historical hour profile this composes with
- [Calendar: status](./calendar-schedule-status.md) — how the day's own context is built
- [Quantile serving & calibration](../ml/quantile-serving-and-calibration.md) — where `uncertaintyMinutes` comes from
