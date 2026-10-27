# `/plan/day`: tiers, sources and `dayPeak`

How the numbers in a `/plan/day` response were produced: the response's `tier`,
the `climatology` look-back, the per-hour `source` on a day that is part measured
and part composed, what `dayPeak` means on each tier, and opening hours taken from
the data. Part of the [`/plan/day` contract](./README.md).

## `tier`

Nothing upstream answers "Taron's queue at 14:00 on 17 October". Several things
exist at different resolutions and reaches, so every response says which produced
it, and a caller must render them differently.

| tier          | what it is                                                                                         | when                                                                 |
| ------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `observed`    | what the queues **actually did**, from the nightly 15-minute rollup (`attraction_hourly_history`)  | a date in the past                                                   |
| `measured`    | the model's own hourly prediction                                                                  | at least one served hour of it — the hourly model reaches **48 h** ahead |
| `composed`    | a day-level prediction scaled by the ride's historical hour shape                                  | within the daily horizon                                             |
| `climatology` | **not a forecast**: the median of comparable days a year earlier (below)                           | past the park's schedule coverage, with ≥ 4 comparable days measured |
| `long_range`  | no day level and too few comparable days, so **no curves**                                         | past the park's schedule coverage                                    |

**The tier is derived from the curves that were served, never from the distance.**
Deciding it by distance sent a day whose hourly rows never arrived (an ML hiccup,
a skipped park) out as composed data under the `measured` label. "Served" is
stricter than "offered": a ride can carry the model's hourly answer and still end
with an empty `hours` when every modelled hour lies before its own opening
([opensAt](./opens-at.md)), and such a response is not `measured`.

There is no fixed daily-horizon constant. The daily forecast (`ml-service/predict.py`)
walks the park's schedule, so it ends where the operator's published calendar
does — 181 to 362 days across the live parks when measured (average 193).

### `climatology`: how it was last year

Past the schedule the model has nothing to say, so the endpoint looks back. **A
retrospective, never a prediction**, and the response says so three times:
`tier: "climatology"`; `climatology.label: "how_it_was_last_year"` (the wording
is the frontend's, the meaning is fixed); `accuracy.basis: "unmeasured"` with no
`expectedError`, `uncertaintyMinutes` or `leadTimeMae`.

- **Reference days.** The same weekday 52 weeks back, up to three weeks either
  side (seven dates), and the same again 104 weeks back
  (`CLIMATOLOGY_WEEKS_EITHER_SIDE`, `CLIMATOLOGY_YEARS_BACK`). A date counts only
  in the **same holiday situation** as the planned day, read from the same
  calendar for both: `holiday` (public holiday or bridge day), `school_vacation`,
  `regular`, or `school_unknown` (past the park's school-holiday data; matches
  only its own kind, PAR-1049). A planned day the calendar has no row for is not
  tried. Returned in `climatology.referenceDates` and `climatology.holidayState`.
- **The curve.** Per ride and hour, the median over the reference days of each
  day's sample-weighted mean (the per-day number `observed` serves). `dayPeak` is
  the median of the days' P90 peaks; `sampleDays` the number of reference days the
  ride was measured on.
- **Floor: 4 days** (`CLIMATOLOGY_MIN_DAYS`, echoed as
  `climatology.minObservationDays`), per ride and per hour. Below it the ride or
  hour is left out; with no ride left the day stays `long_range`. Chosen by
  measurement (2026-10-02, 3,479 held-out ride-days): the median of k neighbours
  has MAE 6.93 / 6.21 / 5.86 / 5.60 / 5.45 / 5.32 min for k = 1…6, so the fourth
  day still takes 4.4 % off and each later one under 3 %. This is about choosing
  the floor, **not** an accuracy figure for the tier.
- **The season rule wins.** The tier is built from the same ride list as
  `composed`, after the season and works-period filters
  ([availability](./availability.md)).
- **Reach.** `attraction_hourly_history` begins 2025-12-24, so the tier can answer
  from about 2026-12-23 on; the 104-week read is empty until a second year exists.

## A day is often part measured and part composed

The hourly forecast covers **now → now + 48 h** and nothing more: today has no
measured hours before the current one, tomorrow is covered end to end, the day
after has none past the current clock hour. Those hours are filled from the
composed curve and each says so:

```jsonc
"hours": [
  { "hour": 9,  "wait": 45 },
  { "hour": 19, "wait": 55, "source": "composed" }
]
```

`source` is present **only where the hour did not come from the response's
`tier`**; a fully measured or fully composed day carries none. (Before, those
hours were simply missing — tomorrow's Disneyland Paris plan stopped at 17:00 and
`dayPeak` was the maximum of what was left.)

## `dayPeak` is the same statistic on every tier

`dayPeak` is the **day's peak wait**: the day-level prediction on a forecast day,
the realised day-P90 on an observed one — the pair the calendar scores against
each other. It is deliberately **not** the maximum of `hours`, which are
typical-hour numbers; taking their maximum made one ride read 20 min today and 42
five days out with the whole difference being the statistic. Past and future days
carry the same statistic where they share a field
([crowd-level rule](../../rules/crowd-levels.md)).

For a chart: with `composer: "peak_scaled"` on the composed tier the curve's
maximum equals `dayPeak` by construction; with the H5 composer and on the
measured and observed tiers, `dayPeak` normally sits **above** the curve.

## Opening hours can come from the data

`context.openHour` / `closeHour` are the park's published day and
`context.hoursSource` is `"schedule"`. Past the operator's publishing horizon
(of 177 parks with published hours, 91 reach 60 days ahead and 38 reach 120) the
endpoint falls back to the hours this park's queues were measured in over the
last year, labelled `"observed"`.

That window is **narrower than the gates' hours, never wider** — it is where we
have readings, not when the park opens. Present it as approximate, and do not
read `status` as a promise the park is open. Nothing is invented for a day the
operator **stated** as closed, or for a park nobody has watched: those answer
`openHour: null` and `rides: []`. On a day past midnight the fallback takes the
widest silence on the 24-hour clock rather than min-to-max — see
[midnight](./midnight.md).
