# `/plan/day`: 15-minute `slots[]` and the H5 composer (PAR-834)

The quarter-hour series on each ride, the H5 profile that composes it, when a
TFT or season level scales it, how production differs from the benchmark it was
ported from, and what H5 cannot change until its decision is judged. Open it
when reading or changing composed curves. Part of the
[`/plan/day` contract](./README.md).

## Behind a flag

`PLAN_DAY_H5_COMPOSER=true` switches it on (`.env.example`). Unset, the response
is the pre-H5 one byte for byte (pinned by a spec against the old output), plus
`composer` on composed rides. It ships off so the forward archive can score old
and new composers side by side first (below).

`PLAN_DAY_H5_MAX_LEAD_DAYS` (a whole number below 90; unset = 90) caps the lead H5 is **served**
at, so it can go on for the leads the forward archive has already decided while the longer ones
are still being measured (PAR-1331). Past the cap the old composer serves; the shadow capture
keeps the full 90 days either way.

**Production serves it since PAR-1341**: `docker-compose.production.yml` sets
`PLAN_DAY_H5_COMPOSER=true` and `PLAN_DAY_H5_MAX_LEAD_DAYS=21`, the leads decided on 2026-11-14.
The code default stays off, so a local run and the specs see the old composer unless they set it.

## What changes with the flag on

1. **Composed hours come from the H5 profile.** The old composer
   (`composeDayCurve`, `composer: "peak_scaled"`) stretched a year's hourly P50 so
   its **maximum** equals the day level; offline that is worse than a plain
   weekday-median profile from d3 on (+0.5 … +1.1 min MAE, first-hour bias
   +2.3 min). H5, the only profile that beat the weekday median at every lead, is:
   - the **first hour after the published opening**: the ride's median at that
     many quarter-hours after opening (the rope-drop ramp, aligned to opening, not
     to the clock);
   - the **last hour before the published closing**: aligned to closing;
   - in between: the hourly median by wall-clock hour, interpolated between hour
     centres onto the quarter-hours;
   - every median over the ride's **last 56 days** (`H5_WINDOW_DAYS`) from
     `attraction_hourly_history`, forward-filled inside each day's published
     window (at most 3 h, `MAX_STALENESS_MINUTES`), wait ≥ 5;
   - the median of the day's type, the country's working days or its weekend
     (`getWeekendDaysForCountry`), where that type has ≥ 3 days at the index
     (`MIN_DAYS_DAYTYPE`), else all days with ≥ 4 (`MIN_DAYS_ALL`). A public
     holiday or a school-break day is typed by its weekday
     ([known limits](#known-limits)).

   The arithmetic is pinned against the benchmark's DuckDB SQL by a golden test
   (`src/common/utils/h5-profile.util.spec.ts`, agreement to 1e-6 on dense,
   weekday/weekend, past-midnight and gap scenarios).
2. **A level is applied by its source** (`h5LevelFor`,
   `src/common/utils/h5-profile.util.ts`, fed `servedLevelSource` of the day's
   served row). Where one applies, the profile is multiplied by **level ÷ the
   ride's median daily P90 over the same 56 days** — never stretched to put its
   maximum at the level — and the rope-drop hour is **not** scaled.
   - **TFT, only where the benchmark says it pays:** a TFT day level (at most 3
     days old) **and** an ex-ante busy ride (q90 of its 56-day slots ≥ 45 min,
     `BUSY_Q90_MINUTES`) **and** a lead ≤ 2 days (`TFT_LEVEL_MAX_LEAD_DAYS`),
     `composer: "h5_tft"`. Level × H5 over all rides is worse at every lead; it
     wins only on busy rides, and only at d0–d2 on the full period (PAR-901,
     `ml-bench/results/20261009-baselines-v2/summary.md`; see
     [ml-bench results](../../ml/ml-bench/results-baselines.md#the-tft-level-on-busy-rides)).
   - **A measured season level, on every ride at every lead** (`composer: "h5_season"`,
     [below](#on-a-day-of-a-running-park-season)). A season **ratio** row
     (`tft+season-ratio`) is TFT's forecast × a ratio and takes the TFT gates
     above; where it passes them the composer is `h5_season` too.
   - **CatBoost's daily level is never applied** — it made the benchmark worse
     (+2.6 min MAE, bias −5.7).

   Everywhere else the plain profile is served (`composer: "h5"`).
3. **`slots[]` appears on every ride with a 15-minute answer**: the model's own
   15-minute predictions on measured hours, the H5 profile on composed ones.
4. **A composed H5 hour in `hours[]` is the mean of its slots**, rounded to 5, so
   the two series agree. A measured hour is unchanged. Exception: a park closing
   on the hour (18:00) has no slot in its closing hour, so that hour carries the
   last slot's value and no slot.
5. **More rides are composed.** The old profile needed 20 measured days and 10 per
   hour over a year; H5 needs 3–4 days per index in the last 56, so rides new this
   season or back from a long closure get a curve. A curve never starts earlier
   than before.

## What does not change

- **The tier ladder.** A ride is composed only where it has a day level, as
  before. H5 is used up to **90 days** out (`H5_MAX_LEAD_DAYS`, the farthest lead
  scored); beyond that, and for a ride with no H5 profile, the old composer stays
  (`composer: "peak_scaled"`). A failed profile read falls back the same way for
  every ride and is logged as an infrastructure error.
- **`dayPeak`** stays the P90-like day level. An H5 curve is a median profile, so
  its top quarter-hour normally sits **below** `dayPeak` — see
  [tiers](./tiers.md#daypeak-is-the-same-statistic-on-every-tier).
- **`sampleDays`** stays the year's measured days whichever composer drew the
  curve, because park.fan's `rideFloor` soft floor (≥ 30 days) reads it; the H5
  window's day count is in **`profileDays`**, present on H5 rides.

## On a day of a running park season

On a day the [season level](../../ml/season-level.md) serves, the day's row is
`tft+season` or `catboost+season` (a measured level, a floor included) or, with
`SEASON_LEVEL_MODE=ratio`, `tft+season-ratio`. Its level is the one the calendar
shows for that day and the one `dayPeak` carries. The H5 composer scales by it
like by a TFT level (ratio to the ride's 56-day reference, rope-drop hour
unscaled), and `h5LevelFor` routes it by what it is:

- **A measured level** (`+season`): on every ride and at every lead H5 is
  served to (`PLAN_DAY_H5_MAX_LEAD_DAYS`).
- **A ratio row** (`+season-ratio`): through the TFT gates, a busy ride
  (`BUSY_Q90_MINUTES`) at a lead of 0–2 days (`TFT_LEVEL_MAX_LEAD_DAYS`), the
  plain profile elsewhere.

Why neither TFT gate for a measured level: both were measured on TFT's
forecast, and past d2, or on a ride that is not busy, its error cost more than
the level added. A measured level is no forecast. It is the median of the
ride's measured daily P90 on this season's days of the same day type, or of a
quieter type as a floor, the same number three days out as three weeks out,
and the same kind of number as the profile's own reference. It replaces the
model's number because the history behind the models misses the season, and
H5's 56-day window is that history: on the first days of Phantasialand's
Wintertraum nearly all of it lies before the season. Served plain, the curve
shows those weeks while the calendar beside it shows the season: two answers
for one park-day.

A ratio row is a forecast: TFT's number for the day times what the ride
measured over TFT's earlier forecasts. It carries TFT's error at every lead,
which is what the gates were measured on, so they hold for it, and nothing has
measured it against the plain profile either. Past the gates the curve is the
plain profile while `dayPeak` is the scaled level, as on a TFT day past d2.

The season level's day types carry what H5 cannot: a public holiday or a
school-break weekday of the park's own region is a day off for the season
level, so on such a day the working-day profile is scaled to the season's
day-off level. The shape stays the working day's ([known limits](#known-limits)).

Nothing has measured either against the plain profile yet. The forward archive
scores them from the first capture: the served slots are coded `e`
(`composed_h5_season`) and filed under `level_season`, both kinds alike, and the
plain profile of the same ride-days is the `shadow_h5` row (see
[the composer A/B](#the-composer-ab-in-the-forward-archive)).

## Opening hours nobody published

On a day with `hoursSource: "observed"` there is no published opening to align a
ramp to, so **nothing is edge-aligned**: every slot is the interpolated hourly
median (scaled where a level applies, first hour included). The opening-aligned
gain was measured where schedules were published and is not claimed elsewhere.

## Where production differs from the benchmark

The arithmetic is the benchmark's; the **input** is not. The benchmark reads
`queue_data`, production the nightly rollup `attraction_hourly_history`, because
56 days of `queue_data` per park is the compressed hypertable. What that costs:

- **Breakdowns are filled over.** The rollup keeps only OPERATING/STANDBY rows
  with wait ≥ 5 (`computeParkHourlyHistoryForDate`) and no DOWN times, so a
  breakdown is filled with the last operating wait for up to three hours.
- **A quarter-hour's value is its readings' mean, rounded to 5** (the rollup's
  `avgWait`); the benchmark takes the reading in force at the slot's midpoint.
- **Yesterday's small hours are missing** for a park past midnight until the next
  night's rollup (one day of 56).
- **The TFT level is an integer** (`getTftDailyPredictions` rounds
  `predicted_peak`); the benchmark scales by the stored float.
- **The weekend is the country's** (`getWeekendDaysForCountry`, as for the
  season level); the benchmark splits Saturday and Sunday for every park. The
  two differ only in a country with another weekend, such as Saudi Arabia's
  Friday and Saturday (the Qiddiya City parks), where the Saturday–Sunday split
  typed a working Sunday as a weekend day and a Friday as a working day.
- **Caching.** Profiles are cached per park and park-local date for six hours
  (`plan-day:h5:v3:<parkId>:<date>`), one in-flight build per key; a failed build
  is replayed in process for 45 s (`H5_NEGATIVE_CACHE_MS`). The read cost and why
  it needs no new index are measured in `src/parks/services/h5-profile.service.ts`.

## The composer A/B in the forward archive

The flag switches what is **served**; what is **measured** does not wait for it.
Every 06:00 / 07:00 forward-archive capture calls `buildPlanDayWithShadow`, which
builds, for every ride with both an old-composer curve and an H5 profile, three
curves on the same quarter-hours: old composer, plain H5, routed H5. They are
stored as `plan_day_shadow_peak` / `_h5` / `_routed` and scored under `UC3S`, one
source per composer, never pooled — see [forward archive](../../ml/forward-archive/capture.md).
The routed row is written only where a TFT level was applied (busy rides, d0–d2).
It is the `tfth5` column of the `planner_h5_slots` decision, so it stays TFT's
routing: a season-scaled ride writes no routed row and is scored as plain H5
there. The season-scaled curve is the served `plan_day` row. Nothing in the endpoint's response carries
the shadow.

## The 15-minute contract

```jsonc
"rides": [{
  "attractionSlug": "taron",
  "hours": [{ "hour": 10, "wait": 25 }, { "hour": 11, "wait": 40 }],
  "slots": [
    { "minute": 600, "start": "2026-10-17T08:00:00.000Z", "wait": 22 },
    { "minute": 615, "start": "2026-10-17T08:15:00.000Z", "wait": 27 }
  ],
  "composer": "h5_tft",
  "dayPeak": 55
}]
```

`composer` is `peak_scaled`, `h5`, `h5_tft` or `h5_season`.

- `minute` is the park-local wall-clock minute the slot **starts** at, counted from
  the planned date's midnight and **unfolded** like `hour` (1440 is the midnight
  ending a late day — see [midnight](./midnight.md)). `floor(minute / 60)` is its
  hour. Multiples of 15, ascending, no duplicates.
- `start` is the same instant in UTC — for anything crossing a DST switch.
- `wait` is **whole minutes, not rounded to 5**. Rounding is a display convention;
  round where a single figure is printed, keep raw minutes for sums.
- `source` follows `hours[].source`: present only where the slot did not come from
  the response's `tier`.
- **Absent `slots`** (flag off, hourly-fallback ride, another tier): read `hours`.
  **A gap inside a present series**: read the slot's hour only when that hour has
  **no slot at all**. An hour with some slots and not others is partly outside the
  ride's window (a 09:30 opening has no 09:00 or 09:15), and its missing
  quarter-hours have no value.
- A ride opening later than the park ([opensAt](./opens-at.md)) starts its slots
  at that opening's quarter-hour.
- `profileDays` is diagnostic; `composer` is for the archive, nothing should
  branch on it. park.fan reads `slots` since PAR-910
  ([wait figures](https://github.com/PArns/park.fan/blob/main/docs/features/trip-planner/wait-figures.md#a-figure-belongs-to-its-quarter-hour)).

## Known limits

- **Known limit: H5 has no holiday typing, and a thin day type reads all days.**
  A public holiday or a school-break weekday reads the working days' profile.
  A day type with fewer than 3 days at an index falls back to all days
  (`MIN_DAYS_DAYTYPE`), so a ride with fewer than three measured weekend days in
  its window (a park back from a long closure, a ride new this season) reads a
  weekend profile of weekdays and weekends together. On a season day the season
  level corrects the height; the shape stays. Neither changes while H5 is the
  profile the live decision `planner_h5_slots` judges (`evaluateOn` 2026-12-22,
  `src/ml/data/ml-decisions.json`): its `h5` column is this profile over the
  28 days before that date, and the offline result behind the decision measured
  this profile with ml-bench's coalesce (`baselines.py`). A change now would
  mix two profiles in one column. **After 2026-12-22** a change to H5's
  statistics goes into `ml-bench/mlbench/baselines.py` first and is served only
  once an ml-bench run has scored it against the current H5; the golden test
  (`h5-profile.util.spec.ts`) moves with the bench.
