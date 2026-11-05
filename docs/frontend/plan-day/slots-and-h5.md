# `/plan/day`: 15-minute `slots[]` and the H5 composer (PAR-834)

The quarter-hour series on each ride, the H5 profile that composes it, when a
TFT level scales it, and how production differs from the benchmark it was ported
from. Open it when reading or changing composed curves. Part of the
[`/plan/day` contract](./README.md).

## Behind a flag

`PLAN_DAY_H5_COMPOSER=true` switches it on (`.env.example`). Unset, the response
is the pre-H5 one byte for byte (pinned by a spec against the old output), plus
`composer` on composed rides. It ships off so the forward archive can score old
and new composers side by side first (below).

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
   - weekday/weekend median where that split has ≥ 3 days at the index
     (`MIN_DAYS_DAYTYPE`), else all days with ≥ 4 (`MIN_DAYS_ALL`).

   The arithmetic is pinned against the benchmark's DuckDB SQL by a golden test
   (`src/common/utils/h5-profile.util.spec.ts`, agreement to 1e-6 on dense,
   weekday/weekend, past-midnight and gap scenarios).
2. **A level is applied only where the benchmark says it pays** (`h5LevelFor`,
   `src/common/utils/h5-profile.util.ts`): a TFT day level (`modelVersion: "tft"`,
   at most 3 days old) **and** an ex-ante busy ride (q90 of its 56-day slots
   ≥ 45 min, `BUSY_Q90_MINUTES`) **and** a lead ≤ 2 days
   (`TFT_LEVEL_MAX_LEAD_DAYS`). Then the profile is multiplied by **level ÷ the
   ride's median daily P90 over the same 56 days** — never stretched to put its
   maximum at the level — and the rope-drop hour is **not** scaled
   (`composer: "h5_tft"`). Everywhere else the plain profile is served
   (`composer: "h5"`). Level × H5 over all rides is worse at every lead; it wins
   only on busy rides, and only at d0–d2 on the full period (PAR-901,
   `ml-bench/results/20261009-baselines-v2/summary.md`; see
   [ml-bench results](../../ml/ml-bench/results-baselines.md#the-tft-level-on-busy-rides)).
   **CatBoost's daily level is never applied** — it made the benchmark worse
   (+2.6 min MAE, bias −5.7).
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
- **Caching.** Profiles are cached per park and park-local date for six hours
  (`plan-day:h5:v2:<parkId>:<date>`), one in-flight build per key; a failed build
  is replayed in process for 45 s (`H5_NEGATIVE_CACHE_MS`). The read cost and why
  it needs no new index are measured in `src/parks/services/h5-profile.service.ts`.

## The composer A/B in the forward archive

The flag switches what is **served**; what is **measured** does not wait for it.
Every 06:00 / 07:00 forward-archive capture calls `buildPlanDayWithShadow`, which
builds, for every ride with both an old-composer curve and an H5 profile, three
curves on the same quarter-hours: old composer, plain H5, routed H5. They are
stored as `plan_day_shadow_peak` / `_h5` / `_routed` and scored under `UC3S`, one
source per composer, never pooled — see [forward archive](../../ml/forward-archive/capture.md).
The routed row is written only where a level was applied (busy rides, d0–d2).
Nothing in the endpoint's response carries the shadow.

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
