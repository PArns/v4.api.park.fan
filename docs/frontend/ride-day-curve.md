# One ride's day curve (`/stats/day`)

The contract for the ~1 KB series behind a day-curve chart: what one ride normally
does per hour, what it has done so far today, and what the model expects for the
rest — plus how to draw the forecast band.

`GET /v1/parks/:continent/:country/:city/:parkSlug/stats/day`

Optional `?attraction=<slug>` pins a ride. Without it the endpoint picks the
park's busiest ride **that actually reported today** — see below, it is the
point of the route.

~1 KB. Cached 5 minutes. `404` when the park has no readable curve.

## Shape

```ts
interface RideDayCurve {
  hours: number[];              // park-local, ascending, derived from the data
  attractionSlug: string;
  attractionName: string;
  p25: Array<number | null>;    // historical spread, lower edge
  p50: Array<number | null>;    // historical median
  p90: Array<number | null>;    // historical spread, upper edge
  today: Array<number | null>;  // measured today
  forecast: Array<number | null>; // expected, for hours not yet measured
  predicted: Array<number | null>; // what the model said BEFORE each hour
                                   // happened, over the whole day
  forecastError: number | null; // the ride's own MAE, minutes
  measuredToday: boolean;
  sampleDays: number;
  timezone: string;
  generatedAt: string;
  schemaVersion: number;        // 2
}
```

## Three sources, and why that matters

- `p25`/`p50`/`p90` come from `queue_data_aggregates`, the nightly hourly rollup, via
  the same profile read as [`/stats/hourly`](./park-hourly-profile.md).
- **`today` cannot come from that table.** The rollup is computed for *yesterday*
  and back (`calculate-percentiles` explicitly aggregates the completed day), so
  it holds no row for today at all. `today` is bucketed live out of raw
  `queue_data` instead: STANDBY only, `OPERATING` only, averaged per park-local
  hour and rounded to five.
- `forecast` comes from `wait_time_predictions` (`predictionType: 'hourly'`),
  `DISTINCT ON (predictedTime)` by newest `createdAt` — the model re-predicts
  through the day and only the latest answer counts.

## Rendering rules

- **Everything is positional against `hours`.** `today[i]`, `forecast[i]` and the
  percentiles all belong to `hours[i]`. A `null` is a gap and must render as one,
  never as a zero.
- **`today` and `forecast` never overlap.** An hour already measured has
  `forecast: null`, so a chart cannot draw the model's guess on top of the fact.
  Joining the two series at the last measured hour is what makes one continuous
  line with a solid past and a dashed future.
- **`forecastError` is a measured, published figure — do not widen it with the
  horizon.** Drawing the forecast as `± forecastError` is honest because that is
  what the number means. Fanning the band out with distance would be an
  uncertainty model nothing here has measured. Null means the ride has not been
  scored (fewer than 10 compared slots, `MIN_BADGE_COMPARISONS`); draw the forecast
  as a line and no band.
- **`forecastError` is CatBoost's error, because the curve is CatBoost's.**
  `forecast` and `predicted` come from `wait_time_predictions`, which holds the
  raw CatBoost answer; the PCN override the ride page serves is never written
  there. Since PAR-854 `attraction_accuracy_stats.mae` (the badge) scores the
  served PCN value, so this field reads `raw_mae` instead — see
  [Reader-facing accuracy figures](../ml/reader-facing-accuracy.md).
- **`measuredToday: false` is a real state**, not an error: the park has not
  opened yet, the ride is closed, or it is out of season. A caller choosing what
  to display should prefer a curve where it is true — which is what the
  ride-picking above already does within one park.
- **The picked ride can differ between two calls on the same day**, as rides open.
  Pin `attraction` where the caller needs stability (a ride page does).
