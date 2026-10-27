# Unified crowd levels (typical-day-peak daily, ratio-vs-P50 live)

Two crowd-level regimes exist and they never meet on one surface. Open this page before you add
or change anything that rates a park or ride as quiet or busy. Formulas, thresholds, services and
the calibration are in [Crowd levels](../analytics/crowd-levels.md).

## The rule

- **Daily and historical aggregates (the calendar) compare a day's peak to a typical day's peak.**
  `day_value` = the average over headliners of each ride's daily P90; the baseline is
  `park_p50_baselines.typicalDayPeak`, the median `day_value` over operating days (548-day window).
  100 % is a typical day, `moderate`. A future day uses the same baseline with predicted headliner
  waits, which come from `MLService.getServingDailyPredictions`: TFT for days 1–60 (`tftDays`)
  wherever it covers the ride, CatBoost beyond and for any ride-day TFT lacks. That merge is for
  serving only; the prediction writer stores pure CatBoost.
- **Point-in-time and live signals use ratio-vs-P50.** `calculateParkOccupancy` is
  `Σ latest_wait ÷ Σ attraction_p50 × 100` over headliners reporting in the last 60 min
  (`getHeadlinerLoad`), falling back to `avg(latest) ÷ park_p50` without per-ride baselines. The
  calendar's "today" cell and the hourly predictions stay on P50 too.
- **Sum the minutes, then divide.** Never average or take a percentile of per-ride ratios: over ten
  headliners a P90 of ratios is the second-busiest ride, it only pushes upwards, and a
  10-minute-baseline ride outvotes a marquee (Phantasialand read `high`, 123 %, with Taron at
  20/45 min). `breakdown.typicalAvgWait` is the baseline of exactly the rides in
  `currentAvgWait`, so the pair divides out to the displayed percentage.
- **The ML feature is a different function on purpose.** `park_occupancy_pct` comes from
  `getCurrentOccupancy` (`avg(latest) ÷ park_p50`); trained models depend on that distribution, so
  moving it needs a retrain. Do not "unify" the two.
- **No invented ratings.** Anything that cannot rate against a real baseline emits `unknown`, never a
  placeholder `moderate` (`rateOrUnknown`, `isParkRatable`, `getLoadRating(_, baseline <= 0)`,
  `calculateParkOccupancy` with no live sample, callers of `getAttractionCrowdLevel`). There is no
  calendar fallback: a missing typical-day-peak means `unknown`. A 0-minute wait against a real
  baseline is a walk-on and rates `very_low`; only an absent wait is "no data". This is the
  [absent-facts rule](absent-facts.md) applied to crowd levels.
- **Consumers read the gated value** (`occupancy.crowdLevel`), never re-derive a tier from
  `occupancy.current`: that bypasses the ratability gate and turns `current = 0` into `very_low`.
- **Every Swagger `enum:` for a crowd level comes from `CROWD_LEVEL_VALUES` /
  `CROWD_LEVEL_WITH_CLOSED_VALUES`** (`src/common/types/crowd-level.type.ts`). A hand-written list
  kept `unknown` out of the published contract for months.
- **Past and future days carry the same statistic where they share a field.**
  `headlinerForecast.avgWait` is a day-peak on both sides: forecasts take the per-day MAX in
  `predict.py`, history `getHeadlinerDailyPeaks` (per-ride day-P90). A mean on one side and a peak
  on the other reads as the park getting busier when only the statistic changed.
