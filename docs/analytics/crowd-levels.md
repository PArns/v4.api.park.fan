# Crowd levels: formulas, baselines and jobs

How every crowd level is computed: the formula per surface, the thresholds, the baselines and the jobs
that write them, the ratability gate, the ML side, why the calendar divides by a typical day's peak, and
how to check the calibration against production. The rule itself (two regimes that never meet, sum then
divide, no invented ratings) is [Unified crowd levels](../rules/crowd-levels.md); how headliners are
chosen is [Headliner identification](headliner-logic.md).

## Formula per surface

| Surface                                                            | Numerator                                                                                                  | Denominator                                                    | Code                                                       |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- | ---------------------------------------------------------- |
| Park live (`occupancy`), the calendar's "today" cell               | `Σ latest_wait` of headliners with a sample in the last 60 min and a positive P50                          | `Σ` of those rides' `attraction_p50_baselines`                 | `getHeadlinerLoad`, `calculateParkOccupancy`               |
| Park live, no per-ride baselines                                   | average latest headliner wait (`getCurrentParkPeakWait`, widens 60 → 240 min when empty)                   | `park_p50_baselines.p50Baseline`                               | `calculateParkOccupancy`                                   |
| Park calendar, past day                                            | `day_value` = AVG over headliners of each ride's daily P90 (`PERCENTILE_CONT(0.9)` over that day's `queue_data`) | `park_p50_baselines.typicalDayPeak`                        | `calculateCrowdLevelForDate` (`baselineType = "typical_day"`) |
| Park calendar and yearly view, future day                          | AVG of predicted headliner waits (a day-peak proxy: `predict.py` takes the per-day MAX of its peak hours)  | `typicalDayPeak`                                               | `CalendarService.buildPredictedCrowdLevels`                |
| Ride live                                                          | current wait                                                                                               | `attraction_p50_baselines`                                     | `getAttractionCrowdLevel`, `getLoadRating`                 |
| Ride calendar (attraction history)                                 | P90 of the day's in-hours 15-minute slot P90s (`attraction_hourly_history`)                                | ride typical-day-peak                                          | `AttractionIntegrationService`, `getAttractionTypicalDayPeak` |
| Hourly within-a-day predictions                                    | predicted wait                                                                                             | P50                                                            | calendar hourly predictions                                |

Cross-ride aggregation follows the [rule](../rules/crowd-levels.md) (sum the minutes, then divide). The
live `breakdown.typicalAvgWait` is the mean P50 of exactly the rides in `breakdown.currentAvgWait`.
`avgWaitToday` and `peakWaitToday` come from one per-headliner query (`averageTodayAcrossRides`), so
`avg ≤ peak` holds by construction; the calendar's past-day headliner figures are per-ride day-P90s
(`getHeadlinerDailyPeaks`) to match the peak-shaped forecasts in the same field.

With no live sample at all `calculateParkOccupancy` returns `crowdLevel: "unknown"`. With no P50
baseline it returns `current: 50`, `crowdLevel: "unknown"`, `confidence: "low"`: the 50 is a placeholder
(hence the rule's "consumers read the gated value").

The legacy field `baseline90thPercentile` carries whichever baseline the surface used (typical-day-peak
on the calendar, P50 live).

## Thresholds

`determineCrowdLevel()` (`src/common/utils/crowd-level.util.ts`) maps the percentage, the same ladder on
both regimes; `rateOrUnknown(numerator, baseline)` returns `unknown` for a missing or non-positive
baseline.

| Level       | Percentage of baseline |
| ----------- | ---------------------- |
| `very_low`  | ≤ 60                   |
| `low`       | 61–89                  |
| `moderate`  | 90–110                 |
| `high`      | 111–150                |
| `very_high` | 151–200                |
| `extreme`   | > 200                  |

On the calendar `moderate` means a statistically typical day; busy seasons (Wintertraum, Easter, promos)
reading high to extreme is correct.

## Ratability gate

- **Park**: `calculateTypicalDayPeak` returns the median and its day count. Below 30 operating days
  (`MIN_BASELINE_OPERATING_DAYS`) `calculateP50Baseline` forces it to 0, `saveP50Baselines` stores
  `NULL` and no Redis key is written. **A park is ratable exactly when `park_p50_baselines."typicalDayPeak"
  IS NOT NULL`.** Live surfaces ask `isParkRatable` / `getRatableParkIds`; P50 and the numeric occupancy
  stay computed for a thin park, only the rating becomes `unknown`.
- **Ride**: `getAttractionTypicalDayPeak` needs 20 days (`MIN_SAMPLE_DAYS`) with hourly P90s
  (`queue_data_aggregates`, `sampleCount ≥ 2`) over 548 days; with fewer it caches 0 on a short TTL and
  the day's rating is skipped. Ride live ratings are also gated on the park's ratability.
- **ML**: training (`ml-service/db.py`) and the reported MAE/accuracy aggregates join
  `park_p50_baselines … "typicalDayPeak" IS NOT NULL`, so thin parks are excluded from both.

## Baselines and where they live

All windows are 548 days and headliner-only for the park.

| Value                         | Computed as                                                                       | Stored                                                                 |
| ----------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Park P50                      | mean of the headliners' P50s (a pooled percentile would be dominated by high-frequency low-wait rides) | `park_p50_baselines.p50Baseline`; Redis `park:p50:{parkId}` (24 h, JSON with confidence) |
| Park P90                      | mean of the headliners' P90s; computed for free, no longer a calendar reference   | `park_p90_baselines`; Redis `park:p90:{parkId}` (24 h)                 |
| Park typical-day-peak         | median over operating days of `day_value`, written atomically with P50/P90        | `park_p50_baselines.typicalDayPeak`; Redis `park:typicalpeak:{parkId}` (2 days, read-through via `getTypicalDayPeakFromCache`) |
| Ride P50, P90                 | one `PERCENTILE_CONT(0.5)` + `(0.9)` scan per ride                                 | `attraction_p50_baselines`, `attraction_p90_baselines`; Redis `attraction:p50:{id}`, `attraction:p90:{id}` (24 h; a missing baseline is cached as `0` for 6 h) |
| Ride typical-day-peak         | median over days of P90-of-hourly-P90s, on demand                                  | Redis `attraction:typicalpeak:{id}` (24 h)                            |
| Headliner set                 | [Headliner identification](headliner-logic.md)                                    | `headliner_attractions`                                                |

`park:typicalpeak:` (no hyphen, a wait) is not `park:typical-peak:` (with hyphen, the typical peak hour
in `getParkStatistics`).

## Jobs

| Job                                                                   | When (UTC)                         | Writes                                                                      |
| --------------------------------------------------------------------- | ---------------------------------- | --------------------------------------------------------------------------- |
| `p50-baseline` / `calculate-park-baselines` (`P50BaselineProcessor`)  | 03:00 daily, and at boot           | Headliners, park P50, P90 and typical-day-peak in one pass, parks in batches of 5 |
| `p50-baseline` / `calculate-attraction-baselines`                     | 04:00 daily                        | Ride P50 and P90                                                            |
| `attraction-hourly-history` / `calculate-yesterday-hourly-history`    | 04:30 daily                        | `attraction_hourly_history`: one row per ride and park-local day with 15-minute slot `p90`, `avgWait`, `sampleCount`; backfill job `backfill-attraction-hourly-history` |

At boot `QueueBootstrapService` queues `calculate-park-baselines` when the P50 rows are stale or no park
has a `typicalDayPeak` yet. Baselines self-heal: a logic change reaches every park by the next 03:00 run.

**The hourly-history rollup is per park-local yesterday.** At 04:30 UTC it is still the previous evening
west of UTC, so a US park's day is rolled up 24 hours later than a European park's. A past-date read that
uses `attraction_hourly_history` (the ride calendar, `/plan/day` for a past day) returns nothing for a day
not yet rolled up, while raw `queue_data` is complete. Ask which table the read path uses before calling
an empty answer a bug.

## ML integration

`ml.service.ts` sends `typicalDayPeakBaseline` with every prediction request; `ml-service/predict.py`
divides the predicted wait by it, falls back to P50, and with neither answers `unknown` (stored as NULL),
never the ride's own rolling average (PAR-943). `predict.py` uses the **park's** typical-day-peak for
per-ride levels too. The baseline only labels the percentage; the model predicts waits, so changing the
denominator needs no retrain. `park_occupancy_pct` is a separate input feature (`getCurrentOccupancy`,
`avg(latest) ÷ park P50`) and keeps that shape on purpose.

## Why the calendar divides by a typical day's peak

Measured on Phantasialand (P50 ≈ 30 min, pooled P90 51.6, typical-day-peak 40.3):

1. **Day P90 ÷ P50** put a normal day at ~170 % (`very_high`); the live calendar was 26 % `extreme` and
   never `very_low`.
2. **`day_value` ÷ pooled P90** skewed low: the median day read 74 % (`low`) and the busiest Wintertraum
   day only 138 % (`high`), because the pooled P90 is inflated by the busiest season.
3. **`day_value` ÷ median of daily peaks** keeps the ordering and lands the buckets:

| Day                       | `day_value` | ÷ P90 (51.6)  | ÷ typical day (40.3) |
| ------------------------- | ----------: | ------------- | -------------------- |
| Jan 11 (Wintertraum peak) |        71.4 | 138 % high    | **177 % very_high**  |
| Apr 18 (Easter Saturday)  |        60.2 | 117 % high    | 150 % high           |
| May 16 (busy Saturday)    |        56.5 | 109 % moderate | 140 % high          |
| Jan 20 (quiet)            |        23.7 | 46 % very_low | 59 % very_low        |

Over 90 days: very_low 8, low 24, moderate 17, high 25, very_high 6, extreme 0.

The live park level used to be the P90 across per-ride ratios; why that was replaced is in the
[rule](../rules/crowd-levels.md).

## Known limitations

- **Future days read more conservatively** than completed ones: the forecast is one smoothed value per
  ride and day, not a real intra-day peak. An ML-accuracy task, not a formula one.
- **A past day is judged against today's headliners and today's typical-day-peak** (rolling window), so
  historical ratings drift slightly as the window moves.
- **The ride calendar's numerator and denominator come from different rollups**
  (`attraction_hourly_history` slots against `queue_data_aggregates` hours), a small method drift.

## Checking the calibration against production

The dev and CI containers have no populated database, so the baselines self-calibrate only against real
data. Validate the **invariant** (a typical day ≈ 100 % = `moderate`) and plausibility against known days,
never a fixed target distribution: a promotion can legitimately fill a park. Run read-only through
`scripts/prod-psql.sh`.

```sql
-- Thin parks: under 30 operating days must have a NULL typical-day-peak (and read `unknown`).
SELECT "parkId", "distinctDays", "typicalDayPeak"
FROM park_p50_baselines WHERE "distinctDays" < 30 ORDER BY "distinctDays";

-- Park sanity: p50 < typicalDayPeak < pooled P90 (PHL ≈ 30 / 40.3 / 51.6).
SELECT b."p50Baseline", b."typicalDayPeak", p."p90Baseline"
FROM park_p50_baselines b JOIN park_p90_baselines p USING ("parkId")
WHERE b."parkId" = '<park-uuid>';

-- Ride typical-day-peak against its P50: expect 1.3–2× and at least 20 sample days.
WITH per_day AS (
  SELECT (qda.hour AT TIME ZONE '<park-tz>')::date AS day,
         PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qda.p90) AS day_peak
  FROM queue_data_aggregates qda
  WHERE qda."attractionId" = '<attraction-uuid>'
    AND qda.hour >= now() - interval '548 days' AND qda."sampleCount" >= 2
  GROUP BY day)
SELECT (SELECT "p50Baseline" FROM attraction_p50_baselines WHERE "attractionId" = '<attraction-uuid>') AS p50,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY day_peak) AS typical_day_peak,
       COUNT(*) AS sample_days
FROM per_day;
```

Then on a dense seasonal park (Phantasialand, Europa-Park): Wintertraum, Easter, Pentecost and summer
Saturdays read high or above, quiet off-season weekdays low or below, and the same future date reads the
same level in the monthly calendar and the yearly view. A whole surface skewed one way points at a wrong
denominator or a missing baseline.

When a level looks wrong for one park, check its headliners and P50 first
([validation checklist](headliner-logic.md#when-a-crowd-level-looks-wrong)).
