# Which compared slot is a forecast error

**Since PAR-912 (2026-10-17).** `prediction_accuracy` compares every hourly
CatBoost slot with the `queue_data` reading nearest to it. Until PAR-912 every
match became a COMPLETED row with an actual, and a ride that was shut or
published no wait was compared against 0. This page says which of those rows
count as a forecast error, how the comparison decides it, and which reader uses
the rule.

## What was wrong

Measured on production, 2026-10-17, hourly COMPLETED rows over six days:
**65–70 % had `actual_wait_time = 0`.** With them every CatBoost version read
MAE 11–12 min and bias +10; without them MAE 6.2–6.6 and bias +2.4…+3.0.

Re-deriving the matched reading for one day (162 755 rows, 111 862 of them
zero) split the zeros into:

| Share of the zeros | What the reading was | Forecast error? |
| --- | --- | --- |
| 41.8 % | CLOSED before the ride's first OPERATING reading of the park-local day (45 478), or REFURBISHMENT (1 308) — the ride was not scheduled to run | no |
| 37.8 % | OPERATING with 0 on a ride that published no wait above 0 in the seven days before (42 328) — status-only feeds, free-flow attractions | no |
| 11.1 % | DOWN (4 167), or CLOSED after the ride had already run that day (8 250) | unplanned closure — existing rule |
| 9.2 % | OPERATING with 0 on a ride that publishes waits (10 331) | yes |

Only 3.5 % of all six days' rows fell outside the park's scheduled hours, so
"outside opening times" was not the cause: `storePredictions` and the ml-service
already drop CLOSED days and hours outside an OPERATING window. The zeros were
inside the park's hours, on rides that were not running or never report a
number.

## The rule

`compareWithActuals` classifies each matched reading with `classifyComparison`
and writes the verdict to `prediction_accuracy.score_class`:

| Reading | Context | `score_class` | actual / errors |
| --- | --- | --- | --- |
| OPERATING, wait > 0 | — | `scored` | wait / computed |
| OPERATING, wait 0 | ride published a wait > 0 in the last 7 days | `scored` | 0 / computed |
| OPERATING, wait 0 | no such wait | `no_wait_signal` | NULL / NULL |
| DOWN | — | `unplanned_closure` | 0 / full prediction, `wasUnplannedClosure` |
| CLOSED | ride had an OPERATING reading earlier the same park-local day | `unplanned_closure` | 0 / full prediction |
| CLOSED | it had not | `ride_not_operating` | NULL / NULL |
| REFURBISHMENT | — | `ride_not_operating` | NULL / NULL |
| OPERATING without a wait value, or any other status | — | stays PENDING → MISSED | — |

- Every classified row is COMPLETED, because it **was** checked against the
  feed — coverage (`overall.coveragePercent`, the low-coverage alert) counts
  checked slots and is unchanged.
- The two "not an error" classes keep **NULL**, never 0, as actual and errors.
  A 0 would be read as a measured empty queue by any reader that filters on the
  actual alone, and the absent-facts rule forbids turning "no reading" or "not
  running" into a measured number (docs/rules/absent-facts.md).
- "Ran earlier today" only looks backwards, so it is decidable when the slot is
  compared: a ride that opens after the slot still had not run at the slot. The
  day is the park's day. A CLOSED reading late in the day after the ride ran
  is counted as an unplanned closure even when it was a planned early close —
  the feed cannot say which, and no accuracy figure averages unplanned closures
  anyway.
- The seven-day wait signal is answered from the batch's own readings first and
  then by one `EXISTS` per remaining ride, cached per process for an hour.
- An unknown reading status is no reading. It used to fall into the closure
  branch.

## The predicate every reader uses

`scoredSlotSql(alias, MAX_PLAUSIBLE_WAIT_TIME)` (and `isScoredSlot` for rows in
memory), in `src/ml/entities/prediction-accuracy.entity.ts`:

```sql
comparison_status = 'COMPLETED'
AND actual_wait_time IS NOT NULL AND actual_wait_time <= 400
AND (score_class = 'scored'
     OR (score_class IS NULL AND "wasUnplannedClosure" = false AND actual_wait_time > 0))
```

The second branch is the transition: rows compared before PAR-912 carry no
class, and for them only a positive wait outside a closure is trusted. Their
real zeros (9 %) drop with the 91 % that are not waits, because nothing on an
old row tells them apart. No backfill: the old rows age out with the 90-day
COMPLETED retention.

## Quantiles

`prediction_accuracy` gained `predicted_q80` and `predicted_q95` — the model's
raw q0.8 (crowd level) and q0.95 (top of the band), whole minutes, NOT the
posted-wait rounding. predict.py returns them as `predictedQ80`/`predictedQ95`;
`storePredictions` hands them to `recordPredictions` index-aligned with the rows
it records. They have no column on `wait_time_predictions` (a compressed
hypertable) and are stripped from every served read (`withoutInternalQuantiles`
in `getParkPredictions` and `getAttractionPredictions`), so they never reach a
public payload.

Since PAR-1060 the row also carries `uncertainty_minutes`, CatBoost's RAW band
(`q0.95 − q0.5`, before the calibration factor), which the daily band
calibration fits on — see
[quantile-serving-and-calibration.md](./quantile-serving-and-calibration.md#catboost-hourly-band-split-conformal-factor-par-1060).

`predicted_wait_time` is the served median: q0.5 rounded to 5 with a floor of
10. Its coverage therefore sits above 0.5 on quiet slots by construction; q80
and q95 are unrounded and measure the model.

Coverage reads on `GET /v1/ml/dashboard` → `performance.operating`
(`getOperatingAccuracy`, 7 days, cache `accuracy:operating:v1:{days}`): per
model version plus an all-versions row, `n`, `mae`, `bias`,
`coverage.{q50,q80,q95}` and the sample counts the quantile coverages rest on.
`q80`/`q95` stay null until rows written after the deploy have been compared.

## Measured with the rule (production, replayed 2026-10-17, 6 days)

Raw CatBoost hourly, scored slots (including real zeros) per model version:

| Model | n | MAE | bias | P(actual ≤ served q0.5) |
| --- | --- | --- | --- | --- |
| v20261011_0600 | 148 328 | 9.01 | +5.89 | 0.867 |
| v20261013_0600 | 54 022 | 8.00 | +5.61 | 0.891 |
| v20261014_0600 | 53 486 | 7.36 | +4.13 | 0.861 |
| v20261015_0600 | 60 805 | 7.47 | +4.76 | 0.877 |
| v20261016_0600 | 71 161 | 7.96 | +4.84 | 0.859 |
| v20261017_0600 | 11 527 | 8.42 | +4.75 | 0.828 |

The system overall MAE (7 days, ratable parks) reads **6.9** instead of 10.1
while only old rows exist (they keep positive waits only), and about 7.5–8 once
new rows bring the real zeros back.

## Who reads `prediction_accuracy`, and what changed

| Reader | Distorted before? | Now |
| --- | --- | --- |
| Accuracy badge (`attraction_accuracy_stats.mae`, served basis), batch badge, live fallback | no — `actual ≥ 5 AND NOT wasUnplannedClosure` already dropped every zero | unchanged; the new NULL actuals fall out of the same filter |
| Ride day-curve band (`raw_mae`) | no — same filter | unchanged |
| `/v1/ml/accuracy/system` + dashboard `live` (`getSystemAccuracyStats.overall`) | **yes** — admitted every 0 on an open park day | `scoredSlotSql`; cache `accuracy:sys:v2:*`, `ml:dashboard:v3` |
| `byPredictionType.HOURLY.mae` | **yes** — averaged every compared row | `scoredSlotSql`; coverage there is now COMPLETED/total like `overall` |
| Accuracy alert (`ml-alert`, MAE > threshold) | **yes** — threshold 13 was calibrated on the inflated figure | threshold 10, ladder 10/13/17 |
| Anomaly detection (daily `detect-anomalies`) | **yes** — unplanned closures and status-only zeros raised `large_error` | non-closures only, `isScoredSlot` |
| Drift (dashboard `drift`), top/bottom, hourly/weekday patterns, daily trends, attraction stats | no — `actual ≥ 5` filter | unchanged |
| Training sample weights (`fetch_attraction_accuracy`) | no — reads `attraction_accuracy_stats` | unchanged |
| `fetch_prediction_errors_for_training` (ml-service) | would be — unused today | filter added |
| `checkRetrainingNeeded`, `shouldRetrain` | would be — never called | unchanged |
| `/plan/day` `leadTimeMae` (`prediction_lead_snapshots`) | no — daily P90 of headliners; 0 zero actuals in production | — |
| `expectedError` (`forecast_accuracy_profile`) | no — TFT vs `queue_data_aggregates`, days with P90 > 0 | — |
| PCN scorer, forward archive | no — read `queue_data` with `waitTime ≥ 5` | — |
| Feature drift | no — reads `features` only | — |

## Diagnostic SQL

```sql
-- How the comparison classified the last day
SELECT score_class, count(*), avg(absolute_error) AS mae
FROM prediction_accuracy
WHERE comparison_status = 'COMPLETED' AND "predictionType" = 'hourly'
  AND target_time >= now() - interval '1 day'
GROUP BY 1 ORDER BY 2 DESC;

-- Quantile coverage per model version, scored slots, 7 days
SELECT "modelVersion", count(*) AS n,
       avg((actual_wait_time <= predicted_wait_time)::int) AS cov50,
       avg((actual_wait_time <= predicted_q80)::int)       AS cov80,
       avg((actual_wait_time <= predicted_q95)::int)       AS cov95
FROM prediction_accuracy
WHERE target_time >= now() - interval '7 days' AND score_class = 'scored'
GROUP BY 1 ORDER BY 1 DESC;
```
