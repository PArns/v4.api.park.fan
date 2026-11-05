# Which compared slot is a forecast error (PAR-912)

`prediction_accuracy` compares every hourly CatBoost slot with the `queue_data` reading nearest to
it. Since PAR-912 (2026-10-17) the comparison classifies each match, and only the class `scored`
counts as a forecast error. This page says how a match is classified, the predicate every MAE
reader uses, the stored quantiles, and which reader applies what. Open it before writing any query
that averages `prediction_accuracy` errors. Which *model* a reader-facing figure measures is
[reader-facing-accuracy.md](reader-facing-accuracy.md).

## Why

Measured on production 2026-10-17, hourly COMPLETED rows over six days: 65–70 % had
`actual_wait_time = 0`; with them every CatBoost version read MAE 11–12 min and bias +10, without
them 6.2–6.6 and +2.4…+3.0. One day re-derived (162,755 rows, 111,862 zeros) split the zeros into:

| Share of the zeros | The reading | Forecast error? |
| --- | --- | --- |
| 41.8 % | CLOSED before the ride's first OPERATING reading of the park-local day, or REFURBISHMENT — not scheduled to run | no |
| 37.8 % | OPERATING with 0 on a ride that published no wait > 0 in the seven days before — status-only feeds, free-flow attractions | no |
| 11.1 % | DOWN, or CLOSED after the ride had run that day | unplanned closure |
| 9.2 % | OPERATING with 0 on a ride that publishes waits | yes |

Only 3.5 % of the rows fell outside the park's scheduled hours: the zeros were inside the hours, on
rides that were not running or never report a number.

## The rule

`compareWithActuals` classifies each matched reading with `classifyComparison` and writes
`prediction_accuracy.score_class`:

| Reading | Context | `score_class` | Actual / errors |
| --- | --- | --- | --- |
| OPERATING, wait > 0 | — | `scored` | wait / computed |
| OPERATING, wait 0 | the ride published a wait > 0 in the last 7 days | `scored` | 0 / computed |
| OPERATING, wait 0 | no such wait | `no_wait_signal` | NULL / NULL |
| DOWN | — | `unplanned_closure` | 0 / full prediction, `wasUnplannedClosure` |
| CLOSED | an OPERATING reading earlier the same park-local day | `unplanned_closure` | 0 / full prediction |
| CLOSED | none earlier that day | `ride_not_operating` | NULL / NULL |
| REFURBISHMENT | — | `ride_not_operating` | NULL / NULL |
| OPERATING without a wait, or any other status | — | stays PENDING → MISSED | — |

- Every classified row is COMPLETED (it was checked against the feed), so coverage
  (`overall.coveragePercent`, the low-coverage alert) is unchanged.
- The two "not an error" classes keep **NULL**, never 0: a 0 would read as a measured empty queue to
  any reader that filters on the actual alone ([absent facts](../rules/absent-facts.md)).
- "Ran earlier today" only looks backwards, so it is decidable at comparison time. A late planned
  early close counts as an unplanned closure — the feed cannot tell them apart.
- The seven-day wait signal is answered from the batch's own readings first, then by one `EXISTS`
  per remaining ride, cached per process for an hour. An unknown status is no reading.

## The predicate every reader uses

`scoredSlotSql(alias, MAX_PLAUSIBLE_WAIT_TIME)` (and `isScoredSlot` for rows in memory) in
`src/ml/entities/prediction-accuracy.entity.ts`:

```sql
comparison_status = 'COMPLETED'
AND actual_wait_time IS NOT NULL AND actual_wait_time <= 400
AND (score_class = 'scored'
     OR (score_class IS NULL AND "wasUnplannedClosure" = false AND actual_wait_time > 0))
```

The second branch covers rows compared before PAR-912, which carry no class: only a positive wait
outside a closure is trusted there (their real zeros drop with the rest). No backfill; old rows age
out with the 90-day COMPLETED retention.

## Quantiles

`prediction_accuracy.predicted_q80` / `predicted_q95` hold the model's raw q0.8 (crowd level) and
q0.95 (top of the band), whole minutes, not posted-wait rounding. predict.py returns them as
`predictedQ80` / `predictedQ95`; `storePredictions` hands them to `recordPredictions`. They have no
column on `wait_time_predictions` and are stripped from every served read
(`withoutInternalQuantiles`), so they never reach a public payload. Since PAR-1060 the row also
carries `uncertainty_minutes`, CatBoost's raw band (q0.95 − q0.5, before calibration), which the
daily band calibration fits on — [quantile serving](quantile-serving-and-calibration.md#catboost-hourly-band-split-conformal-factor-par-1060).

`predicted_wait_time` is the served median (q0.5 rounded to 5, floor 10), so its coverage sits
above 0.5 on quiet slots by construction; q80 and q95 are unrounded and measure the model.

Coverage reads on `GET /v1/ml/dashboard` → `performance.operating` (`getOperatingAccuracy`, 7 days,
cache `accuracy:operating:v1:{days}`): per model version plus an all-versions row, `n`, `mae`,
`bias`, `coverage.{q50,q80,q95}` and the sample counts behind each coverage.

Measured with the rule (replayed 2026-10-17, 6 days, raw CatBoost hourly, scored slots): MAE 7.36 …
9.01 and bias +4.1 … +5.9 per model version (v20261011 … v20261017), P(actual ≤ served q0.5)
0.83–0.89. The 7-day system MAE reads 6.9 instead of 10.1 while only old rows exist, about 7.5–8
once new rows bring the real zeros back.

## Who reads `prediction_accuracy`

| Reader | Rule it applies |
| --- | --- |
| Accuracy badge (`attraction_accuracy_stats.mae`, served basis), batch badge, live fallback | `actual ≥ 5 AND NOT wasUnplannedClosure` — already excluded every zero; the new NULLs fall out of the same filter |
| Ride day-curve band (`raw_mae`) | same filter |
| `/v1/ml/accuracy/system` + dashboard `live` (`getSystemAccuracyStats.overall`) | `scoredSlotSql`; cache `accuracy:sys:v2:*`, `ml:dashboard:v3` |
| `byPredictionType.HOURLY.mae` | `scoredSlotSql`; coverage is COMPLETED / total like `overall` |
| Accuracy alert (`ml-alert`, MAE above threshold) | threshold 10, severity ladder 10 / 13 / 17 (13 was calibrated on the inflated figure) |
| Anomaly detection (daily `detect-anomalies`) | non-closures only, `isScoredSlot` |
| Drift (dashboard), top/bottom, hourly/weekday patterns, daily trends, attraction stats | `actual ≥ 5` |
| ML overview drift watch | `scoredSlotSql` ([ml-overview.md §2a](ml-overview.md#2a-drift-watch-par-1059)) |
| `fetch_prediction_errors_for_training` (ml-service, unused today) | filter added |
| Training sample weights (`fetch_attraction_accuracy`) | reads `attraction_accuracy_stats` |
| `checkRetrainingNeeded`, `shouldRetrain` | never called |

Not readers of this table: `/plan/day` `leadTimeMae` (`prediction_lead_snapshots`),
`expectedError` (`forecast_accuracy_profile`), the PCN scorer and the forward archive (they read
`queue_data` with `waitTime ≥ 5`), feature drift (`features` only).

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
