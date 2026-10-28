# Reader-facing accuracy figures measure the served model (PAR-854)

Three numbers tell a reader how far to trust a forecast, and since PAR-854 (2026-10-11) each one
measures the forecast that reader was actually shown, not the model that happens to write the table
it used to come from. Open this page before changing `leadTimeMae`, the accuracy badge or the
day-curve band. Which compared slots count as an error at all is
[accuracy-scoring-rule.md](accuracy-scoring-rule.md).

| Figure | Surface | Forecast it describes | Scored from |
| --- | --- | --- | --- |
| `leadTimeMae` | `/plan/day` | the **served** daily level: TFT for headliners up to d59, CatBoost elsewhere | `prediction_lead_snapshots`, rows with `source` set |
| accuracy badge (`badge`, `last30Days.mae`) | attraction detail | the **served** intraday wait: PCN + persistence blend when `SERVE_PCN_INTRADAY=true`, CatBoost otherwise; `chronos` where Chronos-2 served the scored slot (rare at the 2-hour scored lead — [chronos-serving.md](forward-archive/chronos-serving.md#where-it-applies)) | `prediction_accuracy.served_*` → `attraction_accuracy_stats.mae` |
| `forecastError` | ride day curve (`/stats/day`) | **raw CatBoost** — the curve plots `wait_time_predictions` | `prediction_accuracy.absolute_error` → `attraction_accuracy_stats.raw_mae` |

The admin boards (`/v1/ml/accuracy/*`, the dashboard, drift) keep scoring raw CatBoost on purpose:
they compare models, and a PCN-vs-CatBoost board must not see PCN on both sides.

## `leadTimeMae`

Before PAR-854 it was the error of CatBoost's raw daily answer while the calendar and `/plan/day`
serve `MLService.getServingDailyPredictions` (TFT over CatBoost up to d59). Now `generate-daily`
still stores raw CatBoost (other boards need it), then reads `getServingDailyPredictions(parkId)` —
a cache hit on the response it just wrote plus one TFT query — and snapshots **that**. Each row
records `source`: `tft` (from `getTftDailyPredictions`, `modelVersion: "tft"`) or `catboost` (TFT had
no forecast, a stale one, or nf-service was down). If the served read fails nothing is snapshotted:
filing the raw answer under a served label would recreate the mix.

The target day is read with `predictionLocalDate`: TFT names the park-local day without an offset,
and `new Date("YYYY-MM-DDT12:00:00")` would read it on the server's clock — a day off for parks
twelve or more hours from UTC. `getLeadTimeMae` averages rows with `source IS NOT NULL` only (Redis
`ml:lead-mae:v2:{bucket}`). The snapshot archive itself: [long-range-forecasting.md](long-range-forecasting.md#6-the-lead-time-snapshot-archive).

## The badge

`storePredictions` (hourly) computes, for every row it records, what `getParkPredictions` would
serve right now: copies of the CatBoost rows through `applyPcnIntradayOverride` (same staleness
guard, blend and anchor read) — and since PAR-911 through the Chronos-2 step too. Three nullable
columns on `prediction_accuracy`:

- `served_wait_time`, `served_source` (`pcn` | `catboost` | `chronos`) — written at generation time,
  never re-derived: the blend's anchor is "the wait now", and recomputing after the slot happened
  would hand the forecast the answer.
- `served_absolute_error` — filled by `compareWithActuals` beside `absolute_error`, against the same
  actual. An unplanned closure charges the full served wait.

`aggregate-stats` and the badge's live fallback score `served_absolute_error` over rows with
`served_source IS NOT NULL`.

**The scored lead is two hours, on purpose.** `prediction_accuracy` keeps the latest write per
slot and the generator runs every 15 minutes, so the old badge's scored row had a median lead of
−0.6 minutes (261,419 rows, 2026-10-11) — a nowcast. For the served value it would be worse: at lead
0 the blend serves the anchor, the reading of that moment, scored against itself. So the served
columns are written only while the slot is at least `SERVED_ACCURACY_MIN_LEAD_MS` (2 h) away, in a
separate upsert statement (TypeORM overwrites a column for every row of a statement once one row
carries it). The value kept is the last one shown 120–135 minutes ahead — at 2 h the served value is
half anchor, half PCN; at 60–75 minutes it would grade persistence and call every steady busy ride
"excellent". The badge therefore reads "accuracy of the forecast about two hours ahead". The raw
columns keep their latest-write semantics.

## The day-curve band stays CatBoost's

`/stats/day` plots `wait_time_predictions`, which holds raw CatBoost (the PCN override is applied on
read paths, never written there). A PCN error drawn around a CatBoost line would describe neither,
so `forecastError` reads `attraction_accuracy_stats.raw_mae` (all compared rows, NULL below the
badge's 10-comparison floor). That the curve is CatBoost while the ride page serves PCN is a
separate served-consistency gap, not fixed.

## Transition — "no figure yet", never the old figure

Rows written before PAR-854 carry no source and count for nothing:

- `leadTimeMae` is absent per bucket until it has 100 scored served rows (`MIN_SCORED`). About 360
  headliner rows per bucket per night get an actual once the target day is over everywhere (the
  03:00 job, UTC−12 cutoff), so lead *L* reports roughly *L* + 2 nights after deploy.
  `accuracy.basis` is unaffected (it reads the forecast-accuracy profile).
- The badge reads `insufficient_data` until a ride has 10 compared served slots. A ride with only
  old rows is rewritten as `insufficient_data` with a NULL `mae`; stats rows for rides with no
  completed row in the window are deleted. Badge cache key `accuracy:badge:v2:…`.
- `forecastError` is unaffected.

## Diagnostic SQL

```sql
-- Served coverage of the badge's input, last day
SELECT served_source, count(*),
       count(*) FILTER (WHERE comparison_status = 'COMPLETED') AS completed,
       avg(served_absolute_error) AS served_mae,
       avg(absolute_error) FILTER (WHERE served_source IS NOT NULL) AS raw_mae_same_rows
FROM prediction_accuracy
WHERE target_time >= now() - interval '1 day'
GROUP BY 1;

-- Served lead-time archive per bucket and source
SELECT lead_days, source, count(*) FILTER (WHERE absolute_error IS NOT NULL) AS scored,
       avg(absolute_error) AS mae
FROM prediction_lead_snapshots
GROUP BY 1, 2 ORDER BY 1, 2;
```
