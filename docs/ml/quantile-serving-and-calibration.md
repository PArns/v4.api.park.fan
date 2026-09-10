# ML Quantile Serving & Crowd-Level Calibration

> Status: current as of 2026-09-10 (q0.95 serving row corrected; band added to
> the calendar's `headlinerForecast.rides[]`). Companion to
> [`model-overview.md`](./model-overview.md) and
> [`../analytics/crowd-levels.md`](../analytics/crowd-levels.md).

How the two prediction models produce quantiles, which quantile becomes which
user-facing number, and the calibration/consistency fixes that keep them honest.

## TL;DR

| Source | Trains | Serves | Used as |
|---|---|---|---|
| **CatBoost** (`ml-service`) | `MultiQuantile:alpha=0.5,0.8,0.95` | **q0.5** → `predictedWaitTime`; **q0.8** → crowd signal | displayed wait (median) + crowd level |
| CatBoost q0.95 | trained | **served as a distance, never as a wait**: `q0.95 − q0.5` → `uncertaintyMinutes` | the half-width of the band, a spread rather than an interval to compute off the published wait |
| **TFT** (`nf-service`) | daily **P90** target (`NF_TARGET_PERCENTILE=0.9`, StudentT) | distribution **median** → `predicted_peak` | a per-day forecast of the daily-P90 peak |

Crowd level is **always** `predicted wait ÷ typical-day-peak` downstream — never a
raw quantile. The quantiles only shape *which wait number* feeds that ratio.

### Where the q0.95 band surfaces

Until 2026-06 the row above read "not served", and that was true: the quantile was
trained and thrown away. It is a published field now, and the two statements are
easy to confuse, because **q0.95 is still never served as a wait time**. What
travels is the *distance* `q0.95 − q0.5`, read as the half-width of a band around
the median, computed in `predict.py` (`uncertainty_minutes`) and declared on
`PredictionResponse` (`main.py`), which is what keeps pydantic from dropping it.

From there two different reads publish it. The attraction endpoint serves the
**stored** rows (`wait_time_predictions.uncertainty_minutes`, via
`getAttractionPredictionsWithFallback`); `/plan/day` and the calendar serve the
**live** ml-service answer out of its Redis cache (`getParkPredictions`), and for
them the stored table is the writer's copy that no read path touches.

| Endpoint | Field |
|---|---|
| `GET /v1/attractions/…` | `hourlyForecast[].uncertaintyMinutes` |
| `GET /v1/parks/…/plan/day` | `rides[].uncertaintyMinutes` (the ride's day band; `hours[]` carries none) |
| `GET /v1/parks/…/calendar` | `headlinerForecast.rides[].uncertaintyMinutes` |

All three write `?? null` and all three answer the same way on the wire: the
`null` never leaves the process, because `ExcludeNullInterceptor` strips
null-valued keys everywhere outside `/v1/admin/*` and `?debug=true`. So a client
gets the key or gets nothing, **`value != null` is the test**, an `=== null`
branch is dead code on a public response, and truthiness is wrong for a separate
reason below.

Three rules hold across them, and all three are about what a missing number
means:

- **Absent is not narrow.** No band means the model reported no usable spread — a
  single-quantile model, a collapsed ensemble, or a source that emits no quantiles
  at all (`use_uncertainty` in `predict.py`). It is not a band of width zero and
  must not be drawn as one.
- **A literal `0` is a measurement, and it does occur.** `use_uncertainty` is
  decided once per batch, and `int(round(spread))` takes a per-row spread under
  half a minute to `0`. Such a row is forwarded as `0` rather than dropped: the
  model did report a spread, and it is smaller than the unit this field is in.
- **A near-term calendar day usually has no band, because of which model answered
  it.** `getServingDailyPredictions` merges TFT over CatBoost for days 1-60, and
  `tft_forecasts` stores `predicted_peak` and no spread, so a TFT-answered day
  carries none. It is not a rule about the date: CatBoost fills in wherever TFT
  does not reach — a ride TFT has no row for (`farCatboost`), a stalled
  nf-service caught by the 3-day staleness guard, an empty TFT result — and those
  days do carry a band inside the same 60. The TFT's StudentT head could produce
  one; persisting it is not built.

Where the endpoints genuinely differ is which row they read, not how they
serialize it. `/plan/day` falls back to the widest of a ride's **hourly** bands,
which reach 24 hours ahead; the calendar reads the day-level prediction alone. So
for today and tomorrow `/plan/day` can answer where `headlinerForecast` does not,
for the same ride on the same date. Neither is wrong: different rows, different
purposes.

The band is measured against the model's **raw** median, while the published wait
is rounded to 5 and floored at 10 (`predict.py`). It is therefore the model's
spread and not an interval to derive from the published number arithmetically —
on a ride whose raw median sits below the floor, `wait − band` goes negative.
Same on all three endpoints.

A past calendar day is built by `buildHistoricalHeadlinerForecasts` instead,
which marks it `actual: true` and writes no band at all: those are recorded
peaks, and an observation has no band (same reasoning as `/plan/day`'s observed
tier). One gap, and it is the month cache rather than this rule:
`assembleFromMonthCaches` re-derives `isToday`, `crowdLevel` and
`todayCrowdLevel` against a fresh `today` but not `headlinerForecast`, so for as
long as a cached month entry survives past park-local midnight, yesterday can
still be served with the forecast — and now its band — that was written while it
was tomorrow.

## CatBoost (ml-service) — per-purpose MultiQuantile serving

`CATBOOST_LOSS_FUNCTION = "MultiQuantile:alpha=0.5,0.8,0.95"` (`config.py:99`). One
model emits three quantiles per row; each has a distinct purpose:

- **`SERVING_WAIT_QUANTILE = 0.5`** (`config.py:102`) — the **median** is the
  honest "what wait should I expect" number shown to users (`predictedWaitTime`).
- **`SERVING_CROWD_QUANTILE = 0.8`** (`config.py:103`) — the 80th percentile is a
  **busy-calibrated** signal that drives the crowd level. (q0.95 was measured and
  rejected for crowd: it over-shoots busy days, bias +16.5.)
- **`alpha=0.95`** — trained as **headroom for the uncertainty band only**
  (`config.py:104`). It is **never** served as a display or crowd value; what
  leaves the service is the distance `q0.95 − q0.5` (see "Where the q0.95 band
  surfaces" above). ⚠️ Do not wire q0.95 to the crowd level — the team explicitly
  rejected it.

### Non-crossing (monotonic) quantiles — fix

CatBoost `MultiQuantile` does **not** guarantee `q0.5 ≤ q0.8 ≤ q0.95` per row. A
crossed row would let the crowd signal (q0.8) fall *below* the displayed median
(q0.5), and the uncertainty width (`q0.95 − q0.5`, clamped at 0) could silently
collapse. `model.py` `predict_quantiles` (≈`:665-672`) now sorts each row's
quantiles into **ascending-alpha order** and applies `np.maximum.accumulate`, so
the served quantiles are always monotonic. The median pick (`argmin|α−0.5|`) and
`predict.py`'s `_pick` are unaffected.

## TFT / nf-service — `predicted_peak` is E[daily-P90], not P90-of-distribution

⚠️ **Naming hazard, by design.** The TFT trains on a **daily-P90 target**
(`NF_TARGET_PERCENTILE=0.9`) with a StudentT `DistributionLoss` (`NF_LOSS`), but
serves the **median of the predictive distribution** as `predicted_peak`. So:

```
predicted_peak  =  E[ daily-P90 ]   (a MEDIAN forecast of a P90 target)
              ≠  P90 of the forecast distribution
```

This is intentional — "predict the *typical/expected* daily-peak for a future
date" — and it is scored apples-to-apples against the realised daily P90. A high
quantile of the P90 target was rejected (it over-inflates quiet days). Anyone
adding prediction intervals or a calibration check on top of nf-service must not
read `predicted_peak` as an upper quantile. (Docstrings in
`nf-service/forecast.py`, `db.py`, `config.py` say this inline.)

## Daily serving merge + stampede guard

`MLService.getServingDailyPredictions` (calendar + yearly source) merges **TFT for
the near term** (≤45d, headliners — where it clearly beats CatBoost) over the
**CatBoost long tail**, keyed by `(attraction|date)`. Both views share this method
so their crowd levels agree.

Because the cold CatBoost rebuild is ~15s, the method is wrapped in an in-process
**single-flight** (`common/utils/single-flight.util.ts`): concurrent calendar/yearly
requests that all miss the cache share **one** rebuild instead of stampeding it
(after a TTL lapse or a warmup eviction). *Follow-up:* extend the same guard to
`getParkPredictions("daily")` and discovery `getGeoStructure`/`getLiveStats`.

## Crowd level from predictions (the P50/P90 bridge)

The API sends the Python service **`p50Baseline`** (for the `park_occupancy_pct`
feature) and **`typicalDayPeakBaseline`** only. The old `p90Baseline` was removed
(Python never read it). `predict.py` derives the crowd level as
`predicted wait ÷ typical_day_peak` with fallback chain
`typical_day_peak → p50 → rolling_avg_7d → 30`, using the byte-identical 6-tier
thresholds (`60/89/110/150/200`) — so ML, calendar and live stay on one scale.
See [`../analytics/crowd-levels.md`](../analytics/crowd-levels.md).

## Other settings worth knowing

- `OCCUPANCY_DROPOUT_RATE = 0.50` (`config.py:148`) — fraction of occupancy
  feature values dropped during training so the model can't lean entirely on it.
- `MODEL_VERSION = "v1.1.0"` — an **ml-service** setting (not a NestJS env var).

## What still needs real-data validation

These were implemented against build + unit tests only (the dev container has no
populated DB). See
[`../development/full-db-validation-checklist.md`](../development/full-db-validation-checklist.md)
for the exact queries/invariants to run once full DB access is available:
served-quantile monotonicity on real predictions, and the crowd-level calibration
invariants.
