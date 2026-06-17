# ML Quantile Serving & Crowd-Level Calibration

> Status: current as of 2026-06-17. Companion to
> [`model-overview.md`](./model-overview.md) and
> [`../analytics/crowd-levels.md`](../analytics/crowd-levels.md).

How the two prediction models produce quantiles, which quantile becomes which
user-facing number, and the calibration/consistency fixes that keep them honest.

## TL;DR

| Source | Trains | Serves | Used as |
|---|---|---|---|
| **CatBoost** (`ml-service`) | `MultiQuantile:alpha=0.5,0.8,0.95` | **q0.5** → `predictedWaitTime`; **q0.8** → crowd signal | displayed wait (median) + crowd level |
| CatBoost q0.95 | trained | **not served** | uncertainty band width only (headroom) |
| **TFT** (`nf-service`) | daily **P90** target (`NF_TARGET_PERCENTILE=0.9`, StudentT) | distribution **median** → `predicted_peak` | a per-day forecast of the daily-P90 peak |

Crowd level is **always** `predicted wait ÷ typical-day-peak` downstream — never a
raw quantile. The quantiles only shape *which wait number* feeds that ratio.

## CatBoost (ml-service) — per-purpose MultiQuantile serving

`CATBOOST_LOSS_FUNCTION = "MultiQuantile:alpha=0.5,0.8,0.95"` (`config.py:99`). One
model emits three quantiles per row; each has a distinct purpose:

- **`SERVING_WAIT_QUANTILE = 0.5`** (`config.py:102`) — the **median** is the
  honest "what wait should I expect" number shown to users (`predictedWaitTime`).
- **`SERVING_CROWD_QUANTILE = 0.8`** (`config.py:103`) — the 80th percentile is a
  **busy-calibrated** signal that drives the crowd level. (q0.95 was measured and
  rejected for crowd: it over-shoots busy days, bias +16.5.)
- **`alpha=0.95`** — trained as **headroom for the uncertainty band only**
  (`config.py:104`). It is **never** served as a display or crowd value. ⚠️ Do not
  wire q0.95 to the crowd level — the team explicitly rejected it.

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
