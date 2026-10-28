# ML Quantile Serving & Crowd-Level Calibration

Which quantile of which model becomes which user-facing number — the shown wait, the crowd
level, the `uncertaintyMinutes` band — why those quantiles were chosen, and how the bands are
calibrated. Open it before touching a prediction's wait, crowd level or band on any endpoint.

## TL;DR

| Source | Trains | Serves | Used as |
|---|---|---|---|
| **CatBoost** (`ml-service`) | `MultiQuantile:alpha=0.5,0.8,0.95` | **q0.5** → `predictedWaitTime`; **q0.8** → crowd signal | displayed wait (median) + crowd level |
| CatBoost q0.95 | trained | **served as a distance, never as a wait**: `q0.95 − q0.5` → `uncertaintyMinutes` | the half-width of the band, a spread rather than an interval to compute off the published wait |
| **PCN** (`pcn-service`) | pinball loss at q0.5 / q0.8 / q0.9, monotonic | **q0.5** → wait, **q0.8** → crowd signal (both through the persistence blend) | no band: `pcn_forecasts` keeps only q0.5 and q0.8, so a PCN slot serves `uncertaintyMinutes: null` |
| **Chronos-2** (45–120 min, when switched on) | zero-shot | q50 → wait, q80 → crowd, q95 − q50 → band | [Chronos-2 served](forward-archive/chronos-serving.md) |
| **TFT** (`nf-service`) | daily **P90** target (`NF_TARGET_PERCENTILE=0.9`, StudentT) | distribution **median** → `predicted_peak` | a per-day forecast of the daily-P90 peak |
| TFT band | **nothing** — `tft_forecasts` stores a point and no spread | `forecast_accuracy_profile.uncertainty_p95` → `uncertaintyMinutes` | a **measured** 95th percentile of that cell's realised residuals, not a model output (PAR-111) |

The crowd level is **always** a wait ÷ a baseline downstream — never a raw quantile. The
quantiles only decide *which wait number* feeds that ratio. How the PCN and Chronos values are
applied: [Intraday serving path](intraday-serving.md).

### Where the q0.95 band surfaces

Until 2026-06 the band was trained and thrown away. It is a published field now, and
**q0.95 is still never served as a wait time**. What travels is the *distance* `q0.95 − q0.5`,
read as the half-width of a band around the median, computed in `predict.py`
(`uncertainty_minutes`) and declared on `PredictionResponse` (`main.py`), which is what keeps
pydantic from dropping it.

Two different reads publish it. The attraction endpoint serves the **stored** rows
(`wait_time_predictions.uncertainty_minutes`, via `getAttractionPredictionsWithFallback`);
`/plan/day` and the calendar serve the **live** ml-service answer out of its Redis cache
(`getParkPredictions`), and for them the stored table is the writer's copy that no read path
touches.

| Endpoint | Field |
|---|---|
| `GET /v1/attractions/…` | `hourlyForecast[].uncertaintyMinutes` |
| `GET /v1/parks/…/plan/day` | `rides[].uncertaintyMinutes` (the ride's day band; `hours[]` carries none) |
| `GET /v1/parks/…/calendar` | `headlinerForecast.rides[].uncertaintyMinutes` |

All three write `?? null` and all three answer the same way on the wire: the `null` never leaves
the process, because `ExcludeNullInterceptor` strips null-valued keys everywhere outside
`/v1/admin/*` and `?debug=true`. So a client gets the key or gets nothing, **`value != null` is
the test**, an `=== null` branch is dead code on a public response, and truthiness is wrong for a
separate reason below.

Three rules hold across them, and all three are about what a missing number means:

- **Absent is not narrow.** No band means the model reported no usable spread — a
  single-quantile model, a collapsed ensemble, a PCN slot, or a source that emits no quantiles at
  all (`use_uncertainty` in `predict.py`). It is not a band of width zero and must not be drawn
  as one.
- **A literal `0` is a measurement, and it does occur.** `use_uncertainty` is decided once per
  batch, and `int(round(spread))` takes a per-row spread under half a minute to `0`. Such a row is
  forwarded as `0` rather than dropped: the model did report a spread, smaller than the unit.
- **A near-term calendar day carries the TFT's measured band, or CatBoost's where CatBoost
  answered.** `getServingDailyPredictions` merges TFT over CatBoost up to d59; CatBoost fills
  in wherever TFT does not reach — a ride TFT has no row for (`farCatboost`), a stalled nf-service
  caught by the 3-day staleness guard, an empty TFT result — so the two kinds of band sit side by
  side inside the same 60 days (see the seam below).

Where the endpoints genuinely differ is which row they read. `/plan/day` falls back to the widest
of a ride's **hourly** bands, which reach 48 hours ahead; the calendar reads the day-level
prediction alone. So inside that window `/plan/day` can answer where `headlinerForecast` does not,
for the same ride on the same date. Neither is wrong: different rows, different purposes.

The band is measured against the model's **raw** median, while the published wait is rounded to
5 and floored at 5 (`predict.py`; 10 until PAR-1027). It is the model's spread, not an interval
to derive from the published number arithmetically — on a ride whose raw median sits below the
floor, `wait − band` goes negative.

A past calendar day is built by `buildHistoricalHeadlinerForecasts`, which marks it
`actual: true` and writes no band: recorded peaks have none. A cached month entry that has turned
past park-local midnight drops its forecast in `assembleFromMonthCaches` rather than serving
yesterday with the prediction (and band) written while it was tomorrow.

## CatBoost — per-purpose MultiQuantile serving

`CATBOOST_LOSS_FUNCTION = "MultiQuantile:alpha=0.5,0.8,0.95"` (`ml-service/config.py`). One model
emits three quantiles per row, each with one purpose:

- **`SERVING_WAIT_QUANTILE = 0.5`** — the **median** is the honest "what wait should I expect"
  number shown to users (`predictedWaitTime`).
- **`SERVING_CROWD_QUANTILE = 0.8`** — a **busy-calibrated** signal that drives the crowd level.
- **`alpha=0.95`** — trained as **headroom for the band only**. Never served as a display or crowd
  value. ⚠️ Do not wire q0.95 to the crowd level — measured and rejected (below).

### Why these quantiles (measured)

| Date | Loss | What the measurement showed |
|---|---|---|
| until 2026-05-23 | `RMSEWithUncertainty` + VirtEnsembles | busy compression: on one recent week the bias was +1.0 on quiet (< 20, 72 % of rows) and −15.9 on ≥ 60 min; the VirtEnsembles band was ~0.17 min wide, so it carried nothing |
| 2026-05-23 | `Quantile:alpha=0.8` + busy weighting (`CATBOOST_BUSY_WEIGHT`) | ≥ 60 bias −15.3 → −4.3, ≥ 60 MAE 20.7 → 18.4, overall 4.99 → 5.17; α 0.9 over-corrected (+3.9 busy, +5.0 quiet); the busy lift came from α, the weighting improved calibration |
| 2026-06-10 | same, live | quiet (< 30, 92 % of slots) bias **+11.2** in `prediction_accuracy` over 7 days — far above the +2.7 of the hold-out: one point forecast cannot serve the quiet display and the busy signal |
| 2026-06-13 | `MultiQuantile` 0.5 / 0.8 / 0.95 | 21-day hold-out, 392 k slots: the median cut ALL MAE 5.15 → 3.93 and quiet 4.03 → 2.39 (bias +3.46 → +0.60); q0.8 stayed busy-calibrated (≥ 60 bias +0.62); q0.95 overshot busy (bias +16.5, MAE 21.5 vs 14.0) |

The busy-tail MAE that is left is the variance of rare spikes, not a bias a quantile can fix.
The same skew trade-off held for the TFT on the intraday surface (StudentT / MQ / Huber medians
best on quiet and worst on busy, Tweedie in between, q0.8 the reverse) — so the answer for every
model is per-purpose serving from one distribution, not a tilted loss.

### Non-crossing (monotonic) quantiles

CatBoost `MultiQuantile` does **not** guarantee `q0.5 ≤ q0.8 ≤ q0.95` per row. A crossed row would
let the crowd signal fall *below* the displayed median, and the band (`q0.95 − q0.5`, clamped at
0) could silently collapse. `model.py` `predict_quantiles` sorts each row's quantiles into
ascending-alpha order and applies `np.maximum.accumulate`, so the served quantiles are always
monotonic. PCN does the same (`metrics.enforce_quantile_monotonicity`).

### Live coverage of q0.8 / q0.95 (PAR-912)

The raw q0.8 and q0.95 of every hourly slot are stored in `prediction_accuracy.predicted_q80` /
`predicted_q95` (whole minutes, not the posted-wait rounding), and their coverage on scored slots
is on the ML dashboard (`performance.operating`). See
[Which compared slot is a forecast error](accuracy-scoring-rule.md#quantiles).

## TFT — the served peak is E[daily-P90], not an upper quantile

⚠️ **Naming hazard, by design.** The TFT trains on a **daily-P90 target**
(`NF_TARGET_PERCENTILE=0.9`) with a StudentT `DistributionLoss` (`NF_LOSS`), but serves the
**median of the predictive distribution** as `predicted_peak`:

```
predicted_peak  =  E[ daily-P90 ]   (a MEDIAN forecast of a P90 target)
              ≠  P90 of the forecast distribution
```

It forecasts the typical daily peak and is scored against the realised daily P90. A high
quantile of the P90 target was rejected (it over-inflates quiet days, [TFT](tft.md#rejected-measured)).
Anyone adding intervals or a calibration check on top of nf-service must not read
`predicted_peak` as an upper quantile (the docstrings in `nf-service/forecast.py`, `db.py`,
`config.py` say this inline).

## The TFT band is measured, and it does not match CatBoost's at the seam

**PAR-111, 2026-09-13.** The TFT has no quantiles to give: `tft_forecasts` holds `predicted_peak`
and no spread, so every calendar day the TFT answered reached a visitor with `uncertaintyMinutes`
absent — **155 of 155** headliner entries at Europa-Park inside the TFT horizon that day — while
CatBoost brought a band from day 59 on.

**What fills it.** `ForecastAccuracyService.rebuild()` (nightly, `accuracy-profile-cron`
03:10 UTC) measures the TFT's realised error per predicted band × lead bucket and stores
`uncertainty_p95`, the 95th percentile of the **signed** residual `actual − predicted_peak` — the
distance above the served number, which is the direction a visitor plans against.
`getTftDailyPredictions` looks the cell up per row and serves it.

**Three load-bearing details.** The column is **nullable**: the table has held rows since PAR-17
and its schema comes from `synchronize`, so `ADD COLUMN real NOT NULL` would be rejected, and a
`DEFAULT 0` would publish a zero-wide band that reads as certainty. A **negative** measured
percentile is stored as NULL rather than clamped to 0 — a cell that systematically
over-forecasts has a different fact about it, not a narrow band. And the lookup buckets by
**`target_date − forecast_date`**, the key `rebuild()` measures on, not the distance from today:
with the 3-day staleness guard, a stalled nf-service would otherwise get the band of a distance
three days shorter than the one it forecast at.

**It is calibrated.** Fitted on target days −45…−15 and checked on −15…−1, all 18 cells cover
**92.3–97.9 %** of realised days against a nominal 95 %:

```
band   bucket  n_eval   width  coverage      (MAE width, for contrast)
busy   d1        1517    45.9    94.8 %         20.8 → 81.1 %
busy   d60      42918    61.0    97.5 %         25.0 → 86.0 %
mid    d1        4387    30.1    93.1 %         12.5 → 78.2 %
mid    d60     125225    45.7    97.3 %         16.8 → 84.5 %
quiet  d1        7011    23.8    92.3 %          8.1 → 67.8 %
quiet  d60     182939    38.1    97.8 %         13.2 → 79.2 %
```

An MAE-wide band covers a typical miss (68–89 %) and is already served by `/plan/day` under its
own name, `expectedError`. Two names, two statistics.

**The finding: CatBoost's daily band is under-dispersed.** CatBoost's `uncertaintyMinutes` is
`q0.95 − q0.5` of its own predictive distribution. Measured the same way over the same 45 days
(n = 5,112; `deduplicatePredictions` keeps one row per target day, so effectively lead 1):

| predicted band | n | mean band | MAE | q0.95 of residual | **coverage** |
|---|---|---|---|---|---|
| busy | 224 | 30.2 | 35.2 | 80.6 | **56.7 %** |
| mid | 1,180 | 19.2 | 22.4 | 58.0 | **54.8 %** |
| quiet | 3,708 | 12.3 | 15.7 | 45.5 | **52.7 %** |

A band named for alpha = 0.95 that contains 53–57 % of outcomes: the model's trained spread,
faithfully reported, and its magnitude lands on the MAE row rather than the q0.95 row.

**So the calendar steps down at day 60** (quiet ~38 → ~11). The step is two statistics meeting,
not the near term being less certain. Do not close it by narrowing the measured side; the fix is
to put CatBoost's side on the same empirical basis, and that is not possible yet — its error past
60 days has never been measurable (`deduplicatePredictions` again), and
`prediction_lead_snapshots` cannot report the `d60` bucket before **2026-11-02**. Tracked as
PAR-167.

**`confidence` moved too.** The TFT path emitted a flat `0.7` — invented, and on the wrong scale
(`predict.py` emits 30–100). It now runs CatBoost's formula (60 % distance term, 40 % spread term,
each floored at 30) fed from the measured band (`MLService.dailyConfidence`).

## CatBoost hourly band: split-conformal factor (PAR-1060)

The daily seam above is still open. The **hourly** band is calibrated, because
`prediction_accuracy` has scored hourly slots to fit on.

- **Target.** `uncertaintyMinutes` is one-sided (`q0.95 − q0.5`, the distance above the served
  wait), so the target is `P(actual − predicted_wait_time ≤ k · band) = 0.95` — the reading the
  forward archive's D8 scores (`truth ≤ pred + band`) and the TFT band is calibrated to. Not
  `|actual − q0.5| ≤ k·band`: that two-sided reading would ask for roughly a q0.975 and widen
  every band beyond what it claims.
- **Fit.** Daily `prediction-accuracy/calibrate-bands`, 04:20 UTC
  (`PredictionAccuracyService.calibrateCatboostBands`). Last 28 days, `scoredSlotSql` AND
  `score_class = 'scored'`, hourly, `served_source = 'catboost'`, raw band > 0. The SQL returns a
  histogram of the score `(actual − predicted) / band` in 0.05 bins per lead bucket × segment;
  `fitBandCalibration` (`src/ml/utils/catboost-band-calibration.util.ts`) takes the smallest bin
  edge reaching 95 %. Lead buckets `h0-2` / `h2-6` / `h6-24` / `h24+` from
  `target_time − prediction_time` (the run that last wrote the slot); segments quiet < 30 ≤ mid
  < 60 ≤ busy on the predicted wait. A cell with fewer than 500 slots (`BAND_MIN_SAMPLE`) borrows
  its **lead bucket's** pool; a bucket pool under 500 gives no factor, and the band is served
  raw. A lead bucket never borrows from another: `uq_pa_attraction_target` keeps one row per slot
  and the latest run overwrites `prediction_time`, so nearly every row lands in `h0-2`, and a
  short-lead factor on a long-lead band would leave it under-covered while reading as calibrated.
  A non-positive factor (the cell over-forecasts so far that 95 % of actuals sit under the median)
  is no factor, never a zero band.
- **The raw band is stored.** `prediction_accuracy.uncertainty_minutes` is the band CatBoost
  produced, before the factor, beside `predicted_q95`. The fit reads only that, so it never fits
  its own output. NULL on rows written before PAR-1060.
- **Deploying a column on `prediction_accuracy`** (plain table, ~16.7 M rows): `ADD COLUMN` is
  catalogue-only, but `synchronize` takes ACCESS EXCLUSIVE without a lock timeout — deploy with no
  long query open on the table and outside 04:00–07:30 UTC.
- **Serve.** Redis `ml:band-calibration:catboost-hourly:v1`, TTL 3 days (two missed runs).
  `MLService.serveHourlyRows` runs the PCN/Chronos overrides and then multiplies
  `uncertaintyMinutes` (rounded to whole minutes) on the hourly rows that stayed CatBoost — never
  a PCN slot (band null) or a Chronos slot (its own band). Applies on all four hourly read paths,
  with or without the override flags. No table, an expired one or no factor for the slot → the
  raw band. The stored `wait_time_predictions` rows stay raw.
- **Read it.** `GET /v1/admin/ml-overview` → `bandCalibration`: per cell `n`, `k`, `source`
  (`cell`/`pooled`), `coverageRaw`, `coverageCalibrated` (in-sample, so ≈ 0.95 by construction
  where the cell has its own k). The out-of-sample check is the forward archive's D8
  (`nBandCov / nBand` on `park_hourly`).

### The served factor is clamped to [1, 2] (PAR-1083)

`bandFactor` serves `min(2, max(1, k))` (`BAND_FACTOR_MIN` / `BAND_FACTOR_MAX`); the table in Redis
and on `/v1/admin/ml-overview` keeps the fitted `k`.

- **Why.** The fitted `k` ranged 0.25 … 4.2 inside one lead bucket, so a ride's band jumped
  between neighbouring slots (Taron 6 → 101 min). The factor was absorbing the **median's bias**:
  a cell whose median under-forecasts needs a wide band to reach 95 %, one that over-forecasts
  reaches it with almost none. A band is a spread, not a bias correction — so it never narrows the
  model's own band and never more than doubles it.
- **Measured (calibration window, variant F of PAR-1083).** Coverage 0.957 (unclamped 0.949); mean
  live band 62 → 34 min; slot-to-slot jumps ≥ 40 min 1437 → 58; largest jump 199 → 74 min.

## Crowd level from predictions

The API sends the Python service `typicalDayPeakBaseline` and `p50Baseline`; the old `p90Baseline`
was removed (Python never read it). `predict.py` `crowd_level_for` rates the q0.8 wait against
the typical-day-peak baseline, the P50 baseline when that is missing, and answers `unknown`
(baseline 0) without either — never a placeholder `moderate`, never the ride's own
`rolling_avg_7d` (PAR-943). Thresholds 60 / 89 / 110 / 150 / 200 %, the same as
`determineCrowdLevel()`. The PCN and Chronos overrides re-tier the row against the same carried
baseline, only when it is > 0. Rules: [Unified crowd levels](../rules/crowd-levels.md),
[Crowd Levels](../analytics/crowd-levels.md).

## Known limits

- **Known limit:** the `h2-6` bucket never gets a factor and the longer buckets stay raw, because
  `prediction_accuracy` keeps only the last writer per slot; the lead-preserving source is PAR-658.
- **Known limit:** at ≥ 12 h lead the median itself runs −37.5 min on mid and −63.4 min on busy
  slots, a level error the clamped factor no longer hides (PAR-1140).
- The daily band seam at day 60 is PAR-167, above.
