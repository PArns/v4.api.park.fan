# What we can forecast, and how far

The trip planner asks for dates weeks or months out. This page is the measured answer to what each
model can give it: horizons read from the forecast tables rather than from configuration, TFT's
error by lead time, the traps that nearly produced wrong fixes, and why the served error is a
looked-up grid cell rather than a per-ride figure. Open it before changing `/plan/day`'s
`expectedError` / `accuracy`, the TFT serving scope, or a lead bucket.

Measured against production on 2026-09-03 (§1–§5) and 2026-09-11 (§7–§8, PAR-17). The SQL behind
them was never committed (`scripts/shape-backtest.sql` and `scripts/lead-time-error.sql` do not
exist in the repo), so these are snapshots; re-measure before quoting them as current. How
`/plan/day` presents the figures is in [the endpoint doc](../frontend/plan-day/accuracy.md).

## 1. The models and how far they reach

| Model | Horizon | Rides | Note |
| --- | --- | --- | --- |
| CatBoost daily | ~183 days | all active | `predict.py` walks the park's schedule, so the horizon is the operator's published calendar (181–362 days across parks) |
| TFT (`tft_forecasts`) | 60 days | 3,581 | covers 1,233 of 1,237 headliners and ~2,350 more rides |
| PCN (`pcn_forecasts`) | 0–1 day (mean lead 6.1 h) | 2,782 | a nowcaster, not a long-range candidate |
| shape (`shape_forecasts`) | 1–14 days | 4,893 | **retired in PAR-870 (2026-10-12)** with its service and tables |

CatBoost's own hourly generation stops at 48 h (`HOURLY_PREDICTIONS`, 24 before 2026-09-23), which
is why `/plan/day` composes anything beyond that from a daily level and a historical shape.

## 2. TFT degrades gently

Error against the realised day-P90, 2.5 M comparisons over 45 days:

| Lead | n | Actual | Predicted | MAE |
| --- | --- | --- | --- | --- |
| 1 day | 50 k | 38.7 | 35.3 | 11.66 |
| 2–3 d | 100 k | 38.7 | 35.0 | 11.93 |
| 4–7 d | 201 k | 38.4 | 34.2 | 12.36 |
| 8–14 d | 350 k | 38.4 | 33.1 | 13.22 |
| 15–30 d | 790 k | 38.2 | 31.6 | 13.92 |
| 31–60 d | 1,016 k | 38.3 | 29.7 | 15.09 |

MAE grows by 29 % while the horizon grows sixtyfold (a week later: 11.84 → 14.96, unchanged within
drift). A forecast two months out is not much worse than one for tomorrow — the fact the planner
rests on.

## 3. Conditioning on the outcome makes a calibrated model look biased

Grouped by the **actual** outcome, days that turned out busy (≥ 60 min) show 58.6 predicted against
88.1 realised — a factor of 1.50 that invites a correction table. That is regression to the mean and
appears for a perfectly calibrated model. Grouped by the **predicted** value, the only thing
available at serving time:

| Predicted band | Lead | n | Predicted | Actual | Factor | MAE raw | MAE corrected |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ≥ 60 | ≤ 7 d | 43 k | 83.3 | 82.2 | 0.987 | 21.93 | 21.89 |
| ≥ 60 | 8–30 d | 118 k | 83.1 | 82.1 | 0.987 | 23.85 | 23.78 |
| ≥ 60 | 31–60 d | 88 k | 84.0 | 84.8 | 1.009 | 25.00 | 25.04 |
| 30–59 | 31–60 d | 302 k | 42.2 | 49.0 | 1.160 | 16.57 | 16.27 |
| < 30 | 31–60 d | 626 k | 16.0 | 26.6 | 1.660 | 12.99 | 11.95 |

When the model says "busy" it is unbiased at every lead, and a multiplicative correction buys about
1 % of MAE — not worth building. The error lives where TFT calls a distant day quiet (< 30): realised
mean 26.6, MAE 13. It does not know *which* far-out days will be busy — missing signal (events,
school-break changes, opening hours; see [level drivers](level-drivers.md#5-ranked-covariates-worth-adding-to-tft--catboost)),
not a miscalibrated scale. Any board that segments by the actual value carries the same artefact in
its bias column.

## 4. TFT beats CatBoost outside the headliners too

At lead 1–2 days against the realised day-P90:

| Group | n | Actual | MAE TFT | MAE CatBoost |
| --- | --- | --- | --- | --- |
| headliners | 5,986 | 54.3 | **15.47** | 23.84 |
| non-headliners | 4,696 | 25.4 | **8.67** | 12.06 |

TFT is 35 % better on headliners and **28 % better on everything else**; serving it only for
headliners leaves that on the table for ~2,350 rides. Compute the lead park-locally on both sides:
`predictedTime::date − createdAt::date` in UTC against a park-local day cut the sample from 10,682
rows to 73.

## 5. The intraday shape: neither proposed change survived

Holdout: train before 2026-08-06, test 2026-08-06 … 09-02, every candidate rescaled to the day's
actual peak so only the shape is judged, the shape model at a fixed 7-day lead. Weighted MAE over
eight parks (12,549 comparisons): weekday-blind shape (current) **6.27**, per-weekday shape 6.64,
shape model 6.59.

- **A per-weekday shape is worse** (+6 %): the noise floor — the same weekday split into even and odd
  weeks — is 0.120 of the day peak against a weekday-vs-blind difference of 0.15. Splitting one year
  of days seven ways costs more precision than weekday specificity buys; Phantasialand, with the
  fewest days, degrades worst (10.59 vs 6.29).
- **The shape model was at parity**, not 35 % behind as its shadow board said: the board compared a
  14-day-ahead shape with CatBoost's freshest (≤ 24 h) prediction, whatever the `lead_bucket` column
  said — CatBoost's MAE was identical across buckets (8.11 / 8.12 / 8.14). `shape_forecasts.target_slot`
  was park-local; reading it as UTC made the model look 47 % worse.

The shape model was retired in PAR-870; the 15-minute planner shape is now the H5 profile
([ml-bench results](ml-bench/results-baselines.md#h5-beats-the-weekday-median-everywhere)).

## 6. The lead-time snapshot archive

CatBoost's daily accuracy beyond one day cannot be measured retrospectively: `deduplicatePredictions`
rewrites a day's prediction on every nightly run, so of its rows for passed target dates 72,795 sat
at lead 1 and 179 at 8–30 days. `prediction_lead_snapshots`
(`src/ml/entities/prediction-lead-snapshot.entity.ts`, `PredictionLeadSnapshotService`) records the
served daily level forward at lead buckets 1/3/7/14/30/60 (`LEAD_BUCKETS`) and backs `/plan/day`'s
`leadTimeMae` once a bucket has `MIN_SCORED` = 100 scored rows; since PAR-854 it snapshots the
**served** level (TFT or CatBoost, `source`) — [reader-facing accuracy](reader-facing-accuracy.md).
Rows written before PAR-854 (2026-10-11) carry no source and count for nothing, so a bucket at
lead *L* reports about *L* + 2 nights after that deploy — d60 not before ~2026-12-12; an empty far
bucket is the designed answer, not a fault. Its near buckets score the daily serving path over headliners, so
they are not comparable line for line with §2. The 15-minute curve's equivalent is the
[forward archive](forward-archive/README.md).

## 7. The served error grid

`forecast_accuracy_profile` holds the (predicted band × lead bucket) MAE grid, rebuilt nightly
(`src/ml/services/forecast-accuracy.service.ts`); `/plan/day` attaches `expectedError` per ride and
an `accuracy` block per day, with `basis: "unmeasured"` past 60 days. The buckets are
**`d1/d3/d7/d14/d30/d60`**, the distances the forward archive samples (four until PAR-17, which
answered a 10-day question with the 30-day cell). Every cell needs 500 comparisons (`MIN_SAMPLE`);
all 18 cleared it by an order of magnitude on 2026-09-11:

| Predicted | ≤ 1 d | ≤ 3 d | ≤ 7 d | ≤ 14 d | ≤ 30 d | ≤ 60 d |
| --- | --- | --- | --- | --- | --- | --- |
| ≥ 60 min | 21.5 | 21.6 | 22.9 | 24.3 | 25.1 | 25.5 |
| 30–59 min | 12.9 | 13.2 | 13.6 | 14.7 | 15.5 | 16.5 |
| < 30 min | 8.6 | 8.7 | 9.0 | 9.9 | 10.7 | 12.5 |

Every band widens by roughly the same 4 minutes over sixty days, but that is **+19 % on a busy ride
and +45 % on a quiet one** — so the served figure is a looked-up cell, never a multiplier on the
prediction. ml-bench's D8 later found the stated error under-states busy rides and over-states quiet
ones at long lead ([results-baselines.md](ml-bench/results-baselines.md#d8--productions-stated-error-d9--openness)).

## 8. Why there is no per-ride error curve

Over the same 45-day window, of 2,643 rides **not one** clears 100 comparisons in all six buckets;
the average ride's thinnest bucket holds 18.

| Bucket | Distinct leads | Rides | Max n/ride | Avg n/ride | Rides ≥ 100 | Rides ≥ 500 |
| --- | --- | --- | --- | --- | --- | --- |
| `d1` | 1 | 2,625 | 45 | 18.4 | 0 | 0 |
| `d3` | 2 | 2,634 | 90 | 36.6 | 0 | 0 |
| `d7` | 4 | 2,633 | 179 | 73.3 | 976 | 0 |
| `d14` | 7 | 2,589 | 308 | 127.0 | 1,297 | 0 |
| `d30` | 16 | 2,528 | 704 | 293.5 | 1,633 | 716 |
| `d60` | 30 | 2,324 | 1,089 | 464.1 | 1,683 | 1,055 |

`d1` spans exactly one lead distance, so a ride contributes at most one comparison per target day —
45 in 45 days. And feasibility runs opposite to usefulness: `d30` / `d60` clear 500 only by pooling
16 and 30 lead distances, while the near buckets a planner reads most cannot carry a per-ride figure.
Widening the window would average two model versions into one number. The second axis therefore
stays the predicted band, which carries most of the spread anyway (13 minutes down a column, 4
across a row).
