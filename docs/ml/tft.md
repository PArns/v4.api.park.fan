# TFT — the daily level (nf-service)

The Temporal Fusion Transformer in `nf-service/` forecasts each ride's daily peak for the next
60 days. This page covers what it serves, its panel and covariates, the nightly run, how the API
reads it, and the measurements behind its place in the stack. Its two nightly shadows are on
[TFT grid shadows](tft-grid-shadows.md).

## What it serves

- **`predicted_peak`** per ride and park-local day, days 1–60 (`NF_HORIZON`): the median of the
  forecast distribution of the ride's daily P90 — E[daily-P90], not an upper quantile
  ([Quantile serving](quantile-serving-and-calibration.md#tft--the-served-peak-is-edaily-p90-not-an-upper-quantile)).
- **Every ride in `tft_forecasts`** (3,581 rides against 1,237 headliners on 2026-09-03), not only
  headliners: it is 28 % better than CatBoost on the other rides too
  ([long-range-forecasting.md §4](long-range-forecasting.md)). The calendar's crowd level still
  averages the headliners (`buildPredictedCrowdLevels` filters them).
- **`MLService.getServingDailyPredictions(parkId, tftDays = 60)`** merges TFT over CatBoost's daily
  rows per (ride, park-local day) — both sides keyed on the park-local date (PAR-818), since a
  CatBoost instant's UTC date can be the next day. CatBoost answers wherever TFT has no row and
  for days 61+. Readers: the calendar, `/plan/day` day levels, the yearly view
  (`getParkPredictionsYearly`), the lead snapshots behind `leadTimeMae`, the forward archive. The
  method sits behind an in-process single-flight, because a cold CatBoost rebuild takes ~15 s and
  a cache lapse would otherwise stampede it.
- **`getTftDailyPredictions`**: the freshest forecast per (ride, day) with
  `forecast_date ≥ today − 3` — a stalled nf-service falls back to CatBoost after three days rather
  than serving old forecasts. Cached in Redis per park-day (`ml:tft-daily:<park>:<days>:<today>`,
  13 h). Each row gets the measured band (`forecast_accuracy_profile.uncertainty_p95`, looked up by
  `target_date − forecast_date`), a confidence on CatBoost's scale, and `crowdLevel: "unknown"` —
  the TFT rates nothing (PAR-1028).
- The prediction generator stores **pure CatBoost** daily rows (`getRawParkPredictions`); only the
  lead snapshots record the served answer.
- **Seam at day 60 → 61:** TFT forecasts E[daily-P90], CatBoost the max of three hourly medians,
  which under-reads the peak (bias −12.7 in the May backtest), so day 61 reads lower than day 60.
  The band changes statistic there too (measured vs model spread). Neither is masked.

## Panel and target

`db.fetch_daily_peak_panel`: per ride and park-local day, `y` = P90 of the OPERATING STANDBY waits
≥ `NF_MIN_WAIT` (5), at least 3 rows, over `NF_WINDOW_DAYS` (730). Closed days are absent, never
zero-filled. Two filters hold regardless of the grid flag (PAR-1033 / PAR-1039):

- **Park-local today is excluded.** Training runs at 03:00 UTC, late morning in Tokyo and
  Shanghai: a partial morning P90 would become the last context value and a training target.
- **Heartbeat rows are excluded** — the forward archive's truth drops them too (`truthRowsSql`),
  so the target matches what it is scored against. Also in `backtest_headliners.py` and
  `backtest_horizon.py`.

The panel holds **operating days only**, and NeuralForecast reads rows by position: with
`freq="D"`, `input_size` = 90 means the last 90 operating days and step k the k-th operating day
after the last one. For a park with rest days or a closed season that spacing is not calendar
days. The calendar-day grid that fixes it runs as a shadow
([TFT grid shadows](tft-grid-shadows.md)).

## Covariates

Future covariates (`db.FUTR_EXOG`), all known for the forecast dates:

| Group | Columns |
|---|---|
| holidays | `is_holiday_primary`, `is_holiday_neighbor` (influencing regions — NL/BE carry Phantasialand), `is_school_holiday`, `is_bridge_day`, `holiday_count_total`, `days_until_holiday`, `days_since_holiday` |
| calendar | `is_weekend`, `dow_sin/cos`, `doy_sin/cos`, `season_code`, `is_peak_season`, `dow_0` … `dow_6` |
| weather | `temp_max`, `precip_mm`, `wind_max`, `is_wet` (precip ≥ 1 mm) from `weather_data`; past the stored forecast, the park's median, then the global median |

`is_peak_season` is June–August and December; south of 15° S it is December–February and June.
The tropics keep the northern months (their seasonality is in the school-holiday flags).

Static covariates (`NF_USE_STATIC`, on): country and region as integer codes. The static frame
must be passed to `nf.predict` as well as `fit`, or the run fails with "static exogenous
variables not found". No past-only covariates in the served model.

Trap: series ids are text on both sides of every join (`attractionId::text`, `parkId::text`). A
UUID on one side once made the covariate join miss — "calendar covariates done: 0 parks" — and
every holiday flag was 0.

## Model and the nightly run

| Setting | Value |
|---|---|
| Model | NeuralForecast `TFT` only (NHITS was dropped: it doubled the time and was flatter) |
| Loss | `NF_LOSS=studentt` — `DistributionLoss` StudentT, levels 80/90; the point is the median |
| Shape | `NF_HORIZON` 60, `NF_INPUT_SIZE` 90, `start_padding_enabled` (short, gappy series) |
| Training | `NF_MAX_STEPS` 500, `NF_HIDDEN_SIZE` 64, lr 1e-3, `scaler_type="robust"`, no validation set, no early stopping |
| Memory | `NF_WINDOWS_BATCH_SIZE` 128, `NF_BATCH_SIZE` 16, `NF_NUM_WORKERS` 4, container 14 g, `shm_size` 2 g |
| Chunks | `NF_PARK_CHUNK_SIZE` 10 parks, each chunk its own TFT |

- **Chunk membership (PAR-1054):** `db.fetch_park_ids` lists only parks with at least one row the
  panel could keep, ordered by (country, park id), unknown country last (`db.order_parks`). Before,
  every park with attractions in UUID order: status-only parks filled chunk slots and a new park
  shifted every later park into another chunk. Now a new park moves only the chunks of its own
  country and after. `NF_PARK_IDS` is used as given. A chunk that yields no training windows is
  skipped; its rides keep their previous `forecast_date` and the reader's 3-day window covers them.
- **No saved model.** Each chunk fits and predicts in one process (`nf.save()` fails on a
  `DistributionLoss` deep copy); the night's forecast goes to `tft_forecasts` and
  `nf_forecast.parquet`.
- **Memory history:** NeuralForecast's default `windows_batch_size` made TFT attention spike above
  14 g at fit start (cgroup OOM, independent of the series count); 128 fixed it (~3.8 g). Training
  runs in a subprocess (`train_runner.py`), so an OOM kills the runner and not the service.
- **The job (`nf-training/train-nf`, 03:00 UTC):** checks `/train/status` (a run older than
  90 min is hung → the job fails rather than skipping quietly), `POST /train`, polls until
  **this** run completes. The runner persists the forecast itself under `forecast_date` = the UTC
  day the run **started** (PAR-814), then trains the two shadows in the same process under the same
  lock; the job waits for all three (~11 min served, ~40–50 min in all, inside the job's 90-min
  deadline). nf-service kills a runner after `NF_TRAIN_DEADLINE_MINUTES` (120) and marks it failed.
  A timed-out or failed night persists nothing, and the reader falls back to the previous
  `forecast_date`. `POST /forecast` only re-persists the last **completed** run's parquet under its
  own date — it once re-wrote the previous night's forecast under the new date (231,480 rows,
  2026-10-09).
- **GPU:** RTX 5080 (Blackwell, sm_120) needs CUDA 12.8+ and PyTorch ≥ 2.7 (cu128 wheel); the
  `accelerator` is `auto`. Never start a TFT training inside the CatBoost 06:00 window.

## Storage

`tft_forecasts (attraction_id, target_date, forecast_date, predicted_peak, model_version,
created_at)`, primary key `(attraction_id, target_date, forecast_date)` — one row per origin, so
the forward record survives. A plain table created by nf-service; no index on `created_at` or
`forecast_date` (the ML-overview probe goes through the primary key). Retention 120 days by
`forecast_date`, pruned at 04:35 UTC with both shadows (PAR-1054,
[schema](../database/schema.md)).

## Why TFT serves days 1–60

CatBoost reads a far-future day through its recent-level features (`rolling_avg_*`, occupancy:
~78 % of importance on 2026-05-23), so it predicts "a typical recent weekday" and its holiday flags
carry ~0 % importance. The TFT takes holidays and the calendar as first-class known-future inputs.
On the Phantasialand PoC (2026-05-23) it put Pfingsten at 61–63 min and Saturdays at 66–70 for
Taron, where CatBoost said 40 (`very_low`).

| Measurement | Population | TFT MAE / bias | CatBoost MAE / bias |
|---|---|---|---|
| Out-of-sample headliner backtest, 2026-05-24 (`backtest_headliners.py`) | 1,295 headliner-days, all | 11.3 / +0.8 | 16.4 / −12.7 |
| | busy ≥ 40 | 15.6 / −5.7 | 29.7 / −29.1 |
| | busy ≥ 70 | 22.0 / −8.9 | 45.4 / −44.9 |
| Live matched scoreboard, lead 1 only, 14 days to 2026-06-10 | all (n 6,987) | 8.1 / −0.1 | 11.9 / −4.1 |
| | busy, P90 ≥ 40 (1,548) | 16.1 / −7.1 | 27.4 / −25.2 |
| | headliners (3,001) | 11.6 / −0.1 | 17.3 / −9.0 |

The scoreboard compared both models on the matched (ride, target date) intersection at the same
lead; the raw board had overstated TFT by scoring CatBoost's few surviving stale forecasts
(`deduplicatePredictions` deletes its forward records) against TFT's fresh ones. TFT at lead
10–13 days beat CatBoost at lead 1–2.

**Horizon:** 30 days (2026-05-24; median 72 operating days of history per headliner then) → 45
(2026-06-10, `backtest_horizon.py`: lead 31–45 still beat CatBoost at lead 1 on busy) → 60
(2026-06-20: lead 46–60 ALL 15.2 / +4.4, busy ≥ 40 16.8 / −2.8, busy ≥ 70 24.6 / −11.2, against
CatBoost at lead 1 17.3 / 27.7 / 45.4). Error by lead now: [long-range-forecasting.md §2](long-range-forecasting.md).

## Rejected (measured)

- **Quantile loss** (headliner sweep): ALL MAE studentt 11.1 → q0.7 13.0 → q0.8 15.6 → q0.9 24.9.
  TFT's busy bias is only ~−8 (CatBoost's −29), so an upper quantile over-steers, inflates the
  quiet majority and would push the calendar's quiet days up. `NF_LOSS=quantile` (with
  `NF_QUANTILE`) remains selectable.
- **More covariates** (weather, holiday distance, day-of-week one-hot): no lift (11.1 → 11.1); kept
  because they cost nothing. The busy spikes are promotion- and event-driven, and no feature
  carries that.
- **Static covariates:** 11.3 → 11.1 overall and 22.0 → 22.7 on busy ≥ 70, inside training noise.
  They are on by default anyway (`NF_USE_STATIC=True`).
- **Averaging CatBoost and TFT** (live forecasts, 21 days): worse than TFT alone on every segment
  (busy ≥ 40 18.9 vs 16.2, busy ≥ 70 32.3 vs 25.7) — both under-predict busy days, so the mean
  blends biases.
- **TFT on the intraday 15-minute surface** (`backtest_intraday_nowcast.py`, rolling origin,
  `available_mask` for closed slots): it beats naive baselines on quiet slots but not on busy ones
  — Shanghai busy ≥ 60 22.9 vs persistence 19.7 MAE on 2026-06-10. Park occupancy as a past
  covariate made it worse (ALL 9.1 → 10.1); 1500 steps were worse than 500 (10.1 → 11.6); hidden
  128 changed nothing; q0.8 fixed the busy bias (−20.4 → −2.5) only by inflating quiet slots
  (+5.8). The first hourly PoC (2026-05-23) had zero-filled closed hours, which alone biased it
  −6.6; the mask brought that to −3.2. Intraday went to PCN.
- **A horizon of a year** is not possible from under a year of history: a sequence model has to
  have seen the cycle.

## Known limits and open work

- **Known limit:** the training values are memory caps, not measured for accuracy; capacity,
  early stopping, chunking by region and a longer horizon are PAR-1118. Run
  `backtest_headliners.py` before changing any of them.
- **Known limit:** no event or promotion covariate exists (PAR-1116).
- Open work: the verdict on the grid and driver shadows, PAR-1119.
