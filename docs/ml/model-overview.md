# ML models — what serves what

The entry page for the forecasting stack: which model answers which surface today, the
switches that change that, what runs only in the shadow, and what was retired. Read it first,
then open the reference page of the model you are changing.

## The served stack

| Surface (who reads it) | Lead | Served model | Falls back to | Switch |
|---|---|---|---|---|
| 15-min wait curve: ride page `hourlyForecast`, park page, `/plan/day` today, calendar today, favourites | 0–12 h | **PCN** q0.5, with the **persistence blend** (first hour = the current wait) | CatBoost hourly, per slot, where PCN has no forecast written in the last 3 h | `SERVE_PCN_INTRADAY` on the API |
| same, slots 45–120 min ahead | 45–120 min | **Chronos-2** `live` q50 when switched on | whatever the row above serves | `SERVE_CHRONOS_INTRADAY` (default off) |
| same, beyond PCN's horizon | 12–48 h | **CatBoost** hourly (q0.5), band × calibration factor | — | — |
| Daily level: calendar crowd levels, `/plan/day` day levels, yearly view | days 1–60 | **TFT** `predicted_peak`, every ride in `tft_forecasts` (the calendar averages the headliners) | CatBoost daily for a ride/day TFT has no row for, or when the newest TFT run is more than 3 days old | — |
| Daily level beyond TFT | days 61–365, up to the operator's published calendar (~183 days measured) | **CatBoost** daily (max of the park-local 12/14/16 h predictions) | — | — |
| `/plan/day` hours past CatBoost's 48 h (`tier: "composed"`) | to the end of the daily horizon | the served daily level × the ride's historical hour shape; the **H5 composer** (`slots[]`) when switched on | — | `PLAN_DAY_H5_COMPOSER` (default off) |

`SERVE_PCN_INTRADAY` and `SERVE_CHRONOS_INTRADAY` default to off in the code and in
`docker-compose.production.yml`; production sets them in the server's `.env`. PCN has served
intraday since 2026-06-29. What is live right now is on `GET /v1/admin/ml-overview`: the `pcn`
and `chronos` rows read `served` or `shadow` from the flags. With both flags off, every hourly
read returns CatBoost apart from the band factor.

How the API applies the overrides, which reads must stay CatBoost-pure, and the blend rules:
[Intraday serving path](intraday-serving.md). Which quantile becomes which number:
[Quantile serving & calibration](quantile-serving-and-calibration.md). The crowd level on every
row is a predicted wait ÷ the park's typical-day-peak baseline:
[Unified crowd levels](../rules/crowd-levels.md).

## The three services

| Service | Model | Hardware | Dev port | Nightly training | Other jobs |
|---|---|---|---|---|---|
| `ml-service/` | CatBoost | CPU (2 gunicorn workers; training in its own subprocess) | 8000 | `ml-training/train-model` 06:00 UTC | `predictions/generate-hourly` every 15 min, `generate-daily` 01:00, model-file cleanup 07:00, driver-level shadow 01:30 |
| `nf-service/` | TFT, Chronos-2 | GPU | 8001 | `nf-training/train-nf` 03:00 UTC (served run + two shadows, ~40–50 min) | `/chronos/forecast` on every forward-archive capture |
| `pcn-service/` | PCN (GraphWaveNet, one per park) | GPU | 8002 | `pcn-shadow/train-pcn` 08:30 UTC | `forecast-pcn` every 15 min, `score-pcn` hourly |

The training times are ordered so the GPU and memory spikes never overlap on the shared host:
TFT at 03:00, CatBoost at 06:00 (after the 04:50 VACUUM, see
[Nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md)), PCN at 08:30. Never
start a TFT training by hand inside the CatBoost window. The API reaches the services through
`ML_SERVICE_URL`, `NF_SERVICE_URL` and `PCN_SERVICE_URL` (`src/config/ml-services.config.ts`)
and does not wait for them at boot. The full job list is on
[Job queues](../architecture/job-queues.md).

## Shadows — written every night, never served

| Shadow | Writes | Compared in |
|---|---|---|
| TFT on a calendar-day grid (PAR-1041) | `tft_forecasts_shadow` | forward archive, model `tft_grid` — [TFT grid shadows](tft-grid-shadows.md) |
| TFT grid + level drivers, time-weighted target (PAR-1057) | `tft_forecasts_shadow_drivers` | model `tft_grid_drivers` — same page |
| Driver-level daily model (ml-service, 01:30 UTC, PAR-1058) | `driver_level_forecasts` | model `driver_level` — [Level drivers §8](level-drivers.md#8-the-production-shadow-par-1058) |
| Chronos-2 zero-shot (`CHRONOS_SHADOW_ENABLED`, default on) | forward-archive surfaces | [Chronos-2 shadow](forward-archive/chronos-shadow.md) |
| PCN's own boards | `pcn_intraday_comparisons`, `pcn_lead_curve_comparisons` | `GET /v1/admin/ml-comparison` — [PCN](pcn.md#shadow-boards) |
| H5 composer while `PLAN_DAY_H5_COMPOSER` is off | forward-archive surface UC3S | [`/plan/day` slots and H5](../frontend/plan-day/slots-and-h5.md) |

The independent judge of every served and shadow curve is the
[forward archive](forward-archive/README.md): it stores what was served and scores it against
BENCH-SPEC truth per lead. Its [model comparison](forward-archive/model-comparison.md)
(`GET /v1/admin/model-comparison`) pairs each candidate with what is served; the admin ML
overview ([ml-overview.md](ml-overview.md)) turns that into switch decisions. Offline candidates go through [ml-bench](ml-bench/README.md) first.

## How a model change is judged

Every switch in this stack followed the same rules, and a change that skips them gets
reverted:

- **Nothing reaches production without a measured win on busy rides or headliners**, on a
  matched population (both models scored on the same ride-slots or ride-days), in a backtest
  and then on a forward record. A switch goes behind a flag first.
- **Never judge on overall MAE.** About 72 % of hourly rows are quiet (< 20 min; one week
  measured 2026-05-23), so the overall figure hides what the busy tail does. Report MAE and bias
  per segment (quiet / mid / busy) and per lead.
- **A lever may not buy busy accuracy by inflating quiet rides.** Tilting the loss
  (upper quantiles, busy weights, blanking features) moves the busy bias and pays for it on the
  quiet majority; the model and quantile pages list where that trade was measured.
- **Feed signal, do not delete data.** Missing structure (a covariate, a graph between rides,
  a mask for closed slots) is the fix; dropping training rows or zero-filling closed hours is
  not.

## Reference pages

| Page | Covers |
|---|---|
| [CatBoost](catboost.md) | the ml-service model, its 75 features, the `/predict` contract, holiday and schedule handling |
| [CatBoost training](catboost-training.md) | training data and filters, dropouts and weights, validation, tree count, champion gate, activation |
| [Quantile serving & calibration](quantile-serving-and-calibration.md) | which quantile becomes the shown wait, the crowd level and the band; band calibration |
| [Intraday serving path](intraday-serving.md) | the API's PCN / Chronos overrides, the persistence blend, raw vs served reads |
| [PCN](pcn.md) | the intraday nowcaster: model, targets, training, forecast tick, `pcn_forecasts`, boards |
| [TFT](tft.md) | the daily level: panel, covariates, nightly run, serving, why it serves days 1–60 |
| [TFT grid shadows](tft-grid-shadows.md) | the calendar-day grid and the level-driver shadows |
| [ml-service query performance](performance-optimizations.md) | caches, rolling statistics in SQL, the uuid-cast rule |
| [What we can forecast, and how far](long-range-forecasting.md) | measured horizons and lead-time error per model |

Evaluation and scoring are on [the forward archive](forward-archive/README.md),
[ml-bench](ml-bench/README.md), [foundation models](foundation-models/README.md),
[accuracy-scoring-rule.md](accuracy-scoring-rule.md) and
[reader-facing-accuracy.md](reader-facing-accuracy.md).

## Retired

- **Shape model** (Level × Shape, `shape-service`, shadow 2026-06-29 → 2026-10-12). It rendered a
  daily level into a 15-min curve from per-ride normalised profiles:
  `smooth(base + 0.5·(crowd − base) + 0.6·(daytype − base))`, crowd = the ride's own tertile of
  daily peaks, daytype = weekend / holiday / school break / bridge / season, ±2-slot smoothing.
  Offline, at the true level, it beat the crowd-only profile on busy slots (16.0 vs 17.3 MAE,
  7 parks); a learned residual over 18 parks took another 2.8 %, weather added nothing. Its live
  board showed it losing everywhere (busy −3.9, bias −20), because it compared a 14-day forecast
  with CatBoost's ≤ 24-hour one. Removed in PAR-870 (service, `shape-shadow` queue, admin
  trigger, `shape_forecasts` and `shape_comparisons` dropped in production). Its idea — a
  historical profile scaled by a daily level — is what `/plan/day`'s H5 composer does.
- **TFT-vs-CatBoost daily scoreboard** (`nf-training/score-comparison` 07:00 UTC, the
  `catboost_daily_forecasts` snapshot and `model_comparisons`, 2026-05-23 → 2026-10-12). Nobody
  read it after the admin page moved to the forward archive's model comparison (PAR-862);
  removed in PAR-870, both tables dropped. Its measurements are on
  [TFT](tft.md#why-tft-serves-days-160).
- **CatBoost `RMSEWithUncertainty` + VirtEnsembles** (until 2026-05-23) and the single
  `Quantile:alpha=0.8` loss (until 2026-06-13): replaced by `MultiQuantile`, see
  [Quantile serving](quantile-serving-and-calibration.md#why-these-quantiles-measured).
- **GP-STGNN** as PCN's backbone: replaced by GraphWaveNet before the 2026-06-29 swap (6× faster,
  better on busy slots); still selectable through `PCN_ARCH`. See [PCN](pcn.md#measured).
