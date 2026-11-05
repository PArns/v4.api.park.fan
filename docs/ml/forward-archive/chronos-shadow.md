# Chronos-2 shadow in the forward archive (PAR-852)

Does the zero-shot foundation model that won parts of the offline benchmark
([chronos-2.md](../foundation-models/chronos-2.md)) also win on live, leakage-free forecasts?
The owner's switch rule needs (1) an offline win, (2) a live confirmation in this archive — ≥ 14
days and ≥ 10 parks per lead — and (3) a visible effect on a user surface. This page is (2): the
shadow capture, its inputs, scoring and switches. Nothing here is served unless
`SERVE_CHRONOS_INTRADAY` is on ([chronos-serving.md](chronos-serving.md)).

## Model and variants

`amazon/chronos-2`, Apache-2.0 for code and weights; `chronos-forecasting==2.3.2` (the version
the bench scored), HF revision pinned in `nf-service/config.py` (`CHRONOS_REVISION`). Zero-shot,
no training. The two bench variants that won are ported without the harness to
`nf-service/chronos_shadow.py`:

| Surface | Bench name | Origin | Context | Horizon asked | Archived |
| --- | --- | --- | --- | --- | --- |
| 6 `chronos_live` | `chronos2` | 10, 12, 14, 16, 18 | the park's last 8,192 15-min slots inside published windows (~170 operating days), nights left out | every window slot of today + tomorrow | next 4 h |
| 7 `chronos_grid` | `chronos2_grid` | 06:00 | regular 15-min UTC grid, last 8,192 steps (~85 days), NaN off-window | grid steps to the last window close of d7 (≤ 1,024) | d0 … d7 |

A park is one multivariate task (every ride a target variate, covariates shared). Rides forecast:
those with a truth slot in the 14 days before the origin. Quantiles 0.5 / 0.8 / 0.95, clipped at
0 and made monotone; only future slots inside a published window are returned. `waits` = q50,
`bands` = q95 − q50 (one-sided, like the served band); the grid row's `dayPeak` is the P90 of its
in-window q50, so `dayPeakMae` compares with the planner's.

## Covariates

The bench's, unchanged (`chronos2_nocov` lost 0.39–0.82 min). No weather: it was ORACLE-only in
the bench and worth < 0.12 min.

| Covariate | Production source |
| --- | --- |
| `ko_min`, `kc_min` | minutes since the published opening / until the published closing: park-wide `OPERATING` rows of `schedule_entries`, earliest opening and latest closing per service day (future days as published at the origin) |
| `in_win` (grid only) | 1 inside such a window, else 0 |
| `tod` | park-local hour + minute / 60 (`parks.timezone`) |
| `is_weekend`, `dow_sin`, `dow_cos` | park-local date, pandas `dayofweek` (0 = Monday, not Postgres' 0 = Sunday) |
| `is_holiday_primary`, `is_school_holiday_primary`, `holiday_count_total`, `school_holiday_count_total`, `is_school_holiday_any` | `holidays` via `holiday_features.park_day_holiday_features` — ml-service's implementation, vendored byte-identical into nf-service (`tests/test_chronos_shadow.py` compares the files) |
| `is_bridge_day` | derived from neighbouring days' `is_holiday_primary` (Mon before a Tue holiday, Fri after a Thu holiday) — the bench's definition |

**A missing covariate is recorded, not zero.** Each curve says whether its day's holiday flags
rested on data — `none` (no `holidays` row for the park's country), `short` (the country's last
row is before the day), `full` — in `model_version` (e.g. `chronos-2@29ec376;hol=short`). Schedule
covariates cannot be missing: a day without a published window is not forecast.

## Information cut and data reads

The context is BENCH truth (STANDBY in force at each slot midpoint, OPERATING, ≥ 5, inside the
published window) for slots that **ended by the origin**; today's readings up to the origin
instant. Schedule and holidays are known-future inputs read at the origin.

nf-service caches each park's truth slots of complete past days as parquet
(`CHRONOS_TRUTH_CACHE_DIR`, `/app/models/chronos-truth`), so a warm capture reads one new day of
`queue_data` plus today's rows. A gap of more than `CHRONOS_INLINE_FILL_DAYS` (3) goes to one
background thread that backfills park after park in 30-day reads (`statement_timeout` 120 s,
`lock_timeout` 5 s) while the request answers `status: "warming"`. After a deploy the context
reaches ~343 days back once, then grows incrementally.

## Paired scoring and the board

The scorer treats surface 6 like `park_hourly` (UC1/UC2 slot leads, band coverage) and surface 7
like `plan_day` slots (UC3 `d0` … `d7`, D3, Spearman, dayPeak) under the sources `chronos2` /
`chronos2_grid` — never into `all`, `level_*`, D1, D9 or the dayPeak ordering, which judge what is
served.

`scorePairedShadows` pairs each shadow slot with a reference **from the same origin instant** where
truth, shadow and reference all have a value, and writes per park and day into
`forecast_archive_paired`:

| Shadow | Reference | What |
| --- | --- | --- |
| `chronos2` | `served` | `park_hourly` as served — or `park_hourly_incumbent` once Chronos-2 is served |
| `chronos2` | `persistence` | the live wait in force at the origin, held flat |
| `chronos2_grid` | `served` | `plan_day` (its hour value on each quarter-hour) |
| `chronos2_grid` | `h5` | the composer shadow's plain H5 curve (surface 4) |

`GET /v1/admin/forecast-archive` returns a `chronos` section: `captureEnabled`
(`CHRONOS_SHADOW_ENABLED`), the `method` (diff = model MAE − reference MAE on identical slots,
negative = Chronos better; 95 % park-cluster bootstrap, 1,000 reps, seed 852; `minDays` 14,
`minParks` 10) and per use case × lead × model × segment the model's own MAE / bias / band
coverage plus `comparisons` with `n`, `days`, `parks`, both MAEs and biases, `diff`, `ciLow`,
`ciHigh`, `enoughData` and `verdict` (`chronos_better` | `reference_better` |
`no_significant_difference` | `insufficient_data`). `diff` pools first
((Σ|e_model| − Σ|e_ref|) / Σn). Fewer than 2 parks: no CI; fewer than 10 parks reaching 14 days:
`insufficient_data`. This verdict is live-only and can read better where the
[model comparison](model-comparison.md) says `unclear`.

## Safety, cost and switches

- **Kill switches:** `CHRONOS_SHADOW_ENABLED=false` on the API (no call at all);
  `CHRONOS_ENABLED=false` on nf-service (503). Both default on.
- **Isolation:** the call starts first in each park's capture and overlaps the served reads; an
  error or timeout (`TIMEOUT_MS` 45 s) returns nothing and the other surfaces are written as
  before. After `MAX_FAILURES_PER_RUN` = 3 failures in one run the remaining parks skip the call. A
  503 is a skip, not a failure. One warning per run.
- **TFT training:** nf-service `/train` unloads the weights and the endpoint answers 503 while a
  run is in progress; origins at 03:00 UTC lose their shadow for those ~11 minutes.
- **Footprint:** weights 0.48 GB on the volume (`/app/models/hf-cache`); ~1.8 GB VRAM peak on a
  30-ride park, capped at 25 % of the GPU (`CHRONOS_VRAM_FRACTION`); ~80 MB of installed wheels. One
  forward pass per park and origin: 2.9 s (live, 96 steps) and 3.2 s (grid, 744 steps) for a
  40-ride park on a laptop CPU; ~5 s per 30-park batch on the RTX 5080 in the bench.
- The ML overview's Chronos row reads the last run summary (`forecast-archive:chronos:last-run`) —
  [ml-overview.md](../ml-overview.md#2-components).

Each park's first shadow rows appear once its context is cached (usually the same day); the
verdict needs 14 scored days.
