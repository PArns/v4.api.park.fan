# CatBoost training

How the nightly CatBoost model in `ml-service/` is trained, validated, gated and activated: the
training rows and their filters, dropouts and weights, the hold-out, the tree count, the
champion gate and the file sweep. The model and its features are on [CatBoost](catboost.md).

## The nightly run

`ml-training/train-model` at 06:00 UTC (`MLTrainingProcessor`) calls the ml-service's
`POST /train` with its time budget (`timeBudgetSeconds` = `ML_TRAINING_TIMEOUT_MINUTES`, default
90), polls `/train/status`, then registers, gates and activates the result. The ml-service runs
the training in a subprocess (`train_standalone.py`), so an OOM kill takes down that process and
not the serving workers. A manual run goes through the same queue:
`POST /v1/admin/train-ml-model`. A direct `POST /train` on the ml-service writes model files and
nothing else — no `ml_models` row, no activation.

`ml-training/cleanup-models` at 07:00 UTC sweeps old model files (below). The 04:50 UTC
`db-maintenance/vacuum-churn-tables` job runs before the training so it does not OOM on bloated
tables; nothing large belongs in 05:50–07:30 UTC
([Nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md#the-vacuum-window-vacuum-churn-tables-0450)).

## Training rows

`db.TRAINING_DATA_SQL` builds one row per ride and UTC hour:

- **Window:** from the first row (2025-12-24) to now, capped at `TRAIN_LOOKBACK_YEARS` (2).
- **Readings:** STANDBY, `waitTime ≥ 5`, no heartbeat rows. The label is the hourly median of the
  **OPERATING** readings only (PAR-1052): a CLOSED, REFURBISHMENT or DOWN row with a standing wait
  (984 rows ≥ 5 min in a 2-day sample) is not a queue. DOWN readings still count for
  `downtime_minutes_today`. Why 5 and not 10: Phantasialand and others legitimately open with
  5-minute waits while areas open in turn; a 10-minute floor made the model under-predict the
  morning. 0- and 1-minute placeholders stay out.
- **Parks:** only parks with a typical-day-peak baseline (`park_p50_baselines."typicalDayPeak"`
  not null, ≥ 30 operating days of valid headliner data) — the thin-data gate that keeps ~19
  noisy parks out of training and out of its metrics.
- **Days** (`TRAINING_DAY_FILTER_SQL`, park-level `schedule_entries` on the park-local date):
  a confirmed OPERATING day; a day with no schedule row or an UNKNOWN one when the operating-day
  heuristic says the park was open (≥ 3 readings that day and ≥ 10 % of them ≥ 5 min); a day
  `park_day_operations.measured_operation` says the rides ran, whatever the schedule says
  (PAR-1054). CLOSED days stay out otherwise.
- **Frozen readings outside the window (PAR-1052).** 11.8 % of the rows (49 parks) lay more than
  60 min outside the published window. A row that far out **and** with a `lastUpdated` more than
  120 min older than its timestamp (or NULL) is dropped (`db.FROZEN_OUT_OF_WINDOW_SQL`, shared
  with serving's `_SERVING_QUEUE_FILTER`). Why 120: frozen re-writes were hours to ~30 days old
  (Ocean Park, Paultons; pre-opening rows at Kennywood, Traumatica, Toverland), real after-close
  readings at Halloween events 13–64 min. Fresh out-of-window readings stay and set
  `after_hours_reading = 1`, which sets `is_park_open = 1`. Only the row's own date's window is
  asked, so a stale slot inside the previous day's past-midnight window is dropped too.

Then `train.remove_anomalies` drops, per ride:

| Rule | Condition | Why |
|---|---|---|
| sensor drop | wait ≤ 5 while the centred 7-row median is > 20 | a busy ride does not fall to 5 for one hour; an API reset does |
| outlier | wait ≥ 400 | API glitches |
| technical heartbeat | wait ≤ 5, the whole park's median at that timestamp ≤ 5, and the hour is before opening + 1 h (opening from the schedule, 09:00 when unknown) | placeholder values before rides open; later in the day a 5-minute park is a quiet day and stays |

## Dropouts and sample weights

Far-future rows are served without the live features, so training hides them on a share of rows:

| Setting | Default | What it replaces |
|---|---|---|
| `OCCUPANCY_DROPOUT_RATE` | 0.50 | `park_occupancy_pct` → the park's weekday × hour mean |
| `ROLLING_AVG_DROPOUT_RATE` | 0.40 | `avg_wait_last_24h` |
| `ROLLING_1H_DROPOUT_RATE` | 0.60 | `avg_wait_last_1h` |
| `ROLLING_7D_DROPOUT_RATE` | 0.30 | `rolling_avg_7d` → the weekday/weekend average |
| `HOLIDAY_DROPOUT_RATE` | 0.70 | every history feature on holiday rows, see below |

Higher rates (occupancy 0.75, 24 h 0.65, 7 d 0.55) destroyed the short-term signal (R² ≈ 0) and
were reverted. Interaction features are recomputed after the dropout.

**Holiday dropout (PAR-1051).** On 70 % of the rows with `is_holiday_primary` or
`is_school_holiday_primary`, the history features become what serving feeds that row when it is
forecast from **before the break** (`train.holiday_pre_break_values`): origin = local midnight
after the park's last non-holiday day before the row; the rolling 7d / weekday / weekend / 28d /
90d means, trend, volatility, base-hour lags and downtime from the ride's last row before it;
`avg_wait_same_dow_4w` / `avg_wait_last_24h` from the 1–4-weeks-back slots that precede the
origin, else its `rolling_avg_7d`; `avg_wait_last_1h` the slot a week back if known, else the
weekday/weekend mean; velocity 0; occupancy the park's same weekday-and-hour mean over the
8 weeks before the origin. Before, it took 7-day windows that inside a multi-day break already
hold the break, and left 28d/90d alone, so the flags never had to carry anything: on
`catboost_v20261024_0600` `is_holiday_primary` 0.00, `is_school_holiday_primary` 0.05,
`holiday_count_total` 0.00, `is_bridge_day` 0.01 against `park_occupancy_pct` 16.84 and
`avg_wait_last_1h` 16.42 (importance, %).

**Weights** (`train.compute_sample_weights`, applied to the fit and to the refit):

- accuracy feedback (`ENABLE_SAMPLE_WEIGHTS`): `1 + (MAE / 20) · 0.5`, MAE per ride from
  `attraction_accuracy_stats` (10 when unknown), clipped to 1–2;
- busyness (`CATBOOST_BUSY_WEIGHT`): `clip(sqrt(wait / 20), 0.4, 2.5)`, so the quiet majority
  does not dominate the loss.

## Validation

- **Chronological hold-out:** the newest 30 days are split off first and never seen by the
  early-stopped fit.
- **Weekly-block validation:** the remaining pool is grouped into ISO weeks; about 20 % of them
  (at least one) are the validation and early-stopping set, so both sides span every
  operational phase. Each week's side is drawn from
  `(CATBOOST_RANDOM_SEED, week)` alone (`select_validation_weeks`, PAR-1040), so a week keeps its
  side when the window slides by a day and the gate compares MAEs measured on the same data.
  Dropout is seeded the same way.
- **Hold-out score, then refit (PAR-815):** the early-stopped model is scored on the hold-out
  (`holdout_metrics`: MAE, busy MAE on actual ≥ 60, RMSE/MAPE/R², counts); then the **served**
  model is refitted on all rows — pool plus hold-out — with the early-stopped tree count, the
  same dropout and weights. Without it the served model never learned the newest 30 days and a
  season change reached it a month late. `TRAIN_REFIT_ON_ALL_ROWS` (default on) switches it off.
  The refit is **skipped** (logged, `refit_skipped` in the metadata and `/train/status`) when
  elapsed time + fit time × (refit rows / fit rows) would come closer than
  `TRAIN_REFIT_SAFETY_MARGIN_SECONDS` (600) to the budget; the early-stopped model is served that
  day. The hold-out score describes the early-stopped model, not the refitted one.
- **Hold-out at served leads (PAR-1040).** The hold-out above scores each row with its real lags —
  a 1-hour nowcast, while the model is served 12 h to a year ahead. `holdout_metrics.origin_leads`
  re-scores the same rows **as known at the origin** (target − lead) for 12 h / 24 h / 7 d / 61 d
  (`as_known_at_origin`): rolling statistics, base-hour lags and downtime from the ride's last row
  at or before the origin; `avg_wait_same_dow_4w` only where all four weekly lags precede the
  origin (beyond 28 d serving's last-year rule); `avg_wait_last_1h` = the target hour a week
  earlier if known, else the weekday/weekend mean; velocity 0; occupancy from a (park, weekday,
  hour) profile of the pool; beyond 16 d (`ORIGIN_WEATHER_FORECAST_DAYS`) climatology weather,
  beyond 30 d (`ORIGIN_SCHEDULE_KNOWN_DAYS`) no published hours. **The 12 h / 24 h / 7 d figures
  are a lower bound on the served error:** they keep observed weather and the final schedule.
  Stored in the `.pkl` metadata, on `GET /model/info/{version}` as `holdoutMetrics` and in
  `ml_models.holdoutMetrics`. The gate does not read it (an owner decision). Approximations: the
  occupancy profile averages all rides (serving: headliners, last 8 weeks); no served lead falls
  between 7 and 28 days.

## Tree count and early stopping (PAR-902)

`CATBOOST_LEARNING_RATE = 0.1`, `CATBOOST_ITERATIONS = 1500`, `CATBOOST_EARLY_STOPPING_ROUNDS = 100`
(od_type `Iter`), `use_best_model=True`, eval set = the validation weeks, eval metric = the
training loss. At the old 0.03 × 2000 the validation loss was still falling at the cap
(`best_iteration` 1999 on every run), so early stopping never fired and every run paid the full
cap twice, fit (~28 min) and refit (~36 min).

Measured on the production frame of 2026-10-15 (4.44 M rows, same fetch, features, dropout and
weighting as the nightly run, no early stopping, losses logged per tree):

| config | trees | MultiQuantile loss val / hold-out | MAE val | MAE hold-out (30 d) | busy MAE hold-out (≥ 60 min) |
|---|---|---|---|---|---|
| lr 0.03 (old) | 2000 | 1.599 / 1.552 | 4.55 | 4.36 | 15.61 |
| lr 0.03 | 3000 | 1.559 / 1.531 | 4.43 | 4.31 | 15.24 |
| lr 0.1 | 1000 | 1.579 / 1.561 | 4.49 | 4.38 | 15.40 |
| **lr 0.1 (new)** | **1500** | **1.550 / 1.542** | **4.39** | **4.33** | **15.17** |
| lr 0.1 | 2000 | 1.533 / 1.536 | 4.35 | 4.30 | 15.05 |
| lr 0.15 | 1500 | 1.550 / 1.557 | 4.41 | 4.34 | 15.23 |

The old row reproduces production (hold-out MAE 4.36 in `v20261015_0600`). **Not a leak:** the
random-week validation set and the out-of-time hold-out improve in step, so a chronological eval
set would not have stopped training earlier — it stays the random weeks, which keeps the gate's
validation MAE the same measurement as the champion's. **Under-fitting, not a broken detector:**
at 0.03 both curves still fall at 3000 trees. 0.1 × 1500 beats 0.03 × 2000 on every column with
25 % fewer trees; 0.15 converges to the same place. Even at 0.1 the detector would fire only
around tree 2500, so **the cap sets the tree count, and the cap is a time decision**: fit
(~0.84 s/tree) and refit (~1.07 s/tree) scale linearly with it, ~57 min in all at 1500. Raise it
only together with the training timeout.

What early stopping did is recorded: `metadata["early_stopping"]` (`best_iteration`,
`tree_count`, `trees_built`, `iterations_cap`, `stopped_early`, `od_wait`, `learning_rate`,
`eval_metric`, `best_validation_loss`); `training_timings.fit` (also in `/train/status`) carries
`trees` and `stopped_early`. A run that hits the cap logs a warning.

## Registration, gate and activation (PAR-815)

- The processor reads the version's metrics from `GET /model/info/{version}` — that version's
  own `metadata_<version>.pkl` — never from `/model/info`, which describes whatever model the
  answering worker still has loaded (once that registered v20261003's MAE 5.06 under v20261006,
  whose own file said 4.43). A version without saved files and metadata is not registered.
- **Gate:** the challenger is rejected only if its validation MAE is more than 1.25× the
  champion's (`REGRESSION_TOLERANCE`). It is a disaster guard (an untuned GPU port reached
  MAE 26 vs 5.6), not a contest: a fresher model wins on normal day-to-day variance.
- **Activation:** `train_standalone.py` writes the model file, its metadata and the training
  status — never the active-version sentinel. For an accepted version the processor retires the
  old champion and inserts the new row in **one** transaction, then calls `POST /model/reload`,
  which loads the DB-active version and writes `/app/models/active_version.txt`; every other
  worker picks the sentinel up on its next `/predict`, and the boot path (`_load_active_model`)
  reads the same DB row. If the reload does not confirm the new version, the DB rolls back to the
  previous champion (one transaction), a best-effort reload points the sentinel back, and the job
  fails. A rejected, failed or timed-out run never changes what is served.
- `fetch_active_model_version` orders by `trainedAt DESC`, so even two active rows resolve
  deterministically. There is deliberately **no partial unique index on `isActive`**: with
  `synchronize: true` in production, an index that cannot be built would stop the API from
  booting.

## Model files (PAR-815)

The API keeps the newest 30 `ml_models` rows plus the active one (`MODELS_TO_KEEP`); older rows
lose their files and their row. Files are listed and deleted through the ml-service
(`GET /models/files`, `DELETE /models/files/{version}`) because the API container does not mount
the models volume. Versions on disk with no row (failed, timed-out or unregistered runs) are
deleted once two days old. The ml-service refuses unsafe version names and the DB-active,
sentinel, loaded and in-training versions. The sweep deletes nothing (and logs an error) when
`ml_models` returns no rows, or when it would delete more than 250 orphans or more than 90 % of
the versions on disk (`ORPHAN_MAX_PER_RUN`, `ORPHAN_MAX_SHARE`).

## Time and memory budget

The window grows daily until it reaches two years, and so does the run: 1682 s on 2026-09-04,
2520 s on 2026-10-07. Every run records per-phase durations and row counts (`fetch`, `features`,
`pool`, `fit`, `refit`, `total_seconds`) as `training_timings` in the metadata, in
`/train/status` (`timings`) and in `/model/info/{version}` (`trainingTimings`) — read those
before changing the budget.

- **Opt-in row cap, not enabled:** `TRAIN_MAX_ROWS` (default 0 = off) bounds the pool after the
  hold-out: the newest `TRAIN_FULL_RESOLUTION_DAYS` (90) of the pool stay whole, older rows are
  thinned by a seeded uniform sample so every month keeps its share. It runs after feature
  engineering and shrinks only the fit, not fetch or feature memory. Enable it only after an A/B
  (full vs capped) on an out-of-time window shows no accuracy loss.
- **Memory:** container `mem_limit` 20 g; 2 gunicorn workers (~4 g with `max_requests`
  recycling); the training subprocess ~5 g Python/data plus `CATBOOST_USED_RAM_LIMIT` 8 g.
  `CATBOOST_TRAINING_RESERVED_CORES` (2) keeps cores free so `/predict` does not time out while
  training uses the rest.
- **CPU only.** `CATBOOST_TASK_TYPE=GPU` exists, but the CPU regularisers (`rsm`,
  `min_data_in_leaf`) do not carry over; without them the GPU model over-fitted (MAE 5.6 → 26,
  R² −1.15). The `CATBOOST_GPU_*` settings are the re-expressed regularisation, untested in
  production.

## After a training

`GET /v1/ml/models/metrics-history` shows the trend; per version `GET /model/info/{version}`
(`metrics.feature_importances` in %, `holdoutMetrics`, `trainingTimings`). Since PAR-1051 the
holiday flags should rise clearly above 0 % importance; compare `holdoutMetrics` with the
previous model before reading the gate's verdict.

## Rejected (measured)

- **Forcing holiday lift by blanking level features** (2026-05-23, three runs: 4 columns NaN,
  all level columns NaN, all NaN plus holiday ×3.5 / bridge ×3 / neighbour ×2.5 weights): holiday
  importance stayed at 0.00–0.02 % and validation MAE rose from 4.99 to 5.90–6.02; the model
  substituted `rolling_avg_weekday`, then `attractionId`, then `volatility_7d` for the level. The
  PAR-1051 dropout above replaces features with what serving really sees instead of NaN.
- **Another gradient-boosting library** (XGBoost, LightGBM): same family, same crowding-out of
  rare flags; not tried.
- Learning rate 0.03 and caps of 1000 / 2000 trees: under-fitting, table above.

## Open

- `TRAIN_MAX_ROWS` A/B never run.
- The gate reads validation MAE only; whether it should read `holdoutMetrics.origin_leads` is an
  owner decision.
- `CATBOOST_CURATED_RIDE_TYPE` waits for curated coverage of the headliners
  ([CatBoost](catboost.md#ride-type-flags-par-1045)).
- Whether PAR-1051 moved the holiday importances has not been recorded here yet.
