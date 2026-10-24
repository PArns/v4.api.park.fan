# Machine Learning Service

## Overview

The ML Service is a standalone Python application responsible for predicting wait times for attractions. It exposes a FastAPI interface for the main NestJS application to query.

## Model Architecture

- **Algorithm**: CatBoost with a multi-quantile loss (`loss_function = MultiQuantile:alpha=0.5,0.8,0.95`, Gradient Boosting on Decision Trees). One model emits three quantiles: **q0.5** is served for the honest wait-time display, **q0.8** is served as the crowd-level signal (busy-calibrated), and **q0.95** is never served as a wait — only as a distance from the median (`q0.95 − q0.5` → `uncertaintyMinutes`), which is the band a client draws. See [Quantile Serving & Calibration](./quantile-serving-and-calibration.md#where-the-q095-band-surfaces).
- **Problem Type**: Regression (Predicting wait time in minutes)
- **Input Features**:
  - `day_of_week`: 0-6 (Mon-Sun)
  - `is_weekend`: Region-aware (e.g., Fri/Sat in Middle East, Sat/Sun elsewhere)
  - `hour_of_day`: 0-23
  - `cyclic_time`: sin/cos encodings for hour, month, day_of_week
  - `is_holiday`: Boolean (Regional & National holidays)
  - `is_school_holiday`: Boolean (Region-specific)
  - `weather_condition`: Categorical code
  - `temperature`: Numerical (Celsius)
  - `temperature_deviation`: Difference from monthly average
  - `precipitation`: the park-local DAY's total, `precipitation_last_3h` = that total / 8 (mm per 3 h)
  - **Weather units (PAR-1031/PAR-1032)**: training reads `weather_data`, one row per park-local day, so the
    model knows daily precipitation/snowfall sums, the daily wind maximum and the daily weather code, plus a
    temperature on a sinusoid (min 02:00, max 14:00 local). Serving reduces the hourly forecast to those same
    daily values per park-local day (`predict.apply_weather_features`; a day needs ≥ 12 forecast hours, sums are
    scaled to 24 h) and keeps the hourly temperature. Every row past the forecast reads the climatology of its
    OWN park-local month (`fetch_weather_climatology`, 3-year averages) — never 20 °C / 0 mm / code 0, and
    never the first row's month. Intra-day rain timing is not a feature: training has no hourly observed
    weather to learn it from.
  - `park_occupancy_pct`: Park-wide occupancy (current recent-peak ÷ **P50** baseline, 0–200%). Matches the API's live `getCurrentOccupancy` reading (ratio-vs-P50).
  - `wait_time_velocity`: mean of the last six hourly changes, (x[t-1h] − x[t-7h]) / 6 in min per hour — at serving the live wait is x[t-1h], only on rows ≤ 3 h ahead, 0 beyond
  - `trend_7d`: Linear regression slope of last 7 days
  - `volatility_7d`: Std of wait times over last 7 days, **dampened** as `log(1 + std)` and **capped** at `VOLATILITY_CAP_STD_MINUTES` (default 40 min) so it acts as a modifier; occupancy and time remain primary drivers.

## Training Pipeline

1. **Data Extraction**: Raw `queue_data` is exported from PostgreSQL with quality filters:
   - **`waitTime >= 5`**: Includes early morning walk-ons. Previously 10 min was used, but analysis showed Phantasialand and others legitimately open with 5 min while area-wide opening is staggered. Technical noise (0/1 min) is still excluded at SQL level.
   - **Schedule JOIN**: Excludes samples from closed days. Training data is joined against `schedule_entries` (park-level, `attractionId IS NULL`) via `JOIN parks p → AT TIME ZONE p.timezone`. Days with no schedule = include; days with `OPERATING` = include; any other type = exclude.
   ```sql
   LEFT JOIN schedule_entries se
     ON se."parkId" = a."parkId"
     AND se.date = DATE(qd.timestamp AT TIME ZONE p.timezone)
     AND se."attractionId" IS NULL
   WHERE qd."waitTime" >= 5
     AND (se.id IS NULL OR se."scheduleType" IN ('OPERATING', 'UNKNOWN'))
   ```
   UNKNOWN days are included to learn from parks without confirmed calendars (e.g. USJ).
2. **Preprocessing & Anomaly Filtering**:
   - **Contextual Anomaly Detection**: Differentiates real walk-ons from technical heartbeats.
     - **Sensor Drops**: Filters `waitTime <= 5` if surrounding context (7h median) is `> 20`.
     - **Technical Heartbeats**: Filters `waitTime <= 5` if the *entire park's* median at that timestamp is also `<= 5` AND it is before 1 hour after park opening (dynamic per park). This preserves valid quiet day data during the rest of the day.
     - **Outliers**: Filters `waitTime > 500` (API glitches).
   - **Feature Engineering**: Holiday lookup, weather curves (sinusoidal), etc.
   - **Occupancy Dropout** (`OCCUPANCY_DROPOUT_RATE=0.50`): 50% of training rows have their `park_occupancy_pct` replaced with DOW×hour means to force learning from temporal/holiday signals.
3. **Validation**: 
   - **Chronological hold-out**: the newest 30 days are split off first and never seen by the early-stopped fit.
   - **Randomized Weekly Block Split**: To ensure robust validation during seasonal transitions (e.g., winter to spring), the remaining pool is grouped into weekly blocks. 20% of these weeks are randomly selected for validation (and early stopping), while the rest are used for training. This ensures that both sets contain representative data from all operational phases.
   - **Hold-out score, then refit** (PAR-815): the early-stopped model is scored on the 30-day hold-out, and `holdout_metrics` (MAE, MAE on busy rows with actual ≥ 60 min, RMSE/MAPE/R², sample counts) is saved in the model metadata. Then the **served** model is refitted on ALL rows — pool plus hold-out — with the early-stopped tree count (best iteration + 1, deliberately not scaled up for the ~15–20% larger set), the same dropout and the same sample weights. Before this, the served model never learned the newest 30 days, so a season change reached it a month late. `TRAIN_REFIT_ON_ALL_ROWS` (default on) switches the refit off. Until PAR-902 early stopping never fired (`tree_count_` = `CATBOOST_ITERATIONS` = 2000 on every run), so the refit ran a full 2000 trees on ~1.25× the rows; see *Tree count and early stopping* below for why, and for the 1500-tree cap that replaced it. The training processor therefore passes its timeout to `/train` (`timeBudgetSeconds`), and the refit is **skipped** — logged, and flagged as `refit_skipped` in the metadata and `/train/status` — when elapsed time + fit time × (refit rows / fit rows) would come closer than `TRAIN_REFIT_SAFETY_MARGIN_SECONDS` (600) to that budget. The early-stopped model is then served for that day. `training_timings.refit` shows the actual cost. The champion/challenger gate still compares the **validation** MAE of the early-stopped fit; the persisted hold-out MAE is there for a future out-of-time gate. Note: the hold-out score describes the early-stopped model, not the refitted one, which has seen those rows.
   - **Fixed seeds and the hold-out at served leads** (PAR-1040): the validation weeks and the training dropout used to be seeded from the wall clock, so each nightly run validated on different weeks and the gate compared two MAEs measured on different data. Dropout now uses `CATBOOST_RANDOM_SEED`, and each ISO week's side of the split is drawn from `(seed, week)` alone (`select_validation_weeks`): when the window slides by a day, every week still in it keeps its side. Separately, the hold-out above scores each row with its real lags — a **1 h nowcast**, while the model is served 12 h to a year ahead. `holdout_metrics.origin_leads` therefore re-scores the same rows **as known at the origin** (target − lead) for the leads 12h / 24h / 7d / 61d (`as_known_at_origin`): rolling statistics, base-hour lags and downtime from the ride's last row at or before the origin; `avg_wait_same_dow_4w` only where all four weekly lags precede the origin, otherwise the origin's `rolling_avg_7d` — beyond 28 d the same weekday and hour 52 weeks back where observed and the ride was first seen ≥ 330 d before the origin (PAR-1045, serving's rule); `avg_wait_last_1h` = the target hour a week earlier if known at the origin, otherwise the weekday/weekend mean (the PAR-1030 far-row definition); velocity 0; occupancy from a (park, weekday, hour) profile built from the pool; beyond 16 d (`ORIGIN_WEATHER_FORECAST_DAYS`, the forecast horizon) the weather is the pool's (park, month, hour) climatology, and beyond 30 d (`ORIGIN_SCHEDULE_KNOWN_DAYS`) the schedule columns read as a day without published hours (open, no window, no event or extra hours). **The 12 h / 24 h / 7 d figures stay optimistic**: they keep the observed weather where serving had a forecast and the final schedule where serving had what was published at the origin, so read them as a lower bound on the served error. Each lead stores MAE, busy MAE and samples; it is logged, kept in the `.pkl` metadata, exposed on `GET /model/info/{version}` as `holdoutMetrics` and written to `ml_models.holdoutMetrics` (jsonb). **The gate does not read it yet** — that is an owner decision. Approximations: the occupancy profile averages all rides (serving: headliners, last 8 weeks), and leads between 7 d and 28 d would need a partial same-weekday mean (no served lead falls there).
   - **Tree count and early stopping** (PAR-902): `CATBOOST_LEARNING_RATE = 0.1`, `CATBOOST_ITERATIONS = 1500`, `CATBOOST_EARLY_STOPPING_ROUNDS = 100` (od_type `Iter`), `use_best_model=True`, eval set = the validation weeks above, eval metric = the training loss (`MultiQuantile:alpha=0.5,0.8,0.95`). The wiring was never broken — the model files from 2026-09-28 to 10-08 all carry `od_type=Iter`, `od_wait=100`, `use_best_model=True` and an eval set — but at the old `0.03 × 2000` the validation loss was still falling at the cap: `best_iteration` was 1999 (once 1998) on every nightly run, after the cap had already been raised from 1000 for the same reason (998/1000). There was no plateau for the detector to find, and every run paid the full cap twice, fit (~28 min) and refit (~36 min), ~73 of the 90-minute timeout.

     Measured on the production frame of 2026-10-15 (4.44 M rows, the same fetch, features, dropout and busy weighting as the nightly run, trained locally without early stopping, losses logged per tree):

     | config | trees | MultiQuantile loss val / hold-out | MAE val | MAE hold-out (30 d, out-of-time) | busy MAE hold-out (≥ 60 min) |
     |---|---|---|---|---|---|
     | lr 0.03 (old) | 2000 | 1.599 / 1.552 | 4.55 | 4.36 | 15.61 |
     | lr 0.03 | 3000 | 1.559 / 1.531 | 4.43 | 4.31 | 15.24 |
     | lr 0.1 | 1000 | 1.579 / 1.561 | 4.49 | 4.38 | 15.40 |
     | **lr 0.1 (new)** | **1500** | **1.550 / 1.542** | **4.39** | **4.33** | **15.17** |
     | lr 0.1 | 2000 | 1.533 / 1.536 | 4.35 | 4.30 | 15.05 |
     | lr 0.15 | 1500 | 1.550 / 1.557 | 4.41 | 4.34 | 15.23 |

     The old row reproduces production (hold-out MAE 4.36 in `v20261015_0600`'s metadata). Three things follow. **Not a leak:** the random-week validation set and the 30-day out-of-time hold-out improve in step (best tree 2998 vs 2877 at 0.03, 2984 vs 2986 at 0.1), so the validation weeks were not flattering a curve that the future would not follow, and switching the eval set to a chronological slice would not have stopped training earlier — it stays the random weeks, which also keeps the gate's validation MAE the same measurement as the champion's. **Under-fitting, not a broken detector:** at 0.03 both curves are still falling at 3000 trees. **0.1 × 1500 beats 0.03 × 2000 on every column with 25 % fewer trees**; 0.15 converges to the same place, so 0.1 is not at the edge. Even at 0.1 the detector (od_wait 100) would only fire around tree 2500, so the cap — not early stopping — sets the tree count, and it is a time decision: both the fit (~0.84 s/tree in production) and the refit (~1.07 s/tree) scale linearly with it, so 1500 trees cost ~21 + ~27 min against ~28 + ~36 before, ~57 min in all. Raise the cap only together with the training timeout.

     Early stopping stays as the safety net for a data set that stops supporting more trees, and what it did is now recorded instead of only logged: `metadata["early_stopping"]` holds `best_iteration`, `tree_count`, `trees_built`, `iterations_cap`, `stopped_early`, `od_wait`, `learning_rate`, `eval_metric` and `best_validation_loss`, and `training_timings.fit` (also in `/train/status`) carries `trees` and `stopped_early`. A run that hits the cap logs it with a warning.
   - **Metrics**: RMSE/MAE metrics are logged and stored in the database.
     The training processor reads them from `GET /model/info/:version`, which
     serves that version's own `metadata_<version>.pkl` — never from
     `/model/info`, which describes whatever model the answering worker still
     has loaded. A version without a saved model file and metadata is not
     registered (PAR-815).
   - **Activation** (PAR-815): `train_standalone.py` only writes the model file,
     its metadata and the training status — never the active-version sentinel.
     The processor saves the `ml_models` row, runs the champion/challenger gate,
     and only for an accepted version retires the old champion and calls
     `POST /model/reload`. That endpoint loads the DB-active version and writes
     `/app/models/active_version.txt`, which every other worker picks up on its
     next `/predict`; the boot path (`_load_active_model`) reads the same DB row.
     Retiring the old champion and inserting the new row happen in ONE
     transaction. `fetch_active_model_version` orders by `trainedAt DESC`, so
     even two active rows would resolve deterministically. If the reload does
     not confirm the new version, the DB is rolled back to the previous champion
     (again in one transaction), a best-effort `/model/reload` points the
     sentinel back at it, and the job fails. There is deliberately no partial
     unique index on `isActive`: with `synchronize: true` in production, an index
     that cannot be built (any second active row) would stop the API from
     booting, which costs more than the invariant it guards. A rejected, failed or timed-out run
     never changes what is served.
   - **Model files** (PAR-815): the API keeps the newest 30 `ml_models` rows
     plus the active one; older rows lose their files and their row. Files are
     listed and deleted through the ml-service (`GET /models/files`,
     `DELETE /models/files/:version`) because the API container does not mount
     the models volume. Versions on disk with no row (failed, timed-out or
     unregistered runs) are deleted once their files are two days old. The
     ml-service refuses unsafe version names and the DB-active, sentinel, loaded
     and in-training versions. The sweep deletes nothing (and logs an error)
     when `ml_models` returns no rows, or when it would delete more than 250
     versions or more than 90% of the versions on disk. The first sweep after
     deploy is expected to remove 196 of 226 versions (~2.4 GB).
4. **Training-time budget** (PAR-815): the window runs from the first row
   (2025-12-24) to now, capped at `TRAIN_LOOKBACK_YEARS` (2), so it grows daily
   (1682 s on 2026-09-04, 2520 s on 2026-10-07). The model trains on the **full**
   pool; the processor's `ML_TRAINING_TIMEOUT_MINUTES` default is 90 to give it
   headroom. Every run records per-phase durations and row counts (`fetch`,
   `features`, `pool`, `fit`, `total_seconds`) as `training_timings` in the model
   metadata, in `/train/status` (`timings`) and in `/model/info/:version`
   (`trainingTimings`); the processor logs them — that is where to look before
   changing anything about the budget.
   - **Opt-in row cap, NOT enabled:** `TRAIN_MAX_ROWS` (default 0 = off) bounds
     the pool after the 30-day hold-out: the newest `TRAIN_FULL_RESOLUTION_DAYS`
     (90) of that pool — days 30–120 before the newest data — stay whole and
     older rows are thinned by a seeded uniform sample, so
     every month keeps its share; it runs after feature engineering, so feature
     values are unchanged; it only shortens the fit and shrinks the fit/Pool
     memory (fetch and feature-engineering memory are unaffected). Enable it only after an
     A/B (full vs capped) on an out-of-time window shows no accuracy loss.


## Usage

The API requests predictions via HTTP:

```http
POST /predict
Content-Type: application/json

{
  "attraction_id": "uuid...",
  "timestamp": "2024-05-20T14:00:00Z",
  "features": { ... }
}
```

## Alignment with API (Occupancy & Baselines)

The API runs two crowd-level regimes — **live** is ratio-vs-P50 (current peak ÷ park P50 baseline) and the **calendar** is typical-day-peak (a day's averaged headliner P90 ÷ the typical-day-peak baseline). The ML pipeline aligns so predictions, the occupancy feature and crowd-level chips live in the same world.

- **Park occupancy** at inference comes from `AnalyticsService.getCurrentOccupancy`, which divides the current recent peak by the **P50 (headliner) baseline** (ratio-vs-P50). The ML service receives this as `featureContext.parkOccupancy` and uses it for `park_occupancy_pct`.
- **Baselines forwarded per request**: the API sends `p50Baseline` and `typicalDayPeakBaseline` in `PredictionRequestDto`. (The old `p90Baseline` field was removed — Python never read it.)
- **Crowd level on predictions**: in `predict.py` the crowd level for a predicted day is the predicted wait ÷ the **typical-day-peak baseline** (the calendar regime). `p50Baseline` covers the live/within-day path.
- **Models retrain at 06:00 daily**. Predictions made between a deploy and the next training cycle recalibrate within ~1 cycle.

See [Crowd Levels](../analytics/crowd-levels.md) §5 for the API-side contract.

### One definition per feature on both paths (PAR-1030, PAR-1032, PAR-1039)

Training (`features.py`) is the definition; serving (`predict.create_prediction_features`) reproduces it. `tests/test_train_serve_feature_parity.py` computes each of these through both paths on one fixture.

| Feature | Definition (both paths) | Serving before |
|---|---|---|
| `avg_wait_last_1h` | mean of [t − 1 h, t). Serving: the live wait on rows ≤ `LIVE_LAG_MAX_LEAD` (3 h) after base_time, the same-hour-last-week profile beyond | live wait on every row, 48 h ahead too |
| `wait_time_velocity` | (x[t−1h] − x[t−7h]) / 6 on the hourly series; serving takes the live wait as x[t−1h], ≤ 3 h, 0 beyond | (now − 30 min ago) / 6, a twelfth of the scale, on every row |
| `time_since_park_open_mins` | minutes since the opening of the window the row is in (its own day, or the previous day's for a slot past midnight), 0 without a window | today's opening for every row, 0 when unknown |
| `is_bridge_day` | per park-local date from the holidays (`add_bridge_day_feature`), computed before the holiday-distance features | one flag for today (`featureContext.isBridgeDay`, no longer sent) |
| `rolling_avg_weekday/_weekend`, `volatility_weekday/_weekend` | Mon–Fri vs Sat–Sun on the **park-local** weekday | UTC weekday, on both paths |
| `park_occupancy_pct` (training) | the park's mean wait at that hour **without the row's own wait** (the target); a ride alone at its hour reads 100 | the mean included the target |
| interaction features | recomputed after training dropout | computed before it, so a dropped-out row kept the real occupancy inside them |
| `is_park_open` (PAR-1045) | 0 before opening and after close on a day with a published window, 1 inside it | stayed 1 outside the window (only `status` turned CLOSED) |
| `avg_wait_same_dow_4w` > 28 d ahead (PAR-1045) | none of the four weekly lags exists yet, so: the same weekday and hour 52 weeks before the row (`LAST_YEAR_LAG` = 364 d), only for a ride with a reading at least 330 days before base_time (`LAST_YEAR_MIN_OBSERVED_DAYS`, the `MIN_OBSERVED_DAYS` rule) and only where that slot holds a reading; otherwise `rolling_avg_7d` as before. The model is unchanged (training rows always have their real lags); the origin hold-out's 61 d lead reads the same rule | `rolling_avg_7d` on every such row |

`queue_data` heartbeats (`is_heartbeat`) are excluded from every training, heuristic, occupancy-profile and serving-context read: a heartbeat copies the previous status and wait with a fresh timestamp and never ends a status.

**Ride-type flags (PAR-1045, behind `CATBOOST_CURATED_RIDE_TYPE`, default off).** `is_coaster`, `is_water_ride`, `is_indoor`, `is_wind_sensitive` come from name regexes. With the flag on, the curated columns overrule the regex **where they are set**, and null (nobody judged the ride) keeps the regex — it is never read as a value: `indoor_outdoor = indoor` → `is_indoor` 1, `outdoor` / `covered_queue` → 0; `attraction_kind` `TRANSPORT` / `SHOW` / `WALKTHROUGH` / `MAZE` → `is_coaster` and `is_water_ride` 0, `RIDE` keeps the name's flags (it does not say which ride); `is_wind_sensitive` stays the regex. One function on both paths (`features.add_attraction_type_features`; serving's copy is gone). The flag is read by **training only** and stored as `curated_ride_type` in the model metadata; serving follows the active model's value, so flipping the env never feeds a regex-trained model curated inputs. Every training logs `Curated ride type coverage … attraction_kind n/N, indoor_outdoor n/N` over the trained rides. **When to flip:** once that line shows the curated columns cover the headliners (the rides that carry the busy MAE), set `CATBOOST_CURATED_RIDE_TYPE=true` on the ml-service; the next training (06:00 UTC, or a manual one through the queue) takes it into the model, and the champion gate decides as for any retrain. Until then coverage can be read ad hoc with `scripts/prod-psql.sh`:

```sql
SELECT count(*) AS rides,
       count(attraction_kind) AS kind_set,
       count(indoor_outdoor) AS indoor_outdoor_set
FROM attractions a
WHERE EXISTS (SELECT 1 FROM wait_time_predictions w
              WHERE w."attractionId" = a.id AND w."createdAt" > now() - interval '2 days');
```

### Historical Occupancy Profile (DOW×Hour) — Timezone Fix

`fetch_historical_park_occupancy` builds a (DOW, hour) occupancy lookup table used for future predictions. This must use **park local time**, not UTC:

```sql
-- CORRECT: local park time
EXTRACT(DOW  FROM qd.timestamp AT TIME ZONE p.timezone)::int as dow,
EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE p.timezone)::int as hour
GROUP BY a."parkId", p.timezone,
         EXTRACT(DOW  FROM qd.timestamp AT TIME ZONE p.timezone),
         EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE p.timezone)
```

Before this fix, UTC-based grouping caused a systematic **1–2 hour shift** in the occupancy profile for all non-UTC parks. For example, a UTC+1 park's 10:00 local occupancy pattern was mapped to 09:00 UTC → looked up at 09:00 local → 1-hour earlier than actual. The inference path (`add_park_occupancy_feature`) already used local time for lookup, so a mismatch existed.

`queue_data` has no `parkId` column — always `JOIN attractions a ON a.id = qd."attractionId"` and then `JOIN parks p ON p.id = a."parkId"` to get the timezone.

## Holiday Logic

Holidays are critical for prediction accuracy.
- **Source**: `holiday_features.py` (`assign_holiday_features`) — the **one** implementation that training (`features.py add_holiday_features`) and inference (`predict.py create_prediction_features`) both call. Region codes go through `holiday_utils.normalize_region_code`, whose alias table (`NRW→NW`, `NDS→NI`, `England→ENG`, …) mirrors `src/common/utils/region.util.ts`.
- **Logic**: Checks school and public holidays for the park's specific region (e.g., NW for Phantasialand), and all its influencing regions.
- **Duplicates are flags, not a type (PAR-816)**: the `holidays` table holds several rows for one (country, region, date) — CH-ZH 2025-04-21 five times. They are collapsed to two flags combined with OR: `public` (any `public`/`bank`/`bridge` row) and `school`. A day can carry both; `observance` sets neither. A located park reads regional OR national rows; a country-level influencing region (`regionCode: null`) matches if any region of that country has the flag.
- **Never assign a merge result back by index label**: a merge onto the holidays table gets longer whenever a key is duplicated. The features are evaluated once per (park, local date) and written back by position, with a row-count assertion.
- **Easter Sunday** counts as a public holiday for parks in `EASTER_COUNTRIES` on both paths (Nager.Date lists it only for DE-BB).
- **Bridge Days**: Detects bridging days between holidays and weekends.

## Schedule Status (OPERATING / CLOSED / UNKNOWN)

Predictions are aligned with park schedule from `schedule_entries` **only when the park has schedule integration** (at least one OPERATING row somewhere). Not all parks have a schedule.

- **Park has no schedule** (no rows, or only UNKNOWN/CLOSED and never OPERATING): Treated as “no schedule” → predictions are **kept** (assume open). No filtering.
- **Park has schedule integration** (at least one OPERATING row):
  - **OPERATING**: Predict wait times; filter by operating hours.
  - **CLOSED**: `predictedWaitTime = 0`, `crowdLevel = “closed”`; daily predictions for that date excluded.
  - **UNKNOWN**: No confirmed hours from source yet → inference sets `is_park_open=0` by default, BUT if `parkLiveStatus=”OPERATING”` (park confirmed open via ride-data heuristic in NestJS), the override flips `is_park_open=1` and predictions are kept.

In **predict.py**, UNKNOWN/CLOSED-only for a date is only applied when `park_has_operating` (park has at least one OPERATING row); otherwise the row is left open. In **schedule_filter.py**, if the query returns only UNKNOWN/CLOSED (no OPERATING dates), we keep all predictions for that park instead of filtering everything out.

### `parkLiveStatus` — ride-based open/closed signal

NestJS passes `featureContext.parkLiveStatus: Record<parkId, “OPERATING”|”CLOSED”>` in every prediction request. This is determined by `getBatchParkStatus` in `parks.service.ts`, which uses:

1. **Primary**: `schedule_entries` — OPERATING entry with `openingTime ≤ now < closingTime`.
2. **Heuristic fallback** (parks not covered by primary): Ride data from the last 2 hours. A park is considered OPERATING if ≥ 3 attractions have recent data AND ≥ 25% report `waitTime ≥ 10 min`. Parks with an explicit `CLOSED` schedule today are excluded from the heuristic.

In `predict.py`, after the schedule loop, any row with `status=”UNKNOWN”` and `is_park_open=0` is corrected to `is_park_open=1` when `parkLiveStatus=”OPERATING”`. This never overrides explicit `CLOSED` entries.

**Full rules (Calendar API + Schedule Sync + ML):** [Calendar, Schedule & ML Rules](../architecture/calendar-schedule-and-ml-rules.md).
