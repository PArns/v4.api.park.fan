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
  - `precipitation`: Current & Last 3 Hours (accumulated)
  - `park_occupancy_pct`: Park-wide occupancy (current recent-peak ÷ **P50** baseline, 0–200%). Matches the API's live `getCurrentOccupancy` reading (ratio-vs-P50).
  - `wait_time_momentum`: Velocity of change over last 30 mins
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
   - **Hold-out score, then refit** (PAR-815): the early-stopped model is scored on the 30-day hold-out, and `holdout_metrics` (MAE, MAE on busy rows with actual ≥ 60 min, RMSE/MAPE/R², sample counts) is saved in the model metadata. Then the **served** model is refitted on ALL rows — pool plus hold-out — with the early-stopped tree count (best iteration + 1, deliberately not scaled up for the ~15–20% larger set), the same dropout and the same sample weights. Before this, the served model never learned the newest 30 days, so a season change reached it a month late. `TRAIN_REFIT_ON_ALL_ROWS` (default on) switches the refit off; it roughly doubles the fit phase, and `training_timings.refit` shows its cost. The champion/challenger gate still compares the **validation** MAE of the early-stopped fit; the persisted hold-out MAE is there for a future out-of-time gate. Note: the hold-out score describes the early-stopped model, not the refitted one, which has seen those rows.
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
     If the reload does not confirm the new version, the DB is rolled back to the
     previous champion and the job fails. A rejected, failed or timed-out run
     never changes what is served.
   - **Model files** (PAR-815): the API keeps the newest 30 `ml_models` rows
     plus the active one; older rows lose their files and their row. Files are
     listed and deleted through the ml-service (`GET /models/files`,
     `DELETE /models/files/:version`) because the API container does not mount
     the models volume. Versions on disk with no row (failed, timed-out or
     unregistered runs) are deleted once their files are two days old. The
     ml-service refuses unsafe version names and the sentinel, loaded and
     in-training versions.
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
