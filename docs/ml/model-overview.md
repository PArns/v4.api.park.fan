# Machine Learning Service

## Overview

The ML Service is a standalone Python application responsible for predicting wait times for attractions. It exposes a FastAPI interface for the main NestJS application to query.

## Model Architecture

- **Algorithm**: CatBoost Regressor (Gradient Boosting on Decision Trees)
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
  - `park_occupancy_pct`: Park-wide occupancy (Current median / P50 baseline, 0–200%). Aligned with API occupancy; training uses P50 for consistency.
  - `wait_time_momentum`: Velocity of change over last 30 mins
  - `trend_7d`: Linear regression slope of last 7 days
  - `volatility_7d`: Std of wait times over last 7 days, **dampened** as `log(1 + std)` and **capped** at `VOLATILITY_CAP_STD_MINUTES` (default 40 min) so it acts as a modifier; occupancy and time remain primary drivers.

## Training Pipeline

1. **Data Extraction**: Raw `queue_data` is exported from PostgreSQL with data quality filters applied at the SQL level:
   - **`waitTime >= 5`**: Excludes walk-on placeholder values (`waitTime=1` used by Queue-Times for water-park slides and similar "open but no queue" attractions). Using `> 0` would include noise that degrades model quality and misaligns with inference (which also filters at ≥ 5).
   - **Schedule JOIN**: Excludes samples from closed days. Training data is joined against `schedule_entries` (park-level, `attractionId IS NULL`) via `JOIN parks p → AT TIME ZONE p.timezone`. Days with no schedule = include; days with `OPERATING` = include; any other type = exclude. Prevents off-season data (e.g., seasonal parks in winter) from polluting training.
   ```sql
   LEFT JOIN schedule_entries se
     ON se."parkId" = a."parkId"
     AND se.date = DATE(qd.timestamp AT TIME ZONE p.timezone)
     AND se."attractionId" IS NULL
   WHERE qd."waitTime" >= 5
     AND (se.id IS NULL OR se."scheduleType" = 'OPERATING')
   ```
2. **Preprocessing**:
   - Outlier removal (e.g., wait times > 300 min).
   - Feature engineering (Holiday lookup via `holiday_utils.py`).
   - Handling missing weather data.
   - **Occupancy Dropout** (`OCCUPANCY_DROPOUT_RATE=0.30`): 30% of training rows have their `park_occupancy_pct` replaced with the DOW×hour mean from that park's training data. This teaches the model to rely on `hour`/`day_of_week` when real-time occupancy is unavailable (future predictions), instead of overfitting to the always-perfect training-time value.
3. **Training**:
   - `train.py` splits data (Train/Test).
   - CatBoost trains on historical data.
   - Model artifacts saved to `models/`.
4. **Validation**: RMSE/MAE metrics are logged.

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

## Alignment with API (P50 & Occupancy)

- **Park occupancy** at inference comes from the NestJS API (`getCurrentOccupancy`), which uses the **P50 (headliner) baseline**. The ML service receives this as `featureContext.parkOccupancy` and uses it for `park_occupancy_pct`.
- **Training**: `add_park_occupancy_feature` in training mode uses **P50 (median)** of historical wait times per park so the scale matches inference. Previously P90 was used; retraining is recommended after this change.
- **Crowd level** on predictions: The API passes `p50Baseline` (per attraction or park). Python uses it for `crowdLevel` (very_low … extreme) so TypeScript and Python produce identical labels.

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
- **Source**: `holiday_utils.py`
- **Logic**: Checks school and public holidays for the park's specific region (e.g., NRW for Phantasialand).
- **Bridge Days**: Detects bridging days between holidays and weekends.

## Schedule Status (OPERATING / CLOSED / UNKNOWN)

Predictions are aligned with park schedule from `schedule_entries` **only when the park has schedule integration** (at least one OPERATING row somewhere). Not all parks have a schedule.

- **Park has no schedule** (no rows, or only UNKNOWN/CLOSED and never OPERATING): Treated as “no schedule” → predictions are **kept** (assume open). No filtering.
- **Park has schedule integration** (at least one OPERATING row):
  - **OPERATING**: Predict wait times; filter by operating hours.
  - **CLOSED**: `predictedWaitTime = 0`, `crowdLevel = "closed"`; daily predictions for that date excluded.
  - **UNKNOWN**: No schedule from source yet (placeholder) → same as CLOSED for that date; excluded from daily.

In **predict.py**, UNKNOWN/CLOSED-only for a date is only applied when `park_has_operating` (park has at least one OPERATING row); otherwise the row is left open. In **schedule_filter.py**, if the query returns only UNKNOWN/CLOSED (no OPERATING dates), we keep all predictions for that park instead of filtering everything out.

**Full rules (Calendar API + Schedule Sync + ML):** [Calendar, Schedule & ML Rules](../architecture/calendar-schedule-and-ml-rules.md).
