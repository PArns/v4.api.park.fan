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

## Training Pipeline

1. **Data Extraction**: Raw `queue_data` is exported from PostgreSQL.
2. **Preprocessing**:
   - Outlier removal (e.g., wait times > 300 min or < 0).
   - Feature engineering (Holiday lookup via `holiday_utils.py`).
   - Handling missing weather data.
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

## Holiday Logic

Holidays are critical for prediction accuracy.
- **Source**: `holiday_utils.py`
- **Logic**: Checks school and public holidays for the park's specific region (e.g., NRW for Phantasialand).
- **Bridge Days**: Detects bridging days between holidays and weekends.
