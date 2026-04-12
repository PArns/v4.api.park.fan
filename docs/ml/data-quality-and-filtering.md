# Data Quality & Anomaly Filtering

This document explains the conceptual approach to maintaining high-quality training data for the wait time prediction model. It details how the system distinguishes between technical noise and legitimate park behavior.

## 1. The 2-Stage Filtering Strategy

To balance data completeness with noise reduction, the system employs a two-stage filtering process.

### Stage 1: SQL-Level Pre-Filtering
Data is exported from the live database with a base threshold of **`waitTime >= 5`**.
*   **Reasoning**: This excludes obvious technical placeholders (0 or 1 min) often used by upstream APIs to indicate "status unknown" or "park closed." 
*   **The Phantasialand Scenario**: Using a higher threshold (like 10 min) would discard valuable data from park openings (e.g., PHL at 09:00), causing the model to under-predict morning rushes.

### Stage 2: Contextual Python Filtering
Since a `5 min` wait can be either a real "walk-on" or a technical "heartbeat," the system uses the surrounding context to decide.

#### A. Technical Heartbeat Detection (Pre-Opening)
*   **Mechanism**: If a ride reports `5 min` but the **entire park's median** is also `<= 5 min`, the data point is likely a heartbeat before actual operations start.
*   **Temporal Constraint**: This filter is only active until **1 hour after park opening** (dynamic per park).
*   **Goal**: Ensure the model doesn't learn "fake" activity before the park is actually accessible to guests.

#### B. Sensor Drop Detection (Unexpected Dips)
*   **Mechanism**: If a ride reports `<= 5 min` while its own **7-hour rolling median** is `> 20 min`, it is flagged as a "sensor drop."
*   **Reasoning**: Rides in a busy park do not naturally drop to 5 minutes for a single hour. This usually indicates an API reset or a temporary data loss.

#### C. Extreme Outlier Removal
*   **Mechanism**: Any value **`> 500 min`** (8.3 hours) is discarded.
*   **Reasoning**: While extreme peaks exist, values above this threshold are statistically likely to be API glitches and would skew the model's ability to predict realistic busy days.

## 2. Dynamic Schedule Integration

The system treats `UNKNOWN` and `OPERATING` schedules differently to maximize training samples:
*   **Explicit OPERATING**: Data is included if it falls within the confirmed hours.
*   **UNKNOWN**: Data is included if a **Ride Heuristic** (≥3 attractions active, ≥25% with `waitTime >= 10`) confirms the park was actually open.
*   **Explicit CLOSED**: Data is strictly excluded to prevent maintenance or off-season "heartbeats" from polluting the model.

## 3. Impact on Model Performance
By allowing 5-minute data but filtering it contextually, the model gains **~12% more valid training samples** for early morning and late evening periods, leading to significantly better predictions during these critical transition phases.
