# Feature Engineering Concepts

This document details the conceptual logic behind the most impactful features in the wait time prediction model. It focuses on visitor psychology and technical alignment.

## 1. Visitor Psychology & Calendar Events

Wait times are driven more by guest intent than by simple historical averages. The system uses three key features to capture this intent.

### A. Long Weekend Detection (`is_long_weekend`)
*   **Concept**: A "Bridge Day" (e.g., a Friday after a Thursday holiday) is part of a larger cluster. 
*   **Significance**: Bridge days are often the busiest days of the year, outperforming even the actual holiday. The model identifies these 3-4 day clusters to differentiate a high-peak Friday from a regular one.

### B. Arrival & Departure Signals
*   **`days_until_next_holiday`**: Measures the distance to the next event. Guests often arrive 1-2 days *before* a long weekend, causing wait times to climb early.
*   **`days_since_last_holiday`**: Captures the "Sunday effect" at the end of a long weekend. As families begin their journey home, wait times often drop earlier than on a normal weekend day.

## 2. Occupancy Alignment (Training vs. Inference)

A critical requirement for reliable ML is that the scale of features during training matches the scale during live prediction (Inference).

*   **The Baseline**: "100% Occupancy" is defined as the average of the P50 (median) wait times of all attractions in a park.
*   **Dynamic Synchronization**: The system fetches pre-calculated `attraction_p50_baselines` from the database for both training and inference. This ensures that "High Occupancy" means exactly the same thing to the model, whether it is looking at data from 2024 or predicting for next week.

## 3. Advanced Weather Dynamics

### Sinusoidal Temperature Profile
*   **Problem**: Flat daily averages (Min+Max/2) fail to capture the impact of midday heat or evening cool-downs.
*   **Solution**: The system simulates an hourly curve where the minimum is at 04:00 and the maximum is at 14:00. This allows the model to learn ride-specific reactions to temperature peaks (e.g., water rides peaking in the afternoon).

### Rain Trend Analysis (`is_rain_starting`, `is_rain_stopping`)
*   **Behavioral Signal**: Guests react instantly when it *starts* raining (flocking to indoor dark rides or shows). The binary change signal is often more informative than the total precipitation value itself.

## 4. Model Generalization & Parity

### Name-based Type Heuristics
*   **Problem**: Explicit attraction types (COASTER, DARK_RIDE) are often missing from third-party APIs.
*   **Solution**: The system uses Regex-based heuristics on attraction names to categorize rides.
*   **Significance**: This enables cross-park learning. The model can learn that "Coasters" are wind-sensitive or that "Water Rides" peak during high heat, applying this knowledge even to new parks with limited history.

### Inference-Training Parity
Reliable predictions require identical logic in both stages:
*   **Trend Logic**: Calculated as `avg_last_24h - rolling_avg_7d`. This "Momentum" signal must have the same scale and mathematical meaning in both pipelines.
*   **Default Values**: Missing historical data defaults to `0.0`. Using inconsistent defaults (e.g., 30.0 in inference vs 0.0 in training) would lead to biased predictions for new attractions.

## 5. Technical Architecture: Vectorization

To handle over 1,000,000 training rows efficiently on standard hardware, the feature engineering pipeline is **100% vectorized**.

*   **Method**: Instead of Python loops, the system uses C-optimized Pandas operations (`merge_asof`, `shift`, `map`).
*   **Efficiency**: This reduces the processing time for complex historical lags (1-4 weeks) from minutes to seconds, allowing for frequent model retraining without overwhelming the host system.
