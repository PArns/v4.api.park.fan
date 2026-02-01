# P50 Unified Crowd Level System

> **Summary**: The Park Fan API uses a **P50 (Median) Baseline** system to calculate Crowd Levels. This creates a "Normal" (100%) reference point that represents a typical day at the park, rather than a peak day (P90).

---

## 1. Core Concept

Historical wait time data is analyzed over a **sliding window of 548 days** (approx 1.5 years) to account for seasonality. We calculate the **Median (P50)** wait time for "Headliner" attractions.

- **Why P50?** P90 (90th percentile) represents "Peak" wait times. Comparing a current Tuesday morning to a historical Peak baseline often resulted in artificially low crowd levels (e.g., "Very Low" on a normal day). P50 represents the "Expected" wait time.
- **Goal:** If `Current Wait == Historical Median`, the Crowd Level is **100% (Moderate)**.

### Formula
```typescript
CrowdLevel% = (Current_Park_Median / Historical_P50_Baseline) * 100
```

> **API Note:** The API response may still use legacy field names like `baseline90thPercentile`, but the *value* populated is now the P50 Baseline.

---

## 2. Calculation Methodology

The system runs a **Daily Background Job (3 AM)** to recalculate baselines.

### Step 1: Identify Headliners
We identify distinct "Headliner" attractions for each park using a **3-Tier Adaptive System** to ensure fair baselines for both Mega-Resorts and small Local Parks.

| Tier | Criteria | Target Park Type |
|------|----------|------------------|
| **Tier 1** | Avg Wait > 15m AND P90 > 25m | Major Theme Parks (Disney, Universal) |
| **Tier 2** | Top 40% wait times AND P90 > 1.5x P50 | Regional Parks (volatile) |
| **Tier 3** | All attractions with avg wait > 3m | Small Parks (consistent low waits) |
| **Fallback** | **NEW:** Top 5 Attractions by P90 (Avg > 0) | Parks with sparse data or <3m waits |

**Logic:**
1. Try to find at least 3 Tier 1 attractions.
2. If <3 found, try to add Tier 2 attractions.
3. If still <3, add Tier 3 attractions.
4. If **0 headliners found**, trigger **Fallback Strategy** (force select Top 5 by P90).

### Step 2: Calculate Park Baseline
Using only the identified Headliners:
1. Aggregate all wait times for these attractions over the last 548 days.
2. Calculate the **Median (P50)** of all samples.
3. Store this single number as the **Park Baseline** (e.g., `25.5` minutes).

---

## 3. Crowd Level Thresholds

We map the percentage (Current / Baseline) to a human-readable level using **Relative Thresholds**.

| Level | Range (Relative to Baseline) | Description |
|-------|------------------------------|-------------|
| **Very Low** | **0% - 50%** | Walk-on conditions (≤ 0.5x Normal) |
| **Low** | **51% - 79%** | Shorter than average waits |
| **Moderate** | **80% - 120%** | **Typical Day** (Within ±20% of median) |
| **High** | **121% - 170%** | Busy day (1.2x - 1.7x Normal) |
| **Very High** | **171% - 250%** | Peak season / Holidays |
| **Extreme** | **> 250%** | Major events / Capacity reached |

> **Note:** "Moderate" is the target for a standard operating day.

---

## 4. Technical Architecture

### Database Entities
- `park_p50_baselines`: Stores the calculated baseline per park.
- `headliner_attractions`: Logs which attractions were selected as headliners.
- `attraction_p50_baselines`: Stores individual attraction baselines (for per-ride crowd levels).

### Redis Cache
- **Key**: `park:p50:{parkId}`
- **TTL**: 24 hours
- **Content**: Plain number (e.g., "25.5")

### Services
- **`AnalyticsService`**:
  - `identifyHeadliners()`: Implements the Tier/Fallback logic.
  - `saveP50Baselines()`: Uses `upsert` to save daily calculations.
  - `getLoadRating()`: Applies the threshold table.
- **`P50BaselineProcessor`**: Background Bull queue processor that runs the nightly calculation.

---

## 5. Machine Learning Integration

The ML Service (`ml-service`) also uses the P50 Baseline as a feature.
- **Input Feature**: `p50_baseline` (passed in request).
- **Training**: The model learns to predict wait times relative to this baseline.
- **Consistency**: Both the Rules-Based API (Current) and ML API (Future) use the same baseline definition.
