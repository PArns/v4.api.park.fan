# P50 Unified Crowd Level System

> **Summary**: The Park Fan API uses a **P50 (Median) Baseline** system for both **parks** and **attractions**. This creates a "Normal" (100%) reference point that represents a typical day (park) or typical wait (attraction), rather than a peak (P90).

**Related**: [Caching Strategy](../architecture/caching-strategy.md) (Redis keys and DB cache tables), [Headliner Logic](headliner-logic.md) (park baseline only).

---

## 1. Core Concept

### Parks
Historical wait time data is analyzed over a **sliding window of 548 days** (approx 1.5 years). We calculate the **Median (P50)** wait time for **Headliner** attractions only and use that as the park baseline.

- **Why P50?** P90 represents "Peak" wait times. Comparing current waits to a Peak baseline often resulted in artificially low crowd levels (e.g., "Very Low" on a normal day). P50 represents the "Expected" wait time.
- **Goal:** If `Current Park Median == Park P50 Baseline`, Occupancy is **100% (Moderate)**.

### Attractions (Rides)
Each attraction has its own **P50 baseline** (548-day median for that attraction). Crowd level for a ride uses that baseline so "moderate" means typical for that ride.

- **Source**: Table `attraction_p50_baselines`, filled by the same daily P50 job. Redis: `attraction:p50:{attractionId}`.
- **Fallback**: If no P50 baseline exists, we use the 548-day sliding-window P50 or P90 from `get90thPercentileWithConfidence`.

### Formula (same for park and attraction)
```typescript
CrowdLevel% = (Current_Wait_or_Median / P50_Baseline) * 100
```

> **API Note:** The API may still use legacy field names like `baseline90thPercentile`; the *value* is the P50 baseline.

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

### Database (cache tables)
| Table | Purpose |
|-------|--------|
| `park_p50_baselines` | Park P50 baseline (headliners only). |
| `attraction_p50_baselines` | Per-attraction P50 baseline (for ride crowd level). |
| `headliner_attractions` | Which attractions were selected as headliners per park. |

See [Caching Strategy](../architecture/caching-strategy.md) for `park_daily_stats` and `queue_data_aggregates`.

### Redis
| Key | TTL | Content |
|-----|-----|--------|
| `park:p50:{parkId}` | 24h | Park P50 baseline (number). |
| `attraction:p50:{attractionId}` | 24h | Attraction P50 baseline (number). |
| `analytics:percentile:sliding:park:{parkId}` | 24h | 548-day P90/P50 fallback (JSON). |
| `analytics:percentile:sliding:attraction:{id}` | 24h | 548-day P90/P50 fallback; shared by single and batch percentile reads. |

### Services
- **`AnalyticsService`**:
  - **Park**: `getP50BaselineFromCache(parkId)` → headliner P50; fallback `get90thPercentileWithConfidence(..., "park")`.
  - **Attraction**: `getAttractionP50BaselineFromCache(attractionId)`, `getBatchAttractionP50s(ids)`; fallback sliding-window P50/P90.
  - `getLoadRating(current, baseline)`, `getAttractionCrowdLevel(waitTime, baseline)` → same threshold table.
- **`P50BaselineProcessor`**: Bull job (daily) for park and attraction P50 baselines.

---

## 5. Machine Learning Integration

The ML Service (`ml-service`) also uses the P50 Baseline as a feature.
- **Input Feature**: `p50_baseline` (passed in request).
- **Training**: The model learns to predict wait times relative to this baseline.
- **Consistency**: Both the Rules-Based API (Current) and ML API (Future) use the same baseline definition.
