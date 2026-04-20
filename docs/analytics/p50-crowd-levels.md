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
| **Fallback** | Top 5 Attractions by P90 (Avg ≥ 5) | Parks with sparse data or <3m waits |

**Logic:**
1. Try to find at least 3 Tier 1 attractions.
2. If <3 found, try to add Tier 2 attractions.
3. If still <3, add Tier 3 attractions.
4. If **0 headliners found**, trigger **Fallback Strategy** (force select Top 5 by P90).

#### Data Quality Filters (applied in all tiers)

All historical wait-time queries used for headliner identification and baseline calculation apply two filters:

1. **`waitTime >= 10`** — excludes walk-on placeholder values. Queue-Times API reports `waitTime=1` for water-park slides and other "open but no queue" attractions. Including these inflates sample counts and deflates the P50 (e.g., Rulantica P50 was 4.4 min with placeholders vs ~20 min without). The real-time path already uses `minWaitTime=5`; baselines must match.
2. **Schedule JOIN** — excludes closed-day data. Historical samples are joined against `schedule_entries` (park-level, no attractionId). Days with no schedule entry are kept (unknown = include). Days with an explicit `CLOSED`, `TICKETED_EVENT`, or similar non-OPERATING type are excluded. This prevents off-season data (e.g., Kennywood Jan–Mar) from dragging baselines down.

```sql
LEFT JOIN schedule_entries se
  ON se."parkId" = a."parkId"
  AND se.date = DATE(qd.timestamp AT TIME ZONE <park_timezone>)
  AND se."attractionId" IS NULL
WHERE qd."waitTime" >= 10
  AND (se.id IS NULL OR se."scheduleType" = 'OPERATING')
```

### Step 2: Calculate Park Baseline
Using only the identified Headliners:
1. Aggregate all wait times for these attractions over the last 548 days.
2. Apply data quality filters (`waitTime >= 10`, schedule JOIN — see above).
3. Calculate the **Median (P50)** of all samples.
4. Store this single number as the **Park Baseline** (e.g., `25.5` minutes).

> **Intra-day trimming not applied to baselines (by design):** The baseline deliberately uses full daily data windows rather than schedule-trimmed windows. Because P50 is the median, the small fraction of pre-opening or post-closing samples (typically <15% of daily data) cannot shift the median — you would need >50% of samples to be "off-hours" for the median to move. The daily crowd-level query *does* apply schedule trimming (see below), which correctly nudges the ratio upward toward more realistic levels on typical days. A forced baseline recalculation is therefore not required when the trimming fix is deployed.

---

## 3. Crowd Level Thresholds

We map the percentage (Current / Baseline) to a human-readable level using **Relative Thresholds**.

| Level | Range (Relative to Baseline) | Description |
|-------|------------------------------|-------------|
| **Very Low** | **0% - 60%** | Walk-on conditions (≤ 0.6x Normal) |
| **Low** | **61% - 89%** | Shorter than average waits |
| **Moderate** | **90% - 110%** | **Typical Day** (Within ±10% of median) |
| **High** | **111% - 150%** | Busy day (1.1x - 1.5x Normal) |
| **Very High** | **151% - 200%** | Peak season / Holidays |
| **Extreme** | **> 200%** | Major events / Capacity reached |

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
  - `calculateCrowdLevelForDate(entityId, type, date, timezone)` — historical crowd level for a specific date. For `type='park'`, fetches the schedule entry for that date and applies a **±5-minute boundary trim** (`openingTime+5min … closingTime-5min`) so pre-opening ride tests and post-closing stragglers are excluded. Falls back to full day (00:00–23:59) if no schedule entry exists.
- **`P50BaselineProcessor`**: Bull job (daily at 3 AM) for park and attraction P50 baselines.

---

## 5. Known Issue: Pre-Opening Data Deflating Crowd Levels

### Problem

Several data sources (Queue-Times, Themeparks.wiki) begin reporting ride status as `OPERATING` before the park's official ride-opening time. Example: Phantasialand gates open at 09:00 but most rides only open at 10:00; however some rides already report `waitTime=10, status=OPERATING` at 08:00 during staff rides / pre-opening tests.

Without time-window trimming, the daily P50 for `calculateCrowdLevelForDate` used data from `00:00–23:59`. These early-morning low values (e.g., a ride that reports 10 min at 08:00 and 45 min for the rest of the day) dragged the daily P50 down, causing the crowd level to appear systematically lower than reality (e.g., "Low" on a genuinely "High" day).

The same issue affected:
- **Daily ride statistics** (`getBatchAttractionStatistics`): `MIN(waitTime)` returned 0 or 10 for rides that were genuinely busy all day, because walk-on or pre-opening 0-minute reports were included.
- **Wait-time history chart** (`getParkWaitTimeHistory`): The chart showed P90 values at 08:00 even though the park was not yet operating.

### Solution (implemented 2026-04-20)

| Component | Change |
|-----------|--------|
| `calculateCrowdLevelForDate` | When a schedule entry exists for the queried date, the P50/P90 window is trimmed to `openingTime+5 min … closingTime-5 min`. Falls back to midnight–23:59 if no schedule. |
| `getParkWaitTimeHistory` | Added `waitTime > 0` filter; end of chart now capped at `closingTime` (or `now`, whichever is earlier). |
| `getBatchAttractionStatistics` | Added `waitTime > 0` so `MIN()` cannot return 0 for a ride that was genuinely busy all day. |

### Why No Baseline Recalculation Is Needed

The P50 baselines (`park_p50_baselines`) are calculated over 548 days of data. Pre-opening data is typically ≤15% of daily samples. Because P50 is the median, this small minority cannot shift it — the median only moves when more than 50% of values change. The 3 AM daily job recalculates baselines automatically, so any minor residual inconsistency resolves within 24 hours.

The direction of the remaining transient bias (baseline slightly low, daily P50 trimmed upward) produces crowd levels that are slightly higher than before — exactly the desired correction away from systematic under-reporting.

---

## 6. Machine Learning Integration

The ML Service (`ml-service`) also uses the P50 Baseline as a feature.
- **Input Feature**: `p50_baseline` (passed in request).
- **Training**: The model learns to predict wait times relative to this baseline.
- **Consistency**: Both the Rules-Based API (Current) and ML API (Future) use the same baseline definition.
