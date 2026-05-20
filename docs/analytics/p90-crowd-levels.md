# P90 Unified Crowd Level System

> **Summary**: The Park Fan API uses a **P90 (peak) baseline** for every user-facing crowd-level reading on parks and attractions. The 100% reference point is "what a typical day's peak looked like" — matching how visitors remember busyness ("Saturday afternoon was crazy" = peak experience, not avg). When a P90 baseline isn't available yet, the API falls back to the existing **P50 (median) baseline** so the math stays apples-to-apples in either case.

**Related**: [Caching Strategy](../architecture/caching-strategy.md) (Redis keys and DB cache tables), [Headliner Logic](headliner-logic.md) (park baseline only).

---

## 1. Core Concept

### Parks
Historical wait-time data is analyzed over a **sliding window of 548 days** (~1.5 years). The cron computes both the **median (P50)** and the **90th percentile (P90)** of wait times for **headliner attractions only** and stores them as the park baselines.

- **Why P90?** P90 represents the "peak" wait experience — what visitors actually remember about a day. P50 (median) tracks the typical average and underweights the headliner spike that defines the day. We still emit P50 for legacy/avg-shaped consumers and as a fallback when P90 hasn't been computed yet.
- **Goal:** If `Current_Park_Peak ≈ Park_P90_Baseline`, occupancy reads as **100% (moderate)**.

### Attractions (Rides)
Each attraction has its own **P90 baseline** (548-day P90 of that attraction's waits) and its own **P50 baseline**. Crowd level for a ride uses P90, falling back to P50, so "moderate" means a typical peak for that ride.

- **Source**: Tables `attraction_p90_baselines` and `attraction_p50_baselines`, filled by the same daily P50/P90 job. Redis: `attraction:p90:{attractionId}` and `attraction:p50:{attractionId}`.
- **No live 548-day fallback:** the previous `get90thPercentileWithConfidence` live PERCENTILE_CONT used to fire on every cache miss. It's been removed from every hot path; the daily cron is the only thing that touches a 548-day data window now.

### Formula (same for park and attraction)
```typescript
CrowdLevel% = (Current_Peak / P90_Baseline) * 100
// Fallback when no P90 baseline row exists yet:
CrowdLevel% = (Current_Avg / P50_Baseline) * 100
```

For **parks live**, "current peak" is computed as per-headliner MAX wait in the last 60 minutes, averaged across headliners. The 60-min window keeps the reading responsive while still being statistically meaningful given the ~5-min sampling cadence.

For **calendar daily values**, "current peak" is the day's P90 wait (computed from raw `queue_data` for that date, also via avg-of-per-headliner-P90 for parks).

For **single attractions**, the current wait IS the current peak (one ride, one wait at any moment), so no aggregation is needed.

> **API note:** the legacy field name `baseline90thPercentile` is retained for backwards compatibility; its semantic now matches its name (it carries the P90, with P50 as a fallback).

---

## 2. Calculation Methodology

A **daily background job** populates baselines: 3 AM for parks, 4 AM for attractions.

### Step 1: Identify Headliners
We identify "headliner" attractions per park using a **3-tier adaptive system** to give fair baselines for both mega-resorts and small local parks.

| Tier | Criteria | Target Park Type |
|------|----------|------------------|
| **Tier 1** | Avg wait > 15m AND P90 > 25m | Major theme parks (Disney, Universal) |
| **Tier 2** | Top 40% wait times AND P90 > 1.5× P50 | Regional parks (volatile) |
| **Tier 3** | All attractions with avg wait > 3m | Small parks (consistent low waits) |
| **Fallback** | Top 5 attractions by P90 (avg ≥ 5) | Parks with sparse data or <3m waits |

**Logic:**
1. Try to find at least 3 Tier 1 attractions.
2. If <3 found, try to add Tier 2 attractions.
3. If still <3, add Tier 3 attractions.
4. If **0 headliners**, force-select top 5 by P90 (fallback strategy).

#### Data Quality Filters (applied in all tiers)

Historical queries used for headliner identification and baseline calculation apply two filters:

1. **`waitTime >= 10`** — excludes walk-on placeholder values. Queue-Times reports `waitTime=1` for water slides and other "open but no queue" attractions. Including them inflates sample counts and deflates P50/P90.
2. **Schedule JOIN** — excludes closed-day data. Historical samples are joined against `schedule_entries` (park-level). Days with explicit `CLOSED` or similar non-OPERATING entries are excluded.

```sql
LEFT JOIN schedule_entries se
  ON se."parkId" = a."parkId"
  AND se.date = DATE(qd.timestamp AT TIME ZONE <park_timezone>)
  AND se."attractionId" IS NULL
WHERE qd."waitTime" >= 10
  AND (se.id IS NULL OR se."scheduleType" = 'OPERATING')
```

### Step 2: Calculate Park Baselines
Using only the identified headliners:
1. For each headliner, the headliner-identification step already stored `p50Wait548d` and `p90Wait548d` from a single PERCENTILE_CONT scan.
2. Park P50 baseline = **average of per-headliner P50s** (avoids the high-frequency-low-wait ride bias of a pooled percentile).
3. Park P90 baseline = **average of per-headliner P90s**, same shape, free since we already have the values.
4. Store both in `park_p50_baselines` / `park_p90_baselines` and prime the Redis cache.

### Step 3: Calculate Attraction Baselines
For each attraction, the daily cron runs a single 548-day scan computing both `PERCENTILE_CONT(0.5)` and `PERCENTILE_CONT(0.9)`. Both percentiles are produced from one sort, so adding P90 cost nothing on top of the existing P50 job. Results land in `attraction_p50_baselines` / `attraction_p90_baselines` plus Redis (`attraction:p50:{id}`, `attraction:p90:{id}`, 24 h TTL).

---

## 3. Crowd Level Thresholds

We map the percentage `(Current_Peak / P90_Baseline) × 100` to a human-readable level using relative thresholds.

| Level | Range (Relative to Baseline) | Description |
|-------|------------------------------|-------------|
| **Very Low** | **0 – 60%** | Walk-on conditions (≤ 0.6× normal peak) |
| **Low** | **61 – 89%** | Quieter than expected peak |
| **Moderate** | **90 – 110%** | **Typical peak day** (within ±10% of normal) |
| **High** | **111 – 150%** | Busier-than-typical peak |
| **Very High** | **151 – 200%** | Peak season / holidays |
| **Extreme** | **> 200%** | Major events / capacity reached |

> "Moderate" is the target for a standard operating day's peak.

---

## 4. Technical Architecture

### Database (cache tables)
| Table | Purpose |
|-------|---------|
| `park_p50_baselines` | Park P50 baseline (headliners only). Drives stats / avg-shaped surfaces, plus fallback for crowd level. |
| `park_p90_baselines` | Park P90 baseline (headliners only). **Primary** for crowd level. |
| `attraction_p50_baselines` | Per-attraction P50 baseline. Fallback for crowd level. |
| `attraction_p90_baselines` | Per-attraction P90 baseline. **Primary** for crowd level. |
| `headliner_attractions` | Which attractions were selected as headliners per park. |

See [Caching Strategy](../architecture/caching-strategy.md) for `park_daily_stats` and `queue_data_aggregates`.

### Redis
| Key | TTL | Content |
|-----|-----|---------|
| `park:p50:{parkId}` | 24 h | Park P50 baseline (JSON `{p50, confidence}`). |
| `park:p90:{parkId}` | 24 h | Park P90 baseline (JSON `{p90, confidence}`). |
| `attraction:p50:{attractionId}` | 24 h | Attraction P50 baseline (number). |
| `attraction:p90:{attractionId}` | 24 h | Attraction P90 baseline (number). |

The legacy `analytics:percentile:sliding:*` keys produced by the deleted `occupancy-calculation` precompute job are no longer written. They naturally TTL out within 24 h of deploy.

### Services
- **`AnalyticsService`**:
  - **Park**: `getP90BaselineFromCache(parkId)` → `getP50BaselineFromCache(parkId)` (fallback).
  - **Attraction**: `getAttractionP90BaselineFromCache(id)` → `getAttractionP50BaselineFromCache(id)` (fallback). Batch variant: `getBatchAttractionP90Baselines(ids)` (MGET + DB hydrate + pipeline writeback) plus the existing `getBatchAttractionP50s(ids)`.
  - `getLoadRating(current, baseline)` and `getAttractionCrowdLevel(waitTime, baseline)` are agnostic to which percentile the baseline carries — callers decide.
  - `getCurrentParkPeakWait(parkId, headlinerIds?, windowMinutes=60)` — live counterpart of the 548-day P90 baseline: per-headliner MAX in window, averaged.
  - `calculateCrowdLevelForDate(entityId, type, date, timezone)` — historical crowd level for a specific date; uses day-P90 ÷ baseline-P90 (P50 fallback when no P90 row).
- **`common/utils/crowd-level.util.ts#determineCrowdLevel(occupancy)`**: single source of truth for the occupancy → CrowdLevel threshold mapping in §3.
- **`P50BaselineProcessor`**: Bull job (daily 3 AM parks, 4 AM attractions) populating both P50 and P90 baseline tables in one pass.

---

## 5. Machine Learning Integration

The Python ML service receives both baselines in every prediction request:
- **`p50Baseline`** — kept for legacy/avg-shaped consumers and rolling-average fallback.
- **`p90Baseline`** — primary baseline for crowd-level alignment.

`getCrowdLevelTrainingData` labels each training day with `day-P90 ÷ P90 baseline` so the labels stay apples-to-apples with the user-facing reading. Models recalibrate within ~1 daily training cycle after the switch.

---

## 6. Migration Notes (from the P50-only system)

The previous system was P50-relative on both sides (current avg ÷ P50 baseline). Switching to P90 changed crowd readings for live and calendar surfaces. The threshold table is unchanged, so the *labels* ("moderate", "high", etc.) keep their human-readable meaning — only the underlying inputs are different. Concretely:

- A normal Saturday at Phantasialand used to read "moderate" (P50 ≈ baseline P50) and still reads "moderate" (P90 ≈ baseline P90).
- A day where the headliner spiked to ~2× normal peak shifts from "moderate" (P50 was only mildly elevated) to "high" (P90 elevated as users actually experienced).

The mixed-percentile bug in the old calendar `peakLoad` (P90 wait ÷ P50 baseline) is fixed as part of this migration — peakLoad and crowdLevel are now both peak-vs-peak.
