# Crowd Level System (Peak-vs-Median)

> **Summary**: The Park Fan API uses a **P50 (median) baseline** as the typical-day reference for every user-facing crowd-level reading on parks and attractions. The 100% mark is "what a typical wait looks like at a typical moment"; > 150% means the current peak is noticeably above typical. P90 stays available as a hard fallback for brand-new entities that don't have a P50 row yet.

**Related**: [Caching Strategy](../architecture/caching-strategy.md) (Redis keys and DB cache tables), [Headliner Logic](headliner-logic.md) (park baseline only).

---

## 1. Core Concept

### Why peak-vs-median?
A P90-vs-P90 system (the previous design) compared "the current peak" against "the 90th-percentile peak day in the last 18 months." That made almost every ordinary day read as "low" or "very_low" — only true outlier days touched the baseline. Switching the denominator to the **median wait** turns the ratio into "current peak experience vs typical wait", which lines up with how users perceive crowd levels.

### Parks (live)
- **Baseline**: P50 of wait times over a 548-day rolling window, averaged across headliner attractions, stored in `park_p50_baselines`.
- **Current value**: per-headliner **MAX wait in the last 20 minutes**, averaged across headliners. 20 min is short enough to reflect a queue dropping (longer windows hold onto stale peaks); with 5-min sampling that's ≈ 4 samples per ride, so the MAX is effectively the recent P90. The window auto-expands to 60 min → 240 min only when the 20-min window has no qualifying samples at all (source lag, sparse-reporting ride).

### Parks (historical calendar)
- **Baseline**: same P50 baseline (median).
- **Daily peak value**: **P90 of in-hours slot P90s** for that date. Each `attraction_hourly_history` row stores per-attraction 15-min-slot rollups; we take the 90th percentile of those slots so a single outlier sample doesn't dominate the day's reading.

### Attractions
Each attraction has its own **P50 baseline** (548-day median of that ride's waits) and an **attraction_p90_baselines** row that acts as fallback.
- Live: `current_wait / P50_baseline`.
- Calendar daily: `P90(slot_P90s) / P50_baseline`.

### Formula (same for park and attraction)
```typescript
CrowdLevel% = (Current_Peak / P50_Baseline) * 100
// Fallback when no P50 baseline row exists yet (new entities):
CrowdLevel% = (Current_Peak / P90_Baseline) * 100
```

For **single attractions** in the live view, the current wait IS the current peak (one ride, one wait at any moment), so no aggregation is needed.

> **API note**: the legacy field name `baseline90thPercentile` is retained for backwards compatibility but now carries the active baseline (P50 by default, P90 fallback only).

---

## 2. Calculation Methodology

A **daily background job** populates baselines: 3 AM for parks, 4 AM for attractions. A 4:30 AM `attraction-hourly-history` cron rolls up per-attraction 15-min-slot data for the previous day so the calendar's daily P90-of-slots reading can serve from one indexed SELECT.

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
For each attraction, the daily cron runs a single 548-day scan computing both `PERCENTILE_CONT(0.5)` and `PERCENTILE_CONT(0.9)`. Both percentiles are produced from one sort, so populating P50 and P90 together costs nothing on top of a single-percentile job. Results land in `attraction_p50_baselines` / `attraction_p90_baselines` plus Redis (`attraction:p50:{id}`, `attraction:p90:{id}`, 24 h TTL).

### Step 4: Hourly History Rollup
`AttractionHourlyHistoryProcessor` runs daily at 04:30 and writes one row per (attractionId, date) into `attraction_hourly_history` with a JSONB array of 15-min-slot rollups (`time_slot`, `p90`, `avgWait`, `sampleCount`). The calendar daily view reads these rollups directly and computes **P90 of in-hours slot P90s** as the day's representative peak. Backfills can be queued via `backfill-attraction-hourly-history`.

---

## 3. Crowd Level Thresholds

We map the percentage `(Current_Peak / P50_Baseline) × 100` to a human-readable level using relative thresholds. The ladder is the same as before the P50 switch — only the inputs changed.

| Level | Range (Relative to Baseline) | Description |
|-------|------------------------------|-------------|
| **Very Low** | **0 – 60%** | Walk-on (current peak well below typical wait) |
| **Low** | **61 – 89%** | Quieter than typical |
| **Moderate** | **90 – 110%** | Current peak ≈ typical wait (target band) |
| **High** | **111 – 150%** | Above typical |
| **Very High** | **151 – 200%** | Peak season / holiday traffic |
| **Extreme** | **> 200%** | Major event / capacity stress |

> "Moderate" means the current peak matches what a typical wait looks like — i.e., the baseline. Anything above 150% means the queue is significantly busier than a typical-moment wait.

---

## 4. Technical Architecture

### Database (cache tables)
| Table | Purpose |
|-------|---------|
| `park_p50_baselines` | Park P50 baseline (headliners only). **Primary** for crowd level + stats / avg-shaped surfaces. |
| `park_p90_baselines` | Park P90 baseline (headliners only). Fallback when no P50 row yet. |
| `attraction_p50_baselines` | Per-attraction P50 baseline. **Primary** for crowd level. |
| `attraction_p90_baselines` | Per-attraction P90 baseline. Fallback. |
| `attraction_hourly_history` | Per-day 15-min-slot rollup (JSONB) used by the calendar daily reading. |
| `headliner_attractions` | Which attractions were selected as headliners per park. |

See [Caching Strategy](../architecture/caching-strategy.md) for `park_daily_stats` and `queue_data_aggregates`.

### Redis
| Key | TTL | Content |
|-----|-----|---------|
| `park:p50:{parkId}` | 24 h | Park P50 baseline (JSON `{p50, confidence}`). |
| `park:p90:{parkId}` | 24 h | Park P90 baseline (JSON `{p90, confidence}`). |
| `attraction:p50:{attractionId}` | 24 h | Attraction P50 baseline (number). |
| `attraction:p90:{attractionId}` | 24 h | Attraction P90 baseline (number). |

### Services
- **`AnalyticsService`**:
  - **Park**: `getP50BaselineFromCache(parkId)` (primary) → `getP90BaselineFromCache(parkId)` (fallback).
  - **Attraction**: `getAttractionP50BaselineFromCache(id)` (primary) → `getAttractionP90BaselineFromCache(id)` (fallback). Batch variants: `getBatchAttractionP50s(ids)` and `getBatchAttractionP90Baselines(ids)` (MGET + DB hydrate + pipeline writeback).
  - `getLoadRating(current, baseline)` and `getAttractionCrowdLevel(waitTime, baseline)` are agnostic to which percentile the baseline carries — callers pick.
  - `getCurrentParkPeakWait(parkId, headlinerIds?, windowMinutes=20)` — live counterpart: per-headliner MAX in window, averaged. Auto-expands to 60 min → 240 min when the requested window has no data.
  - `calculateCrowdLevelForDate(entityId, type, date, timezone)` — historical crowd level for a specific date; uses day-P90 ÷ P50 baseline (P90 fallback when no P50 row).
- **`AttractionIntegrationService` (calendar daily)**: reads `attraction_hourly_history`, computes P90 of in-hours slot P90s, divides by attraction P50 baseline.
- **`common/utils/crowd-level.util.ts#determineCrowdLevel(occupancy)`**: single source of truth for the occupancy → CrowdLevel threshold mapping in §3.
- **`P50BaselineProcessor`**: Bull job (daily 3 AM parks, 4 AM attractions) populating both P50 and P90 baseline tables in one pass.
- **`AttractionHourlyHistoryProcessor`**: Bull job (daily 4:30 AM) populating `attraction_hourly_history` for yesterday; `backfill-attraction-hourly-history` for date-range backfills.

---

## 5. Machine Learning Integration

The Python ML service receives both baselines in every prediction request:
- **`p50Baseline`** — primary baseline for crowd-level alignment.
- **`p90Baseline`** — passed for backwards compatibility and as fallback.

`getCrowdLevelTrainingData` labels each training day with `day-P90 ÷ P50 baseline` so the labels stay apples-to-apples with the user-facing reading. The ML model predicts wait times directly; the baseline only affects the labelled percentage exposed for evaluation, so swapping the denominator doesn't require retraining.

---

## 6. Migration Notes (from the P90-only system, PR #46)

PR #46 had switched everything to P90-vs-P90 ("today's peak vs typical peak"). The math was apples-to-apples but the user experience suffered: a typical day rarely reaches the all-time P90 peak, so almost every day read as "low" or "very_low". This document describes the corrected design — P50 baseline + short-window current peak.

Threshold table is unchanged; the *labels* keep their human-readable meaning. Concretely:

- A typical Saturday at Phantasialand reads "moderate" (current peak ≈ P50 baseline).
- A day where the headliners spiked materially above typical shifts up the ladder ("high" / "very_high") instead of staying stuck at "low".
- Calendar daily readings no longer collapse to "very_low" for ordinary operating days.

The old P90 baseline columns and Redis keys are retained — they're the fallback path. They can be reclaimed in a future cleanup once we're confident every entity has a populated P50 row.
