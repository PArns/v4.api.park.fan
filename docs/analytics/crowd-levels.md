# Crowd Level System (Typical-Day-Peak Daily / Ratio-vs-P50 Live)

> **Summary**: The Park Fan API runs two crowd-level regimes. **Daily/historical** aggregates (the calendar) are **typical-day-peak** — a day's AVG-across-headliners-of-each-ride's-P90 (`day_value`) divided by the **typical-day-peak baseline** (the median over operating days of that same `day_value`, 548-day window), so a statistically typical day reads ≈ 100% = `moderate` and genuinely busy seasons (Wintertraum, Easter, promos) correctly read high/very_high/extreme. **Point-in-time/live** signals (live overview, the calendar "today" cell, hourly predictions) stay **ratio-vs-P50** — current peak ÷ P50 baseline. The two are never mixed on a single surface.

> **Authoritative calibration writeup**: see [Typical-Day-Peak Baseline (Calendar)](crowd-level-typical-day-peak.md) for the full investigation, calibration numbers, and deploy steps.

**Related**: [Caching Strategy](../architecture/caching-strategy.md) (Redis keys and DB cache tables), [Headliner Logic](headliner-logic.md) (park baseline only).

---

## 1. Core Concept

### Daily vs live — two regimes
- **Daily/historical (calendar) = typical-day-peak.** A day's representative value (`day_value`) is the **AVG across headliner rides of each ride's daily P90**, divided by the **typical-day-peak baseline** = the **median over operating days (548-day window) of that same `day_value`**. The numerator and the baseline use the identical cross-ride aggregation, so a statistically typical day ≈ 100% = `moderate`. Genuinely busy days (holidays/promos) read high/very_high/extreme — that is correct, not a bug.
- **Point-in-time/live = ratio-vs-P50.** The live park overview / `getCurrentOccupancy`, the calendar "today" cell (today is an incomplete day, so it uses the live signal), and the hourly within-a-day predictions divide a current/median value by the **P50 baseline**. `getCurrentOccupancy` is also an ML feature, so its shape is intentionally fixed.

Rationale: daily aggregates ask "is this day busier than a typical day?" (peak ÷ typical peak); live signals compare the current moment against a typical wait (ratio-vs-P50). The earlier pooled-P90 reference was abandoned because the pooled P90 lives in its own 548-day window and is inflated by the busiest season, so it compressed the top — a typical day skewed low and even peak Wintertraum days couldn't reach very_high (see [Migration Notes](#6-migration-notes)).

### Parks (live)
- **Per-headliner baselines**: P50 (median wait) per attraction over a 548-day rolling window, stored in `attraction_p50_baselines`.
- **Park occupancy**: for each headliner with a recent sample, compute `latest_wait ÷ that_ride's_P50_baseline`; the **P90 across those ratios × 100** is the occupancy percentage. A park with 9 quiet rides and 1 marquee at typical wait surfaces as "moderate" instead of being averaged away.
- **Current value per ride**: latest reported `waitTime` within a 60-min freshness window. Latest-per-ride (not window MAX) keeps the reading responsive when a queue drops; the 60-min window is long enough to catch sparse-reporting headliners (Mario Kart, Harry Potter only emit every 10-15 min).
- **Park-wide P50 fallback**: when per-ride baselines are unavailable (brand-new park, no `attraction_p50_baselines` rows), we degrade to a park-wide computation: average latest across reporting headliners ÷ park P50 baseline from `park_p50_baselines`.

### Parks (historical calendar) — typical-day-peak
- **Numerator (`day_value`)**: the **AVG across headliner rides of each ride's daily P90** (each headliner's P90-of-day is `P90 of in-hours slot P90s` from `attraction_hourly_history`; the slot percentile keeps a single outlier sample from dominating). Every headliner contributes equally — this is an AVG across rides, not a percentile across rides.
- **Baseline (typical-day-peak)**: the **median over operating days (548-day window) of `day_value`** — i.e. what a typical day's averaged peak looks like. For Phantasialand this is ≈ 40.3 min (vs the pooled P90 baseline 51.6).
- **Future/predicted days**: same typical-day-peak baseline; numerator = AVG of predicted headliner waits.
- 100% reads as "a statistically typical day" = `moderate`.
- **No calendar fallback**: the typical-day-peak is written atomically with P50/P90 in the daily cron, so a missing value means the park has no baseline at all (brand-new) → neutral `moderate` default. There is no typical→P90→P50 fallback chain.

### Attractions
Each attraction has its own **P50 baseline** (median, in `attraction_p50_baselines`) and a **P90 baseline** (548-day P90, in `attraction_p90_baselines`).
- Live (ratio-vs-P50): `current_wait / P50_baseline`.
- Calendar daily (per-attraction): `P90(slot_P90s) / P90_baseline`. **Note**: this per-attraction calendar branch is currently uncalled (no surface invokes it) and has **no** typical-day-peak baseline of its own, so it still divides by the attraction P90 (P50 fallback). A per-attraction typical-day-peak could be added later if per-ride calendar crowd levels need their own calibration.

### Formulas

**Per attraction (live or calendar-current-day):**
```
CrowdLevel% = (Current_Wait / P50_Baseline) * 100
```

**Park live (per-headliner ratio path):**
```
ratios = headliners
  .filter(h => h.latestWait >= 5min AND h.attractionP50 > 0)
  .map(h => h.latestWait / h.attractionP50)
CrowdLevel% = P90(ratios) * 100
```

**Park live fallback (no per-ride baselines):**
```
CrowdLevel% = avg(latest waits) / park_P50_baseline * 100
```

**Calendar day (historical, park) — typical-day-peak:**
```
day_value      = AVG over headliners of [ P90(that ride's in-hours slot_P90s) ]
typical_day_peak = median over operating days (548d) of day_value
CrowdLevel%    = day_value / typical_day_peak * 100
```

**Calendar day (historical, attraction) — per-attraction (currently uncalled):**
```
slot_P90s   = attraction_hourly_history.slots[].p90 (filtered by operating hours)
CrowdLevel% = P90(slot_P90s) / attraction_P90_Baseline * 100   // no typical-day-peak; P50 fallback
```

**Fallbacks:**
- Calendar/daily park surface: **none** — typical-day-peak is written atomically with P50/P90, so its absence means a brand-new park with no baseline at all → neutral `moderate` default.
- Live surfaces: when per-ride P50 baselines are unavailable, use the park-wide P50 fallback above.

> **API note**: the legacy field name `baseline90thPercentile` is retained for backwards compatibility; it carries the active baseline for that surface (typical-day-peak on the calendar/daily park surface, P50 on live).

---

## 2. Calculation Methodology

A **daily background job** populates baselines: 3 AM for parks, 4 AM for attractions. A 4:30 AM `attraction-hourly-history` cron rolls up per-attraction 15-min-slot data for the previous day so the calendar's daily P90-of-slots reading can serve from one indexed SELECT.

### Step 1: Identify Headliners
We identify "headliner" attractions per park using a **3-tier adaptive system** to give fair baselines for both mega-resorts and small local parks.

| Tier | Criteria | Target Park Type |
|------|----------|------------------|
| **Tier 1** | Avg wait > 20m AND P90 > 30m | Major theme parks (Disney, Universal) |
| **Tier 2** | Top 40% wait times AND P90 > 1.5× P50 | Regional parks (volatile) |
| **Tier 3** | Top 50% wait times (avg wait ≥ park median) | Small parks (consistent low waits) |
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
3. Park P90 baseline = **average of per-headliner P90s**, same shape, free since we already have the values. P90 is no longer the calendar reference (see Step 2b); it is kept because it is essentially free to compute, carries `confidence`/metadata, and is available for future use.
4. **Typical-day-peak** = `calculateTypicalDayPeak(parkId, headlinerIds)` = the **median over operating days of `AVG(per-ride daily P90)`** (`day_value`). Computed **atomically** in the same pass as P50/P90 (`calculateP50Baseline` returns it; `saveP50Baselines` persists it).
5. Store P50 and the typical-day-peak in `park_p50_baselines` (the latter in the new `typicalDayPeak` column), P90 in `park_p90_baselines`, and prime the Redis caches (including `park:typicalpeak:{parkId}`).

### Step 3: Calculate Attraction Baselines
For each attraction, the daily cron runs a single 548-day scan computing both `PERCENTILE_CONT(0.5)` and `PERCENTILE_CONT(0.9)`. Both percentiles are produced from one sort, so populating P50 and P90 together costs nothing on top of a single-percentile job. Results land in `attraction_p50_baselines` / `attraction_p90_baselines` plus Redis (`attraction:p50:{id}`, `attraction:p90:{id}`, 24 h TTL).

### Step 4: Hourly History Rollup
`AttractionHourlyHistoryProcessor` runs daily at 04:30 and writes one row per (attractionId, date) into `attraction_hourly_history` with a JSONB array of 15-min-slot rollups (`time_slot`, `p90`, `avgWait`, `sampleCount`). The calendar daily view reads these rollups directly and computes **P90 of in-hours slot P90s** as each ride's day-peak. Backfills can be queued via `backfill-attraction-hourly-history`.

---

## 3. Crowd Level Thresholds

We map the percentage to a human-readable level using the same relative thresholds on both regimes (daily: `day_value / typical_day_peak × 100`; live: `current_peak / P50_Baseline × 100`).

| Level | Range (Relative to Baseline) | Daily (typical-day-peak) | Live (ratio-vs-P50) |
|-------|------------------------------|--------------------------|---------------------|
| **Very Low** | **0 – 60%** | Day well below a typical day | Current peak well below typical wait |
| **Low** | **61 – 89%** | Quieter day than typical | Quieter than typical |
| **Moderate** | **90 – 110%** | Statistically typical day (target band) | Current peak ≈ typical wait (target band) |
| **High** | **111 – 150%** | Busier day than typical | Above typical |
| **Very High** | **151 – 200%** | Peak season / holiday traffic | Peak season / holiday traffic |
| **Extreme** | **> 200%** | Major event / capacity stress | Major event / capacity stress |

> On the calendar, "moderate" means a statistically typical day (`day_value` ≈ the typical-day-peak). On live surfaces, "moderate" means the current peak matches a typical wait. Busy holidays/promos correctly reading high/very_high/extreme is expected behaviour.

---

## 4. Technical Architecture

### Database (cache tables)
| Table | Purpose |
|-------|---------|
| `park_p50_baselines` | Park P50 baseline (headliners only) **and** the new `typicalDayPeak` column. P50 is **Primary** for live / ratio-vs-P50 surfaces + stats; `typicalDayPeak` is **Primary** for the calendar/daily park surface. |
| `park_p90_baselines` | Park P90 baseline (headliners only). Computed for free in the same cron; carries `confidence`/metadata. **No longer** the calendar reference and **no longer** a calendar fallback. |
| `attraction_p50_baselines` | Per-attraction P50 baseline. **Primary** for live. |
| `attraction_p90_baselines` | Per-attraction P90 baseline. Used by the (currently uncalled) per-attraction calendar branch. |
| `attraction_hourly_history` | Per-day 15-min-slot rollup (JSONB) used by the calendar daily reading. |
| `headliner_attractions` | Which attractions were selected as headliners per park. |

> **Schema note**: the `typicalDayPeak` column (`numeric(10,2)`, nullable) is created automatically on deploy — the runtime runs TypeORM `synchronize=true` (prod included). No manual `ALTER TABLE` needed. (The `.env.live_debug` file sets `synchronize=false`, but that's only the read-mostly debug-script config, not the app runtime.)

See [Caching Strategy](../architecture/caching-strategy.md) for `park_daily_stats` and `queue_data_aggregates`.

### Redis
| Key | TTL | Content |
|-----|-----|---------|
| `park:p50:{parkId}` | 24 h | Park P50 baseline (JSON `{p50, confidence}`). |
| `park:p90:{parkId}` | 24 h | Park P90 baseline (JSON `{p90, confidence}`). |
| `park:typicalpeak:{parkId}` | 24 h | Park typical-day-peak (median of daily `day_value`). Read-through: Redis → `typicalDayPeak` column → cache. |
| `attraction:p50:{attractionId}` | 24 h | Attraction P50 baseline (number). |
| `attraction:p90:{attractionId}` | 24 h | Attraction P90 baseline (number). |

### Services
- **`AnalyticsService`**:
  - **Park (live)**: `getP50BaselineFromCache(parkId)`.
  - **Park (calendar)**: `getTypicalDayPeakFromCache(parkId)` (Redis → `typicalDayPeak` column → cache). `calculateTypicalDayPeak(parkId, headlinerIds)` computes the median-of-daily-peaks baseline.
  - **Attraction**: `getAttractionP50BaselineFromCache(id)` (primary, live) and `getAttractionP90BaselineFromCache(id)`. Batch variants: `getBatchAttractionP50s(ids)` and `getBatchAttractionP90Baselines(ids)` (MGET + DB hydrate + pipeline writeback).
  - `getLoadRating(current, baseline)` and `getAttractionCrowdLevel(waitTime, baseline)` are agnostic to which baseline the caller passes.
  - `getCurrentParkPeakWait(parkId, headlinerIds?, windowMinutes=20)` — live counterpart: per-headliner MAX in window, averaged. Auto-expands to 60 min → 240 min when the requested window has no data.
  - `calculateCrowdLevelForDate(entityId, type, date, timezone)` — historical crowd level for a specific date; the park branch divides `day_value` (AVG of per-ride daily P90) by the typical-day-peak baseline (`baselineType = "typical_day"`).
- **`CalendarService` (future days)**: `buildPredictedCrowdLevels` → AVG of predicted headliner waits ÷ typical-day-peak.
- **`AttractionIntegrationService` (calendar daily, per-attraction)**: reads `attraction_hourly_history`, computes P90 of in-hours slot P90s, divides by attraction P90 baseline. Currently uncalled; no typical-day-peak.
- **`common/utils/crowd-level.util.ts#determineCrowdLevel(occupancy)`**: single source of truth for the occupancy → CrowdLevel threshold mapping in §3.
- **`P50BaselineProcessor`**: Bull job (daily 3 AM parks, 4 AM attractions) populating P50, P90 **and** `typicalDayPeak` in one pass.
- **`AttractionHourlyHistoryProcessor`**: Bull job (daily 4:30 AM) populating `attraction_hourly_history` for yesterday; `backfill-attraction-hourly-history` for date-range backfills.

---

## 5. Machine Learning Integration

The Python ML service derives its crowd level on the **same scale as the calendar**. `ml.service.ts` now passes **`typicalDayPeakBaseline`** in every prediction request; `ml-service/predict.py` divides the predicted wait by it, with a fallback chain of typical-day-peak → P50 → `rolling_avg_7d` → 30. The dead `p90Baseline` that the request used to carry (Python never read it) was removed.

This keeps `getCrowdLevelTrainingData`-style labels, the yearly-predictions endpoint, and the stored `wait_time_predictions.crowdLevel` apples-to-apples with the user-facing calendar. The ML model predicts wait times directly; the baseline only affects the labelled percentage exposed for evaluation, so swapping the denominator doesn't require retraining. Note: `getCurrentOccupancy` (the live ÷P50 signal) is a separate ML *input feature* (`park_occupancy_pct`) and intentionally keeps its P50 shape.

---

## 6. Migration Notes

The calendar/daily park surface now uses the **typical-day-peak baseline** (`day_value ÷ median-of-daily-peaks`). It got there in two steps:

1. **peak-vs-median (`day_P90 ÷ P50 baseline`)** — the original bug. Because the daily numerator is a P90 but the denominator was a P50 (for Phantasialand P90/P50 ≈ 51.6/30 ≈ 1.7×), a *normal* day rendered as `very_high` and `very_low` was effectively unreachable.

2. **peak-vs-peak (`day_value ÷ pooled P90 baseline`)** — a fix that restored apples-to-apples cross-ride aggregation, but skewed **low**. The pooled P90 baseline lives in its own 548-day window and is inflated by the busiest season, so it **compressed the top**: a typical day landed ≈ 74% = `low`, and even the busiest Wintertraum day only reached ≈ 138% = `high` — nothing hit very_high/extreme.

3. **typical-day-peak (`day_value ÷ median-of-daily-peaks`)** — the chosen fix. Dividing by the **median of daily peaks** (≈ 40.3 min for Phantasialand) instead of the pooled P90 (51.6) is the right reference for a calendar ("is this day busier than a typical day?"). Same day ordering, but the buckets land naturally: the Jan 11 Wintertraum peak (`day_value` 71.4) moves from 138% (high) under pooled-P90 to 177% (very_high), while a typical day centers at 100% = moderate.

The threshold table is unchanged (60/89/110/150/200); the *labels* keep their human-readable meaning. A statistically typical day at Phantasialand reads "moderate", genuinely busy seasons (Wintertraum, Easter, promos) correctly read high/very_high/extreme, and `very_low` is reachable for genuinely quiet days.

The **live/point-in-time surfaces did not change** — the live overview / `getCurrentOccupancy`, the calendar "today" cell, and the hourly within-a-day predictions deliberately stay on the **P50 baseline** (current peak / median wait). Daily aggregates use the typical-day-peak; point-in-time/live signals use ratio-vs-P50.

The typical-day-peak is written atomically with P50/P90, so there is **no** typical→P90→P50 calendar fallback chain; a missing value means a brand-new park with no baseline → neutral `moderate`. P50 stays load-bearing (live + ML feature + crowd-level fallback in ML). P90 is still computed (essentially free, carries confidence/metadata) but is no longer the calendar reference. See [Typical-Day-Peak Baseline (Calendar)](crowd-level-typical-day-peak.md) for the full calibration writeup and deploy steps.
