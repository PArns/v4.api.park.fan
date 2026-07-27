# Crowd Level System (Typical-Day-Peak Daily / Ratio-vs-P50 Live)

> **Summary**: The Park Fan API runs two crowd-level regimes. **Daily/historical** aggregates (the calendar) are **typical-day-peak** — a day's AVG-across-headliners-of-each-ride's-P90 (`day_value`) divided by the **typical-day-peak baseline** (the median over operating days of that same `day_value`, 548-day window), so a statistically typical day reads ≈ 100% = `moderate` and genuinely busy seasons (Wintertraum, Easter, promos) correctly read high/very_high/extreme. **Point-in-time/live** signals (live overview, the calendar "today" cell, hourly predictions) stay **ratio-vs-P50** — current minutes ÷ P50 baseline. The two are never mixed on a single surface.
>
> Within each regime, cross-ride aggregation is a **weighted mean of minutes**, never a percentile across per-ride ratios. Sum the numerators and sum the baselines, then divide. A percentile across ratios is an extreme-value estimator: over a ten-ride headliner set its P90 is just the second-busiest ride, it can only ever push the reading up, and it lets a ride with a small baseline outvote a marquee. See [Migration Notes](#6-migration-notes).

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
- **Park occupancy**: the **baseline-weighted mean** across reporting headliners — `Σ latest_wait ÷ Σ that_ride's_P50_baseline`, × 100 (`getHeadlinerLoad`). Reads as "the headliners together are queueing X% of their typical minutes". Each ride carries the weight of the queue it actually represents, so a marquee with a 45-min median counts for more than a family ride with a 10-min one, and the reading moves in both directions.
- **No wait floor**: unlike the spot-peak query, this one applies **no** `MIN_WAIT_TIME_THRESHOLD`. In a weighted mean a 10-minute floor would delete the quietest queues from the numerator and bias the park upward on exactly the days that should read low; an OPERATING headliner at 5 min is signal. Rides whose source reports no usable waits are still excluded — they have no positive P50 baseline.
- **Composition-proof**: only rides that actually reported enter both sums, so a closed headliner (Chiapas on a winter day) leaves the numerator *and* the denominator instead of deflating one against a fixed reference.
- **Current value per ride**: latest reported `waitTime` within a 60-min freshness window. Latest-per-ride (not window MAX) keeps the reading responsive when a queue drops; the 60-min window is long enough to catch sparse-reporting headliners (Mario Kart, Harry Potter only emit every 10-15 min).
- **Park-wide P50 fallback**: when per-ride baselines are unavailable (brand-new park, no `attraction_p50_baselines` rows), we degrade to a park-wide computation: average latest across reporting headliners ÷ park P50 baseline from `park_p50_baselines`.
- **The reported breakdown divides out to the percentage**: `breakdown.typicalAvgWait` is the mean P50 over exactly the rides in `breakdown.currentAvgWait`, so `currentAvgWait / typicalAvgWait ≈ current / 100`. Do not substitute the park-wide P50 here — that mismatch is what produced payloads showing "25 min now / 30 min typical" beside "+23% busier than typical".

### Parks (historical calendar) — typical-day-peak
- **Numerator (`day_value`)**: the **AVG across headliner rides of each ride's daily P90** (each headliner's P90-of-day is `PERCENTILE_CONT(0.9)` over that ride's raw `queue_data` for the day — `calculateCrowdLevelForDate`, park branch). Every headliner contributes equally — this is an AVG across rides, not a percentile across rides.
- **Baseline (typical-day-peak)**: the **median over operating days (548-day window) of `day_value`** — i.e. what a typical day's averaged peak looks like. For Phantasialand this is ≈ 40.3 min (vs the pooled P90 baseline 51.6).
- **Future/predicted days**: same typical-day-peak baseline; numerator = AVG of predicted headliner waits. Those predictions are a **day-peak proxy** — `ml-service/predict.py` predicts several peak-window hours per day and collapses them to the per-day MAX — so the future numerator is peak-shaped like the historical one. Keep it that way: past and future land in the same response field (`headlinerForecast.avgWait`) and the same UI slot, and a mean-on-one-side/peak-on-the-other mismatch reads as the park getting busier next week when only the statistic changed.
- 100% reads as "a statistically typical day" = `moderate`.
- **No calendar fallback**: the typical-day-peak is written atomically with P50/P90 in the daily cron, so a missing value means the park has no baseline at all (brand-new, or gated as not-ratable below `MIN_BASELINE_OPERATING_DAYS`) → `unknown`. There is no typical→P90→P50 fallback chain, and no invented `moderate`.

### Attractions
Each attraction has its own **P50 baseline** (median, in `attraction_p50_baselines`), a **P90 baseline** (548-day P90, in `attraction_p90_baselines`, computed-for-free metadata) and a **typical-day-peak baseline** (median over operating days of the day's peak — computed on-demand from `queue_data_aggregates`, cached in Redis `attraction:typicalpeak:{id}`).
- Live (ratio-vs-P50): `current_wait / P50_baseline`.
- Calendar daily (per-attraction): `P90(slot_P90s) / attraction_typical_day_peak`. This is the attraction analogue of the park calendar — dividing by the typical-day-peak (not P50/P90) keeps a normal day at ≈ 100% = `moderate`. No fallback: when the typical-day-peak is absent (too little data), the day's rating is skipped.

### Formulas

**Per attraction (live or calendar-current-day):**
```
CrowdLevel% = (Current_Wait / P50_Baseline) * 100
```

**Park live (headliner-load path):**
```
reporting = headliners.filter(h => h.hasSampleInLast60min AND h.attractionP50 > 0)
CrowdLevel% = sum(reporting.latestWait) / sum(reporting.attractionP50) * 100
```
A weighted mean, **not** a mean or percentile of the per-ride ratios — see [Migration Notes](#6-migration-notes) §live for why the percentile version was wrong.

**Park live fallback (no per-ride baselines):**
```
CrowdLevel% = avg(latest waits) / park_P50_baseline * 100
```

**Calendar day (historical, park) — typical-day-peak:**
```
day_value      = AVG over headliners of [ that ride's daily P90 (PERCENTILE_CONT(0.9) over queue_data) ]
typical_day_peak = median over operating days (548d) of day_value
CrowdLevel%    = day_value / typical_day_peak * 100
```

**Calendar day (historical, attraction) — per-attraction typical-day-peak:**
```
slot_P90s              = attraction_hourly_history.slots[].p90 (filtered by operating hours)
day_peak               = P90(slot_P90s)
attraction_typical_day_peak = median over operating days of day_peak   // from queue_data_aggregates
CrowdLevel%            = day_peak / attraction_typical_day_peak * 100   // no P50/P90 fallback
```

**Fallbacks:**
- Calendar/daily park surface: **none** — typical-day-peak is written atomically with P50/P90, so its absence means a park with no usable baseline → `unknown`.
- Live surfaces: when per-ride P50 baselines are unavailable, use the park-wide P50 fallback above.

**No invented ratings.** Every surface that cannot rate against a real baseline emits `unknown`, never a placeholder `moderate` — a made-up "typical" is indistinguishable to a reader from a measured one. That covers `rateOrUnknown`, `getLoadRating(current, baseline<=0)`, `getAttractionCrowdLevel` returning null (caller maps to `unknown`), the calendar's hourly predictions (no hardcoded reference wait), and the ratability gate on `statistics.crowdLevel`. A **0-minute wait against a real baseline is still rated** — that is a walk-on, not missing data, so it reads `very_low`.

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
| `attraction_p90_baselines` | Per-attraction P90 baseline. Computed-for-free metadata; **no longer** the calendar reference (the per-attraction calendar now divides by the attraction typical-day-peak, cached in Redis `attraction:typicalpeak:{id}`). |
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
  - `getLoadRating(current, baseline)` and `getAttractionCrowdLevel(waitTime, baseline)` are agnostic to which baseline the caller passes. Both refuse to invent one: `getLoadRating` returns `unknown` when the baseline is missing or non-positive, `getAttractionCrowdLevel` returns `null` for the caller to map.
  - `getHeadlinerLoad(parkId, headlinerIds)` — **the live park reading**: `Σ latest_wait ÷ Σ attraction P50` over the headliners with a sample in the last 60 min, plus the matched `averageCurrentWait` / `averageTypicalWait` pair the API's `breakdown` reports. Returns `null` when no ride has both a sample and a baseline, which is what selects the park-wide fallback below.
  - `getCurrentParkPeakWait(parkId, headlinerIds?, windowMinutes=60)` — the park-wide fallback: per-headliner latest in window, averaged. Retries at `minWaitTime = 0`, then auto-expands 60 min → 240 min when the requested window has no data.
  - `calculateCrowdLevelForDate(entityId, type, date, timezone)` — historical crowd level for a specific date; the park branch divides `day_value` (AVG of per-ride daily P90) by the typical-day-peak baseline (`baselineType = "typical_day"`).
  - `getHeadlinerDailyPeaks(attractionIds, from, to, timezone)` — actual recorded per-ride **day-P90s**, powering the calendar's past-day headliner figures. Peak-shaped on purpose, to match the forecast side of the same field.
  - `averageTodayAcrossRides(rows)` — collapses per-ride "today" rows to the `avgWaitToday` / `peakWaitToday` pair. Both come from the same rides, and per ride AVG ≤ MAX, so the pair the page renders is ordered by construction.
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

The **live/point-in-time surfaces keep the P50 baseline** — the live overview / `getCurrentOccupancy`, the calendar "today" cell, and the hourly within-a-day predictions all divide by a median wait, not a typical-day-peak. Daily aggregates use the typical-day-peak; point-in-time/live signals use ratio-vs-P50. What *did* change on the live side is how the per-ride ratios are combined into one park number — see below.

### Live: from P90-of-ratios to the baseline-weighted mean

The live park level used to be the **P90 across the per-headliner ratios** (`latest_wait ÷ that ride's own P50`). The intent was to stop one quiet ride averaging a marquee away. The effect was the opposite of a crowd measure:

- With the headliner set capped at 10 (`MAX_TIER1_HEADLINERS`), the P90 index `(n-1) × 0.9` lands on the **second-highest ratio**. The park level was an extreme-value estimator over at most ten rides.
- It is **one-sided**: an outlier can push the reading up but never down, so quiet days could not surface as quiet.
- Comparing each ride to *its own* median makes the ratio's noise scale with `1/baseline`. A ride whose typical wait is 10 minutes swings a full crowd tier on a 10-minute change.

Observed at Phantasialand: the park read `high` (123%) on an afternoon when Taron sat at 20/45 min and F.L.Y. at 20/40. Both were at the bottom of the sorted ratio list and contributed nothing; Crazy Bats (45/30) and Wakobato (40/30) set the level for the whole park.

The replacement sums minutes before dividing — `Σ latest_wait ÷ Σ P50` — so each ride is weighted by the queue it represents. Same afternoon: 240/290 = 83% = `low`. The threshold table is unchanged; only the cross-ride aggregation is.

Three consistency fixes landed with it:

- `breakdown.typicalAvgWait` is now the baseline of exactly the rides in `currentAvgWait`, so the displayed pair divides out to the displayed percentage.
- `avgWaitToday` and `peakWaitToday` come from one per-headliner query. The "average" used to be `ParkDailyStats.p90WaitTime` (a P90 pooled over *all* attractions) while the "peak" was the mean of per-headliner maxima — two statistics over two populations, which is how the park page came to render `avgWaitToday: 45` next to `peakWaitToday: 40`.
- The calendar's past-day headliner figures switched from a day-AVG to a day-P90, matching the peak-proxy the forecast side of that same field already carried.

The typical-day-peak is written atomically with P50/P90, so there is **no** typical→P90→P50 calendar fallback chain; a missing value means a park with no usable baseline → `unknown`. P50 stays load-bearing (live + ML feature + crowd-level fallback in ML). P90 is still computed (essentially free, carries confidence/metadata) but is no longer the calendar reference. See [Typical-Day-Peak Baseline (Calendar)](crowd-level-typical-day-peak.md) for the full calibration writeup and deploy steps.
