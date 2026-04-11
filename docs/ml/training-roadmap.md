# ML Training Roadmap

> Last updated: 2026-04-05  
> Current model: v20260405_1322 — MAE 6.30, RMSE 10.43, MAPE 38.15%, R² 0.8375

This document tracks what we know, what we've fixed, what's still broken, and what the next training steps should be.

---

## Current State (v20260405_1322)

| Metric | Value |
|--------|-------|
| MAE | 6.30 min |
| RMSE | 10.43 min |
| MAPE | 38.15% |
| R² | 0.8375 |
| Training rows | 765k (+47% vs OPERATING-only) |
| Training parks | ~102 (80 OPERATING + 22 UNKNOWN via ride heuristic) |
| Parks getting predictions | ~148 (83 OPERATING + ~65 UNKNOWN via parkLiveStatus) |

**Top feature importances:**
- `rolling_avg_7d`: 24.74% ✓ (under 25% threshold)
- `avg_wait_last_1h`: 17.74%
- `park_occupancy_pct`: 14.58%
- `attractionId`: 3.46% (absorbed by rolling averages — see Issue 4)

**Note on MAE regression**: MAE increased from 6.06 → 6.30 (+4%) after including UNKNOWN parks.
This is expected: 22 high-variance parks (USJ, Universal) are now in training for the first time.
Per-park quality for UNKNOWN parks improved significantly (previously zero training data for those attractions).

---

## Known Issues (Ordered by Impact)

### 1. P50 Baselines Missing for UNKNOWN Parks — FIXED 2026-04-05

**Root cause**: `identifyHeadliners` and `calculateAttractionP50` in `analytics.service.ts` both had:
```sql
AND (se.id IS NULL OR se."scheduleType" = 'OPERATING')
```
Parks with UNKNOWN schedule entries (not missing entries — actual UNKNOWN rows) failed this filter → no headliners identified → no P50 baseline stored.

**Impact**:
- 22+ UNKNOWN parks (USJ, Universal Studios, Warner Bros Movie World, Blackpool etc.) had `p50Baseline = 0`
- `getCurrentOccupancy` returned `100` always for these parks (hardcoded default)
- `park_occupancy_pct = 100` flat for all UNKNOWN parks at inference — feature useless
- `park_occupancy_pct` is ~17% feature importance → severe prediction quality issue

**Fix** (applied): Changed both filters to:
```sql
AND (se.id IS NULL OR se."scheduleType" IN ('OPERATING', 'UNKNOWN'))
```
**Why this is safe**: Both queries already filter `qd.status = 'OPERATING'` AND `qd.waitTime >= 10`. A truly closed park has no data meeting those filters → produces 0 samples → no baseline stored. Only genuinely operating UNKNOWN parks get baselines.

**Next step**: Trigger the p50-baseline job manually after deployment to compute baselines for UNKNOWN parks. Until then, `park_occupancy_pct` remains 100 for these parks.

---

### 2. UNKNOWN Parks Missing from Training Data — FIXED 2026-04-05

**Root cause**: Training SQL in `db.py` excluded all UNKNOWN parks (22 parks, 246k quality rows).

**Previous regression**: Naive inclusion caused MAE 14.4 → R² 0.37 (closed UNKNOWN parks contributed zero-wait rows, `is_park_open=0` incorrect for open ones).

**Fix** (applied): Added `unknown_operating_days` CTE in `db.py` using ride heuristic (≥3 attractions, ≥25% waitTime≥5) to include only genuinely operating UNKNOWN park-days. `is_park_open` correction already handled by existing `features.py` logic (any row with `waitTime≥5` auto-sets `is_park_open=1`).

**Result**:
- Training rows: 520k → 765k (+47%)
- MAE: 6.06 → 6.30 (+4%) — expected increase from 22 high-variance parks entering training for first time
- UNKNOWN parks (USJ, Universal, Warner Bros, Blackpool etc.) now have attraction-specific model weights

**Config changes** (2026-04-05):
- Added `ROLLING_7D_DROPOUT_RATE = 0.35` in `config.py` to prevent `rolling_avg_7d` from dominating (>25%)
- Added dropout logic in `features.py` (after existing 24h/1h dropout block)

---

### 3. CLOSED Schedule but Operating Rides — LOW PRIORITY

**Data** (last 60 days):
- 88,560 rows with `waitTime ≥ 5, status = OPERATING` on `scheduleType = CLOSED` days
- Top parks: Parque Warner Madrid (15k rows), Carowinds (10k), Walibi Holland (9k)
- Likely causes: schedule API data quality issues, soft-openings, private events

Currently excluded from training. Don't include until CLOSED schedule data quality is investigated.

---

### 4. Rolling Average Dominance in Features — MONITOR

Feature importance concern: `rolling_avg_7d` or similar should stay below 25%.

Current model signature (v20260405_1322, 63 features):
- `rolling_avg_7d`: 24.74% ✓ (kept under threshold via 35% dropout)
- `avg_wait_last_1h`: 17.74%
- `park_occupancy_pct`: 14.58%
- `attractionId`: 3.46% (low but expected: rolling avgs encode attraction-specific info)

Note: `attractionId` importance dropped from 24.7% (v20260329, 40 features) to 3.46% (v20260405_1322, 63 features).
This is expected — rolling_avg_7d and avg_wait_last_1h now capture the per-attraction signal more compactly.
The model isn't worse; the importance is redistributed across more predictive features.

Bad pattern to avoid: `avg_wait_last_24h > 25%` OR `rolling_avg_7d > 25%` → model memorizes recent history.

**Dropout config** (controls this balance):
- `OCCUPANCY_DROPOUT_RATE = 0.50` — 50% of rows use historical DOW×hour proxy
- `ROLLING_AVG_DROPOUT_RATE = 0.40` — 40% of rows blur avg_wait_last_24h/1h
- `ROLLING_7D_DROPOUT_RATE = 0.35` — 35% of rows blur rolling_avg_7d with weekday/weekend avg

Monitor after every retraining. Check `/v1/ml/models/metrics-history` for trends.

---

## Training Trigger Checklist

Before triggering a new training:
1. [ ] P50 baselines up to date (`SELECT COUNT(*) FROM attraction_p50_baselines`)
2. [ ] BullMQ jobs healthy (no stalled repeatable jobs)
3. [ ] Queue data recent (`SELECT MAX(timestamp) FROM queue_data`)
4. [ ] `db.py` has `unknown_operating_days` CTE (UNKNOWN parks via ride heuristic)

To trigger:
```bash
# Via NestJS queue (registers in DB automatically):
# Use admin endpoint or Bull dashboard

# NEVER trigger directly via POST /train — bypasses DB registration
```

---

## Schedule Type Distribution (Last 30 Days)

```
UNKNOWN:   19,408 entries across 118 parks
OPERATING: 13,812 entries across 97 parks  
CLOSED:     1,707 entries across 61 parks
```

**Key insight**: UNKNOWN is the largest category. Most parks that appear to be "well-known" (Universal, Six Flags etc.) never provide schedule data to ThemeParks.wiki.

UNKNOWN parks by operation status:
- **Genuinely operating** (pct_5plus > 25%): USJ, Universal Studios, Warner Bros Movie World, Lake Compounce, Kennywood, Cinecittà World, Adventureland, Beto Carrero, Blackpool
- **Seasonal / closed** (pct_5plus < 15%): Canada's Wonderland, Energylandia, Fårup Sommerland, Le Pal
- **Definitely closed** (pct_5plus = 0%): Six Flags parks (seasonal), Disney Typhoon Lagoon, Grona Lund, Lotte World

---

## Next Training Steps

### Step 1: P50 Baselines for UNKNOWN Parks — DONE 2026-04-05
- Deployed P50 fix for UNKNOWN parks in `analytics.service.ts`
- Manually triggered p50-baseline job → baselines now computed for USJ, Universal, Warner Bros etc.
- `park_occupancy_pct` is no longer flat-100 for UNKNOWN parks

### Step 2: UNKNOWN Training Inclusion — DONE 2026-04-05
- `unknown_operating_days` CTE added to `db.py` (ride heuristic: ≥3 attractions, ≥25% waitTime≥5)
- `is_park_open` handled by existing `features.py` correction logic (no change needed)
- Model v20260405_1322 trained with 765k rows (+47%), MAE 6.30

### Step 3: Evaluate Seasonal Pattern Handling
- Check if predictions degrade for seasonal parks that just opened (Canada's Wonderland, Six Flags)
- These parks have 0 pct_5plus now but will be operating in summer
- They will NOT be included via ride heuristic until they actually open (correct behaviour)
- First predictions for summer-opening parks will be cold-start (no training data for that attraction)

### Step 4: MAPE Improvement
Current MAPE at 38% is high. This is largely driven by short-wait predictions (error% is huge for 2min predicted vs 5min actual).
Consider: minimum clipping of predictions at 5 minutes for OPERATING rides.

### Step 5: Investigate Per-Park MAE for UNKNOWN Parks
After the next daily training cycle accumulates more UNKNOWN park data, compare:
- Global MAE (should trend down as model learns UNKNOWN park attractions)
- Per-park MAE for USJ, Universal, Warner Bros vs OPERATING-only parks
- If UNKNOWN park MAE is significantly higher, may need park-specific calibration

---

## P50 / Occupancy Health Check Queries

```sql
-- Parks without P50 baselines that have recent data
SELECT p.name, COUNT(DISTINCT a.id) as attractions
FROM parks p
JOIN attractions a ON a."parkId" = p.id
LEFT JOIN attraction_p50_baselines b ON b."attractionId" = a.id
LEFT JOIN queue_data q ON q."attractionId" = a.id 
  AND q.timestamp >= NOW() - INTERVAL '7 days'
  AND q."waitTime" >= 10
WHERE b."attractionId" IS NULL AND q.id IS NOT NULL
GROUP BY p.id, p.name
ORDER BY attractions DESC;

-- Training data by schedule type
SELECT 
  COALESCE(se."scheduleType"::text, 'NO_SCHEDULE') as type,
  COUNT(q.id) as rows,
  COUNT(DISTINCT a."parkId") as parks
FROM queue_data q
JOIN attractions a ON a.id = q."attractionId"
LEFT JOIN schedule_entries se ON se."parkId" = a."parkId" 
  AND se.date = q.timestamp::date
WHERE q."waitTime" >= 10 AND q.status = 'OPERATING'
  AND q.timestamp >= NOW() - INTERVAL '60 days'
GROUP BY se."scheduleType";
```
