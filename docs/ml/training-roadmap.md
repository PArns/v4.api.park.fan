# ML Training Roadmap

> Last updated: 2026-04-05  
> Current model: v20260405_121220 — MAE 6.06, RMSE 10.32, MAPE 36.10%, R² 0.8396

This document tracks what we know, what we've fixed, what's still broken, and what the next training steps should be.

---

## Current State (v20260405_121220)

| Metric | Value |
|--------|-------|
| MAE | 6.06 min |
| RMSE | 10.32 min |
| MAPE | 36.10% |
| R² | 0.8396 |
| Training parks | 80 (OPERATING schedule only) |
| Parks getting predictions | ~148 (83 OPERATING + ~65 UNKNOWN via parkLiveStatus) |

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
**Why this is safe**: Both queries already filter `qd.status = 'OPERATING'` AND `qd.waitTime >= 5`. A truly closed park has no data meeting those filters → produces 0 samples → no baseline stored. Only genuinely operating UNKNOWN parks get baselines.

**Next step**: Trigger the p50-baseline job manually after deployment to compute baselines for UNKNOWN parks. Until then, `park_occupancy_pct` remains 100 for these parks.

---

### 2. UNKNOWN Parks Missing from Training Data — OPEN

**Root cause**: Training SQL in `db.py` (line ~136):
```sql
AND (se.id IS NULL OR se."scheduleType" = 'OPERATING')
```
This excludes 22 UNKNOWN parks from training (246k quality rows, 700 park-days).

**What we tried**: Including UNKNOWN caused regression (MAE 14.4 → R² 0.37). The issue was that closed UNKNOWN parks (Six Flags seasonal etc.) contributed zero-wait rows, AND `is_park_open=0` was incorrect for open UNKNOWN parks, confusing the model.

**SQL data breakdown** (last 365 days):
| Category | Park-days | Quality rows (waitTime≥5) |
|----------|-----------|--------------------------|
| Would include (heuristic OPERATING) | 700 | 246,145 |
| Would exclude (heuristic CLOSED) | 4,030 | 0 |

**Correct approach** (not yet implemented):
Use the ride heuristic to reclassify UNKNOWN (park, date) pairs in training:
```sql
WITH unknown_operating AS (
  SELECT se."parkId", se.date
  FROM schedule_entries se
  JOIN attractions a ON a."parkId" = se."parkId"
  JOIN queue_data q ON q."attractionId" = a.id
    AND q.timestamp::date = se.date
    AND q."waitTime" IS NOT NULL
  WHERE se."scheduleType" = 'UNKNOWN'
  GROUP BY se."parkId", se.date
  HAVING COUNT(*) >= 3 
    AND 100.0 * COUNT(CASE WHEN q."waitTime" >= 5 THEN 1 END) / COUNT(*) >= 25
)
-- Then in main query: OR EXISTS (SELECT 1 FROM unknown_operating uo WHERE uo."parkId" = ...)
```
AND reclassify `is_park_open = 1` for those rows in training.

**Risk**: Needs careful testing. Validate MAE on a hold-out set before deploying.

**Expected gain**: +23% training data, better predictions for 22 major parks including USJ, Universal, Warner Bros.

---

### 3. CLOSED Schedule but Operating Rides — LOW PRIORITY

**Data** (last 60 days):
- 88,560 rows with `waitTime ≥ 5, status = OPERATING` on `scheduleType = CLOSED` days
- Top parks: Parque Warner Madrid (15k rows), Carowinds (10k), Walibi Holland (9k)
- Likely causes: schedule API data quality issues, soft-openings, private events

Currently excluded from training. Don't include until Issue 2 is resolved.

---

### 4. Rolling Average Dominance in Features — MONITOR

Feature importance concern: `rolling_avg_7d` or similar should stay below 25%.

Good model signature (v20260329_085046):
- `attractionId`: 24.7%
- `park_occupancy_pct`: 17.1%
- `volatility_weekday`: 12.7%

Bad pattern: `avg_wait_last_24h > 25%` → model memorizes recent history, bad for future predictions.

Monitor after every retraining. Check `/v1/ml/models/metrics-history` for trends.

---

## Training Trigger Checklist

Before triggering a new training:
1. [ ] P50 baselines up to date (`SELECT COUNT(*) FROM attraction_p50_baselines`)
2. [ ] BullMQ jobs healthy (no stalled repeatable jobs)
3. [ ] Queue data recent (`SELECT MAX(timestamp) FROM queue_data`)
4. [ ] Check `db.py` training SQL filter (should be `'OPERATING'` unless testing UNKNOWN fix)

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

### Step 1: Redeploy API + Force P50 Recalculation (Now)
1. Deploy NestJS changes (P50 fix for UNKNOWN parks)
2. Manually trigger p50-baseline job to compute baselines for UNKNOWN parks
3. Verify baselines exist for USJ, Universal Studios, Warner Bros Movie World
4. Check `park_occupancy_pct` is no longer flat-100 for these parks at inference

### Step 2: Test UNKNOWN Training Inclusion (Next sprint)
1. Implement ride-heuristic reclassification in `db.py` training SQL
2. Also fix `is_park_open` for reclassified rows (set to 1)  
3. Train a shadow model (don't activate yet)
4. Compare MAE on hold-out set: should improve, especially for UNKNOWN parks
5. Verify feature importances: `attractionId` and `park_occupancy_pct` should dominate, not rolling averages

### Step 3: Evaluate Seasonal Pattern Handling
- Check if predictions degrade for seasonal parks that just opened (Canada's Wonderland, Six Flags)
- These parks have 0 pct_5plus now but will be operating in summer
- May need park-specific opening season awareness

### Step 4: MAPE Improvement
Current MAPE at 36% is high. This is largely driven by short-wait predictions (error% is huge for 2min predicted vs 5min actual).
Consider: minimum clipping of predictions at 5 minutes for OPERATING rides.

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
  AND q."waitTime" >= 5
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
WHERE q."waitTime" >= 5 AND q.status = 'OPERATING'
  AND q.timestamp >= NOW() - INTERVAL '60 days'
GROUP BY se."scheduleType";
```
