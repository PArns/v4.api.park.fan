# P50 Headliner Identification & Validation

> **Context**: To calculate accurate Crowd Levels, we compare current wait times against a "Normal" (P50) baseline. This baseline is derived ONLY from "Headliner" attractions, which best represent the crowd flow.

## The 3-Tier Adaptive System

Parks vary wildly in size and ride composition. A "one-size-fits-all" rule for selecting headliners fails for small parks. We use an adaptive 3-tier system.

### Tier 1: True E-Tickets (Major Parks)
*Designed for Disney, Universal, Phantasialand.*

- **Avg Wait**: > 20 minutes
- **P90 Wait**: > 30 minutes
- **Max Headliners**: 10 (top N by avg_wait if more qualify — prevents borderline rides diluting the baseline)
- **Goal**: Select only the big mountains/coasters.

### Tier 2: Popular Rides (Regional Parks)
*Designed for Heide Park, Six Flags.*

- **Avg Wait**: Top 40% of all rides in the park
- **Volatility**: P90 > 1.5x P50 (Indicates it gets busy)
- **Goal**: Used if < 3 Tier 1 rides are found.

### Tier 3: Any Operating Ride (Small Parks)
*Designed for Local Parks.*

- **Avg Wait**: > 3 minutes
- **Goal**: Used if < 3 rides found in Tier 1 & 2.

### Fallback Strategy (The "Nuclear Option")
If even Tier 3 fails to find 3 attractions (e.g., extremely sparse data or tiny park):
- **Force Select**: Top 5 attractions by historical P90 wait time.
- **Constraint**: Must have avg wait ≥ 5 (same data-quality filter as all other tiers).

## Data Quality Filters

All tiers and the fallback apply these filters to historical wait-time data:

### `waitTime >= 10` filter
Queue-Times API reports `waitTime=1` for water-park slides and other "open but no queue" attractions (walk-on placeholder, not a real measurement). Including these values:
- Inflates sample counts with non-representative data.
- Deflates P50 baselines (e.g., Rulantica P50 was 4.4 min with placeholders vs ~20 min without).
- Causes a systematic mismatch: real-time crowd level uses `minWaitTime=5` but baselines did not → "Extreme" level when rides are actually normal.

**Rule**: All historical aggregations use `waitTime >= 10`. The real-time path already uses the same threshold.

### Schedule JOIN (closed-day exclusion)
Seasonal parks accumulate queue data during off-season months (e.g., Kennywood Jan–Mar, Canada's Wonderland winter). Without filtering, this off-season data depresses P50 baselines. Each wait-time sample is joined against the park-level schedule for that day:

- **No schedule entry** → include (unknown = open).
- **`OPERATING`** → include.
- **Any other type** (`CLOSED`, `TICKETED_EVENT`, `PRIVATE_EVENT`, etc.) → exclude.

```sql
LEFT JOIN schedule_entries se
  ON se."parkId" = a."parkId"
  AND se.date = DATE(qd.timestamp AT TIME ZONE <park_timezone>)
  AND se."attractionId" IS NULL
WHERE qd."waitTime" >= 10
  AND (se.id IS NULL OR se."scheduleType" = 'OPERATING')
```

The `attractionId IS NULL` condition selects park-level schedule rows only (not per-attraction entries).

## Validation Checklist

When investigating "weird" crowd levels (e.g., "Very Low" when it looks busy):

1. **Check Headliners**: Are the right rides selected?
   - Database: `SELECT * FROM headliner_attractions WHERE "parkId" = '...'`
2. **Check Baseline**: Is the P50 value reasonable?
   - Database: `SELECT * FROM park_p50_baselines WHERE "parkId" = '...'`
   - *Example*: If P50 is 5 mins, the park will always look "Extreme". If P50 is 90 mins, it will always look "Very Low".
3. **Check Real-Time Data**: Are the headliners actually operating?
   - If all headliners are `DOWN`, the system falls back to available rides, which might skew comparisons.

**Attraction-level baselines:** Per-ride P50 baselines are stored in `attraction_p50_baselines` and used for ride crowd levels (load rating). See [P50 Crowd Levels](p50-crowd-levels.md).
