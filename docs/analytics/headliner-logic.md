# P50 Headliner Identification & Validation

> **Context**: To calculate accurate Crowd Levels, we compare current wait times against a "Normal" (P50) baseline. This baseline is derived ONLY from "Headliner" attractions, which best represent the crowd flow.

## The 3-Tier Adaptive System

Parks vary wildly in size and ride composition. A "one-size-fits-all" rule for selecting headliners fails for small parks. We use an adaptive 3-tier system.

### Tier 1: True E-Tickets (Major Parks)
*Designed for Disney, Universal, Phantasialand.*

- **Avg Wait**: > 15 minutes
- **P90 Wait**: > 25 minutes
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
- **Constraint**: Must have avg wait > 0.

## Validation Checklist

When investigating "weird" crowd levels (e.g., "Very Low" when it looks busy):

1. **Check Headliners**: Are the right rides selected?
   - Database: `SELECT * FROM headliner_attractions WHERE "parkId" = '...'`
2. **Check Baseline**: Is the P50 value reasonable?
   - Database: `SELECT * FROM park_p50_baselines WHERE "parkId" = '...'`
   - *Example*: If P50 is 5 mins, the park will always look "Extreme". If P50 is 90 mins, it will always look "Very Low".
3. **Check Real-Time Data**: Are the headliners actually operating?
   - If all headliners are `DOWN`, the system falls back to available rides, which might skew comparisons.
