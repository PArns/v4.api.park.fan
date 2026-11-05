# Headliner identification

Which rides count as a park's headliners, the filters on the history they are chosen from, and what to
check when a park's crowd level looks wrong. Headliners are the only rides in the park baselines and in
the live park reading ([Crowd levels](crowd-levels.md)). Code: `AnalyticsService.identifyHeadliners`,
run by the 03:00 UTC `calculate-park-baselines` job; result in `headliner_attractions`.

## The history the selection reads

548 days of `queue_data` per ride, filtered to:

- `queueType = 'STANDBY'`, `status = 'OPERATING'`;
- `waitTime >= 10` and `< 380`. Queue-Times reports `waitTime = 1` for water slides and other "open, no
  queue" attractions; counting it deflated P50s (Rulantica read 4.4 min instead of ~20) and made normal
  days read `extreme`;
- days whose park-level schedule row (`attractionId IS NULL`) is missing, `OPERATING` or `UNKNOWN`.
  Any other type (`CLOSED`, `TICKETED_EVENT`, `PRIVATE_EVENT`, …) is excluded, which keeps off-season
  data (Kennywood January–March) out of the baselines.

## The tiers

Per ride: average wait, P50, P90 and operating days. Then, per park:

| Tier                     | Criteria                                                                                          | Used when                         |
| ------------------------ | ------------------------------------------------------------------------------------------------- | --------------------------------- |
| 1 (major parks)          | average > 20 min, P90 > 30 min, operating days > 60 % of the park's busiest ride's                | always                            |
| 2 (regional parks)       | average in the park's top 40 %, P90 > 1.5 × P50, operating days > 60 % of the busiest ride's      | fewer than 3 tier-1 rides         |
| 3 (small parks)          | average ≥ the park's median ride average                                                          | fewer than 3 in tier 1 and tier 2 |
| Fallback                 | top 5 rides by P90 (same filters)                                                                 | the tiers found no ride at all    |

A ride in several tiers keeps its best one. When tier 1 yields more than 10 rides, only the 10 with the
highest average wait are kept (`MAX_TIER1_HEADLINERS`): borderline rides at 20–22 minutes diluted the P50
baseline and the ML occupancy feature.

New rides become headliners once they have the history; closed ones age out of the 548-day window. The
set is recomputed every night.

## When a crowd level looks wrong

1. **Headliners**: `SELECT * FROM headliner_attractions WHERE "parkId" = '<park-uuid>';` Are these the
   park's big rides?
2. **Baselines**: `SELECT * FROM park_p50_baselines WHERE "parkId" = '<park-uuid>';` A P50 of 5 minutes
   makes the park look `extreme` all the time, one of 90 makes it look `very_low`. A `NULL`
   `typicalDayPeak` means the park is not ratable yet and reads `unknown`.
3. **Live data**: are the headliners reporting? Only rides with a sample in the last 60 minutes enter the
   live sums.
4. **Schedule**: without an `OPERATING` row for the day, `calculateCrowdLevelForDate` reads the whole
   park-local day (00:00–23:59), pre-opening and post-closing data included, which can depress the level.
