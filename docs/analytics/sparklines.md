# Sparklines: today's wait-time history per ride

The compact `{ timestamp, waitTime }[]` arrays the frontend draws as mini charts on ride cards: what
they contain, which of the two batch functions to call, and how the day's start is chosen per park.
Open it before adding a surface that shows a sparkline.

## What a sparkline holds

Today's readings for one ride, from the park's **effective start time** onwards:

- `queueType = 'STANDBY'`, `status = 'OPERATING'`, non-null `waitTime`;
- rounded to the nearest 5 minutes;
- a point only where the rounded value changes from the previous one.

## Which function

| Context                                                | Call                                                                                                       |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| One park, start time already known                     | `getBatchAttractionWaitTimeHistory(attractionIds, startTime)` — the park payload (`park-integration.service`) |
| Rides from several parks                               | `getAttractionSparklinesBatch([{ id, parkId, timezone }])` — groups by park, resolves each park's start once, merges the maps. Used by `/v1/analytics/realtime` and favorites |
| One ride's detail                                      | `getAttractionStatistics(id, startTime, timezone)`, which fetches it internally                           |

Passing one `startTime` for rides of different parks gives every park but one the wrong day.

## Effective start time

`getEffectiveStartTime(parkId, timezone)`:

1. today's `OPERATING` schedule row's `openingTime`, if there is one (so staff test runs before opening
   are left out);
2. otherwise local midnight (`getStartOfDayInTimezone`).

Cached in Redis for 1 hour with a real opening time, 5 minutes for the midnight fallback, so a schedule
sync later in the hour is picked up. UTC midnight would cut the first nine hours of a Tokyo day; one
local midnight for all parks would mix in yesterday for parks west of UTC.

## Where it appears

- Park payload: `statistics.history` per ride.
- Attraction detail: `statistics.history` (`AttractionStatisticsDto`).
- `/v1/analytics/realtime`: `AttractionStatsItemDto.sparkline` for the longest and shortest wait, next to
  `avgWaitToday`, `minWaitToday`, `peakWaitToday`, `peakWaitTimestamp`, `typicalWaitThisHour` and
  `currentVsTypical`, all filled by the same `getAttractionStatistics` the detail endpoint uses.
- `/v1/favorites`: per favourite ride, via `getAttractionSparklinesBatch`.
