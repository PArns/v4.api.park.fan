# Sparklines: Wait-Time History for Attractions

> **Summary**: Sparklines are compact timestamp/waitTime arrays used by the frontend to render mini line-charts on ride cards. This document explains the two-layer API, when to use each, and how park timezone is handled correctly.

---

## 1. What a Sparkline Is

A sparkline is an array of `{ timestamp: string; waitTime: number }` pairs covering today's operating window for one attraction. Values are:

- Rounded to the nearest 5 minutes (reduces noise, keeps arrays small).
- Deduplicated: a new point is only appended when the wait time changes from the previous recorded value.
- Filtered to `status = OPERATING` and `queueType = STANDBY`.

The window starts at the park's **effective start time** (schedule opening, or local midnight as fallback) rather than UTC midnight, so the chart does not include pre-opening staff-ride data.

---

## 2. API: Which Function to Use

### `getBatchAttractionWaitTimeHistory(ids, startTime)` — low level

```typescript
async getBatchAttractionWaitTimeHistory(
  attractionIds: string[],
  startTime: Date,
): Promise<Map<string, { timestamp: string; waitTime: number }[]>>
```

**Use when** you are inside a single-park context and already hold the correct `startTime` (obtained via `getEffectiveStartTime`). The park controller does this: it resolves one `startTime` for the whole park and passes it to this function together with all attraction IDs.

```typescript
// Park controller — one park, shared startTime, one call
const startTime = await analyticsService.getEffectiveStartTime(park.id, park.timezone);
const historyMap = await analyticsService.getBatchAttractionWaitTimeHistory(attractionIds, startTime);
```

### `getAttractionSparklinesBatch(attractions)` — multi-park

```typescript
async getAttractionSparklinesBatch(
  attractions: { id: string; parkId: string; timezone: string }[],
): Promise<Map<string, { timestamp: string; waitTime: number }[]>>
```

**Use when** the attractions may come from different parks (different timezones / opening times). The function groups by `parkId`, resolves `getEffectiveStartTime` once per park, then calls `getBatchAttractionWaitTimeHistory` per group. Results are merged into a single map.

```typescript
// Global stats — two rides from potentially different parks
const sparklineMap = await analyticsService.getAttractionSparklinesBatch([
  { id: longestRide.id, parkId: longestRide.parkId, timezone: longestRide.timezone },
  { id: shortestRide.id, parkId: shortestRide.parkId, timezone: shortestRide.timezone },
]);
const sparkline = sparklineMap.get(rideId) ?? [];
```

### Decision table

| Context | Function |
|---|---|
| Single park, all rides share timezone | `getBatchAttractionWaitTimeHistory` (startTime already known) |
| Multiple parks / unknown park | `getAttractionSparklinesBatch` |
| Single attraction detail (ride controller) | `getAttractionStatistics` — wraps the fetch internally |

---

## 3. Effective Start Time

`getEffectiveStartTime(parkId, timezone): Promise<Date>` determines the window start:

1. Looks up today's `OPERATING` schedule entry for the park.
2. Returns `openingTime` if found (the moment the park opens, not midnight).
3. Falls back to `getStartOfDayInTimezone(timezone)` (local midnight) if no schedule exists.
4. Result is cached in Redis: 1 h TTL when a real opening time is known, 5 min TTL for the midnight fallback (so a schedule sync within the hour is picked up quickly).

**Why this matters for sparklines:** A park in Tokyo (UTC+9) opens at 09:00 JST = 00:00 UTC. Using UTC midnight as `startTime` would miss the first 9 hours of data. Using `getStartOfDayInTimezone("UTC")` globally would mix yesterday's data for UTC− parks. `getEffectiveStartTime` gives each park the correct local anchor.

---

## 4. Current Usage

| Endpoint | How sparklines are fetched |
|---|---|
| `GET /v1/parks/:slug` (attraction list) | `getBatchAttractionWaitTimeHistory` — one `startTime` for the whole park, all attraction IDs in one call |
| `GET /v1/attractions/:slug` (single ride) | via `getAttractionStatistics` → `getBatchAttractionWaitTimeHistory` internally |
| `GET /v1/analytics/realtime` (longest/shortest ride) | `getAttractionSparklinesBatch` — rides may be from different parks |
| Favorites (`GET /v1/favorites`) | via `buildIntegratedResponse` → `getAttractionStatistics` per cache miss |

---

## 5. DTO

`AttractionStatsItemDto` (used in `/v1/analytics/realtime`) carries:

```typescript
// Sparkline
sparkline: { timestamp: string; waitTime: number }[] | null;

// Today's statistics — same fields as the attraction detail endpoint
avgWaitToday:       number | null;
minWaitToday:       number | null;
peakWaitToday:      number | null;
peakWaitTimestamp:  string | null;  // ISO 8601

// Trend fields
typicalWaitThisHour: number | null;  // 2-year historical avg for this hour/weekday
currentVsTypical:    number | null;  // % deviation (positive = busier than usual)
```

Full attraction responses expose sparkline data under `statistics.history` and the same statistics fields under `statistics.*` (defined in `AttractionStatisticsDto`).

The realtime endpoint fetches all these fields by calling `getAttractionStatistics(id, startTime, timezone)` — the exact same method used by the attraction detail endpoint — so values are always consistent.
