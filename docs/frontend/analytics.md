# Platform analytics (`/v1/analytics/*`)

The contract for the four platform-wide boards: the global counters and extremes, open parks per
region, the wait-time ticker and the global best time to visit. Open it when rendering the homepage
numbers, the world map, the ticker or the best-time hub. Crowd-level rules: [crowd levels](../rules/crowd-levels.md).

| Endpoint | Returns | Cache (HTTP and Redis) |
| --- | --- | --- |
| `GET /v1/analytics/realtime` | `{ counts, mostCrowdedPark, leastCrowdedPark, longestWaitRide, shortestWaitRide }` | 5 min |
| `GET /v1/analytics/geo-live` | `{ continents[]: { slug, openParkCount, countries[]: { slug, openParkCount } } }` | 5 min |
| `GET /v1/analytics/ticker` | `{ items[], generatedAt }` | 5 min |
| `GET /v1/analytics/best-times` | `{ byDayOfWeek[7], byMonth[12], meta }` | 24 h |

"Open park" means one set everywhere here, `OPEN_PARKS_CTES` (`src/analytics/utils/open-parks.sql.ts`):
a published schedule covers now, or, for a park that published nothing for today, its rides report
fresh and moving waits. That is why the hero badge, the map and the ticker agree.

## `realtime`

| Field | Is |
| --- | --- |
| `counts.openParks` | open parks with at least one `OPERATING` standby reading in the last 24 h |
| `counts.parks`, `attractions`, `shows`, `restaurants` | rows in each table, **retired rows included** (cached 1 h) |
| `counts.openAttractions` | non-retired rides whose newest reading of the last 24 h is `OPERATING`, **in any park**, open or not |
| `counts.queueDataRecords` | exact, recounted every 6 h |
| `counts.totalWaitTime` | sum of the newest wait of every `OPERATING` ride in an open park, counting waits of 10–360 min only |
| `mostCrowdedPark`, `leastCrowdedPark` | the open parks with the highest and lowest `averageWaitTime` |
| `longestWaitRide`, `shortestWaitRide` | among rides in open parks whose newest reading is `OPERATING` with a wait of 10–360 min |

What a client may not conclude:

- **`averageWaitTime` on a park is a P90**, the 90th percentile of its rides' newest standby waits,
  rounded to 5. It is not a mean, and it ranks the two parks; the crowd level does not.
- **The parks' `crowdLevel` is re-derived from `occupancy.current`**, not the gated level the park
  payload serves, so it can read `very_low` where the park page says `unknown`.
- **`shortestWaitRide` is never under 10 minutes**: shorter waits are left out of the comparison.
- All four extremes are absent while no park is open. A zero board (no parks, no rides) is not cached.

## `geo-live`

Only continents and countries with at least one open park appear: a missing slug means no open park,
not missing data. Cities and average waits are no longer part of it.

## `ticker`

```jsonc
{ "parkName": "Tokyo DisneySea", "parkSlug": "tokyo-disneysea", "continentSlug": "asia", …,
  "attractionName": "Soaring: Fantastic Flight", "attractionSlug": "soaring-fantastic-flight",
  "waitTime": 147, "trend": "rising", "crowdLevel": "very_high",
  "url": "/v1/parks/asia/japan/tokyo/tokyo-disneysea/attractions/soaring-fantastic-flight" }
```

- **One ride per open park, at most 40, not the 40 longest waits.** Within a park the ride is picked
  by priority: a headliner at or above `max(P50, 20 min)`, then any ride at or above it, then a
  headliner, then the rest, the longest wait first. The list is ordered the same way.
- Only `OPERATING` standby readings with a wait above 0.
- **`waitTime` is the raw reading, not rounded to 5.** Round it before display.
- `trend` compares with the newest reading 15–45 minutes ago: ±5 min or more is `rising`/`falling`,
  less is `stable`; absent without such a reading.
- `crowdLevel` is the wait against the ride's P50 baseline; absent without a baseline. It is not gated
  on the park being ratable.

## `best-times`

```jsonc
"byDayOfWeek": [{ "key": 0, "relativeIndex": 1.08, "crowdLevel": "high",
                  "avgWaitP50": 24, "sampleDays": 9120, "parkCount": 118 }, …]
```

- `key`: 0 = Sunday … 6 = Saturday; 1 = January … 12 = December. Always 7 and 12 entries.
- `relativeIndex`: each park's average P50 for the bucket divided by the park's own average, then
  averaged over parks, over the last 24 months of `park_daily_stats`. 1.0 is a park's typical day, so
  large and small parks count equally. Rank on it.
- `crowdLevel` comes from the index by fixed cut-offs (below 0.8 `very_low` … from 1.25 `extreme`,
  `relativeIndexToCrowdLevel` in `analytics.service.ts`): relative to the all-park average, not a park's baseline.
  Render it, do not re-classify.
- **An empty bucket reads `relativeIndex: 1` with `crowdLevel: "unknown"` and `sampleDays: 0`.**
  The 1.0 there is a placeholder, not a typical day.
- `avgWaitP50` mixes park sizes and is context only.
- **`meta.displayable` is the render gate**: true once at least 20 parks have 60 or more days of data
  in the window. A park below 60 days contributes nothing.
