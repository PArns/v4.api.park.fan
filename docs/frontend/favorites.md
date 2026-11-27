# Favorites (`/v1/favorites`)

The contract for a visitor's saved parks, rides, shows and restaurants: what the request takes, what
each list carries, and what a missing entry does not mean. Open it when rendering a favorites page or
card. How the coordinates are found: [location resolution](../architecture/location-resolution.md).

```
GET /v1/favorites?parkIds=…&attractionIds=…&showIds=…&restaurantIds=…&lat=…&lng=…
→ { parks[], attractions[], shows[], restaurants[], userLocation? }
```

## The request

- Each list is comma-separated uuids (`FavoritesService.filterUuids`). A value that is not a uuid is
  dropped without an error; with no valid id at all the answer is four empty lists, without a lookup.
- `lat`/`lng` outside −90…90 / −180…180 answer 400. With both present they are the visitor's
  position; otherwise the API tries GeoIP on the request's address. `userLocation` echoes what was
  used and is absent when neither worked; a GeoIP position is city-level.

## The lists

- **Not in request order.** Each list comes back in database order; sort it yourself.
- **A retired ride, show or restaurant leaves its list** but nothing tells the client it was retired.
  Keep the stored id: the row can come back, and the favorite with it.
- **A retired park stays in `parks`**, and this payload carries no `retiredAt` to say so; the park
  endpoint does ([retired parks](./retired-parks.md)).
- **`distance`** is metres to the visitor, rounded; absent without `userLocation` or without
  coordinates on the entity.

| List | Carries | Read with care |
| --- | --- | --- |
| `parks` | the nearby park card: `status`, `liveWaitTimes`, `totalAttractions`, `operatingAttractions`, `analytics { avgWaitTime, crowdLevel, occupancy, updatedAt }`, `todaySchedule`, `nextSchedule`, `timezone`, `url` | no `hasOperatingSchedule` here, unlike `/v1/discovery/nearby`; `avgWaitTime` and `occupancy` are absent for a park we cannot read ([live wait times](./live-wait-times-availability.md)); `updatedAt` is when the reading was taken ([one reading per park](./discovery.md#one-reading-per-park)) |
| `attractions` | the attraction detail payload, minus `hourlyForecast`, `forecasts` and `predictionAccuracy`, when that payload is warm in Redis; otherwise a rebuild with the stored facts, `status`, `effectiveStatus`, `queues`, `crowdLevel`, `statistics.history`, `bestVisitTimes`, `ropeDrop`, `outage`, `notRunToday` and `weatherClosure`. Either way `isHeadliner`, as on the park payload | which of the two a ride gets depends on the cache, so read every other field (today's `statistics` figures, `typicalWaits`, `tooColdToGetWet`, …) as optional; statuses: [effective status](./effective-status.md) |
| `shows` | `status`, `showtimes[] { type, startTime, endTime? }` (`endTime` only when believable, [shows](./shows.md)), `park` | `status` is the show's own last reading, not gated on the park being open (the park payload gates it, [shows](./shows.md)); `CLOSED` also means no reading, or an `OPERATING` one older than 48 h |
| `restaurants` | `status`, `waitTime`, `cuisineType`, `park` | a 0-minute wait arrives absent, like no wait |

`url` on every entry is the **API** path (`/v1/parks/<continent>/<country>/<city>/<park>[…]`), absent when
a geo slug is missing. A show's and a restaurant's is their park's path: neither has a route of its
own, and the park payload lists both.

## The park page's card

A favourite ride renders as the park page's card, so the fields that card reads come from the
services the park payload asks, not from a second computation:

- **`isHeadliner`** is the park payload's flag (`AnalyticsService.getHeadlinerAttractionIds`), on
  every ride, cached or rebuilt; the ride detail payload has none of its own.
- **`bestVisitTimes`** on a rebuild ranks the stored predictions first and the park's live hourly
  call only for a ride without any, with the measured wait in the current slot: the park
  payload's rule. A cached ride carries the detail payload's, built the same way.
- **`outage`, `notRunToday`, `weatherClosure`** on a rebuild are asked of
  `AttractionOutageService` and `WeatherClosureService` per park, with the park payload's gates
  and order. A failed outage lookup leaves the line out, never the ride.

What it costs: one Redis read per park for the headliners (6 h cache); on a rebuild one indexed
query for the stored predictions over all rebuilt rides, and per park up to three queries for the
shut lines (the outage one only for a park with a `wikiEntityId`, the weather one only when a
ride is shut). They run once per answer, which is cached for five minutes (below).

## Caching

`private, no-store` at the edge (`NoCdnCacheInterceptor`: the answer depends on the caller). In
Redis the whole answer is cached for 5 minutes per set of ids and position (rounded to three
decimals, about 100 m), and refreshed in the background once less than a minute is left, so a list
can be up to five minutes old.
