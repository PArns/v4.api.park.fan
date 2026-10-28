# Discovery (`/v1/discovery/*`)

The contract for the geographic tree, the country pages and `nearby`: what each endpoint returns,
which numbers are live, and what a missing field means. Open it when rendering a continent, country or
city listing, a country landing page or the "near me" list. How `nearby` finds the visitor:
[location resolution](../architecture/location-resolution.md).

| Endpoint | Returns | HTTP cache |
| --- | --- | --- |
| `GET /v1/discovery/geo` | `{ continents[], continentCount, countryCount, cityCount, parkCount, attractionCount, generatedAt }` | 5 min |
| `GET /v1/discovery/continents` | `continents[]` of the same tree | 5 min |
| `GET /v1/discovery/continents/:continent` | `{ data: countries[], breadcrumbs }` | 5 min |
| `GET /v1/discovery/continents/:continent/:country` | `{ data: cities[], breadcrumbs }` | 5 min |
| `GET /v1/discovery/continents/:continent/:country/summary` | the country summary below | 24 h |
| `GET /v1/discovery/:continent/:country` | `{ data, breadcrumbs }`, `data` being what `GET /v1/parks/:continent/:country` returns | 5 min |
| `GET /v1/discovery/nearby` | `{ type, userLocation, data }` | `private, no-store` |

An unknown slug answers 404. `breadcrumbs` is `[{ name, url }]` with site paths (`/europe/germany`), not
API paths.

## The geo tree

Continent → country → city → park, for every park that is not retired and has all three geo slugs.
The skeleton is cached for 24 hours; the live fields are filled in on each request from a 5-minute
statistics cache (`DiscoveryService.hydrateStructure`).

- **A park carries no rides.** The Swagger example still shows `attractions[]`; the tree has
  `attractionCount` only.
- **`url` is the API path** (`/v1/parks/<continent>/<country>/<city>/<park>`).
- **`status` is `OPERATING` or `CLOSED`**, never `REFURBISHMENT` despite the enum: open while a
  published schedule covers now, or, when nothing was published for today, while rides report
  ([calendar status](./calendar-schedule-status.md)).
- **`analytics.statistics` is over the last 30 minutes of readings.** `operatingAttractions` is the
  park's rides minus those whose last reading is not `OPERATING`, so a ride without a reading counts as
  operating; it is 0 while the park is closed. `closedAttractions` counts only rides that read
  something else, so it is not `total − operating`. `avgWaitTime` is rounded to 5.
- **Check `liveWaitTimes.available` before showing a number.** For a park we cannot read, the whole
  `analytics` block is an aggregate over nothing ([live wait times](./live-wait-times-availability.md)).
- **`crowdLevel` is absent when the park's occupancy cache is cold**, which is not `unknown`.
- **`openParkCount` and `averageWaitTime`** on a city, country or continent: the open parks, and the
  mean of their `avgWaitTime` over those with a wait (rounded to 5). `averageWaitTime` is absent when
  no open park has one.
- **A country is grouped by its ISO code.** When parks spell the country slug two ways (`fr`,
  `france`), the tree keeps the longer one; a park's own `countrySlug` can differ from its country's.

## The country summary

```jsonc
{ "countrySlug": "germany", "parkCount": 15, "cityCount": 8,
  "topParks": [{ "name": "Europa-Park", "slug": "europa-park", "city": "Rust",
                 "path": "/parks/europe/germany/rust/europa-park", "avgAnnualCrowdScore": 3.4 }],
  "avgPeakMonths": [7, 8, 10], "avgQuietMonths": [1, 2, 11] }
```

- `avgAnnualCrowdScore` is the mean of the park's monthly average P50 wait over the last 12 months
  of `park_daily_stats`, divided by 10 and clamped to 1.0–5.0. **A park without statistics scores 1.0**,
  the quietest value, not an absent one. `topParks` is the five highest.
- `avgPeakMonths` / `avgQuietMonths` are three months each (1 = January), and both are empty when
  fewer than six months have data.
- A miss (no park in that country) is remembered in the process for an hour, so a country that gets
  its first park can answer 404 for up to an hour after.

## `nearby`

```
GET /v1/discovery/nearby?lat=&lng=&radius=1000&limit=6
```

- `radius` (metres, 0–10000, default 1000) decides `type`: **`in_park`** when the nearest park's centre
  is within it, else **`nearby_parks`**. `limit` (1–50, default 6) caps `nearby_parks`. Both are
  clamped, not rejected.
- A coordinate that is sent but unusable (only one of the pair, or out of range) is a 400; it never
  falls back to the IP. Without coordinates and without a GeoIP answer it is a 400 too.
- **`nearby_parks`** → `{ parks[], count }`: the nearest non-retired geocoded parks, in the park-card
  shape [favorites](./favorites.md) also uses, here with `hasOperatingSchedule`.
- **`in_park`** → `{ park, rides[] }`. `park` carries `status`, `hasOperatingSchedule`,
  `analytics { avgWaitTime, crowdLevel, operatingAttractions }`, `todaySchedule`, `nextSchedule`.
  `rides` are the park's rides with coordinates, nearest first; a ride without coordinates is left
  out, and so is one whose `isCurrentlyInSeason` is `false`.

What a ride in `rides` may not be read as:

- **`status` is the newest reading of the last 30 minutes** (standby first), else `CLOSED`. It is not
  `effectiveStatus`: it never reads `UNKNOWN`, ignores whether the park is open and whether its wait
  times are readable ([effective status](./effective-status.md)).
- **`CLOSED` can mean "no reading".** The list is not filtered for retired rides
  (`LocationService.findRidesInPark`), and a retired ride, having no reading, shows as `CLOSED`.
- `waitTime` is rounded to 5 and absent without a reading. `crowdLevel` is rated against the ride's
  P50 (P90 when there is none) only while it is `OPERATING` with a wait; `unknown` when the park is not
  ratable, absent otherwise. `analytics` is today's `p50`/`p90`; Swagger's `avgWaitToday` is never
  sent.

Nothing per visitor is cached; only the list of park coordinates is, for 10 minutes.
