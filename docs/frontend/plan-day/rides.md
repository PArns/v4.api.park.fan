# `/plan/day`: the ride object and which rides a day carries

The fields on `rides[]`, the two rider facts (`minimumHeight`, `mayGetWet`), how
the set of rides for a day is formed, and the `SHAPE_RIDES` cap that does not
bite. Part of the [`/plan/day` contract](./README.md).

## Fields

| field                                   | notes                                                                                                                                                                                                       |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `attractionSlug`, `attractionName`, `land` |                                                                                                                                                                                                         |
| `attractionId`                          | the ride's UUID, which the planner's events name it by (the site statistic's `plan_item_added`); the slug stays the plan's key. `showId` on a show and `parkId` on the answer are the same for those |
| `hours[]`, `slots[]`, `composer`, `profileDays` | the curves — see [tiers](./tiers.md) and [slots and H5](./slots-and-h5.md)                                                                                                                       |
| `dayPeak`                               | the day's peak wait, the same statistic on every tier — see [tiers](./tiers.md#daypeak-is-the-same-statistic-on-every-tier)                                                                                 |
| `uncertaintyMinutes`                    | half-width of the model's band, from the same row as `dayPeak`. **Absent where the model reports no spread** — not a zero-width band, and must not be drawn as one                                           |
| `expectedError`                         | how wrong `dayPeak` typically is — see [accuracy](./accuracy.md)                                                                                                                                            |
| `sampleDays`                            | the year's measured days behind the historical shape; `1` on an observed day, `0` for a ride the shape does not cover                                                                                       |
| `opensAt`, `opensAtConfidence`, `runsOnWeekdays` | when the ride (not the park) opens, and on which weekdays it runs — see [opens-at](./opens-at.md)                                                                                                  |
| `latitude` / `longitude`                | **numbers**, not the strings TypeORM returns for a decimal column; `null` when unknown. A straight line between two is a lower bound on the walk, nothing more                                              |
| `downYesterday`                         | the ride was **DOWN** at some point on the park-local day before the planned date and **never OPERATING** in it. A ride CLOSED all day is a season or refurbishment and is not flagged. Only for today and tomorrow |
| `isHeadliner`                           | the park's **curated** headliner set, never re-derived from `dayPeak`                                                                                                                                       |
| `minimumHeight`, `mayGetWet`            | below                                                                                                                                                                                                       |

Rides are sorted busiest first (`dayPeak` descending, name as tie-break) on the
forecast tiers, by name on an observed day. A ride with nothing to give it a shape
is **omitted** rather than drawn flat, and on a past day a ride the rollup has no
row for is omitted rather than drawn at zero — the rollup not having reached a day
is not an empty queue.

## Who may ride: `minimumHeight` and `mayGetWet`

Facts about the riders, here for the same reason as the coordinates: a planner asks
them once for a whole park instead of fetching one attraction payload per ride.

- **`minimumHeight` is always centimetres.** The attraction payload keeps
  `minimumHeightUnit` for display (a US park posts 52″); a planner compares the
  number with a child's height, so it gets the number.
- **Both resolve through `resolveCuratedFacts`**: the curated column wins, and a
  curated height of `0` means no minimum at all.
- **Both are omitted where nothing is known, and absence is the point.** No height
  recorded is not "anyone may ride"; an unflagged ride is not a dry one (upstream
  sets the wet flag for a few dozen of ~7,000 attractions). A `false` on
  `mayGetWet` is a real statement and travels; a missing field never becomes one
  ([absent facts](../../rules/absent-facts.md)).
- Both mappers (`forecastRides`, `observedRides`) carry them, so a past day states
  them too.

## Which rides a day carries

`rides[]` is neither the park's catalogue nor the calendar's five headliners.

- **Composed day** (anything past the model's 48-hour reach): a ride needs **both**
  a day level and an hour shape from the last year. The set is smaller than
  either, and the season, works-window and retired filters
  ([availability](./availability.md)) cut it again.
- **Measured day**: the ride loop unions the model's hourly rows with the composed
  ones, so a ride the shape never covered can appear (`sampleDays: 0`).
- **Observed day**: the rollup's own list.

Measured on 2026-09-11 for 2026-09-18 (composed tier): Alton Towers 32 shaped
rides, 40 with a TFT level → 32 served; Europa-Park 29 / 39 → 29; Universal
Studios Japan 43 / 25 → 22. The first two are held by the shape, USJ by the day
level.

### The `SHAPE_RIDES = 60` cap does not bite

The shape comes from `getParkHourlyProfile(park, 1, SHAPE_RIDES, 20)` — the same
read as [`/stats/hourly`](../park-hourly-profile.md) at a 60-ride depth. The cap
applies to the `eligible` set (rides with ≥ `minAttractionDays` = 20 measured days
at `MIN_SAMPLES_PER_HOUR`), counted before the per-hour test. Across the 120 parks
with a measurable profile that set peaks at 43 rides (USJ) and averages 16.2.

It is headroom, and worth knowing for the day a park crosses it: the profile SQL
over-fetches `Math.min(topN * 3, 60)` (`park-historical-stats.service.ts`), so at
`topN = 60` the over-fetch is zero and the park would silently lose both the rides
past the cap **and** the peak-hour re-rank the over-fetch exists for. Watch the
eligible set, not the attraction count — Hansa-Park's 82 attractions are not 82
shapes.
