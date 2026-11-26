# Year rankings of rides

`GET /v1/rankings/attractions/:continent?year=2026&limit=50` and
`GET /v1/rankings/attractions/:continent/:country?year=2026&limit=50`: the rides
with the longest waits of one calendar year in a continent or a country. Needs
`stats.history` ([data API keys](./api-keys.md)).

## What a row is

Every row is the ride's own row in its park's
[`topAttractions`](./park-stats-and-peak-time.md), computed over the calendar
year instead of the rolling window: the same query, the same 20-day floor
(`meta.minAttractionDays`), the same exclusions (retired rides, days a park
schedule calls closed, hours outside opening hours, single-sample hours). So a
ride's order among its park's rides is the same here as on the park's statistics
page.

| Field                        | Meaning                                                      |
| ---------------------------- | ------------------------------------------------------------ |
| `rank`                       | Place, 1 = longest waits. Sequential; no shared places       |
| `avgWaitP90`                 | Average of the year's hourly P90 waits, rounded to 5 minutes |
| `avgWaitP50`                 | Average of the year's hourly P50 waits, rounded to 5 minutes |
| `sampleDays`                 | Park-local days with measured waits in the year              |
| `parkName`                   | The park's curated name where one is set                     |
| `continentSlug` … `citySlug` | The park's geographic path, for the ride page link           |

The order comes from the **unrounded** P90, then the unrounded P50, then the
name. Two rides showing the same rounded `avgWaitP90` can therefore hold
different places; a client must not re-sort by the rounded figure.

## The year window

The year is park-local. While it runs, each park's window ends yesterday in its
own time zone; `meta.dataTo` is the latest last day across the ranked parks and
`meta.yearComplete` turns true once every ranked park's window reaches
31 December. A year that has not started anywhere answers an empty list.
`year` is required: a ranking always names its year, so nothing changes meaning
on 1 January.

The measured history starts in late 2025 for most parks, so 2026 is the first
year with a full ranking; for 2025 only the last days exist and few rides pass
the 20-day floor.

## Caching

Each park's year rows are cached for 24 hours under its historical-stats key
(`park:historical-stats:v<n>:<park>:1:20:year:<year>`), so whatever clears a
park's statistics clears them too. The merged ranking is cached for 24 hours
per scope and year, and the response carries a 24-hour HTTP cache.

`404` when the continent or country has no parks.
