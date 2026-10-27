# Parks whose wait times we cannot read (`liveWaitTimes`)

The curated flag that says a park publishes its wait times nowhere a server can
read, what the API withholds for such parks (and for parks whose feed has gone
silent), what a client still has to do, and when a single ride reads `UNKNOWN`.
Open it before rendering any wait, count, average or crowd badge.

From the outside a park with no source and a park shut for the night look
identical — zero rides operating, empty `queues`, Ø 0 min; at 03:00 every park looks
like that. Hansa-Park serves its wait times only from its own app on the park WLAN,
so the API says which is which.

## The field

```ts
liveWaitTimes: { available: boolean; reason: "in_park_app_only" | "not_published" | null }
```

| Where                                               | Field                     |
| --------------------------------------------------- | ------------------------- |
| `GET /v1/parks/{continent}/{country}/{city}/{park}` | `liveWaitTimes`           |
| `GET /v1/parks/…/attractions/{attraction}`          | `park.liveWaitTimes`      |
| `GET /v1/parks/…/plan/day`                          | `context.liveWaitTimes`   |
| `GET /v1/discovery/continents/*` (park cards)       | `liveWaitTimes` per park  |
| `GET /v1/discovery/nearby`, `/v1/favorites/*`       | `liveWaitTimes` per park  |

- **`available: false` is permanent, not a freshness signal.** A feed that went
  silent this morning stays `true`.
- `in_park_app_only`: the park publishes to its own app inside its own network;
  `not_published`: nowhere at all. **The strings are contract.**

## What the API withholds

For a park with `available: false` (the API emits `unknown` rather than a placeholder
tier — [crowd levels](../rules/crowd-levels.md)):

|                                      | readable park                     | `available: false` |
| ------------------------------------ | --------------------------------- | ------------------ |
| ride `effectiveStatus`, park open    | the feed's, or `OPERATING` with no row (park payload; [effective status](./effective-status.md)) | **`UNKNOWN`** |
| ride `crowdLevel`                    | rated against its P50             | **`unknown`**      |
| park `statistics.crowdLevel`         | rated                             | **`unknown`**      |
| `statistics.peakHour*`               | today's peak or a forecast        | **`null` / 0**     |
| `statistics.avgWaitTime`, `avgWaitToday`, `peakWaitToday` | minutes      | **`null`**         |
| `analytics.occupancy`, `analytics.percentiles` | present                 | **omitted**        |
| `hourlyForecast`, `bestVisitTimes`   | ML predictions                    | **withheld**       |

- The aggregates read `null`, not 0, because 0 is what a division by the empty set
  returns. `occupancy` is omitted rather than nulled: clients round it, and
  `Math.round(null)` is 0 next to a "typical" verdict.
- **The optimistic fallback is skipped.** It exists so a park whose feed drops one
  row does not read "open, all rides closed"; for a park with no source it would
  assert all 82 attractions running on the schedule alone. **Queue rows do not change
  that**: Hansa-Park's upstream publishes a row per attraction, permanently `CLOSED`,
  never a minute, and their number varies through the day — so for a listed park the
  rows are dropped and `queues` comes back empty.
- **A closed park still closes its rides**: `CLOSED` comes from the schedule, which
  we can read. Only the open-park case becomes `UNKNOWN`.

**The park payload withholds the same for a park whose feed has gone quiet** for
`PARK_FEED_SILENT_DAYS` (30). `liveWaitTimes.available` stays `true` there, so a
client cannot derive this case — it reads the withheld fields themselves. The gate,
and the surfaces it does not cover:
[status regimes §5](../architecture/attraction-status/status-regimes.md#5-a-whole-park-gone-silent).

## What the client still has to do

Every surface but the park payload carries the flag rather than a gap, and a client
that ignores it renders an empty park as a quiet one. Check `liveWaitTimes.available`
before showing a wait time (or its absence as a walk-on), "x of y rides open", any
average, peak or crowd badge, or a ride as "closed". `totalAttractions`, the ride
list, the schedule, the weather and the historical calendar stay meaningful. In
park.fan this is `lib/utils/live-wait-times.ts` (its `docs/api/parks-without-wait-times.md`).

## Adding a park

`PARKS_WITHOUT_LIVE_WAIT_TIMES` in `src/parks/data/live-wait-time-sources.ts`, keyed
by `citySlug` + `parkSlug` (park slugs repeat: `disneyland-park` is Anaheim and
Paris), each entry with a `note` recording what established it. No admin endpoint,
no column: it is knowledge about where a park publishes, reviewed with the code.
Rule out a broken ingestion mapping first; the signature is `/stats` at
`totalSampleDays: 0` while the attraction list and the schedule feed work.

## A single ride can read `UNKNOWN`

In an otherwise readable, open park, a ride reads `UNKNOWN` when **every** current
queue row was written by reverse reconciliation: no source has mentioned it for
24 hours, which says our data stopped arriving, not that the operator closed it.

- Render `UNKNOWN` as "no information", never as closed; `queues` is empty.
- A free-flow attraction (`open_with_park`) stays `OPERATING`: its flag, not the
  feed, makes it open.
- An `UNKNOWN` ride is not counted as operating.

Why, and which rides it catches:
[status regimes §3](../architecture/attraction-status/status-regimes.md#3-no-source-reports-it--unknown),
[feed drops](../architecture/attraction-status/feed-drops.md).
