# Parks whose wait times we cannot read (`liveWaitTimes`)

Some parks publish wait times in a place no server can reach. Hansa-Park serves
them from its own app, only to devices on the park's WLAN; there is no public
endpoint, and no amount of ingestion will produce a number.

That is not the same as a feed being down, and it is not something a client can
work out for itself. From the outside, a park with no source and a park shut for
the night look identical: zero rides operating, empty `queues`, an average of
0 min. At 03:00 local **every** park in the catalog looks like Hansa-Park.

So the API says which is which.

---

## The field

`liveWaitTimes` rides on every response that carries a park:

```ts
{
  available: boolean;                              // false → no readable source
  reason: "in_park_app_only" | "not_published" | null;
}
```

| Where | Field |
| ----- | ----- |
| `GET /v1/parks/{continent}/{country}/{city}/{park}` | `liveWaitTimes` |
| `GET /v1/parks/…/attractions/{attraction}` | `park.liveWaitTimes` |
| `GET /v1/discovery/continents/*` (park cards) | `liveWaitTimes` per park |
| `GET /v1/discovery/nearby`, `/v1/favorites/*` | `liveWaitTimes` per park |

**`available: false` is permanent, not a freshness signal.** A park whose feed
went silent this morning stays `true` — that case is handled by the staleness and
movement rules in the live queries. This flag says the source does not exist.

`reason` distinguishes two situations a visitor experiences differently:

- **`in_park_app_only`** — the park does publish wait times, to its own app,
  inside its own network. Someone standing in the park can see them; we cannot.
- **`not_published`** — the park publishes them nowhere at all.

The strings are contract. Renaming one is a breaking change.

## What the API already suppresses

Everything derived from a wait time is unknowable for these parks, and this API's
standing rule is to emit `unknown` rather than a placeholder tier (see
`claude.md` → *No made-up ratings*). So:

| | Readable park | `available: false` |
| --- | --- | --- |
| Ride with no queue row, park open | `OPERATING` (optimistic fallback) | **`UNKNOWN`** |
| Ride `crowdLevel` | rated against the P50 baseline | **`unknown`** |
| Park `statistics.crowdLevel` | rated | **`unknown`** |
| `statistics.peakHour*` | today's peak or a forecast | **`null` / 0** |
| `analytics.percentiles` | today's distribution | **omitted** |
| `hourlyForecast`, `bestVisitTimes` | ML predictions | **withheld** |

The optimistic fallback is the one worth understanding. It exists so a park whose
feed drops a row does not read "open, all rides closed" — if the park is open and
a ride is missing, the ride is assumed to be running. For a park with no source
*every* ride is missing, always, so that fallback would assert all 82 of
Hansa-Park's attractions are operating on the strength of the schedule alone.
It is skipped, and the rides read `UNKNOWN`.

**A closed park still closes its rides.** `CLOSED` comes from the schedule, which
we *can* read — that is a fact, not a gap, and it survives untouched. Only the
open-park case becomes `UNKNOWN`.

## What the client still has to do

The response shape does not change, so a client that ignores the flag renders an
empty park as a quiet one. Check `liveWaitTimes.available` before showing:

- wait times, or the absence of one as a walk-on
- "x of y rides open" — the count is honest (none is *known* to run) and reads as
  a catastrophe
- any average, peak or crowd badge
- ride status as "closed"

`totalAttractions` and the ride list itself stay meaningful — the catalog is
real, only the live layer is missing. A park page can still show the rides,
the schedule, the weather and the historical calendar; it just cannot say
anything about queues.

## Adding a park

`src/parks/data/live-wait-time-sources.ts`, keyed by `citySlug` + `parkSlug`
(park slugs are not globally unique — `disneyland-park` exists in Anaheim and in
Paris). Every entry carries a `note` recording what established it, so a later
reader can re-check rather than trust it. There is no admin endpoint and no
database column: this is knowledge about where a park publishes, and it belongs
in review alongside the code that acts on it.

Before adding one, rule out the alternatives — a broken ingestion mapping and a
park that has genuinely stopped reporting both look the same from here. A park
that has never had a single row in `/stats` (`totalSampleDays: 0`) while carrying
a full attraction list and a working schedule feed is the signature.
