# The park's day shape — `/stats/hourly` (frontend guide)

> The matrix behind a "when is the queue longest" table: median and busy wait
> per hour of the operating day, ride by ride. Shipped 2026-08-24.

## Where it lives

```
GET /v1/parks/:continent/:country/:city/:parkSlug/stats/hourly
    ?years=1&topN=8&minAttractionDays=20
→ ParkHourlyProfileDto
```

Cached 24 h (Redis + `HttpCacheInterceptor`), like `/stats`. The inputs are the
daily hourly percentile rollups, which only change once a day — do not poll it.

## Why it is its own endpoint

Everything in it could be read off the attraction detail endpoint. That one
answers ~53 KB for a single ride, about 45 % of it a `schedule` nobody renders,
so an eight-ride table would cost ~424 KB. This projection is ~2 KB for the same
eight rides.

It reads the same `queue_data_aggregates` rollup as `/stats`, so its numbers
agree with the top-attraction ranking by construction rather than by luck.

## Shape

```ts
interface ParkHourlyProfile {
  hours: number[];                 // park-local, ascending, e.g. [9,10,…,18]
  attractions: Array<{
    attractionSlug: string;
    attractionName: string;        // curated name winning
    land?: string | null;
    p50: Array<number | null>;     // one entry per `hours` entry, same order
    p90: Array<number | null>;
    peakHour: number | null;       // the hour in `hours` where p50 peaks
    sampleDays: number;
  }>;
  meta: {
    parkSlug: string;
    dataFrom: string;              // YYYY-MM-DD, park tz
    dataTo: string;
    windowYears: number;
    totalSampleDays: number;
    displayable: boolean;
    generatedAt: string;           // ISO 8601 UTC
    schemaVersion: number;         // 1
  };
}
```

## Rendering rules

- **`hours` is derived from the data, never assumed.** A park that opens at 11
  starts at 11. Both axes come from the payload; hardcoding 9–18 would put
  Phantasialands winter season an hour to the left of where it happened.
- **The two arrays are positional, not keyed.** `p50[i]` belongs to `hours[i]`.
  A `null` is a gap — the ride reported nothing in that hour — and must render
  as a dash, never as a zero. A zero says "no queue", which is a different
  claim from "we were not watching".
- **Gate on `meta.displayable`.** It is false when fewer than three hours
  survived or no ride cleared the sample floor. There is nothing to draw from a
  two-column matrix.
- **`peakHour` is per ride**, not per park: Voletarium peaks at rope drop and
  Wodan climbs all afternoon, which is the whole point of the table. Highlight
  each row's own peak cell.
- **`windowYears` defaults to 1**, not 2 like `/stats`. The table describes what
  a day looks like *now*; a park that moved its opening time or rebuilt a queue
  line last spring would otherwise average the old shape into the new one.

## Which hours become columns

An hour needs **10 measured days** across the window. Parks routinely hold a
handful of late-summer evenings open until 20:00, and those four evenings would
otherwise add a 20:00 column that reads as "the park is open then".

## Which rides make the table

1. A ride needs `minAttractionDays` (default 20) measured days — the same floor
   `/stats` now applies to its ranking.
2. SQL pre-ranks by all-day average P90 and over-fetches (`topN × 3`, capped at
   60), because …
3. … the projection then **re-ranks by each ride's busiest hour** and cuts to
   `topN`. A ride with one sharp rope-drop spike belongs in a table about *when*
   a queue happens; its all-day average would have hidden it.

## Timezone

The hour bucket is extracted `AT TIME ZONE` the **park's** timezone. Reading it
in UTC shifts Gardaland's morning by two hours in summer and one in winter —
i.e. by a different amount inside the same window, which smears the peak instead
of moving it.
