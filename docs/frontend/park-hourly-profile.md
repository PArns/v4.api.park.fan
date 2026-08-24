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
  hours: number[]; // park-local, ascending, e.g. [9,10,…,18]
  attractions: Array<{
    attractionSlug: string;
    attractionName: string; // curated name winning
    land?: string | null;
    p50: Array<number | null>; // one entry per `hours` entry, same order
    p90: Array<number | null>;
    peakHour: number | null; // the hour in `hours` where p50 peaks
    sampleDays: number;
  }>;
  meta: {
    parkSlug: string;
    dataFrom: string; // YYYY-MM-DD, park tz
    dataTo: string;
    windowYears: number;
    totalSampleDays: number;
    displayable: boolean;
    generatedAt: string; // ISO 8601 UTC
    schemaVersion: number; // 2
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
  a day looks like _now_; a park that moved its opening time or rebuilt a queue
  line last spring would otherwise average the old shape into the new one.

## Which hours become columns

Three tests, and an hour must pass all of them:

1. **10 measured days** across the window — an absolute floor for small or young
   parks.
2. **At least 40 % of the best-observed hour's day count.** A flat threshold
   cannot decide this alone, because "the park was open" is not a fixed number of
   days: Europa-Parks Winterzauber runs 11:00–20:00 for about six weeks, so any
   single number either keeps 20:00 (drawing a winter-only hour as part of a
   normal day) or drops hours a smaller park only ever measures forty times.
   Measuring each hour against the hours the park is _always_ open scales to
   both.

3. **At least half the rides in the table report it.** The two day-count tests
   ask the _best-observed_ ride whether an hour exists, so one ride is enough to
   mint a column: Europa-Park opened at 07:00 and 08:00 with seven of eight rows
   empty, which is the hotel guests' early entry through one queue, not an hour
   of the park's day. This test runs after the ranking and the `topN` cut, so it
   asks the rides the table will actually show. `peakHour` is recomputed against
   the trimmed axis — a peak in a cut hour would point at a column the response
   no longer carries.

The day count behind the first two is **per (ride, hour)** — the days that hour was
measured. It is not the ride's `sample_days`, which counts its measured days
across the whole window and is identical for all 24 of its hours. Reading the
wrong one is what shipped 21:00–23:00 columns for Europa-Park at 58–68 minutes:
every hour inherited ~157 days and the filter never fired.

A single ride's cell is blanked by the same 10-day floor even when the hour
survives as a column, so a ride that opened mid-season does not put a number
built on six days next to one built on 118.

## Which rides make the table

1. A ride needs `minAttractionDays` (default 20) measured days — the same floor
   `/stats` now applies to its ranking.
2. SQL pre-ranks by all-day average P90 and over-fetches (`topN × 3`, capped at
   60), because …
3. … the projection then **re-ranks by each ride's busiest hour** and cuts to
   `topN`. A ride with one sharp rope-drop spike belongs in a table about _when_
   a queue happens; its all-day average would have hidden it.

## Timezone

The hour bucket is extracted `AT TIME ZONE` the **park's** timezone. Reading it
in UTC shifts Gardaland's morning by two hours in summer and one in winter —
i.e. by a different amount inside the same window, which smears the peak instead
of moving it.
