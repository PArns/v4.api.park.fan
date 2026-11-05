# The park's day shape (`/stats/hourly`)

The contract for the matrix behind a "when is the queue longest" table: P25/P50/P90
wait per park-local hour of the operating day, ride by ride, in ~2 KB. Covers the
window parameters, the shape, which hours become columns, which rides make the
table, and the SQL fence that must not be inlined. `/plan/day` composes its curves
from the same read ([plan-day rides](./plan-day/rides.md#which-rides-a-day-carries)).

```
GET /v1/parks/:continent/:country/:city/:parkSlug/stats/hourly
    ?years=1&topN=8&minAttractionDays=20     # or &from=YYYY-MM-DD&to=YYYY-MM-DD
→ ParkHourlyProfileDto
```

Code: `ParkHistoricalStatsService.getParkHourlyProfile`
(`src/analytics/park-historical-stats.service.ts`).

- `years` default **1** (max 5) — not 2 like `/stats`: the table describes a day as it
  looks now, and a park that moved its opening time last spring would otherwise
  average the old shape in. `topN` default 8, max 20. `minAttractionDays` default 20.
- **An explicit window (PAR-978).** `from` + `to` read those park-local days, both
  inclusive, instead of the last `years`; `meta.dataFrom`/`dataTo` echo them and
  `meta.windowYears` reports the span in years (one decimal). Both or neither: one
  alone, a day the calendar does not have, `from` after `to`, or a span over 1830
  days is a 400. Too few measured days is not an error but `meta.displayable: false`
  (lower `minAttractionDays` for a short window). Each window is its own cache entry.
- **The bounds are the park's midnight** (PAR-999), not UTC: a UTC+9 park used to lose
  the first local hours of `from` and read hours past `to`.
- Cached 24 h (Redis + `HttpCacheInterceptor`) from the daily
  `queue_data_aggregates` rollup — do not poll it. It reads the same rollup as
  `/stats`, so its numbers agree with the top-attraction ranking by construction.
- It exists because the attraction detail answers ~53 KB per ride (45 % of it an
  unrendered `schedule`): an eight-ride table would cost ~424 KB.

## Shape

```ts
interface ParkHourlyProfile {
  hours: number[]; // park-local, ascending, e.g. [9,10,…,18]
  attractions: Array<{
    attractionSlug: string;
    attractionName: string; // curated name wins
    land?: string | null;
    p25: Array<number | null>; // schema v4 and up
    p50: Array<number | null>; // one entry per `hours` entry, same order
    p90: Array<number | null>;
    peakHour: number | null;   // this ride's busiest hour in `hours`
    sampleDays: number;
  }>;
  meta: { parkSlug; dataFrom; dataTo; windowYears; totalSampleDays; displayable;
          generatedAt; schemaVersion: 4 };
}
```

- **`hours` comes from the data**, never assumed (a park opening at 11 starts at 11).
- **The arrays are positional**: `p25[i]`, `p50[i]`, `p90[i]` belong to `hours[i]`. A
  `null` is a gap ("we were not watching") and renders as a dash, never 0 ("no
  queue").
- **`p25` may be absent** from an older response; then draw the band median-to-busy
  and label it so — a P50–P90 fill captioned "P25–P90" claims something nothing
  measured.
- **Gate on `meta.displayable`**: false when fewer than three hours survive or no
  ride clears the floor.
- **`peakHour` is per ride** — Voletarium peaks at rope drop, Wodan climbs all
  afternoon; highlight each row's own cell.

## Which hours become columns

An hour must pass all three (`visibleHours`):

1. **10 measured days** across the window (`MIN_DAYS_PER_HOUR`).
2. **≥ 40 % of the best-observed hour's day count** (`MIN_HOUR_DAY_RATIO`). No flat
   threshold works alone: Europa-Park's Winterzauber runs 11:00–20:00 for six weeks,
   so any fixed number either keeps 20:00 or drops hours a small park only measures
   forty times.
3. **At least half the table's rides report it** (`MIN_HOUR_RIDE_RATIO`), tested after
   the ranking and `topN` cut. One ride's hotel early entry minted 07:00 and 08:00
   columns at Europa-Park with seven of eight rows empty. `peakHour` is recomputed on
   the trimmed axis.

The day count is **per (ride, hour)**, never the ride's `sample_days` (which is the
same for all 24 of its hours — reading it shipped 21:00–23:00 columns at
Europa-Park). A single cell is blanked by the same 10-day floor even when its hour
survives, so a ride that opened mid-season does not put six days next to 118.

## Which rides make the table

1. A ride needs `minAttractionDays` (default 20) measured days, the floor `/stats`
   applies to its ranking.
2. SQL pre-ranks by all-day average P90 and over-fetches (`topN × 3`, capped at 60) …
3. … because the projection **re-ranks by each ride's busiest hour** and then cuts to
   `topN`: a ride with one rope-drop spike belongs in a table about *when* a queue
   happens.

Hours are bucketed `AT TIME ZONE` the **park's** zone; in UTC Gardaland's morning
would shift by a different amount in summer and winter, smearing the peak.

## The query's shape is load-bearing (PAR-832)

`queryHourlyProfile` reads rows in a `measured` CTE that ends in **`OFFSET 0`** and
groups outside it. Written as one grouped SELECT (the closed-day anti join beside
`COUNT(DISTINCT local day)`), PostgreSQL 18.6 + TimescaleDB 2.24.0 failed at plan
time with `XX000 no tlist entry for key 6` for five parks, and only at the 60-ride
fetch `/plan/day` uses — so `/stats/hourly` looked healthy while `/plan/day` lost its
rides. `SET timescaledb.enable_chunk_append = off` makes the old statement run, which
places the fault in ChunkAppend. It depends on row estimates and does not reproduce
on the E2E database; a unit test pins the shape. **Do not inline the fence.**

The same error appears for `queryHeadlinerDayValues` with the park-local bound, but
**only in `EXPLAIN`** (`no tlist entry for key 5`); the statement itself runs with
identical results (PAR-1014). Time that query with `\timing`, not `EXPLAIN ANALYZE`.
