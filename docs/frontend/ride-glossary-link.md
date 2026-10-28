# Ride ↔ glossary link (`rideProfile`)

Curated per-ride data linking an attraction to park.fan's glossary — its track
figures in ride order, ride types, manufacturer, model, opening year and
measurements — plus the reverse lookup from a glossary term to its rides. Open it
when rendering a ride profile or a term's ride list. Every id
stored is a **glossary term id** defined only in park.fan (`lib/glossary/data.ts`),
so one table serves both directions.

| Layer      | File                                                                                   |
| ---------- | -------------------------------------------------------------------------------------- |
| Entity     | `src/attractions/entities/attraction-ride-profile.entity.ts` (`attraction_ride_profiles`) |
| Read       | `src/attractions/services/ride-profile.service.ts`, DTO `src/attractions/dto/ride-profile.dto.ts` |
| Write      | `src/admin/content/admin-ride-profile.service.ts` (admin editor)                        |
| Controller | `src/attractions/glossary-rides.controller.ts`                                          |

## Shape

```ts
{
  elements: ["lifthill", "first-drop", "vertical-loop", "zero-g-roll"], // ride order, repeats meaningful
  types: ["inverted-coaster", "terrain-coaster"],   // unordered set
  manufacturer: "Bolliger & Mabillard",             // display name, set when known
  manufacturerTermId: "b-and-m",                    // null → render the name unlinked
  model: "Inverted Coaster",
  openedYear: 2006,
  inversions: 4,
  stats: { topSpeedKmh: 80, heightM: 26, lengthM: 768, durationSeconds: 47,
           source: "mixed", sourceId: "Q1065056", attribution: { label: "Wikidata", url: "…" } },
}
```

- **`elements` is the ride order.** Never sort or dedupe it — the ride page renders
  it as a numbered layout walkthrough, and two corkscrews in a row are listed twice.
- **`inversions` is what the park publishes** and may disagree with `elements`: parks
  count non-inverting figures inconsistently.
- Everything is metric and **every field is independently nullable**, `stats`
  included. A ride with a speed and no duration is normal.

## Measurements (`stats`)

Two writers that never share a cell: `stats` from `RideStatsService` (Wikidata,
CC0, thin coverage) and `curated_stats` (hand-checked). `mapRideProfile` merges them
**field by field**, curated winning, and says where the result came from: `source`
(`curated` / `wikidata` / `mixed`), `sourceId` (null unless an imported value
survived), `attribution` (null when every surviving number is curated).

**Credit the numbers from `attribution` and nothing else**: render
`attribution.label` linked to `attribution.url` when present, nothing when absent;
localise the sentence around the label, never the label. Deriving the credit from
`source`/`sourceId` once made the frontend credit Wikidata on curated rides and link
to `/undefined`.

Why curate when there is an importer: Wikidata has a top speed for under a fifth of
the coasters here and misses most headliners (Fury 325, Steel Vengeance, Millennium
Force), and has plain mistakes (Lynet's length entered as its height).

## API surface

| Endpoint                                                  | Serves                                       |
| --------------------------------------------------------- | -------------------------------------------- |
| `GET /v1/parks/…/:park/attractions/:attraction` → `rideProfile` | ride → glossary (detail)               |
| `GET /v1/parks/…/:park` → `attractions[].rideProfile`     | ride → glossary (embedded, one batched read per park) |
| `GET /v1/glossary/terms/:termId/attractions`              | glossary → rides (cached 1 h)                |
| `GET /v1/glossary/terms/counts`                           | term id → ride count, for badging (cached 1 h) |

The park response carries profiles because park.fan's ride page renders from the
park payload.

### The reverse lookup

Matches the term in **all three** places (track figure, ride type, manufacturer) in
one query, so `/zero-g-roll` and `/b-and-m` work alike, backed by GIN
`jsonb_path_ops` indexes on `elements` and `types` created in
`RideProfileService.onModuleInit()`. Answers `{ termId, total, data }`, where
`total` is the number of rows returned (`limit` default 200, max 500).

- **`sort=park`** (default; unknown values fall back to it): park name, then ride
  name.
- **`sort=popularity`**: ranked by typical peak wait — `typicalPeakWait`, the 548-day
  P90 from `attraction_p90_baselines` — with the baseline's confidence bucket
  outranking the value (a five-sample fluke must not lead), P90 descending, then park
  and ride name so paging is deterministic.
- Each row carries `typicalPeakWait` and `isHeadliner`. The join is a `LEFT JOIN`:
  rides without a baseline sort last and stay in the list, and their
  `typicalPeakWait` is **absent** — branch on absence, never print 0, which reads as
  "never a queue".
- Measured 2026-07-28: 630 of 710 curated rides (88.7 %) had a baseline, 599 at
  high/medium confidence; a term can have a single ride (`celestial-spin`).

## Where the rows come from

Hand-curated in the admin; no feed, no seed file. Writing, publishing, the term-id
audit and the sourcing rules: [curation → ride profiles](../admin/curation.md#ride-profiles).
