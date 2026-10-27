# Ride ↔ glossary link (`rideProfile`)

Curated per-ride data linking an attraction to park.fan's glossary — its track
figures in ride order, ride types, manufacturer, model, opening year and
measurements — plus the reverse lookup from a glossary term to its rides. Open it
when rendering a ride profile, a term's ride list, or curating profiles. Every id
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
  `typicalPeakWait` is **absent from the JSON** (the global `ExcludeNullInterceptor`
  strips nulls) — branch on absence, never print 0, which reads as "never a queue".
- Measured 2026-07-28: 630 of 710 curated rides (88.7 %) had a baseline, 599 at
  high/medium confidence; a term can have a single ride (`celestial-spin`).

## Curating profiles

There is no upstream feed and no seed file: **the `attraction_ride_profiles` rows are
the source of truth.** Write them in the admin (`/admin/attractions/<id>`, ride
profile editor → `PUT` / `DELETE /v1/admin/content/attractions/:id/ride-profile`,
editor role, audited). The write checks every term id against park.fan's
`/api/glossary-term-ids` and refuses unknown ones, stamps `seeded_at`, and publishes
(evict, revalidate, revalidate again after the CDN window) — see
[curation](../admin/curation.md#ride-profiles). Never touch `stats` /
`stats_updated_at`; those belong to the Wikidata importer.

A row written by hand in SQL must set `seeded_at = now()` (NOT NULL, no default) and
then be published with `POST /v1/admin/publish-ride-profiles { "sinceHours": 24 }`,
which evicts and revalidates every profile stamped in the window; without it a fix
surfaces only as TTLs expire (up to 6 h Redis, 900 s edge, a day in the frontend's
data cache). Match such SQL on `parks.slug` **and** `attractions.slug` — park slugs
repeat (`disneyland-park` is Anaheim and Paris) and so do ride names ("Goliath" is six
coasters).

**Term-id audit.** `GET /v1/admin/ride-profile-term-audit` diffs every stored id
against park.fan's `/api/glossary-term-ids` and names the broken ids and the rides
they shorten; it aborts rather than reporting when the glossary answers empty. It
also runs daily at 06:30 (`CuratedDataProcessor`, `ride-profile-term-audit-cron`) and
logs a warning without failing the job — the ids only break when park.fan renames a
term. **Still unguarded:** a ride deleted and re-created upstream loses its profile
silently (the FK cascades).

### Sourcing rules

Ride layouts are facts about a physical object. Assemble them from the park's
attraction page, the manufacturer's project page (Mack, Intamin, Vekoma, B&M and
Gerstlauer publish element lists), Wikipedia ride articles and on-ride footage. RCDB
is a link target and a check of manufacturer/model/year, **never a text source** —
its terms permit linking, not reuse. Where sources disagree on a figure's name, use
the manufacturer's name for that installation. Add only an element you can point at
in footage or a published layout: an incomplete list is fine, a wrong one is worse
than none.

Measurements, stricter, because a number looks equally authoritative checked or not:

- Join sources on the **RCDB id**, never the name; confirm the source describes *our*
  installation (our own id pointed at a predecessor ride for four coasters).
- Found by name, the article must name the ride **and one park** — a search for
  "Winja's Force Phantasialand" returns Taron.
- Require two sources within 5 %; where they disagree, write nothing.
- Sanity: no coaster beats `sqrt(2gh)` without a launch or is taller than half its
  track. The admin rejects values outside 0–400 km/h, 0–250 m, 0–10,000 m,
  0–3,600 s.

Invariants to check by hand, each a mistake that happened: no unknown term ids; a
coaster's list starts with a lift, launch or drop (Hydra's pre-lift jojo roll is the
exception); no ride claims inversions with no inverting figure in its list; **two
rides with a byte-identical element list are a lead, not a fact** — every time it was
chased down, members turned out to be another model (a "Boomerang" family coaster, an
"SLC" that is an MK-900, a B&M floorless filed under a sister ride). Verify each
against its own RCDB id.

```sql
-- identical element lists shared by several rides
SELECT rp.elements, count(*), array_agg(a.slug)
  FROM attraction_ride_profiles rp JOIN attractions a ON a.id = rp."attractionId"
 WHERE jsonb_array_length(rp.elements) > 4
 GROUP BY rp.elements HAVING count(*) > 1 ORDER BY count(*) DESC;

-- rides with an RCDB id and no profile
SELECT p.slug, a.slug, a.rcdb_id
  FROM attractions a JOIN parks p ON p.id = a."parkId"
  LEFT JOIN attraction_ride_profiles rp ON rp."attractionId" = a.id
 WHERE rp."attractionId" IS NULL AND a.rcdb_id IS NOT NULL;
```
