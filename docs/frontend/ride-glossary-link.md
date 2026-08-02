# Ride ↔ Glossary link (`rideProfile`)

Curated data connecting an attraction to the frontend glossary: which named
track figures it contains, what kind of ride it is, who built it, when it
opened. Every id stored is a **glossary term id** from `park.fan`
(`lib/glossary/data.ts`), so the link works in both directions off one table.

---

## Why it exists

A ride page could say "Black Mamba is an inverted coaster with four
inversions" — but that is a dead end. Storing the _terms_ instead means the
ride page can link "Zero-G Roll" into a glossary entry that explains and
animates it, and the glossary entry can list the other rides that have one.

## Shape

| Layer          | File                                                         |
| -------------- | ------------------------------------------------------------ |
| Entity         | `src/attractions/entities/attraction-ride-profile.entity.ts` |
| Seed data      | `src/attractions/data/ride-profile-seed.ts`                  |
| Seed types     | `src/attractions/data/ride-profile-seed.types.ts`            |
| Term allowlist | `src/attractions/data/glossary-term-ids.ts` (generated)      |
| Service        | `src/attractions/services/ride-profile.service.ts`           |
| DTO            | `src/attractions/dto/ride-profile.dto.ts`                    |
| Controller     | `src/attractions/glossary-rides.controller.ts`               |
| Seed job       | `src/queues/processors/manual-metadata.processor.ts`         |

```ts
{
  // ordered! repeats are meaningful — two corkscrews in a row list it twice
  elements: ["lifthill", "first-drop", "vertical-loop", "zero-g-roll", ...],
  types: ["inverted-coaster", "terrain-coaster"],   // unordered set
  manufacturer: "Bolliger & Mabillard",             // display name, always set when known
  manufacturerTermId: "b-and-m",                    // null → render the name unlinked
  model: "Inverted Coaster",
  openedYear: 2006,
  inversions: 4,
  stats: {                                          // metric, every field optional
    topSpeedKmh: 80, heightM: 26, lengthM: 768, durationSeconds: 47,
  },
}
```

`elements` order is the **ride order**. Never sort it — the ride page renders
it as a numbered layout walkthrough.

`inversions` is what the park publishes and may legitimately disagree with
`elements`: parks count non-inverting figures inconsistently, and an element
appearing once in the list can occur twice in the layout.

## Measurements (`stats`)

Speed, height, length and duration reach the API from two writers that never
share a cell:

| Column          | Written by                         | What it is               |
| --------------- | ---------------------------------- | ------------------------ |
| `stats`         | `RideStatsService` (Wikidata, CC0) | automatic, thin coverage |
| `curated_stats` | the seed                           | hand-checked, deliberate |

`mapRideProfile` merges them **field by field** with curated winning, so a ride
can serve a curated speed next to an imported duration. The served object says
where it came from:

```jsonc
"stats": {
  "topSpeedKmh": 153, "heightM": 99.1, "lengthM": 2012, "durationSeconds": 205,
  "source": "mixed",          // "curated" | "wikidata" | "mixed"
  "sourceId": "Q1065056"      // null unless an imported value survived the merge
}
```

Everything is metric and **every field is independently nullable** — including
`stats` itself. Render what is there; a ride with a speed and no duration is
the normal case, not an error.

Why curate at all when there is an importer: Wikidata states a top speed for
fewer than a fifth of the coasters here, and misses most of the headliners —
Fury 325, Steel Vengeance and Millennium Force all have an entity with a height,
a length and no speed. It also has plain mistakes (Lynet's length entered as its
height), which a curated value overrules.

To add measurements, put them in the seed's `stats` and follow the update flow
below. Curate what the sources agree on and omit the rest: an absent field falls
through to the import, a wrong one overrules it.

## API surface

| Endpoint                                                 | What it serves                      |
| -------------------------------------------------------- | ----------------------------------- |
| `GET /v1/parks/:geo/:park/:attraction` → `rideProfile`   | ride → glossary (detail)            |
| `GET /v1/parks/:geo/:park` → `attractions[].rideProfile` | ride → glossary (embedded, batched) |
| `GET /v1/glossary/terms/:termId/attractions`             | glossary → rides                    |
| `GET /v1/glossary/terms/counts`                          | term id → ride count, for badging   |

The park response carries it too because the frontend ride page renders from
the park payload, not the detail endpoint — one batched `IN (...)` read per
park, not an N+1.

The reverse lookup matches the term in **all three** places it can appear
(track figure, ride type, manufacturer) in one query, so `/zero-g-roll` and
`/b-and-m` both work through the same call. It is backed by GIN indexes with
`jsonb_path_ops` on `elements` and `types`, created by
`RideProfileService.onModuleInit()` (same fire-and-forget pattern as the search
service's trigram indexes).

## Updating the data

There is no upstream feed and there will not be one — nobody publishes ride
layouts as data. The workflow is:

1. Edit `src/attractions/data/ride-profile-seed.ts`.
2. Deploy.
3. `POST /v1/admin/apply-ride-profiles` — pure database work over a few hundred
   rows, finishes in seconds. Idempotent; entries whose slugs match no
   attraction are skipped silently (park and ride slugs drift as things are
   renamed, and one stale line must not fail the run).

In practice step 1 means asking Claude to research a park and extend the file.

### Sourcing rules

Ride layouts are **facts about a physical object**, not anybody's text. Entries
are assembled from the park's own attraction page, the manufacturer's project
page (Mack, Intamin, Vekoma, B&M and Gerstlauer all publish element lists),
Wikipedia's ride articles and published on-ride footage. RCDB is used exactly
as it already is for `rcdbId` — as a link target and to confirm
manufacturer/model/year — never as a text source; their terms permit linking,
not reuse of their content.

Where sources disagree on a figure's name (a "junior Immelmann" vs a
"half-loop", a "stall" vs a "zero-g stall") use the name the manufacturer uses
for that installation.

Only add an element you can point at in footage or a published layout. An
incomplete list is fine; a wrong one is worse than none.

For **measurements**, the same rule takes a stricter form, because a number
looks equally authoritative whether or not anybody checked it:

- Join sources on the **RCDB id**, never on the ride's name. "Goliath" is six
  different coasters and "Journey to Atlantis" is three.
- Confirm the source is about _our_ installation before believing it — an
  infobox that names another park, or states an RCDB id that is not ours, is
  describing a different ride. Both happen: our own id pointed at the
  predecessor ride for four coasters until this check found them.
- When there is no id to join on and the ride has to be found by name, the park
  alone is not proof: a search for "Winja's Force Phantasialand" returns Taron,
  right park and all. The article has to name the ride too — and name **one**
  park, because an article covering three installations states one speed and
  never says whose.
- Require two sources within 5%. Where they disagree, write nothing — a blank
  field costs a line on a ride page, a wrong one is repeated back as fact.
- Sanity-check the result: a coaster cannot beat `sqrt(2gh)` without a launch,
  and cannot be taller than half its own track. Both catch unit slips, and the
  physics one caught an entire entry filed under the wrong ride.

## Keeping term ids honest

Nothing at runtime can tell us a glossary term was renamed or removed — the
ride page would just silently drop it and the layout would read short. So the
frontend's full id list is mirrored here and checked by
`ride-profile-seed.spec.ts`. Regenerate it from the **frontend** repo whenever
a term is added, renamed or removed:

```bash
node scripts/export-glossary-term-ids.mjs \
  > ../v4.api.park.fan/src/attractions/data/glossary-term-ids.ts
```

The spec also enforces six invariants that catch real curation mistakes:

- no unknown term ids,
- no duplicate ride keys (a second entry would silently overwrite the first),
- every coaster's element list starts with a lift, a launch or a drop (a layout
  that starts mid-air means the list was truncated),
- no ride claims inversions while its element list contains no inverting figure
  (this one caught four wrong entries on its first run),
- every curated measurement is inside what a ride can physically be (5–260 km/h,
  1–200 m, 20–8500 m, 10–900 s) — the shape a unit slip takes,
- no ride is taller than half its own track, which is what a length read in feet
  next to a height read in metres looks like.
