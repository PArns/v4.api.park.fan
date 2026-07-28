# Ride ↔ Glossary link (`rideProfile`)

Curated data connecting an attraction to the frontend glossary: which named
track figures it contains, what kind of ride it is, who built it, when it
opened. Every id stored is a **glossary term id** from `park.fan`
(`lib/glossary/data.ts`), so the link works in both directions off one table.

---

## Why it exists

A ride page could say "Black Mamba is an inverted coaster with four
inversions" — but that is a dead end. Storing the *terms* instead means the
ride page can link "Zero-G Roll" into a glossary entry that explains and
animates it, and the glossary entry can list the other rides that have one.

## Shape

| Layer          | File                                                        |
| -------------- | ----------------------------------------------------------- |
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
}
```

`elements` order is the **ride order**. Never sort it — the ride page renders
it as a numbered layout walkthrough.

`inversions` is what the park publishes and may legitimately disagree with
`elements`: parks count non-inverting figures inconsistently, and an element
appearing once in the list can occur twice in the layout.

## API surface

| Endpoint                                                     | What it serves                       |
| ------------------------------------------------------------ | ------------------------------------ |
| `GET /v1/parks/:geo/:park/:attraction` → `rideProfile`        | ride → glossary (detail)             |
| `GET /v1/parks/:geo/:park` → `attractions[].rideProfile`      | ride → glossary (embedded, batched)  |
| `GET /v1/glossary/terms/:termId/attractions`                  | glossary → rides                     |
| `GET /v1/glossary/terms/counts`                               | term id → ride count, for badging    |

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

The spec also enforces four invariants that catch real curation mistakes:

- no unknown term ids,
- no duplicate ride keys (a second entry would silently overwrite the first),
- every coaster's element list starts with a lift, a launch or a drop (a layout
  that starts mid-air means the list was truncated),
- no ride claims inversions while its element list contains no inverting figure
  (this one caught four wrong entries on its first run).
