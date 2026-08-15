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

| Layer     | File                                                         |
| --------- | ------------------------------------------------------------ |
| Entity    | `src/attractions/entities/attraction-ride-profile.entity.ts` |
| Data      | the `attraction_ride_profiles` table itself — see below      |
| Service   | `src/attractions/services/ride-profile.service.ts` (read-only) |
| DTO       | `src/attractions/dto/ride-profile.dto.ts`                    |
| Controller | `src/attractions/glossary-rides.controller.ts`              |

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
| `curated_stats` | hand-written SQL                   | hand-checked, deliberate |

`mapRideProfile` merges them **field by field** with curated winning, so a ride
can serve a curated speed next to an imported duration. The served object says
where it came from:

```jsonc
"stats": {
  "topSpeedKmh": 153, "heightM": 99.1, "lengthM": 2012, "durationSeconds": 205,
  "source": "mixed",          // "curated" | "wikidata" | "mixed"
  "sourceId": "Q1065056",     // null unless an imported value survived the merge
  "attribution": {            // null when every surviving number is curated
    "label": "Wikidata",
    "url": "https://www.wikidata.org/wiki/Q1065056"
  }
}
```

Everything is metric and **every field is independently nullable** — including
`stats` itself. Render what is there; a ride with a speed and no duration is
the normal case, not an error.

### Crediting the numbers: read `attribution`, nothing else

```tsx
{stats.attribution && (
  <a href={stats.attribution.url}>{t("statsSource", { source: stats.attribution.label })}</a>
)}
```

That is the whole rule, and it is deliberately the only one a client needs.
`attribution` is null exactly when nobody outside is owed a credit, so a client
that renders it when present and nothing when absent cannot produce a false
citation. Localize the sentence around `label`, never the label itself.

**Do not derive the credit from `source` or `sourceId`.** Those two are
provenance — useful to know, not a rule to reimplement. Deriving it is how the
frontend once rendered a credit for every ride that had any measurement,
naming a source none of its numbers came from and linking to `/undefined`; most
rides here are `curated`, and a curated ride has no outside source to name.

Note that `attribution` is also null when a ride **has** an entity but the
curation overruled it on every field — `sourceId` goes null with it, for the
same reason: the displayed numbers did not come from there.

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
layouts as data. **The `attraction_ride_profiles` rows are the source of
truth.** There is no seed file, no `apply` job and no admin endpoint: you edit
the table directly, and nothing in a deploy can overwrite what you wrote.

In practice that means asking Claude to research a park and write the rows.

```sql
UPDATE attraction_ride_profiles rp
   SET elements   = '["lifthill","first-drop","vertical-loop","brake-run"]'::jsonb,
       inversions = 1,
       seeded_at  = now()
  FROM attractions a, parks p
 WHERE rp."attractionId" = a.id AND a."parkId" = p.id
   AND p.slug = 'phantasialand' AND a.slug = 'black-mamba';
```

Match on `parks.slug` **and** `attractions.slug` together — park slugs are not
globally unique (`disneyland-park` exists in Anaheim and in Paris), and ride
names repeat across parks ("Goliath" is six different coasters).

A ride with no profile yet needs an INSERT, and `seeded_at` is NOT NULL with no
default — leave it out and the insert fails:

```sql
INSERT INTO attraction_ride_profiles
       ("attractionId", "parkId", elements, types, manufacturer_name,
        manufacturer_term_id, model, opened_year, inversions, seeded_at)
SELECT a.id, a."parkId",
       '["lifthill","first-drop","vertical-loop","brake-run"]'::jsonb,
       '["steel-coaster"]'::jsonb,
       'Bolliger & Mabillard', 'b-and-m', 'Sitting Coaster', 1999, 1, now()
  FROM attractions a JOIN parks p ON p.id = a."parkId"
 WHERE p.slug = 'some-park' AND a.slug = 'some-ride'
    ON CONFLICT ("attractionId") DO NOTHING;
```

`elements` and `types` are NOT NULL too, but default to `'[]'`, so omit them
rather than writing `null`. Never touch `stats` or `stats_updated_at` — those
belong to the Wikidata importer; hand-checked numbers go in `curated_stats`.

### After editing: publish, then audit

```
POST /v1/admin/publish-ride-profiles   { "sinceHours": 24 }   # make it visible
GET  /v1/admin/ride-profile-term-audit                        # check the ids
```

Both exist because SQL cannot do what the seed job did around the write.

**Publish** evicts `park:integrated:{parkId}` for every profile whose
`seeded_at` falls in the window, tells the frontend to revalidate, and queues
the second sweep past the CDN window — the same evict-then-revalidate order the
metadata seed uses, and the reason your `UPDATE` must set `seeded_at = now()`.
Without it a corrected ride surfaces only as the TTLs expire: up to 6 h in Redis
for a closed park, 900 s at the Cloudflare edge, and a day in the frontend's own
data cache. Safe to call speculatively — with nothing curated in the window it
evicts nothing.

**Audit** answers the question the deleted CI check used to answer. The frontend
publishes the term ids that actually resolve to a page
(`park.fan/api/glossary-term-ids`), this diffs them against every id stored in
`elements`, `types` and `manufacturer_term_id`, and names both the broken ids
and the rides they shorten. It aborts rather than reporting when the glossary
side answers empty — a frontend blip must not read as "the whole curation is
dead".

**The audit runs itself daily at 06:30** (`CuratedDataProcessor`), so a renamed
glossary term surfaces as a warning naming the ids and the rides they shorten,
rather than waiting for somebody to ask. It deliberately does not fail the job:
the ids are correct until the frontend ships a rename, and a nightly red job
teaches people to ignore it. Call the endpoint yourself when you want the
detail — after renaming a term, or at the end of a curation session.

**Publish stays manual**, and that is the right split: it belongs at the end of
a curation session, which is a human moment anyway.

**Still nobody's job:** a ride deleted and re-created upstream loses its profile
without a warning, because the FK cascades. Park and ride slugs drifting is no
longer a problem — rows key on `attractionId`, not slugs.

Sanity queries worth keeping to hand:

```sql
-- rides with an RCDB id and no profile at all
SELECT p.slug, a.slug, a.rcdb_id
  FROM attractions a
  JOIN parks p ON p.id = a."parkId"
  LEFT JOIN attraction_ride_profiles rp ON rp."attractionId" = a.id
 WHERE rp."attractionId" IS NULL AND a.rcdb_id IS NOT NULL;

-- profiles claiming inversions with an element list that has none
SELECT p.slug, a.slug, rp.inversions, rp.elements
  FROM attraction_ride_profiles rp
  JOIN attractions a ON a.id = rp."attractionId"
  JOIN parks p ON p.id = rp."parkId"
 WHERE rp.inversions > 0 AND jsonb_array_length(rp.elements) > 0
   AND NOT rp.elements ?| array['vertical-loop','corkscrew','immelmann',
       'dive-loop','zero-g-roll','zero-g-stall','cobra-loop','cobra-roll',
       'batwing','sea-serpent','sidewinder','inline-twist','heartline-roll',
       'jojo-roll','flying-snake-dive','pretzel-loop','barrel-roll-drop',
       'twisted-horseshoe-roll','step-up-under-flip','flat-spin','raven-turn',
       'cutback','butterfly','bowtie','interlocking-loops','norwegian-loop',
       'banana-roll','inclined-loop','scorpion-tail','celestial-spin'];

-- identical element lists shared by several rides: the copy-paste tell that
-- has now produced wrong data four times (SLCs, Boomerangs, B&M floorless)
SELECT rp.elements, count(*), array_agg(a.slug)
  FROM attraction_ride_profiles rp
  JOIN attractions a ON a.id = rp."attractionId"
 WHERE jsonb_array_length(rp.elements) > 4
 GROUP BY rp.elements HAVING count(*) > 1 ORDER BY count(*) DESC;
```

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
ride page just silently drops it and the layout reads short. The mirrored
allowlist (`glossary-term-ids.ts`) and the spec that failed CI on an unknown id
both went with the seed, because there is no longer a checked-in file to check.

`GET /v1/admin/ride-profile-term-audit` is what replaced them, and a daily
06:30 job now calls it — so a rename shows up in the log the next morning
instead of on a ride page. Before writing a new id by hand, confirm it exists:

```bash
grep "id: 'zero-g-stall'" ../park.fan/lib/glossary/data.ts
curl -s https://park.fan/api/glossary-term-ids | jq -r '.ids[]' | grep zero-g-stall
```

The audit does the same comparison across every stored id at once. To find the
rows yourself — after removing a term, say:

```sql
SELECT p.slug, a.slug, rp.elements
  FROM attraction_ride_profiles rp
  JOIN attractions a ON a.id = rp."attractionId"
  JOIN parks p ON p.id = rp."parkId"
 WHERE rp.elements @> '["the-removed-id"]'
    OR rp.types    @> '["the-removed-id"]'
    OR rp.manufacturer_term_id = 'the-removed-id';
```

The invariants the spec used to enforce are still the right ones to check by
hand — these are the mistakes that actually happened:

- no unknown term ids,
- every coaster's element list starts with a lift, a launch or a drop (a layout
  that starts mid-air means the list was truncated — Hydra's pre-lift jojo roll
  is the one legitimate exception, so allow a figure or two before the lift),
- no ride claims inversions while its element list contains no inverting figure
  (this caught four wrong entries the first time it ran),
- every curated measurement is inside what a ride can physically be (5–260 km/h,
  1–200 m, 20–8500 m, 10–900 s) — the shape a unit slip takes,
- no ride is taller than half its own track, which is what a length read in feet
  next to a height read in metres looks like,
- and the one no automated check ever had: **two rides sharing a byte-identical
  element list are a lead, not a fact.** Model families do share layouts, but
  every time that pattern was actually chased down it turned up members that
  were not that model at all — a "Boomerang" that is a family coaster, an "SLC"
  that is an MK-900, a Vekoma that is a Schwarzkopf, a B&M floorless filed under
  a sister ride's layout. Verify each one against its own RCDB id.
