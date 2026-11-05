# Duplicate detection: attractions and parks

How two rows of one ride, or two rows of one park, are found — and the cases where name matching
gets it wrong in both directions. Open this before changing `attraction-merge.util.ts` or
`ParkValidatorService.findDuplicates`, or before merging by hand. A merge itself must be proven
first ([upstream identity rule](../../rules/automated-retirement-and-identity.md#3-identity-is-the-upstream-id--and-the-upstream-re-issues-ids)).

## Attractions

**A map number in a ride name.** Queue-Times publishes some parks' own map numbers inside the name
(Energylandia `Draken (155)` against the wiki's `Draken`); the park held 138 rows for 46 rides.
Name comparison strips a trailing `(number)` of **at most three digits**
(`attraction-merge.util.ts`), because `HAUNTED HOUSE: Texas Chainsaw Massacre (2022)` is a year and
the 2022 maze is not the 2023 one. A shared Queue-Times id **finds** a pair but does not **decide**
it — Carowinds runs two slide complexes under one id.

**One source naming two things alike.** ThemeParks.wiki publishes several Heide Park entities all
called `PLAYGROUND` (five playgrounds with five coordinate pairs). Two ids from the **same** source
are that source saying they are two things, so an automatic merge requires a pair to span two
sources.

**A trailing country code stays.** `normalizeForMatching` keeps ` (DK)` / ` (UK)`
([data ingestion](../data-ingestion.md#name-matching-keeps-a-trailing-country-code)).

## Parks

### `sharedPoint`: one water park, two names (PAR-160)

`Wet'n'Wild` (wiki) and `Wet 'n' Wild Gold Coast` (`qt-park-146`) are one park in Oxenford: same
coordinates to seven decimals, same city and `park_type`, the same 13 slides. Every branch of
`findDuplicates` required name similarity ≥ 0.85 and this pair scores **0.6923** (the nine letters of
`GoldCoast`). A distance of 0 km is a stronger statement than any string comparison, so the
`sharedPoint` branch lets the physical facts lead:

- coordinates within `SHARED_POINT_KM` (**0.01 km**). At 0.05 the ticket's proposal would have caught
  Hurricane Harbor Rockford (0.0424 km from Hurricane Harbor Chicago, a real park 110 km away whose
  Queue-Times geocode says Gurnee). Between 0.0000 and 0.0424 the catalogue is empty.
- sources disjoint (`sourcesDisjoint`, read from which source columns a row fills): a source that
  lists both rows knows two parks (Queue-Times lists PortAventura Park and Ferrari Land apart).
- name similarity over `SHARED_POINT_NAME_SIMILARITY` (**0.65**), a floor, not a verdict. A water
  park beside its theme park scores at or above the pair to catch (Legoland Windsor 0.7429, Alton
  Towers 0.6923), so the radius carries that class — as long as the venue has its own geocode
  (closest such pair: 0.1174 km). Another park of the same brand stays below the floor
  (`Wet 'n' Wild Las Vegas` against the Gold Coast row, 0.6061; a spec pins it). At 0.6 the Rockford pair would rest on the radius alone; the 63
  pairs in [0.60, 0.65) are all different parks and all outside the radius.

`0, 0` is not a point: `usableCoordinate` refuses Null Island (two failed geocodes are 0 km apart) and
coerces Postgres `decimal` strings. That also narrows the four name-led branches, which used to read
two failed geocodes as neighbours; ghost pairs stay reachable through `sameCity` and
`nameSimilarity >= 0.95 && sharedEntityId`, and a spec pins it.

**Residual risk:** a venue that inherits its resort's geocode sits at 0.0000 km, so only the name
floor and `sourcesDisjoint` speak. PortAventura Park, Ferrari Land and Caribe Aquatic Park share one
point today and the floor holds all three pairs (0.16–0.20); a water park scoring like Legoland
Windsor's on its resort's point from a disjoint source would pass. The review gate below is what
makes that safe.

### The gate in front of the merge (PAR-247)

`POST /v1/admin/merge-duplicate-parks` with `autoDetect: true` used to merge every detected pair in
one transaction with no undo. Now:

- **Every pair carries `safe`**, true only for a shared upstream entity **value** together with a name
  score ≥ 0.95 (the existing `nameSimilarity >= 0.95 && sharedEntityId` branch). `sharedPoint` implies
  disjoint sources, so it is never safe.
- **`autoDetect` writes nothing without `dryRun: false`**, and even then only merges safe pairs; the
  rest come back under `skipped` with their winner resolved, to send back as a manual pair. The
  response carries `dryRun`, `planned`, `skipped`; `GET /v1/admin/duplicate-parks` counts `safe` and
  `needsReview`.
- **A manual pair (`park1Id` + `park2Id`) still merges by default** — the admin's button sends that
  body — but honours an explicit `dryRun: true`.
- **A flag it cannot read is a 400.** The body has no DTO, form-encoded values are strings, so
  `"true"`/`"false"` are read and anything else on `dryRun` or `autoDetect` is refused.

Not covered: `ParksService.repairDuplicates()` groups parks by a shared `queue_times_entity_id` in its
own SQL and merges without asking the name. It runs only from `syncParks()` when `findAll()` returned
no parks, i.e. on bootstrap (PAR-262).

### `attractionsAgree`: a row filed on another park's point (PAR-310)

A Queue-Times-only park row copies the feed's coordinate and reverse-geocodes its city from it, so
point and city agree with each other even when both are wrong; only the rides contradict them. Three
such rows were undetected for about a year (`qt-park-297` filed at Gurnee for Hurricane Harbor
Rockford, `qt-park-294` at Marietta for Hurricane Harbor Oklahoma City, `qt-park-29` in Houston for
Sesame Place Langhorne), each sharing every ride with the real park.

`attractionsAgree` pairs two rows when at least `ATTRACTION_OVERLAP_MIN_SHARED` (5) names agree and
they are at least `ATTRACTION_OVERLAP_MIN_SHARE` (0.8) of the shorter list, only with
`sourcesDisjoint`. That condition is what keeps chains apart: Fantawild's water parks share up to 15
of 16 ride names, but all come from ThemeParks.wiki alone; among other pairs the highest share is
0.125 (measured 2026-10-02, 211 parks). Such a pair is never `safe`; it appears in
`GET /v1/admin/duplicate-parks` with a `reviewReason` giving the distance, and above
`MAX_INHERIT_DISTANCE_KM` (10) it says to correct the wrong row's location first, because a merge over
a longer distance drops the loser's source ids. `findDuplicates` logs the count with a 🎢 warning after
every park metadata sync (`validateAll`).
