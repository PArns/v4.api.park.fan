# Search

Fuzzy, typo-tolerant search over parks, attractions, shows, restaurants and geo locations
(`GET /v1/search?q=…&type=…&limit=…`, `src/search/search.service.ts`). Open this when a query finds
too much or too little. The frontend's palette and post search are in the frontend repo's
`docs/features/site-search.md`.

## Two execution paths

`search()` matches each entity type one of two ways, chosen by `indexReady`:

1. **In-process (normal).** After warmup the indices are arrays in the service and matching runs in
   JS (`searchParksInProcess`, `searchAttractionsInProcess`, …), no database round-trip per keystroke.
   The indices are also stored in Redis (`search:index:v1:{parks,attractions,shows,restaurants}`,
   `INDEX_TTL` 2 h) so a fresh process can load them without rescanning Postgres.
2. **Postgres (fallback)** before the indices are ready or when they fail to load: `pg_trgm` SQL
   (`ILIKE`, `%`, `<%`, `dmetaphone`) on GIN trigram indexes created by
   `initializeFuzzySearchIndices()` (declared `{ synchronize: false }` on the entities,
   [the entities are the schema](../rules/the-entities-are-the-schema.md)).

Redis never matches anything: it caches the per-query result (`search:fuzzy:v1:{type}:{q}`,
`CACHE_TTL` 5 min) and the index.

## Ranking (`scoreEntryPre`)

Both paths share one tier ladder; lower is better, ties broken by trigram similarity:

| tier | signal |
| --- | --- |
| 0 | exact name |
| 1 | normalised exact (`F.L.Y.` ↔ `fly`) |
| 2 | name or word prefix |
| 3 | substring |
| 4 | normalised substring |
| 5 | secondary field substring (an attraction's land; a park's city, country or continent) |
| 6 | whole-string trigram similarity ≥ 0.3 (`trgmSim` reimplements pg_trgm so both paths agree) |
| 7 | word-level typo tolerance |

A popularity boost (top parks and attractions from `PopularityService`) can lift a match.

## Typo tolerance

Whole-string trigram similarity collapses on one typo in a short word (`similarity("epuc", "Universal
Epic Universe") = 0.10`), so:

- **in-process:** tier 7, `wordFuzzyMatch` — every query word of three or more letters must match some
  name word by prefix or by `boundedLevenshtein` (≤ 1 edit for words ≤ 4 letters, ≤ 2 above).
  Secondary fields use `secondaryFuzzyMatch`, which budgets against the **shorter** word: with the
  query's budget, "taron" matched the city word "tan" in "Ying Tan Shi" and two Yingtan parks ranked
  above the ride Taron;
- **Postgres:** `pg_trgm.word_similarity_threshold` is lowered from 0.6 to 0.4 with
  `ALTER DATABASE … SET` in `initializeFuzzySearchIndices`, so `<%` accepts the typo. Safe because
  search is the only `<%` consumer; it applies to connections opened afterwards.

## A literal hit switches typo tolerance off

Typo tolerance is for a query that names nothing. Once any match carries the query as typed
(`isLiteralHit` = tiers 0–5), `search()` drops every match only the typo tiers found, in every type.
The case: `phantasialand` against the land `Fantasyland` is a trigram similarity of exactly 0.30, so
the park came back with ten Disney Fantasyland rides under it. A query without a literal match keeps
its fuzzy matches (`fantasialand` still finds Phantasialand, `taaron` finds Taron).
`searchAttractionsInProcess` and `searchParksInProcess` sort literal matches first before cutting to
`limit`, because the popularity boost can lift a typo match above a land match.

Measured 2026-10-22 against an index built from the production API (211 parks, 7,447 rides), 104
queries at `limit` 10: 65 lists changed, none of the typo queries did (`taron` 24 → 1 results,
`heide park` 17 → 3, `dollywood` 26 → 2).
