# Search Architecture

Fuzzy, typo-tolerant search over parks, attractions, shows, restaurants, and geo locations
(`GET /v1/search?q=…&type=…&limit=…`). Implemented in `src/search/search.service.ts`.

## Two execution paths

`search()` runs each entity type through one of two matchers, chosen by `this.indexReady`:

1. **In-process (primary).** Once the in-memory indices are built (`indexReady = true`, after
   warmup), matching runs in JS against arrays held in the service
   (`searchParksInProcess` / `…AttractionsInProcess` / …). This is the normal hot path — no
   DB round-trip per keystroke. The indices are also cached in Redis
   (`search:index:v1:{parks,attractions,shows,restaurants}`, TTL 2 h) so a fresh process can
   rehydrate them without rescanning Postgres.
2. **Postgres (fallback).** Before the indices are ready (or if they fail to load), matching
   runs as SQL via `pg_trgm` (`searchParks` / … with `ILIKE`, `%`, `<%`, `dmetaphone`,
   GIN trigram indexes). Created in `initializeFuzzySearchIndices()`.

> **The search itself is NOT done in Redis.** Redis only (a) caches the per-query *result*
> (`search:fuzzy:v1:{type}:{q}`, TTL 5 min — `CACHE_TTL`) and (b) caches the *index* the
> in-process matcher loads. The actual matching is in-process JS (primary) or Postgres
> (fallback).

## Matching / ranking (`scoreEntry`)

Both paths share the same tier ladder (lower tier = better, ties broken by trigram `sim`):

| tier | signal |
|---|---|
| 0 | exact name |
| 1 | normalized exact (`F.L.Y.` ↔ `fly`) |
| 2 | name / word prefix |
| 3 | substring |
| 4 | normalized substring |
| 5 | `extra` field substring (e.g. attraction `landName`) |
| 6 | whole-string trigram similarity ≥ 0.3 (mirrors pg_trgm `%`) |
| 7 | **word-level typo tolerance** (see below) |

`trgmSim()` reimplements pg_trgm's Jaccard-over-padded-trigrams so the in-process path agrees
with the Postgres path.

## Typo tolerance (2026-06-01)

Whole-string trigram similarity collapses on a single-character typo in a short word
(`similarity("epuc", "Universal Epic Universe") = 0.10`, well under 0.3), so `"epuc"` /
`"epuc univ"` did **not** find **Universal Epic Universe** even though `"epic"` did (via the
substring tier). Fixes:

- **In-process (primary):** new tier 7 `wordFuzzyMatch()` — every query word must match some
  name word by prefix or by a short edit distance (`boundedLevenshtein`, ≤1 for words ≤4
  chars, ≤2 for longer). `lev("epuc","epic") = 1` → match. Verified: `epuc`, `epuc univ`,
  `epic` → match; `xyz`, `park` → no false match.
- **Postgres (fallback):** lowered `pg_trgm.word_similarity_threshold` from its 0.6 default to
  **0.4** (`ALTER DATABASE … SET`, in `initializeFuzzySearchIndices`), so the `<%` operator
  accepts the typo (`word_similarity("epuc", name) = 0.40`). Safe globally — the search is the
  only `<%` consumer. Effective for connections opened after it runs (the on-demand pool
  connections that serve searches).

Both are pure recall additions — they only add matches, never remove existing ones. Stale
empty results for a previously-failed query self-heal within the 5 min result-cache TTL.
