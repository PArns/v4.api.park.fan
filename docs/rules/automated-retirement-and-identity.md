# Automated Retirement, Upstream Identity and Production One-offs

Written after the weekend of 2026-10-03/04 (PAR-621 → PAR-682, PAR-684, PAR-685,
PAR-686, PAR-687). One new job retired 186 attractions on its first run, 141 of
them Halloween mazes between two seasons; the sync had grown a `-2` twin for
every maze ThemeParks.wiki re-issued under a new id; Movie Park's mazes showed
Dutch names. Each rule below is one thing that went wrong, and what stops it
next time. Detail: [Attraction Status & Seasonality §5.9/5.9a](../architecture/attraction-status-and-seasonality.md),
[Curation](../admin/curation.md).

## 1. Absence from a feed is not removal

- A row missing from `/children` is **out of season** before it is **gone**.
  `retireAbsentAttractions` skips a row whose season is known
  (`isSeasonalRow`: `curated_is_seasonal` wins, `false` included).
- The detector cannot know a season before `MIN_OBSERVED_DAYS` (330) of
  watching, so **a maze in its first year is protected only by curation**. When a
  seasonal event starts or ends, its attractions get a season window in the
  admin — from the operator's event page, with the dates of that page.
- Whatever slips through lands on the **"season or gone?" list**
  (`/admin/data-quality`, `absenceRetiredUnreviewed`; one WARN per park from the
  nightly data-quality job): rows the absence step retired while
  `curated_is_seasonal` is unset. Answering it is setting that column — `true`
  with the months (the sync lifts the retirement when the wiki lists the ride
  again), `false` when it is really gone (PAR-684, option B).
- A retirement written by the sync must undo itself
  (`RECLASSIFIED_UPSTREAM_REASONS`); a human retirement must never be undone by
  the sync.

## 2. A destructive job's first run is a mass write — count it before merging

A job that retires, merges or deletes rows meets the whole backlog on its first
run. Before such a PR merges:

1. Run its selection as a read-only query against production and **write the
   count into the PR**.
2. **Break the count down by kind** (seasonal / has a live twin / never had a
   reading / facility rather than ride) and read a sample of each class by name.
   "All gates held" is not the same as "every row should go": PAR-621's gates
   all held and 141 of 186 were still wrong.
3. State in the PR what the first run will do and how it is undone.
4. **A gate the database cannot answer has to be fetched.** This job decides
   absence against `/children`, and no query over `attractions` reproduces
   that: `updatedAt` is a proxy, not the feed. On 2026-10-05 the selection for
   PAR-656 passed 8 rows through every database-side gate and **1** through the
   feed — the other 7 were listed upstream the whole time. Counting without
   the fetch would have overstated the first run eightfold. Fetch `/children`
   for each park that owns a candidate (23 parks, one request each) and make
   the listing check part of the count that goes into the PR.

## 3. Identity is the upstream id — and the upstream re-issues ids

- ThemeParks.wiki hands seasonal attractions (and once a whole park) a **new
  entity id** each season. A rename keeps the id; a re-issue drops the old one
  from `/children`. That — not the name — is what tells them apart
  (`findExistingAttraction` with `listedExternalIds`).
- Two rows of one ride are **merged**, never retired side by side and never
  renamed by hand; the merge keeps history (`AttractionMergeService`).
- Before a merge, each pair is **proven**: old id no longer listed, upstream
  coordinates within a building (G-137), and the operator's page naming the
  attraction once. Distance alone is not proof — seasonal overlays sit 0–20 m
  from a permanent ride.
- Re-issues under a **different** name (`HAUNTED HOUSE: SAW…` → `SAW…`, Dutch
  vs English) are not caught by the name match. They are **listed, never merged
  by a machine** (PAR-686, option B): `/admin/duplicates` shows every
  absence-retired row with a younger live row within 30 m
  (`findReissueCandidates`), with both upstream ids, last readings and a name
  hint (`reissueNamesMatch`), and a human merges (`adoptLoserExternalId`, so
  the survivor takes the id the feed lists — without it the sync grows the
  loser back) or dismisses with a `not_a_duplicate` mark. Distance alone is
  never enough: Walibi Belgium's three 4D films sit at 0 m.
- **`attractions.externalId` is unique globally, `slug` only per park.** The
  wiki hangs single children on **both** parks of a resort, so the second park
  asks for a row it can never insert: the lookup in `syncAttraction` is
  park-scoped, finds nothing, and the insert hits
  `UQ_a94e9dc2762dfca8a463d173657`. Four such children on 2026-10-07 —
  `Gremmie Lagoon` and `Beach House Slides` (Knott's Berry Farm, also listed
  under Knott's Soak City), `Muffleheads Beach Bar` and `Cedar Creek` (Cedar
  Point, also under Cedar Point Shores). The child is skipped and logged; which
  park should own the row is open (PAR-743).

## 4. Names come from the operator, never from a translation

`curated_name` carries the name the **operator** uses on its own site — Movie
Park calls its mazes "Hell House" and "The Slaughterhouse" on the German page
too. A translated name is a name the park does not have. Before renaming a row,
search the park for an existing row of the same attraction (§3).

## 5. A slug change needs an alias

Attraction URLs are indexed. A slug that stops answering needs an
`attraction_slug_aliases` row (PAR-687); the merge writes one itself, a manual
change writes it in the same transaction (SQL in [Curation](../admin/curation.md)).

## 6. One child must not cost its park the whole feed

- A per-park `try/catch` around a loop over children is **not** error handling,
  it is a kill switch: the first child that throws ends the park, and
  everything downstream of the loop — the reclassification steps, the absence
  step, the mapping job, the cache eviction — never runs. Each child gets its
  own `try/catch` (`handleFetchChildren`, all three loops plus the Queue-Times
  fallback).
- Found by PAR-714 after **71 days**: Knott's Soak City wrote 1 of 9 rows per
  run, Cedar Point Shores 2 of 17, both to a single unwritable child at feed
  index 1 (§3). Nothing in the run said so — the park-wide handler logs one
  line and returns zeros, which reads like a park with no children.
- **Locate such a stall at the feed order, not in the code.** Line the feed's
  order up against each row's `updatedAt`: the boundary between the rows
  written tonight and the rows frozen months ago names the child that throws.
  It found both parks with one query.
- A skip is **never silent**: it carries park, child name, upstream id and the
  error, and is counted per park and per run (`Skipped Children: N`). A listed
  child without a row is a fact someone has to see.

## 7. Production one-offs run the deployed code, from a clean checkout

- A one-off script that imports repository code (a merge through
  `AttractionMergeService`, say) runs from a **clean checkout of the deployed
  commit**, never from a worktree with work in progress. On 2026-10-04 a merge
  run imported a half-built dependency list naming a table production did not
  have yet; all 36 merges aborted. They were transactional, so nothing was
  lost — that is luck, not process.
- `synchronize: false` on every DataSource a script opens against production;
  production runs `DB_SYNCHRONIZE=true`, a script must never.
- Ad-hoc SQL goes through `scripts/prod-psql.sh` (timeouts), every write in one
  transaction that **counts before it writes**, and the count goes into the
  issue's worker log.
- A write outside the admin path skips the cache eviction and the frontend
  revalidation: say so, and either evict or state the TTL until it shows.
