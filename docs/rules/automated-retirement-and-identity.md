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
  vs English) are not caught by the name match (PAR-686).

## 4. Names come from the operator, never from a translation

`curated_name` carries the name the **operator** uses on its own site — Movie
Park calls its mazes "Hell House" and "The Slaughterhouse" on the German page
too. A translated name is a name the park does not have. Before renaming a row,
search the park for an existing row of the same attraction (§3).

## 5. A slug change needs an alias

Attraction URLs are indexed. A slug that stops answering needs an
`attraction_slug_aliases` row (PAR-687); the merge writes one itself, a manual
change writes it in the same transaction (SQL in [Curation](../admin/curation.md)).

## 6. Production one-offs run the deployed code, from a clean checkout

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
