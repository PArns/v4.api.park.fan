# Automated retirement, upstream identity and production one-offs

Rules for any job or one-off that retires, merges, renames or moves rows, and for deciding which
row is which ride. Each rule is one thing that went wrong in the weekend of 2026-10-03/04: a new job
retired 186 attractions on its first run, 141 of them Halloween mazes between two seasons; the sync
had grown a `-2` twin for every maze ThemeParks.wiki re-issued under a new id; Movie Park's mazes
showed Dutch names. The mechanisms are in
[Reclassification and absence](../architecture/attraction-status/reclassification-and-absence.md).

## 1. Absence from a feed is not removal

- A row missing from `/children` is **out of season** before it is **gone**: the absence step skips a
  row whose season is known (`isSeasonalRow`, `curated_is_seasonal` first, `false` included).
- **Absence is read from the feed's own columns**, `last_listed_at` and `absent_since`, never from
  `updatedAt` (which answers "who last wrote this row").
- The detector cannot know a season before `MIN_OBSERVED_DAYS` (330), so **a maze in its first year
  is protected only by curation**: when a seasonal event starts or ends, give its attractions a season
  window in the admin, from the operator's event page.
- Whatever slips through is the **"season or gone?" list** (`absenceRetiredUnreviewed` on
  `GET /v1/admin/data-quality`). Answer it by setting `curated_is_seasonal`: `true` with the months
  (the sync lifts the retirement when the wiki lists the ride again), `false` when it is gone.
- A retirement written by the sync must undo itself (`RECLASSIFIED_UPSTREAM_REASONS`); a human
  retirement must never be undone by the sync. A row that was never a ride of its own is retired with
  `kind: "reclassified"` (`retired_kind`), and the sync never lifts a row whose kind is set.

## 2. A destructive job's first run is a mass write — count it before merging

A job that retires, merges or deletes rows meets the whole backlog on its first run. Before the PR
merges:

1. Run its selection read-only against production and **write the count into the PR**.
2. **Break the count down by kind** (seasonal / has a live twin / never had a reading / facility
   rather than ride) and read a sample of each by name. "All gates held" is not "every row should go":
   PAR-621's gates all held and 141 of 186 were wrong.
3. State in the PR what the first run will do and how it is undone.
4. **A gate the database cannot answer has to be fetched.** Absence is decided against `/children`,
   and no query over `attractions` reproduces it: for PAR-656, 8 rows passed every database gate and
   **1** passed the feed. Fetch `/children` for each park that owns a candidate and make the listing
   check part of the count.

## 3. Identity is the upstream id — and the upstream re-issues ids

- Identity is the `externalId`, never the slug or the name
  ([identity](../architecture/attraction-status/identity.md)). A rename keeps the id; a re-issue
  drops the old one from `/children` — that, not the name, tells them apart.
- Two rows of one ride are **merged** (`AttractionMergeService`, which keeps history), never retired
  side by side and never renamed by hand.
- **A merge is proven first:** old id no longer listed, upstream coordinates within a building
  (G-137), the operator's page naming the attraction once. Distance alone is not proof — seasonal
  overlays sit 0–20 m from a permanent ride, and Walibi Belgium's three 4D films sit at 0 m.
- **A re-issue under a different name is listed, never merged by a machine**
  (`findReissueCandidates`); a human merges with `adoptLoserExternalId` so the survivor takes the id
  the feed lists, or dismisses with a `not_a_duplicate` mark.
- **A ride both upstreams report has one row, one `externalId`, and the other source's id as an
  `external_entity_mapping` row.** Both sync paths write that mapping.
- **`attractions.externalId` is unique globally, `slug` only per park.** A child the wiki moves to
  another park of the resort is moved, not re-created (`applyParkMoves`).

## 4. Names come from the operator, never from a translation

`curated_name` carries the name the **operator** uses on its own site — Movie Park calls its mazes
"Hell House" and "The Slaughterhouse" on the German page too. A translated name is a name the park
does not have. Before renaming a row, search the park for an existing row of the same attraction (§3).

## 5. A slug change needs an alias

Attraction URLs are indexed. A slug that stops answering needs an `attraction_slug_aliases` row
(PAR-687); the merge and `applyParkMoves` write one themselves, a manual change writes it in the same
transaction ([curation](../admin/curation.md#names-are-display-names)).

## 6. One child must not cost its park the whole feed

A `try/catch` around a loop over children is a kill switch, not error handling: give each child its
own, log every skip with park, child, upstream id and error, and count it. Making a step reachable is a
change to that step — a skipped id must not reach steps that assume its replacement row was written
(`skippedExternalIds`). Locate a stall at the row whose `last_listed_at` stands still, not in the code.

## 7. Production one-offs run the deployed code, from a clean checkout

- A one-off script that imports repository code (a merge through `AttractionMergeService`, say) runs
  from a **clean checkout of the deployed commit**, never from a worktree with work in progress. On
  2026-10-04 a merge run imported a half-built dependency list naming a table production did not have;
  all 36 merges aborted, and only their transactions saved the data.
- `synchronize: false` on every DataSource a script opens against production
  ([why](the-entities-are-the-schema.md#scripts)).
- Ad-hoc SQL goes through `scripts/prod-psql.sh` (session timeouts,
  [DB health runbook §0](../troubleshooting/db-health-runbook.md)), every write in one transaction that
  **counts before it writes**, and the count goes into the issue's worker log.
- A write outside the admin path is not published by itself: [publish outside-in](publish-outside-in.md).
