# Attraction status, seasonality and identity

How the API decides what an attraction is doing (OPERATING, CLOSED, UNKNOWN …), whether it is in
season, which row is which ride, and what a detector may and may not conclude from a feed. Open the
page for the question you have; the rule all of them apply is
[An absent fact never becomes a confident one](../../rules/absent-facts.md).

| Page | Covers |
| --- | --- |
| [Status regimes](status-regimes.md) | Real feed, free-flow `open_with_park`, `UNKNOWN` when no source reports a ride, the optimistic fallback, a whole park gone silent |
| [Seasonality](seasonality.md) | `is_seasonal` vs `season_months`, `MIN_OBSERVED_DAYS`, `detect-seasonal`, curating a free-flow area and its months |
| [The `MAZE` rule](maze-rule.md) | The one `attraction_kind` verdict a job may write, and the scare-zone rubric |
| [Identity](identity.md) | `externalId` vs slug, the name grouping in the park payload, which row of a name group the payload and sitemap keep |
| [Review marks](review-marks.md) | `attraction_review_marks`, and how to triage retirement candidates before researching them |
| [Feed drops](feed-drops.md) | When a cluster of rides goes quiet: the three events behind one symptom, the cluster query and the two checks |
| [Duplicates](duplicates.md) | Attraction and park duplicate detection: map numbers in names, one source naming two things alike, `sharedPoint`, the merge gate, `attractionsAgree` |
| [Reclassification and absence](reclassification-and-absence.md) | An entity that changed `entityType`, left `/children`, was re-issued under a new id or moved parks — and how the sync retires and un-retires its row |

The standing rules for retirement, upstream identity and production one-offs are in
[Automated retirement and upstream identity](../../rules/automated-retirement-and-identity.md).
The curated columns themselves are in [Curation](../../admin/curation.md).

**Client contracts.** What a client reads and must do lives in `docs/frontend/`:
[`status` and `effectiveStatus`](../../frontend/effective-status.md),
[seasonality](../../frontend/seasonality.md) (the three values of `isCurrentlyInSeason` and its SQL
twin `attractionIsOutOfSeason`, which change together),
[parks whose wait times we cannot read](../../frontend/live-wait-times-availability.md).

## Old section numbers

The released changelog (`docs/changelog.md`) cites this subject as
`attraction-status-and-seasonality.md` with a section number. Where each section now lives:

| § | Now |
| --- | --- |
| 1 | [absent-facts rule](../../rules/absent-facts.md) |
| 2, 2.1–2.4 | [status-regimes.md](status-regimes.md) |
| 3, 3.1–3.3 | [seasonality.md](seasonality.md) |
| 4 (two writers) | [curation.md](../../admin/curation.md); the free-flow months boundary in [seasonality.md](seasonality.md#free-flow-rows-and-their-months) |
| 4a | [identity.md](identity.md) |
| 4b | [review-marks.md](review-marks.md) |
| 5.1 | [seasonality.md](seasonality.md#running-and-changing-the-detector) |
| 5.2, 5.2a | [feed-drops.md](feed-drops.md) |
| 5.3–5.5b | [duplicates.md](duplicates.md) |
| 5.6, 5.7, 5.9, 5.9a | [reclassification-and-absence.md](reclassification-and-absence.md) |
| 5.8 | [status-regimes.md](status-regimes.md#5-a-whole-park-gone-silent) |
| 6 | split: [status-regimes.md](status-regimes.md#is-a-closure-ours-or-the-operators), [feed-drops.md](feed-drops.md#the-cluster-query), [seasonality.md](seasonality.md#checking-month-lists) |
| 7, 7a | [seasonality.md](seasonality.md#curating-a-free-flow-attraction) |
| 8 | [maze-rule.md](maze-rule.md) |
