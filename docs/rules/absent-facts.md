# An absent fact never becomes a confident one

Our own bookkeeping is never somebody else's statement. Where no source says something, the API
says "unknown" — it does not fill the gap with the most likely answer. Open this page before
writing any default, fallback or derived value that a reader will take as a fact. Every case
below shipped once the other way.

## The rule, case by case

- **A ride no source reports reads `UNKNOWN`, never `CLOSED`.** Reverse reconciliation's CLOSED row
  records that our data stopped arriving, not that the operator shut the ride (Europa-Park's
  attractions dropped from the feed read "closed" for ten weeks, in August).
  [Status regimes](../architecture/attraction-status/status-regimes.md).
- **An observation is read through `observedReadingsSql()`** (`closure-gap.sql.ts`), never a
  hand-rolled predicate: reconciliation and heartbeat rows are our bookkeeping, not readings. Three
  readers differ on purpose: `truthRowsSql` (forecast scoring drops heartbeats only), `isSourceAbsent`
  (reads `dataSource` alone, PAR-162) and the children sync's `withoutRecentReadings`, which counts a
  row with a NULL `is_heartbeat` as a reading — the cautious side for a job that retires rows.
  [Status regimes](../architecture/attraction-status/status-regimes.md#is-a-closure-ours-or-the-operators).
- **`season_months` is not derived from less than a year of watching** (`MIN_OBSERVED_DAYS` = 330).
  Below that the months are our recording window, not a season — every stored list was one.
  [Seasonality](../architecture/attraction-status/seasonality.md).
- **A free-flow attraction (`open_with_park`) is not seasonal** because its feed never says OPERATING;
  that is a playground's normal state.
- **A park whose whole feed has been silent for `PARK_FEED_SILENT_DAYS` (30) announces no future
  operating days.** `saveScheduleData` stores them as `UNKNOWN` without hours, and its rides read
  `UNKNOWN` (La Ronde: silent 89 days, schedule claiming 344 OPERATING days including a Montréal
  January). [Schedule sync](../architecture/schedule-sync-and-calendar.md#a-silent-park-does-not-announce-future-operating-days).
- **A day the schedule source withdraws stops being an operating day**, but only inside the months
  the fetch reports as answered — an empty or throttled month says nothing about its days.
  [Schedule sync](../architecture/schedule-sync-and-calendar.md#a-day-the-source-withdraws-stops-being-an-operating-day).
- **A school break nobody has published yet is not "no school holiday".** Past a region's last
  stored school row the flag is unknown; nothing is projected to fill it.
  [School-holiday coverage](../architecture/school-holiday-coverage.md).
- **Two writers, never one cell.** A correction sits in a `curated_*` column beside the synced one,
  never in it, because the sync overwrites its own cell on every run. Read through
  `resolveCuratedFacts` / `resolveCuratedPark`. [Curation](../admin/curation.md).
- **A stored `false` that means "nobody said" may not clear a `true`.** `has_fast_pass = false` is
  served like `null`; a merge never lets a loser's default `open_with_park = false` unset a winner's
  `true`. [Curation](../admin/curation.md#merges).
- **A Swagger `enum:` for an attraction status must come from `ATTRACTION_STATUS_VALUES`**
  (`src/common/types/status.type.ts`); a hand-written list is how `UNKNOWN` stayed out of the
  published contract for months. No DTO reads it yet: `status` and `effectiveStatus` on the park
  payload and the attraction detail declare no enum, and three lists are still hand-written without
  `UNKNOWN` — `nearby-response.dto.ts` (also without `REFURBISHMENT`), `search-result.dto.ts` and
  `queue-data-item.dto.ts`. Crowd levels follow the same rule: [Unified crowd levels](crowd-levels.md).
- **Research, never recall.** Facts about real rides and parks (heights, wet flags, free-flow,
  seasons, school calendars) come from the operator's or publisher's own pages. A name pattern is not
  evidence.
- **Identity is the `externalId`, never the slug.** A slug is a frozen name that renames deliberately
  leave alone. [Identity](../architecture/attraction-status/identity.md),
  [Automated retirement and upstream identity](automated-retirement-and-identity.md).
- **A behavioural detector cannot remember.** Duplicate and retirement detection describe the feed,
  so a cleared candidate returns tomorrow unless the verdict is written to
  `attraction_review_marks`, with a `recheck_after` whenever the answer can change.
  [Review marks](../architecture/attraction-status/review-marks.md).
