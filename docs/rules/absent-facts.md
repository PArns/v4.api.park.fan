# An absent fact never becomes a confident one

Our own bookkeeping is never somebody else's statement. Where no source says something, the API
says "unknown" — it does not fill the gap with the most likely answer. Open this page before
writing any default, fallback or derived value that a reader will take as a fact. Every case
below shipped once the other way.

## The rule, case by case

- **A ride no source reports reads `UNKNOWN`, never `CLOSED`.** Reverse reconciliation's CLOSED row
  records that our data stopped arriving, not that the operator shut the ride (~140 attractions in
  ten parks read "closed" for weeks).
  [Status regimes](../architecture/attraction-status/status-regimes.md).
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
- **Every Swagger `enum:` for `AttractionStatus` comes from `ATTRACTION_STATUS_VALUES`**
  (`src/common/types/status.type.ts`); a hand-written list is how `UNKNOWN` stayed out of the
  published contract for months. Crowd levels follow the same rule:
  [Unified crowd levels](crowd-levels.md).
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
