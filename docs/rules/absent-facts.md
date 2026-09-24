# An Absent Fact Never Becomes a Confident One

Moved out of the repo's `claude.md` so that a session reads an index first and opens only the pages its task needs. The wording is unchanged.

- **Detailed Guide**: [Attraction Status & Seasonality](docs/architecture/attraction-status-and-seasonality.md)
- The `unknown` crowd-level rule (§3) is the same rule as these, and they have
  all been broken the same way — **our own bookkeeping served as somebody
  else's statement**:
  - A ride **no source reports** reads `UNKNOWN`, never `CLOSED`. Reverse-
    reconciliation's row records that our data stopped arriving, not that the
    operator shut the ride. ~140 attractions across ten parks read "closed" for
    weeks this way.
  - **`season_months` may not be derived from less than a year of watching**
    (`MIN_OBSERVED_DAYS` = 330). Below that the months are the observation
    window, not a season — every list in the database was one.
  - A **free-flow** attraction (`open_with_park`) is not seasonal just because
    its feed never says OPERATING; that is a playground's normal state.
  - A park whose **whole feed has been silent** for `PARK_FEED_SILENT_DAYS` does
    not get to announce future operating days: `saveScheduleData` stores them as
    `UNKNOWN`, without hours. The schedule and the live data come from the same
    upstream, and it keeps publishing after it stops measuring — La Ronde was
    silent for 89 days while its schedule claimed all 344 remaining calendar
    days as OPERATING, January in Montréal included. Past days and `CLOSED` days
    the source delivered are left alone. Gap-filled `CLOSED` days are not:
    they rest on the park's last OPERATING day, the guard moves that day into
    the past, and `fillScheduleGaps` then demotes every future gap-filled
    `CLOSED` day to `UNKNOWN`. Full rule and the production numbers:
    [Schedule Sync & Calendar](../architecture/schedule-sync-and-calendar.md).
- **Two writers, never one cell.** `curated_may_get_wet`, `curated_minimum_height`
  and `curated_stats` sit _beside_ the synced column, never in it, because the
  sync overwrites its own cell on every run. Read via `resolveCuratedFacts`.
- **`AttractionStatus` includes `UNKNOWN`** and every Swagger `enum:` for it
  comes from `ATTRACTION_STATUS_VALUES` — hand-written lists are how `unknown`
  stayed out of the published contract for months.
- **Research, never recall.** Facts about real rides and parks (heights, wet
  flags, whether something is free-flow, seasons) come from the operator's own
  pages. A name pattern is not evidence.
- **Identity is the `externalId`, never the slug.** A slug is a frozen name that
  renames deliberately do not move, so 281 rows carry a slug that no longer
  matches — that is the system working. Resolve a disputed name against the
  upstream entity, not against what its slug implies.
- **A behavioural detector cannot remember.** Duplicate and retirement detection
  describe the feed, so a cleared candidate returns tomorrow unless the verdict
  is written to `attraction_review_marks`. Give it a `recheck_after` whenever the
  answer can change.

---
