# Data ingestion: live wait times from several sources

Where live wait times and statuses come from, how several sources for one park are merged, and how
waits are normalised before they reach `queue_data`. Open this before adding a source or changing how
a reading is stored. Opening hours: [Schedule sync](schedule-sync-and-calendar.md); what the API
serves when a source stops reporting: [Reverse reconciliation](reverse-reconciliation.md) and
[status regimes](attraction-status/status-regimes.md).

## Sources

| Source | Client | Role |
| --- | --- | --- |
| ThemeParks.wiki | `src/external-apis/themeparks` | Base source: live data, `/children` metadata, schedules. Rate limits: [schedule sync](schedule-sync-and-calendar.md#the-upstream-budget-and-why-the-bulk-run-is-the-only-job-that-spends-it) |
| Queue-Times.com | `src/external-apis/queue-times` | Live waits and lands; the only source for some parks |
| wartezeiten.app | `src/external-apis/wartezeiten` | Live waits for German/European parks; opening times (`wartezeiten-schedule`). Parks created from it alone are whitelisted in `data-sources/config/wartezeiten-only-parks.ts` |

Not live data: `six-flags` (ride heights), `wikidata` (ride stats), `nager-date`, `open-holidays`,
`korea` (holidays, [holiday sources](holiday-sources-outside-europe.md)), `weather` (Open-Meteo,
[weather](weather.md)), `geocoding`.

## One park, several sources

`WaitTimesProcessor` (queue `wait-times`, every 5 min) asks `MultiSourceOrchestrator.fetchParkLiveData`
(`src/external-apis/data-sources/`) for every source the park has an external id for, in parallel;
`ConflictResolverService.aggregateParkData` merges them:

- **The wiki is the anchor.** Its entities are added first; Queue-Times and wartezeiten entities are
  matched to them by `normalizeForMatching` name, exact first, then a string similarity above 0.9 for
  names longer than five characters. Unmatched entities are added. Lands come from Queue-Times only;
  operating hours from the wiki, falling back to wartezeiten.
- **Wait time consensus:** with three values, two that agree win, else the median; with two, the
  mean; then `roundToNearest5Minutes`.
- **Status:** a wiki CLOSED or DOWN is overruled when another source reports OPERATING with a wait of
  at least 5 min (the wiki can lag on "closed").

Park-to-park matching across sources (discovery, `EntityMatcherService`) uses fuzzy names plus
geography; forced aliases for renamed parks live in `data-sources/config/manual-park-matches.ts`.
Attraction-level mappings are written by `EntityMappingsProcessor` into `external_entity_mapping`
(and `attractions.queue_times_entity_id`); `WaitTimesProcessor` resolves live entities through them
as `<source>:<externalId>`.

### Name matching keeps a trailing country code

`normalizeForMatching` (`src/common/utils/slug.util.ts`) does not strip a suffix like ` (DK)`. In the
data it marks the language of a show, not a duplicate: on 2026-10-13 exactly 3 rows carried one, all
LEGOLAND Billund shows (`V.I.M. Dance Party (DK)` / `(UK)`). Merging two performances costs more than
a missed merge, and the fuzzy branch still catches a real cross-source duplicate. The same applies to
attractions.

## Wait times are stored in five-minute steps

Every STANDBY, SINGLE_RIDER and PAID_STANDBY wait is rounded with `roundToNearest5Minutes` in
`QueueDataService.buildQueueCandidates`, the one place every source's row passes through. The
exception is 13: Disney posts it for a walk-on, so it is kept exactly (`WALK_ON_WAIT_MINUTES`). The
park, attraction and favorites payloads serve `queue_data.waitTime` as stored, so a value off the
grid in the table is off the grid for the user.

The rounding in `ConflictResolverService` does not cover this: it writes the merged entity's
`waitTime`, while `WaitTimesProcessor.adaptEntityLiveData` stores the source's `queue` object whenever
one is present (always for the wiki, and for wartezeiten when a ride has a wait). Feeds do send
off-grid values (Movie Park 61 on 2026-10-04, Europa-Park 1, 3 and 6 on 2026-10-05, PAR-715). With
several sources, the stored wait is therefore the queue object's figure, not the consensus.

A row is written only when the value changes, plus an hourly heartbeat (`isSignificantChange`,
[reverse reconciliation](reverse-reconciliation.md#the-heartbeat-writehourlyheartbeats)).
