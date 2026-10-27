# Attraction status: the regimes

Which status an attraction is served with, and who is allowed to say so. Open this when a ride reads
OPERATING, CLOSED or UNKNOWN and you need to know why, or before you add a status path.

`AttractionStatus` = `OPERATING | CLOSED | DOWN | REFURBISHMENT | UNKNOWN`, from
`ATTRACTION_STATUS_VALUES` in `src/common/types/status.type.ts`; every Swagger `enum:` reads that
list ([absent-facts rule](../../rules/absent-facts.md)).

## 1. A real feed reported it

The normal path: the status comes off the latest `queue_data` row. Which upstream feeds there are
and how they are merged: [Data ingestion](../data-ingestion.md).

## 2. Free-flow: the flag reports it

Playgrounds, splash pads and climbing structures have no queue, so the feed calls them CLOSED all
day. `attractions.open_with_park` overrules that through one function,
`isFreeFlowOpen({ openWithPark, parkStatus, waitTimesReadable, seasonMonths, parkTimezone })` in
`src/common/utils/free-flow-status.util.ts`, called by `park-integration`, `attraction-integration`
and `favorites` (the rule once existed three times and the park's ride list lacked it). It is true
only when all hold:

| condition | why |
| --- | --- |
| the flag is set | it is what marks a free-flow attraction |
| the park is `OPERATING` | a playground in a closed park is behind a gate |
| wait times are readable | nothing below an unreadable park (Hansa-Park) may claim to run |
| today, park-local, is in `season_months` (null or empty = all year) | seasonal free-flow areas can carry the flag ([seasonality](seasonality.md)) |

Free-flow outranks the source-absence rule below: the flag, not the feed, makes it open. In
`favorites` that ordering needs an explicit guard, because the absence check reads the raw rows.

## 3. No source reports it → `UNKNOWN`

[Reverse reconciliation](../reverse-reconciliation.md) writes a CLOSED `queue_data` row
(`data_source = 'system-reconciliation'`) for any attraction no upstream has mentioned in 24 h. The
row is ours, not the operator's, so `src/common/utils/source-absent-status.util.ts` serves a ride
whose current rows were **all** written by `system-reconciliation`, in an operating park, as
`UNKNOWN` with its `queues` emptied. *All* matters: a ride still publishing a real STANDBY queue is
being observed, whatever sits beside it.

The row is no longer seasonal evidence (`detect-seasonal` reads observed rows only, PAR-32). It
still ends a run in the outage reconstruction (`outage-rows.sql.ts`) instead of joining two `DOWN`
states across the silence.

## 4. No row inside the window → the optimistic fallback

The park's ride list has one branch that invents an answer: an open park whose ride has no current
row at all serves it as `OPERATING` (`statusWithoutLiveData`,
`src/common/utils/no-live-data-status.util.ts`), so a feed going quiet mid-day does not produce
"park open, every ride closed". A ride the season rules out (`isCurrentlyInSeason === false`) never
takes it, and neither does a ride of a silent park (§5).

**"No current row" is a query decision.** `QueueDataService.findCurrentStatusByPark` reads the latest
row per attraction since a cutoff. A `queue_data` row is written only on change plus an hourly
heartbeat, so a ride shut for months carries a reading from before today's opening. Cutting at the
opening time threw it away: Phantasialand (opens 09:00, polled about hourly) served all 40 rides as
running until the 09:23 poll, among them two ice rinks the wiki had called CLOSED since April. The
cutoff is therefore the **earlier** of today's opening and the `maxAgeMinutes` fallback (6 h): the
last word the feed said is the answer; the fallback is for silence, not for "unchanged".

**Still open (PAR-1100):** a ride no source has *ever* reported, in a park whose feed works, still
reads OPERATING through this branch (Disneyland's *Main Street Pumpkin Festival*). A park whose whole
feed is silent no longer reaches it (§5).

## 5. A whole park gone silent

The fallback above is written for one ride going quiet at a park whose feed works. A park where
nothing arrives fell into it for every ride: La Ronde (silent since 2026-06-24, both upstreams
answering empty) served 38 rides as OPERATING at `very_low` while each ride's own page said CLOSED.
On 2026-09-16 nine parks were over the line, five of them also scheduled open in the next week (110
attractions).

**The gate.** `ParkIntegrationService` reads
`waitTimesKnowable = waitTimesReadable && parkObservedRecently`, the second half being
`QueueDataService.hasObservedReadingWithin(parkId, PARK_FEED_SILENT_DAYS)` — a bounded `EXISTS` over
`observedReadingsSql()`, so our own reconciliation and heartbeat rows cannot clear a park. Ride
status, `crowdLevel`, best visit times and the park's wait statistics read it; a silent park's rides
land on `UNKNOWN`. Free-flow rides still open on their flag (only `waitTimesReadable` switches that
off).

**Why 30 days** (`PARK_FEED_SILENT_DAYS`, `no-live-data-status.util.ts`): days since the last
observed reading per park, 2026-09-16 — 195 parks at 0–1, **0** at 2–30, 2 at 31–90, 3 over 90,
4 never. The threshold sits in the empty band. The same constant gates the schedule
([silent-park guard](../schedule-sync-and-calendar.md#a-silent-park-does-not-announce-future-operating-days)).

What the gate does to the payload:

- `closedAttractions` reads 0 at an open, silent park — "38 of 38 closed" under an OPERATING badge
  was the same false page from the other side. `total = operating + closed` therefore does not hold
  there; the DTO descriptions say the remainder is rides nobody can speak for. In a CLOSED park the
  count stands.
- `liveWaitTimes.available` stays `true`: it means "this park publishes wait times somewhere"
  (`live-wait-time-sources.ts`), not "we have data".
- `/location` and the favourites park card inherit it on a `park:integrated` cache hit; their own
  miss paths (built from `AnalyticsService`) do not.
- **Not gated** (each needs its own park-level probe): the ride's own endpoint
  (`AttractionIntegrationService`, still CLOSED with an hourly forecast), the park list
  (`ParkEnrichmentService`), and the ride-alert subscription check. `avgWaitTime`, `avgWaitToday`,
  `peakWaitToday` and `occupancy` keep their empty-set zeros and
  `comparisonStatus: "typical"` (PAR-298).

Cost, measured 2026-09-16: the probe takes 1.8 ms (Europa-Park) to 7.0 ms (La Ronde) behind 45 ms of
hypertable planning warm, 338 ms cold once per pooled connection. Only an `OPERATING` park's answer
changes, and its payload TTL runs to the next five-minute boundary, so it is not cached longer.

**Reporting.** `DataQualityMonitorService.findScheduledButSilentParks()` lists silent parks with an
operating day in the next `SILENT_PARK_LOOKAHEAD_DAYS` (7) nightly; the window keeps a park shut for
the winter out of the log. It complements `findSilencedClusters` ([feed drops](feed-drops.md)), whose
`park_health` gate needs three rides OPERATING in two days and so can never see a fully silent park.
Each detector catches its own throw and the job counts how many ran, so three failures do not print
as "nothing wrong".

## Is a closure ours or the operator's?

```sql
SELECT data_source, is_heartbeat, status, count(*), max(timestamp)::date
  FROM queue_data
 WHERE "attractionId" = '<id>' AND timestamp > now() - interval '3 days'
 GROUP BY 1,2,3;
```

`system-reconciliation` means no upstream reported the ride. Two look-alikes: an entity the wiki
recategorised to `SHOW` reports into `show_live_data` against a different row while its stale
`attractions` row sees only reconciliation ([reclassification](reclassification-and-absence.md)); and
a heartbeat carries the previous row's `data_source` forward, so a real-looking source is not by
itself a live reading — that is what `observedReadingsSql()` (`closure-gap.sql.ts`) excludes. Never
hand-roll that predicate: a version that tested only `system-reconciliation` and `is_heartbeat` missed
`system-heartbeat` and read a NULL `is_heartbeat` as a reading.
