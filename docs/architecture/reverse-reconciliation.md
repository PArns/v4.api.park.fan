# Reverse Reconciliation for Stale Attractions

## Problem

Our upstream data providers (ThemeParks.wiki, Queue-Times.com, Wartezeiten.app) sometimes **stop reporting an attraction entirely** rather than sending it with `status=CLOSED`. Typical cases:

- **Seasonal mazes** that only appear during Halloween Horror Fest or Christmas (e.g. Movie Park Germany's *A Quiet Place*, *Traumatica* mazes).
- **Refurbished rides** that are silently dropped from the feed instead of flagged.
- **Renamed/removed attractions** where the external ID vanishes.

The wait-times sync loop (`WaitTimesProcessor.handleSyncWaitTimes`) only updates entities that appear in the current feed. If an attraction disappears, its last persisted `queue_data` row remains untouched, and the hourly heartbeat keeps re-stamping the same `status=OPERATING`, `waitTime=0` indefinitely. The API therefore reports the ride as "open with 0 min wait" long after it has actually closed.

## Mechanism

The orchestrator-layer processor now maintains a **last-seen index** per attraction and performs a reverse-reconciliation step at the end of every 5-minute sync cycle.

### 1. Last-seen tracking (real sources only)

File: `src/queues/processors/wait-times.processor.ts`

```
Key:   attraction:last-seen:{attractionId}
Value: Date.now() (ms, as string)
TTL:   14 days
```

The key is touched **only** when an attraction is encountered in a real upstream feed during entity processing. The hourly heartbeat (`writeHourlyHeartbeats`) deliberately does **not** update it — otherwise the heartbeat would mask the staleness it is supposed to expose.

### 2. Reconciliation step (per park, per cycle)

After the entity loop finishes for a park, the processor:

1. Builds `seenAttractionIds: Set<string>` from the current cycle's mappings.
2. Diffs it against the full set of attractions attached to the park.
3. For each missing attraction:
   - **Grace period**: skip if `createdAt < 24h ago` (never closes an attraction before its very first successful sync has populated last-seen).
   - **Recent sighting**: skip if Redis `last-seen` < 24h old (transient feed gap).
   - **Stale → close**: call `QueueDataService.saveLiveData(id, { status: CLOSED })`. The service's `shouldSaveQueueData` delta-strategy deduplicates (no spam writes on subsequent cycles once already `CLOSED`).

**Safety guard**: reconciliation only runs when `seenAttractionIds.size > 0`. A park whose live fetch failed completely (all sources down) produces an empty set and the step is skipped — so a provider outage never mass-closes a park's attractions.

### 3. Heartbeat fix

`writeHourlyHeartbeats` previously preserved `last.status` for any attraction silent for >1h, which meant stale `OPERATING` status survived forever. It now reads the same `attraction:last-seen:{id}` key and **skips heartbeat writes** when the attraction has been missing from all sources for >24h. The reconciliation step has already written `CLOSED`, so nothing needs backfilling.

### 4. Seasonal detection pickup

`QueuePercentileProcessor.handleDetectSeasonal` (scheduled daily at 2:30 am) looks for attractions whose current status is `CLOSED` on days when the park was demonstrably open. Once reconciliation flips a disappeared attraction to `CLOSED`, the next run of `detect-seasonal` will automatically:

- Flag `attraction.isSeasonal = true`.
- Derive `seasonMonths` from historical `OPERATING` months.
- Expose `isCurrentlyInSeason` on the API response.

To re-run seasonal detection immediately after a deployment (without waiting for 2:30 am), an admin trigger is available:

```
POST /admin/detect-seasonal
```

(Queues `detect-seasonal` on the `analytics` Bull queue.)

## Constants

| Setting                 | Value                           | Where |
|-------------------------|---------------------------------|-------|
| Stale threshold         | `24h`                           | `WaitTimesProcessor.STALE_THRESHOLD_MS` |
| Redis TTL for last-seen | `14 days`                       | `WaitTimesProcessor.LAST_SEEN_TTL_SECONDS` |
| Grace period (new rides)| `24h` from `attraction.createdAt` | `reconcileMissingAttractions` |
| Sync cadence            | every 5 min                     | `QueueSchedulerService` (`wait-times-cron`) |
| Seasonal detection      | daily 02:30                     | `QueueSchedulerService` (`seasonal-detection-cron`) |

## Observability

- The processor logs `🗑️  {parkName}: closed N stale attraction(s) (not seen in any source for >24h)` per park when the step fires.
- `QueueDataService.saveLiveData` deduplicates via its delta strategy — a stable stale attraction produces exactly one `CLOSED` write, not one per cycle.

## Related Files

- `src/queues/processors/wait-times.processor.ts` — `touchAttractionLastSeen`, `reconcileMissingAttractions`, patched `writeHourlyHeartbeats`.
- `src/queue-data/queue-data.service.ts` — `saveLiveData` status-only branch (lines 92–134).
- `src/queues/processors/queue-percentile.processor.ts` — `handleDetectSeasonal` consumer of `status=CLOSED`.
- `src/admin/admin.controller.ts` — `POST /admin/detect-seasonal` manual trigger.
