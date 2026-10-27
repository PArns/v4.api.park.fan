# Reverse reconciliation: closing attractions no source reports

Upstream feeds (ThemeParks.wiki, Queue-Times, wartezeiten.app) sometimes stop reporting an attraction
instead of sending it as CLOSED — a seasonal maze after its event, a ride dropped during a refurb, an
id that vanished. The wait-times sync only writes what appears in a feed, so without a counter-signal
the last `OPERATING` row and the hourly heartbeat would keep the ride "open, 0 min" for ever. Open this
when a ride shows `data_source = 'system-reconciliation'` rows, or before changing the heartbeat. What
the API *serves* for such a ride (`UNKNOWN`, not CLOSED) is in
[status regimes](attraction-status/status-regimes.md).

## Mechanism (`src/queues/processors/wait-times.processor.ts`)

**Last seen.** `touchAttractionLastSeen` sets `attraction:last-seen:{attractionId}` (epoch ms,
`LAST_SEEN_TTL_SECONDS` = 14 days) whenever an attraction appears in a real feed. The heartbeat never
touches it, or it would hide the staleness it exists to expose.

**Per park, per 5-minute cycle,** after the entity loop, `reconcileMissingAttractions` diffs the
cycle's `seenAttractionIds` against the park's attractions and, for each missing one:

- skips it if `createdAt` is under 24 h old (grace before its first successful sync);
- skips it if its last-seen key is under `STALE_THRESHOLD_MS` (24 h) old (a transient gap);
- otherwise writes a CLOSED row through `QueueDataService.saveLiveDataBatch` with
  `source: "system-reconciliation"`. The delta check (`isSignificantChange`) deduplicates, so a stable
  stale ride gets one CLOSED row, not one per cycle.

**Safety guards.** The step runs only when `seenAttractionIds.size > 0`: a park whose fetch failed
entirely produces an empty set and is skipped, so a provider outage never mass-closes a park. An
unreadable last-seen lookup (Redis error) also closes nothing. The processor logs
`🗑️ {park}: closed N stale attraction(s) (not seen in any source for >24h)`.

## The heartbeat (`writeHourlyHeartbeats`)

The hourly heartbeat re-stamps a quiet ride's last status so readers have a row inside their window.
It reads the same last-seen key and:

- writes **no** heartbeat for a ride missing from every source for over 24 h — reconciliation has
  already written CLOSED;
- writes **no** heartbeat for a ride a feed delivered in the last `FEED_WILL_WRITE_MS` (10 min, two
  sync ticks), because the next sync writes its hourly row itself as an observation through the
  60-minute rule in `isSignificantChange` (PAR-720). Before, the heartbeat — running right after the
  sync in the same job — won the race, and 76 % of the heartbeats 3–6 h after an observation were
  followed by an observation with the same wait (2026-10-01). A source not fetched every tick
  (wartezeiten-app rotates) keeps its heartbeat, so its rows stay inside the downtime
  reconstruction's 70 minutes.

## Who reads these rows

- **The read path** turns a ride whose current rows are all reconciliation rows into `UNKNOWN`
  ([status regimes](attraction-status/status-regimes.md#3-no-source-reports-it--unknown)).
- **The outage reconstruction** (`outage-rows.sql.ts`) uses the row to **end** a run.
- **Not `detect-seasonal`.** Its evidence queries read observed rows only (`observedReadingsSql()`,
  PAR-32), so a ride in this state has no current status there and is neither flagged nor given months
  ([seasonality](attraction-status/seasonality.md#what-the-detector-skips)). The code comment on
  `reconcileMissingAttractions` still names the detector as the reason for the write; it is stale.

| Setting | Value | Where |
| --- | --- | --- |
| Stale threshold / grace period | 24 h | `STALE_THRESHOLD_MS` |
| last-seen TTL | 14 days | `LAST_SEEN_TTL_SECONDS` |
| Heartbeat skip after a fresh reading | 10 min | `FEED_WILL_WRITE_MS` |
| Sync cadence | every 5 min | `wait-times-cron` |
