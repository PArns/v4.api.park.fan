# Ride downtime: the reconstruction

What the nightly downtime job builds: the outage interval, its exposure denominator, the tables, the
job's deadlines and the traps found running it. Open it before touching
`src/analytics/utils/outage-reconstruction.sql.ts`, `DowntimeReconstructionProcessor` or the downtime
tables. Index: [README](README.md).

## The outage interval

**An outage is a maximal interval in which a ride's STANDBY state is `DOWN`, which intersects at
least one park `OPERATING` window, and whose in-window share is at least five operating minutes
(`MIN_OUTAGE_OPERATING_MINUTES`).** It is an interval, never a day: a ride down from Monday 16:00 to
Wednesday 12:00 is one row with `operatingDays = 3`.

Two runs are stitched into one outage when the second starts within 20 hours of the first
(`OUTAGE_STITCH_HOURS`) and no `OPERATING` reading lies between them. That is what keeps a multi-day
outage one row.

### What does not count

| Excluded                                                       | How                                                                                                                                                                                                 |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CLOSED` while the park is open                                | Neither numerator nor denominator. This class killed ML's `unexpected_closure` (534 of 602 anomalies were genuine closures). In blind parks it feeds the separate [closure-gap signal](closure-gap-signal.md). |
| `REFURBISHMENT`                                                | Planned work. A `DOWN` run ending in it is an _observed_ end (`endReason = 'reclassified'`).                                                                                                       |
| A weekly closure (Wakobato at Phantasialand, closed Sundays)   | No rule needed: the ride reports `CLOSED`, and the denominator is the ride's own operating minutes, so the Sunday adds 0 to both sides.                                                             |
| A ride out of season                                           | No predicate. `attractionIsOutOfSeason()` reads `NOW()` and would erase last July from a ride that left season in September. An out-of-season ride has no operating minutes anyway.                |
| `data_source = 'system-reconciliation'` rows                   | Become a pseudo-status `ABSENT` that **ends** the run (`source_absent`). Filtering them out would turn a ride gone from every feed for twelve days into a twelve-day outage.                       |
| Carried heartbeats past the fourth in a row                    | `MAX_CARRIED_HEARTBEATS` = 4. A heartbeat confirms a state, it does not prove it indefinitely; from the fifth the run ends `unconfirmed`.                                                          |
| Anything outside a park `OPERATING` window                     | `EXTRA_HOURS`, `TICKETED_EVENT`, `PRIVATE_EVENT` are out on both sides.                                                                                                                            |
| A run under five operating minutes                             | `queue_data.timestamp` is our write time, shared per park per poll, so each edge carries ~U(0,5) minutes of error.                                                                                 |
| A run over seven wall-clock days                               | Flagged `likelyWorksPeriod` (`MAX_OUTAGE_DAYS`). Tokyo Disneyland's Dumbo carried one unchanged `CLOSED` row for five months.                                                                     |
| Retired rides, free-flow rides (`open_with_park`), rides whose `last_merged_at` falls in the window | A merge reparents `queue_data` with no `conflictColumns`, so two interleaved series flap on top of each other.                                     |
| Parks without `parks.wiki_entity_id`                           | No source there can emit `DOWN` (Queue-Times maps `is_open` onto `OPERATING`/`CLOSED`; Wartezeiten maps `closed*` onto `CLOSED`).                                                                  |
| A curated works period                                         | `attractionIsCuratedOutOfService()`, asked about the interval's **operating** day (`start_op_day`), not the calendar date of `started_at`: 1,032 of 163,969 stored intervals differed (2026-09-16). |

## Exposure: the denominator

**Exposure is the ride's own `OPERATING` minutes inside the flattened (disjoint) park `OPERATING`
windows, and nothing else.** A ride cannot break while broken or while shut, so neither is at risk.

- **70-minute cap per row** (`MAX_SEGMENT_MINUTES`). An observed ride's rows sit at most ~65 minutes
  apart (5-minute sync, 60-minute rewrite rule, hourly heartbeat). A longer silence becomes
  `unobservedMinutes` and leaves the denominator: the error is always less exposure, never invented
  exposure.
- **Unobserved time inside an outage** above 10 minutes (`MAX_UNOBSERVED_MINUTES_IN_SPELL`) censors its
  duration.
- **The 21 schedule-less parks** get the live line and nothing else: their window is inferred from
  ride activity, so normalising ride downtime by it would be circular.
- **Invariant** per (ride, operating day): `operating + down + closed + refurbishment + absent +
  unobserved = parkOpenMinutes` (±1). Verified read-only over 14 days on 2026-09-06: 88,814 days,
  0 violations. The processor logs a warning when it breaks.
- **What exposure cannot tell:** "the poll ran and nothing changed" from "the poll died". `queue_data`
  is a change log, not a poll log; only a per-park poll-attempt row would fix that.

### End reasons and censoring

`recovered` and `reclassified` are observed ends. `ongoing` (still down), `window_edge`, `closed`,
`gap`, `unconfirmed` and `source_absent` are censored. A ride's duration figures are withheld when
more than a quarter of its outages are censored (`heavily_censored`). Left truncation is avoided by
starting the scan one day before the earliest open or in-window outage (`OUTAGE_SCAN_START_SQL`).

Counting in operating minutes is what keeps censoring low: a closed park is a pause, not an ending.
On raw `queue_data` runs 68 % end in something other than `OPERATING`; on the stitched,
operating-minute reconstruction over the same 180 days, 15.6 %.

## Tables

| Table                          | One row per                    | Notes                                                                                                                                         |
| ------------------------------ | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `attraction_outages`           | reconstructed interval         | The only place an outage exists. PK (`attractionId`, `startedAt`); `signal` is `down` or `closed_gap`.                                        |
| `attraction_exposure_days`     | ride × park-local operating day | Minutes only, never an event, so a multi-day outage cannot be rebuilt by summing it.                                      |
| `attraction_downtime_profiles` | ride                           | The published aggregate, rewritten nightly: `publishable`, `withheldReason`. Nothing serves `attraction_outages` directly.                    |
| `park_downtime_coverage`       | park                           | The regime, for every park ([publishing](publishing.md#park-regimes)).                                                                        |
| `downtime_recovery_curves`     | park or pooled × signal × bucket | The [recovery estimate](recovery-estimate.md).                                                                                              |

All are registered in `src/parks/utils/merge-dependencies.ts` and created by entity sync
([the entities are the schema](../../rules/the-entities-are-the-schema.md)). The job prunes
`attraction_outages` and `attraction_exposure_days` past 400 days (`RETENTION_DAYS`). The write-path
columns `queue_data.is_heartbeat` (true from `writeHourlyHeartbeats`, false on observed rows),
`queue_data.raw_status` (the status before `ConflictResolverService` overrode it) and
`attractions.last_merged_at` are **nullable with no default on purpose**: `NULL` means "written before
the column existed" and readers fall back to `"lastUpdated" = timestamp`. A `default: false` on
`is_heartbeat` would promote a year of carried rows to observations, and TimescaleDB refuses that
`ALTER` on compressed chunks. The columns were applied by `synchronize` at boot on 2026-09-07 without a
`lock_timeout`; it was harmless only because they are nullable with no default.

## The nightly job

- Queue `downtime`, job `reconstruct-downtime`, cron `0 5 * * *` UTC (after the 04:30 hourly-history
  rollup and the 04:45 operating-day verdicts), default three attempts.
- The window is 30 days (`DEFAULT_WINDOW_DAYS`), but the scan floor from `OUTAGE_SCAN_START_SQL` reaches
  back to an open `down` spell's start, capped at twice the window, so the nightly scan is **60 days**.
- By hand: `POST /v1/admin/rebuild-downtime?days=N` — a **trailing** window, clamped 1–400, one attempt.
  The parameter is `days`; `?windowDays=` is ignored and silently runs the 30-day default.
- Statements 1 (intervals) and 2 (exposure) run in parallel, then the closure-gap statement, the
  writes, the profile rebuild and the curve rebuild. The curve rebuild is in its own `try`: it adds to a
  reconstruction that already succeeded, so a failure there is logged, not retried.

### Deadlines (PAR-820)

From 2026-10-06 the job failed every night (`job stalled more than allowable limit`): the closure-gap
statement as PAR-157 rewrote it did not finish. The planner put `rows=1` on every CTE after
`blind_parks` and nested-looped the grouped joins (60-day scan: not done in 12 hours; with
`enable_nestloop = off`, 12.7 s). The `blind_parks` evidence check was also a correlated subquery that
seq-scanned `attraction_exposure_days` once per park; as one grouped pass the statement takes ~6 s.

Since then:

- Every statement runs in its own transaction under `SET LOCAL` deadlines from
  `src/common/utils/statement-limits.util.ts` (`reconstructionReadLimits`, `WRITE_LIMITS`): reads 10
  minutes, or 10 s per scanned day for a staged fill; writes 5 minutes; `lock_timeout` 30 s.
  `statement_timeout` protects the database (a running statement holds its snapshot and chunk locks,
  and a compression job's strong lock request queues every later reader behind them);
  `lock_timeout` only makes the job fail fast.
- The closure-gap statement runs with `enable_nestloop = off` (`CLOSURE_GAP_PLANNER_SETTINGS`). A timeout
  there costs one night of closure gaps; the `DOWN` reconstruction is kept.
- Postgres does not notice a dead client until it writes to it, so a statement from a killed run keeps
  running on the server. The deadlines are what end it.

### Cost

Measured 2026-09-06 with `EXPLAIN (ANALYZE, BUFFERS)`:

| Window   | intervals statement | exposure statement |
| -------- | ------------------: | -----------------: |
| 7 days   |              3.58 s |             2.21 s |
| 30 days  |             21.96 s |            14.07 s |
| 180 days |              ~7 min |                  — |

Not linear: 6× the window costs ~19× the time, and 30 days already spills ~700 MB of temp for one
statement. Both run under one `Promise.all`, so a long first fill must be **staged**.

### The first production fill (2026-09-11)

Each stage recomputes its whole trailing window and supersedes the previous one, so stopping between
stages leaves a consistent table.

| Window | Intervals | Closure gaps | Exposure days | Duration |
| ------ | --------: | -----------: | ------------: | -------: |
| 30 d   |    38,619 |        3,316 |       256,032 |    118 s |
| 60 d   |    57,792 |        3,330 |       359,865 |    207 s |
| 90 d   |    90,112 |        4,186 |       506,846 |    356 s |
| 120 d  |   114,639 |        4,584 |       633,245 |    558 s |
| 180 d  |   138,969 |        4,740 |       809,573 |  1,037 s |
| 270 d  |   154,150 |        5,000 |       936,141 |  1,713 s |

270 days reaches `queue_data`'s oldest chunk (2025-12-24). Host load stayed between 1.3 and 4.2 on 24
cores. `downtime_recovery_curves` went from 202 rows to 414.

**Closure gaps do not scale with the window.** The signal is confined to blind parks, and far fewer of
them were tracked in December (102 gaps) than in August (1,548). So a count cannot be normalised across
windows by dividing: compare a nightly run against earlier nightly runs at the same window. The 30-day
baseline is 3,316 gaps.

## Traps found running it

- **Two outages merged into one.** In `DOWN DOWN CLOSED OPERATING DOWN DOWN` the `OPERATING` row landed
  in a group with no run in it, the stitch read the seam as one interrupted outage, and Cedar Point's
  longest outage over seven days read 68 hours instead of 20 (intervals 378 instead of 400). The stitch
  now keeps a running count of `OPERATING` sightings per ride.
- **`repository.delete({})` throws in TypeORM** (empty criteria are rejected). The curve rebuild threw
  every night, Bull retried three times, nothing logged it, and `downtime_recovery_curves` stayed empty.
  Use a query builder for a full-table delete.
- **Slugs are not park identities.** `disneyland-park` is Anaheim and Paris; grouping on `parkSlug`
  merged them. Group on `parkId` and carry `parkCity` for humans.
- **`queue_data."attractionId"` is `uuid`.** Join `a.id = qd."attractionId"`, never `a.id::text`.
- **No cast on `queue_data.timestamp` in a `WHERE`.** `(qd.timestamp AT TIME ZONE tz)::date` hides the
  column from chunk exclusion: 254 chunks, 35,967 buffers, 1.1 s cold for nine rows.
- **A `DOWN`-hour count is not an outage count.** `HistoryDayDto.downCount` (deprecated) counts distinct
  clock hours with a `DOWN` reading over the whole park-local day, with no opening-hours bound: Universal
  Studios Singapore's Revenge of the Mummy read 16 on a 10-hour day. Use `outageCount` / `outageMinutes`,
  which come from the intervals; a multi-day outage increments only the day it began.

## Other downtime code

| Code                                         | Role                                                                                                                                                                                           |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `trackDowntime()` (`wait-times.processor.ts`) | Redis only (`downtime:*` keys), read by the ML feature `downtime_minutes_today`. Conflates `DOWN`/`CLOSED`/`REFURBISHMENT` and credits a spell only on recovery. Not fed from the reconstruction. |
| `downYesterday()` (`plan-day.service.ts`)    | A same-day warning for the planner, not a statistic.                                                                                                                                           |
| `getRideOpeningTimes()` (`analytics.service.ts`) | The template for the per-ride derivation, including its `HAVING count(*) >= 5` floor.                                                                                                     |

Fixture cases for the reconstruction (multi-day outage, window edge, ingestion gap, carried heartbeats,
a Wakobato Sunday, overlapping schedule rows, a past-midnight close, a second timezone) are in
`src/analytics/utils/outage-reconstruction.sql.spec.ts` and the E2E specs under `test/e2e/`
(`closure-standing`, `closure-gap-operating-day`, `outage-curated-window-operating-day`).
