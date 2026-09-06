# Ride downtime: what we can measure, and what we may publish

> Status, 2026-09-06: **phases 0 and 2 shipped, plus the curated works period. Nothing historical
> is published.** Every threshold below is still provisional — phase 0 exists now but has not been
> run against production, so the event floor of 24 has not yet been re-derived from counted events.
>
> | Phase | State |
> | --- | --- |
> | 0. Count events, publish nothing | **built** — `GET /v1/admin/downtime-measurement`, read-only. Not yet run. |
> | 1. Write-path columns (`is_heartbeat`, `raw_status`, `last_merged_at`) | not started. Blocks every duration figure and the erasure measurement. |
> | 2. The live line | **shipped** — `outage` on the attraction payload, one line under the status badge in the frontend. |
> | 3. Reconstruct, publish nothing | not started. |
> | 4. Publish the four measured numbers | not started. |
> | 5. Measure the erasure | not started, needs phase 1's 30-day clock. |
>
> Two open decisions from §10 are settled: the live line shipped on its own rather than waiting for
> a joint release, and the curated "out of service from/to" field was built (§4). The other two
> stand.

The question this answers: *how often and for how long are rides down, and can we say when the next
outage is coming and how long it will last?*

Three questions, three different answers.

| Question | Answer | Why |
| --- | --- | --- |
| How often | **Yes**, as a counted number of *reported* outages with its window. Not as a rate. | The count needs no estimator and no interval. A rate needs a stationary numerator, and ours is not. |
| How long | **Yes**, gated, as the empirical median and the longest of the observed outages. | Reconstructible from the transition pair, once our own heartbeat is prevented from inventing duration. |
| When next, for how long | **No.** Not as a number, not as a sentence, not as a softer rewording. | Four independent reasons, each sufficient. See [§6](#6-why-there-is-no-probability). |

---

## 1. The signal

**An outage is a maximal interval in which a ride's reconstructed STANDBY state is `DOWN`, which
intersects at least one park `OPERATING` window, and whose in-window share is at least five minutes.**

Nothing else. In particular an outage is an *interval*, never a day: a ride that stands still from
Monday 16:00 to Wednesday 12:00 is **one** row with `operatingDays = 3`, not three rows.

### What does not count, and how each case is detected

| Excluded | Detection |
| --- | --- |
| `CLOSED` while the park is open | The enum value. This is the class that killed ML's `unexpected_closure` (534 of 602 anomalies were genuine closures). Neither numerator nor denominator. |
| `REFURBISHMENT` | The enum value. Planned work. A `DOWN` run ending in `REFURBISHMENT` is an *observed* end (`endReason = 'reclassified'`), not a lost sight. |
| A recurring weekly closure (Wakobato at Phantasialand, closed Sundays) | **No rule at all.** On a Sunday the ride reports `CLOSED`, never `DOWN`, and the denominator is the ride's *own* operating minutes, so the Sunday contributes 0 to both sides. |
| A ride out of season | No predicate. `attractionIsOutOfSeason()` computes against `EXTRACT(MONTH FROM NOW())` (`season-window.sql.ts:43`) and would retroactively delete July from a ride that leaves season in September. An out-of-season ride contributes no operating minutes anyway. |
| `data_source = 'system-reconciliation'` rows | Mapped to a pseudo-status `ABSENT` that **terminates** the run. Filtering them out instead would turn a ride that vanishes from every feed for twelve days into a twelve-day outage. |
| Heartbeat rows past the fourth in a row | `is_heartbeat` (new column). A heartbeat confirms a state, it does not prove it indefinitely. From the fifth carried row the run ends with `endReason = 'unconfirmed'` and is right-censored. |
| Anything outside a park `OPERATING` window | `EXTRA_HOURS`, `TICKETED_EVENT`, `PRIVATE_EVENT` excluded on both sides. |
| A run under five minutes in-window | `queue_data.timestamp` is our write decision, shared per park per poll, so each edge carries ~U(0,5) minutes of error. |
| A run over seven wall-clock days | `likelyWorksPeriod`. Evidence: Tokyo Disneyland's Dumbo carries one unchanged `CLOSED` row since 16 April, five months from a single row. |
| Retired rides, free-flow rides (`open_with_park`), rides whose `last_merged_at` falls in the window | The merge reparents `queue_data` with `UPDATE ... SET attractionId = winner` and **no** `conflictColumns` (`merge-dependencies.ts`), so two interleaved series sit on top of each other and flap. |
| Every park without `parks.wiki_entity_id` | No source there can emit `DOWN`. The absence of a report is not a statement about the ride. |

---

## 2. Why coverage, not statistics, is the binding constraint

`DOWN` is produced by exactly one mapper: `themeparks-data-source.ts:161-176`. Queue-Times is
`is_open ? OPERATING : CLOSED`. Wartezeiten maps `maintenance` to `REFURBISHMENT` and
`closed`/`closedice`/`closedweather` all to `CLOSED`, so the weather and ice distinction is destroyed
at ingest and unrecoverable.

On top of that, `ConflictResolverService:277-298` silently rewrites `DOWN` to `OPERATING` whenever
Queue-Times or Wartezeiten reports `waitTime >= 5` and `OPERATING`. That fires on dual-sourced parks,
and within them preferentially on the popular rides with long queues, because a queue does not drain
in the five minutes after a ride stops.

Two independent live measurements, 2026-09-06:

| Sample | Rows | OPERATING | CLOSED | REFURB | **DOWN** |
| --- | ---: | ---: | ---: | ---: | ---: |
| All 65 currently-open parks | 2064 | 1667 | 360 | 31 | **6** (4 parks) |
| 15 currently-open parks, 603 attractions | 603 | 358 | 135 | 9 | **4** (3 parks) |

`DOWN` is 0.3-0.7 % of live rows and appears in a minority of parks. `CLOSED` while the park is open
is roughly thirty times more common.

### The unit everyone got wrong first

What has been used as a coverage signal so far is `downCount`, and it counts
`COUNT(DISTINCT date_trunc('hour', timestamp))` over the whole park-local calendar day with no
opening-hours bound (`analytics.service.ts:5846`).

Proof that it does not count events: **Universal Studios Singapore, Revenge of the Mummy, 12 August,
`downCount = 16`, on a park day running 10:00-20:00 local.** Sixteen is larger than ten. Two halts in
one hour read as 1; a four-day outage reads as about 40.

`HistoryDayDto.downCount` is described as "Number of times the attraction was DOWN on this day". That
description is wrong and is corrected in the same commit that derives the value from intervals.

---

## 3. Exposure: the denominator

**Exposure is the ride's own `OPERATING` minutes inside a flattened park `OPERATING` window, and
nothing else.** It is a risk set, not a schedule: a ride cannot break while it is already broken, and
a ride that is shut is not at risk.

Per (attraction, park-local operating day): `operatingMinutes` = the sum over reconstructed segments
of the overlap of `[seg_start, seg_end)` with the *disjoint union* of that day's `OPERATING` windows,
each segment capped at 70 minutes from its own row.

- **The 70-minute cap** is what keeps an ingestion gap from becoming downtime. `writeHourlyHeartbeats`
  writes when the newest STANDBY row is over 60 minutes old and the sync runs every 5, so an observed
  ride's rows sit at most ~65 minutes apart. A longer gap has four indistinguishable causes and none of
  them is evidence. The time becomes `unobservedMinutes` and leaves the denominator. The error is always
  *less* exposure, never invented exposure.
- **The four-heartbeat cap** is what keeps our own writer from inventing duration. A heartbeat copies
  the previous status *and* the previous `data_source`, so a `DOWN` that vanishes from every feed carries
  itself forward hourly for up to 24 hours while looking exactly like a themeparks-wiki observation.
- **The 21 schedule-less parks** get the live line and nothing else. Their window is itself inferred
  from ride activity, so normalising ride downtime by it is circular.

### Censoring, stated exactly

`ongoing` (still down at `as_of`), `window_edge` (administrative), `closed` (went `CLOSED` without a
recovery), `gap` / `unconfirmed` / `source_absent` (we lost sight). `reclassified` is an *observed* end
and is excluded from the censored share. A profile withholds duration when censored spells exceed one
quarter. Left truncation: the scan window extends back to `MIN(startedAt) - 1 day` over every
open-ended or in-window outage, so a running spell is not born at the edge.

### What exposure cannot do

It cannot tell "the poll ran and nothing changed" from "the poll died". Nothing in this codebase can:
there is no poll-attempt log, and `SELECT DISTINCT timestamp` is a change log, not a poll log, so it
fails hardest on small quiet parks where three cycles with no park-wide write are ordinary. A per-park
poll-attempt row is the only real fix. Until it exists, the methodology page says so.

---

## 4. Data model

### Write-path columns (Phase 1, prerequisites for everything else)

| Column | Written where | Why |
| --- | --- | --- |
| `queue_data.is_heartbeat boolean NULL` | `true` at `writeHourlyHeartbeats`, `false` at both `saveLiveDataBatch` paths | Without it a measured duration cannot be told from a carried-forward one. **Nullable on purpose**: `NULL` means "written before the column existed" and the reader falls back to `"lastUpdated" = timestamp`, rather than a `DEFAULT false` promoting a year of carried rows to observations. |
| `queue_data.raw_status text NULL` | Only when the ConflictResolver override fires, carrying the pre-override status | Converts the erasure from unmeasurable to countable. Changes nothing that is served. |
| `attractions.last_merged_at timestamptz NULL` | `AttractionMergeService.merge`, on the winner | There is no merge row in `admin_audit_log`, so this is the only way to know a ride's history is two interleaved series. |

### Tables (Phase 3)

- **`attraction_outages`** - the event table, one row per reconstructed interval, the only place an
  outage exists. PK (`attractionId`, `startedAt`). Carries `endedAt` (NULL while ongoing),
  `operatingMinutes`, `observedOperatingMinutes`, `wallMinutes`, `operatingDays`, `endReason`,
  `durationUsable`, `likelyWorksPeriod`, `rowsInSpell`, `heartbeatRows`, `startCensored`,
  `fingerprintVersion`, `computedAt`.
- **`attraction_exposure_days`** - the denominator, one row per (attraction, park-local operating day).
  **Minutes only, never an event**, so a multi-day outage cannot be reconstituted by summing it.
  Invariant: `operating + down + closed + refurbishment + absent + unobserved = parkOpenMinutes` (±1).
  A violation raises a monitored alert and marks the row suspect; it never silently NULLs the day.
- **`attraction_downtime_profiles`** - the published aggregate, one row per attraction, rewritten
  nightly, with `publishable` and `withheldReason`.
- **`park_downtime_coverage`** - the honesty label, one row per park, published for *every* park:
  `regime` in `not_capable | reports | artefact | no_schedule`, with `downCapable` read from
  `parks.wiki_entity_id`, that is from configuration and never from the outcome.

All four are registered in `src/parks/utils/merge-dependencies.ts` in the same commit.

### Schema application

This repo has **no** `src/migrations` and no `migrations` entry in `src/config/typeorm.config.ts`; the
schema comes from `synchronize`. The two `queue_data` columns are therefore a reviewed, hand-run
`ALTER TABLE ... ADD COLUMN ... NULL`, timed against a restore first, because `queue_data` is a
compressed hypertable with 254+ chunks. The four new tables come from entity sync.

---

## 5. What may be published, and what it says

| Metric | Publish | Gate | German wording |
| --- | --- | --- | --- |
| `outage.since` (live) | **yes** | Only while the ride reads `DOWN`, only in a `wiki_entity_id` park | „Störung gemeldet seit 14:20 Uhr." A clock time in one of three forms, never an elapsed counter. |
| `outage.minutesToday` (live) | gated | Park publishes hours, total ≥ 20 min, both edges bracketed, carried share < ½ | „Heute rund 40 Minuten Störung gemeldet." |
| `park.ridesDownNow` | gated | Only when ≥ 1. **There is no zero form.** | „Für zwei Bahnen ist gerade eine Störung gemeldet." Never „alles läuft". |
| `outageCount` (90 d) | gated | regime `reports`, ≥ 40 observed operating days, split-half homogeneity not rejected | „In den letzten 90 Tagen hat die Datenquelle für diese Bahn 34 Störungen gemeldet, an 61 von 88 beobachteten Betriebstagen." |
| `medianOutageMinutes` | gated | Event floor: ≥ 24 outages, ≥ 12 usable, censored share ≤ 25 %, carried share ≤ 50 % | „Die Hälfte der 28 beobachteten Störungen war nach 25 Minuten vorbei." |
| `longestOutageMinutes` + date | gated | Same event floor. A maximum is the most sampling-sensitive statistic there is. | „Die längste dauerte 3 Stunden 20 Minuten am 14. Juli." |
| `downShare` | gated | Same floor plus ≥ 150 operating hours. **One denominator on the card, and it is this one.** | „Das sind rund 2 Prozent der Zeit, in der die Bahn lief." |
| `parkDowntimeCoverage.regime` | **yes, always** | none | Four refusal sentences, one per reason. |
| Rate per 100 operating hours | **no** | Fitted internally (hierarchical gamma-Poisson) to answer whether a rate could ever be published | - |
| Outage starts by hour of day | **no** | An hour-of-day shape built from a flag the ConflictResolver preferentially erases on popular rides shows queue drainage, not failures | - |
| Probability / expected duration / MTBF / MTTR | **no** | Refused, see §6 | - |

### The four refusals

- `not_capable`: „Für diesen Park meldet keine Datenquelle Störungen. Ausfälle sehen wir hier nicht."
- `artefact`: „Für diesen Park liegen uns Störungsmeldungen nur stundengenau vor. Eine Dauer lässt sich daraus nicht ablesen."
- `no_schedule`: „Für diesen Park sind keine Öffnungszeiten veröffentlicht, deshalb zeigen wir keine Minuten."
- `reports` but nothing found: „Für diese Bahn wurde in den letzten 90 Tagen keine Störung gemeldet."

Every published figure attributes the source (`gemeldet` / `reported` / `signalée` / `gemeld` /
`segnalata` / `notificada`), never „die Bahn war kaputt".

### Non-negotiables

1. No cross-park comparison, no ranking, no reliability score, no badge in a listing, no sort and no
   filter by outages. The difference between two parks is a difference between two feeds.
2. None of these numbers appears in structured data. No `aggregateRating`, no review, no derived
   JSON-LD property. A grep in the check suite holds that.
3. The event floor applies to **every** derived quantity, not only to a rate.
4. Nothing ships before the methodology page exists in six languages, linked from every figure *and*
   every refusal, and added to `pnpm check:agent-ready`.

---

## 6. Why there is no probability

Four reasons, each sufficient on its own.

1. **The arithmetic is already decided.** At the observed magnitudes, *p* for the next three operating
   hours is about 0.005 to 0.01. Separating a calibrated 0.006 from a 0.012 at 80 % power needs roughly
   `(2.8)²/p` ≈ 1300 events per bin, which is several hundred ride-years.
2. **The numerator is not stationary, for a reason no model reaches.** It is not "the ride broke", it is
   "themeparks-wiki reported DOWN *and* our ConflictResolver did not overwrite it". The second half
   changes when a source mapping is added or a name match starts working, with no deploy and no trace in
   the data.
3. **Censoring depends on the outcome.** The override fires as soon as a second source reports
   `waitTime >= 5`. A queue does not empty the moment a ride stops, it drains over fifteen to thirty
   minutes, so what disappears preferentially is the *short* outages of the *popular* rides. Missingness
   that depends on both the outcome and the covariate of interest cannot be weighted or imputed.
4. **The duration distribution is left-truncated at an unknown point.** Writes are delta-only, so an
   outage that starts and ends between two polls writes no row at all. Nobody knows what share that is,
   so the truncation point cannot be estimated.

**What is built anyway, though none of it appears:** `queue_data.raw_status` measures the erasure for
thirty days, counted in rows *and* in the minutes they carried (rows alone understate it, because a
state rewritten to `OPERATING` is then stable and writes no further row). That one number decides
whether the question can ever be asked again. If it comes out small, the answer is still no, because of
reasons 1 and 4.

**Explicitly also excluded:** any softer rewording of the same claim. No „rechnerisch etwa eine Störung
an einem Tag mit neun Betriebsstunden", no „eher unzuverlässig", no conversion of a rate onto a visit,
an hour or a day.

---

## 7. Plan

| Phase | Repo | Work | Ships |
| --- | --- | --- | --- |
| **0. Count events, publish nothing** (2-3 d) | api | Run statement 1 read-only over 90 days for ~30 rides across Magic Kingdom, Epcot, Anaheim, Disneyland Paris, Universal Studios Florida, Efteling, Europa-Park. Report the **event** count and a duration histogram, not DOWN-hours. Capability census (`COUNT(*) FROM parks WHERE wiki_entity_id IS NOT NULL`). `EXPLAIN (ANALYZE, BUFFERS)` over a 45-day-old chunk. | Nothing user-facing. Every threshold in phases 3-4 is written from this table, and the event floor of 24 is re-derived or replaced. |
| **1. Write-path columns** (1 d + restore timing) | api | `is_heartbeat`, `raw_status`, `last_merged_at`. Hand-run `ALTER`, timed against a restore. | No behaviour change. Starts the 30-day clock on the erasure measurement. |
| **2. The live line** (2-3 d) | api + fe | `outage-rows.sql.ts` as the shared row filter, `AttractionOutageService.getCurrentOutages`, attached from one service in **both** integration paths so the two precedence chains cannot disagree. Frontend: one line under the existing `ParkStatusBadge`, in the status row so no panel is created, `data-nosnippet`, six locales. | „Störung gemeldet seit 14:20 Uhr" wherever a `DOWN` already renders, including the 21 schedule-less parks. No new table, no denominator. |
| **3. Reconstruct, still publish nothing** (1.5 wk) | api | The four tables, the two statements, the park-open-window SQL/TS twin with a shared spec. `downtime-reconstruction.processor.ts`: one statement per time chunk across all parks, delete-then-insert per (park, window). Then a **hand check**: the twenty rides that would clear the gates, verified against the parks' own channels. More than two of twenty disagree and nothing proceeds. | The tables plus the corrected per-day entry on `attraction_hourly_history`. Nothing on the API. |
| **4. Publish the four measured numbers** (1.5-2 wk) | api + fe | Profile table, event floor on every derived quantity, discriminated-union DTO (`{kind:'figures'}` / `{kind:'withheld', reason}`) so no decision input rides along. Frontend: a chapter only where figures publish, one inline line where they do not. Own message namespace, server-read. Methodology page in the same release. Withdrawal path: a regime flip clears the profile and POSTs `parkCacheTag` to `/api/revalidate` with `expire: 0` the same night. | Count, median, longest, `downShare`, four refusal states, one methodology page. |
| **5. Measure the erasure** (3-4 d) | api | Thirty days after phase 1: how many `DOWN` readings the override rewrote, per park and per ride, in rows and in minutes. Falsification kit (Fano factor, time-rescaling KS, shuffled control). | A written verdict, most likely a documented refusal with a number attached. |

---

## 8. Rejected, with reasons

Kept here so none of it gets re-proposed.

1. **Any probability, expected duration, MTBF, MTTR.** See §6.
2. **Kaplan-Meier median as the published duration.** Under censoring the KM median is not "half of the
   observed outages", so the sentence beside it would be checkable against the counts beside it and
   false. The empirical median of outages with an observed end is published instead, with its counting
   rule named.
3. **Quasi-Poisson dispersion with a `sqrt(phi)` widening.** At expected daily counts well under one,
   the Pearson statistic is dominated by days with tiny exposure, so the widening would be a random
   number.
4. **A ride compared against its park's median**, and with it the whole multiplicity question. At a few
   hundred published rides and nominal 5 %, about fifteen named rides read as worse than their park by
   chance. The comparison is deleted, not corrected.
5. **`MAX(source_count) = 1` as a merge filter.** `data_source` is not provenance: the ConflictResolver
   never rewrites it on merge and the heartbeat copies it, so the filter reads 1 exactly on merged rides
   and instead fires on genuine dual-source parks. Replaced by `attractions.last_merged_at`.
6. **A poll log derived from `SELECT DISTINCT timestamp`.** With delta-only writes a poll that changed
   nothing writes no row, so both censoring flags fire on healthy data, correlated with park size.
7. **`attractionIsOutOfSeason()` in the historical reconstruction.** It computes against
   `EXTRACT(MONTH FROM NOW())`.
8. **A rendered "closed every Sunday" claim.** Six observed Sundays are not an operating plan, and
   `detect-seasonal` requires 330 observed days before it names a single month. Internal diagnostic only.
9. **An outage derived from `CLOSED` during opening hours.** 534 of 602 anomalies in the deleted
   `unexpected_closure` class were genuine closures. Building the same thing under a new name in a
   different service repeats the mistake with a better surface.
10. **A `schedule_entries` row per attraction.** The column exists and is never written; writing here
    creates a second schedule writer with no reconciliation and no owner.
11. **A downtime field in the 5-minute projection or on `leanParkForShell`.** The park page renders none
    of the historical numbers. Only the live object travels, and it is absent for 99.7 % of rides.
12. **Its own `ChapterHeading` on every ride page.** On the large majority of ~7000 rides it would be a
    heading promising outages over a sentence taking it back, six languages deep, on the
    second-highest-cardinality route in the site.
13. **A backfill over compressed chunks further than 180 days back**, and any change to
    `compress_segmentby` as part of this work. That is its own measurement with its own before and after.

---

## 9. Prior art this replaces or feeds

| Existing | What happens to it |
| --- | --- |
| `trackDowntime()` (`wait-times.processor.ts:820-906`) | Redis only, 25 h TTL, read by the ML feature builder alone. Conflates `DOWN`/`CLOSED`/`REFURBISHMENT`, credits a spell only on **recovery** (a ride that goes down and never reopens records zero minutes), skips anything starting within 60 min of closing. Left alone until phase 3, then its feature is fed from `attraction_exposure_days`. |
| `downCount` (`analytics.service.ts:5846`, `HistoryDayDto`) | Keeps its value and its column. Description corrected and marked deprecated. `outageCount` / `outageMinutes` derived from intervals are written beside it. |
| `downYesterday()` (`plan-day.service.ts:873-910`) | Unchanged. It is a same-day warning, not a statistic. |
| `getRideOpeningTimes()` (`analytics.service.ts:5633`) | The template for the exposure model's per-ride derivation, including its `HAVING count(*) >= 5` floor and its "a median is a detection time, not an opening" rounding rule. |

---

## 10. Open decisions

1. **Ship phase 2 alone?** The live line repeats in the present what the park's own feed says right now
   and needs none of the historical machinery. The historical numbers are a claim about a company and
   need the methodology page, the event floor and the capability check first.
2. ~~**A curated "out of service from/to" field** under `/admin/attractions/<id>`?~~ **Done.**
   `curated_out_of_service_from` / `_to`, read through `isCuratedOutOfService()` and its SQL twin
   `attractionIsCuratedOutOfService()`. Inside the window neither the live line nor a future
   reconstruction reports anything. Unlike `attractionIsOutOfSeason()` the SQL half takes the day as
   a parameter instead of reading `NOW()`, which is what makes it safe to apply to history.
3. **Does the event floor stay at 24** after phase 0 has counted, even if that leaves a few hundred of
   ~7000 rides with figures and everything else with a refusal sentence?
4. **Who answers a press office** that disputes a number, in what time, and is the park's figure switched
   off meanwhile? That belongs before the first publication.

---

## Appendix: reconstruction SQL

Verified against PostgreSQL 16 on a fixture schema built from the real entities, with cases for a
three-day outage, an outage open at the window edge, an outage open now, a five-hour ingestion gap,
rows outside opening hours, merged and over-carried heartbeat rows, a Wakobato Sunday, an overlapping
schedule day, a misdated past-midnight close, a retired ride, a reconciliation terminator, and a second
park in another timezone whose outage crosses local midnight inside one window.

Two column traps that were checked in the entities:

- `queue_data."attractionId"` is **uuid** at DB level despite `@Column({type:'text'})`. Join
  `a.id = qd."attractionId"`, never `a.id::text`. The damage report is in `plan-day.service.ts:857-869`.
- **No cast on `queue_data.timestamp` in a WHERE clause.** `(qd.timestamp AT TIME ZONE tz)::date` hides
  the column from chunk exclusion: 254 chunks, 35,967 buffers, 1.1 s cold for nine rows.

```sql
-- src/analytics/utils/outage-reconstruction.sql.ts
--
-- VERIFIED: both statements were run against PostgreSQL 16 on a fixture schema
-- built from the real entities (queue_data, attractions, schedule_entries, parks),
-- with cases for every requirement: a three-day outage, an outage open at the
-- window edge, an outage open now, a five-hour ingestion gap, rows outside
-- opening hours, heartbeat rows (merged, and carried past the cap), a Wakobato
-- Sunday, an overlapping/duplicated schedule day, a misdated past-midnight close,
-- a retired ride, a reconciliation terminator, and a second park in another
-- timezone whose outage crosses local midnight inside one window.
--
-- Column notes that are easy to get wrong and were checked in the entities:
--  * queue_data."attractionId" is uuid at DB level despite @Column({type:'text'}).
--    Join a.id = qd."attractionId", never a.id::text (plan-day.service.ts:857-869).
--  * queue_data has data_source, "lastUpdated", "queueType", status, timestamp.
--  * schedule_entries has "parkId", "attractionId" (nullable), "scheduleType",
--    "openingTime", "closingTime".
--  * attractions has retired_at, open_with_park, "parkId".
--  * parks has timezone, wiki_entity_id.
--  * is_heartbeat (queue_data) and last_merged_at (attractions) are added by phase 1.
--
-- Access shape: ONE statement per time chunk across ALL parks ($1 = NULL), the
-- shape refreshOperatingDayRollup uses (queue-percentile.processor.ts:819).
-- queue_data compresses after 30 days with no segmentby, so a per-park-per-day
-- loop decompresses the same chunks hundreds of times. Pass $1 as a park array
-- only for a targeted repair.
--
-- $1 uuid[] park filter or NULL, $2 scan_start, $3 win_end, $4 as_of (now() in
-- production; a parameter so the spec can pin it). The writer DELETEs every
-- outage of the covered parks with startedAt >= $2 before inserting, so a
-- recomputed multi-day outage replaces itself instead of leaving a fragment.
-- $2 is chosen from the data: MIN(startedAt) - 1 day over every open-ended or
-- in-window outage, never from a calendar.

-- ===========================================================================
-- STATEMENT 1 - OUTAGE INTERVALS
-- ===========================================================================
WITH params AS (
  SELECT $1::uuid[] AS park_ids, $2::timestamptz AS scan_start, $3::timestamptz AS win_end,
         $4::timestamptz AS as_of,
         INTERVAL '70 minutes' AS max_carry,   -- heartbeat fires at >60 min, poll every 5
         4                     AS max_carried_hb,
         INTERVAL '20 hours'   AS stitch_gap,
         5                     AS min_minutes,
         7                     AS max_run_days
),
park_tz AS (
  SELECT pk.id AS park_id, pk.timezone AS tz
  FROM parks pk, params p
  WHERE (p.park_ids IS NULL OR pk.id = ANY(p.park_ids))
    AND pk.wiki_entity_id IS NOT NULL          -- only a wiki-mapped park can emit DOWN
),
windows_raw AS (
  -- The SQL twin of normalizeClosingTime (operating-window.util.ts), which is a
  -- write-path repair with no backfill: legacy rows still carry a close before
  -- the open, a 34-hour day and a 3-year day. Filtering them away would delete
  -- the evening from both sides; re-anchoring keeps it.
  SELECT z.park_id, z.tz, se."openingTime" AS opens_at,
         CASE WHEN se."closingTime" > se."openingTime"
               AND se."closingTime" <= se."openingTime" + INTERVAL '24 hours'
              THEN se."closingTime"
              ELSE (((date_trunc('day', se."openingTime" AT TIME ZONE z.tz)
                      + (se."closingTime" AT TIME ZONE z.tz)::time) AT TIME ZONE z.tz)
                    + CASE WHEN ((date_trunc('day', se."openingTime" AT TIME ZONE z.tz)
                                  + (se."closingTime" AT TIME ZONE z.tz)::time) AT TIME ZONE z.tz)
                                <= se."openingTime" THEN INTERVAL '1 day' ELSE INTERVAL '0' END)
         END AS closes_at
  FROM params p
  JOIN park_tz z ON TRUE
  JOIN schedule_entries se ON se."parkId" = z.park_id
  WHERE se."attractionId" IS NULL              -- per-attraction rows are never written
    AND se."scheduleType" = 'OPERATING'        -- EXTRA_HOURS / TICKETED_EVENT out, both sides
    AND se."openingTime" IS NOT NULL AND se."closingTime" IS NOT NULL
    AND se."closingTime" > p.scan_start - INTERVAL '2 days' AND se."openingTime" < p.win_end
),
w_ord AS (
  SELECT park_id, tz, opens_at, closes_at,
         MAX(closes_at) OVER (PARTITION BY park_id ORDER BY opens_at
                              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
  FROM windows_raw WHERE closes_at > opens_at
),
w_grp AS (
  SELECT *, SUM(CASE WHEN prev_max IS NULL OR opens_at > prev_max THEN 1 ELSE 0 END)
            OVER (PARTITION BY park_id ORDER BY opens_at ROWS UNBOUNDED PRECEDING) AS grp
  FROM w_ord
),
win AS (
  -- A DISJOINT union per park. Two OPERATING rows on one day (or a duplicated
  -- upstream re-publish) would otherwise count every overlapping minute twice
  -- and, worse, split one outage into two by breaking the contiguity test.
  SELECT park_id, MIN(opens_at) AS opens_at, MAX(closes_at) AS closes_at,
         (MIN(opens_at) AT TIME ZONE MIN(tz))::date AS op_day   -- the WINDOW's opening date,
                                                                -- never the segment's own instant
  FROM w_grp GROUP BY park_id, grp
),
obs_raw AS (
  -- queue_data is a CHANGE LOG, not a sample (isSignificantChange,
  -- queue-data.service.ts:576), so a status holds until the next row.
  -- STANDBY only: a BOARDING_GROUP row describes a different queue.
  -- No season predicate: attractionIsOutOfSeason is NOW()-based and would erase
  -- last July from a ride that left season in September.
  SELECT qd."attractionId" AS aid, a."parkId" AS pid, qd.timestamp AS ts,
         CASE WHEN qd.data_source = 'system-reconciliation' THEN 'ABSENT' ELSE qd.status END AS st,
         COALESCE(qd.is_heartbeat, qd."lastUpdated" IS NOT NULL AND qd."lastUpdated" = qd.timestamp) AS is_hb
  FROM params p
  JOIN park_tz z ON TRUE
  JOIN attractions a ON a."parkId" = z.park_id AND a.retired_at IS NULL AND NOT a.open_with_park
                    AND (a.last_merged_at IS NULL OR a.last_merged_at < p.scan_start)
  JOIN queue_data qd ON qd."attractionId" = a.id AND qd."queueType" = 'STANDBY'
                    AND qd.timestamp >= p.scan_start AND qd.timestamp < p.win_end
),
obs_hb AS (
  SELECT o.*,
         COUNT(*) FILTER (WHERE NOT o.is_hb)
           OVER (PARTITION BY o.aid ORDER BY o.ts ROWS UNBOUNDED PRECEDING) AS real_no
  FROM obs_raw o
),
obs_rank AS (
  SELECT o.*, ROW_NUMBER() OVER (PARTITION BY o.aid, o.real_no ORDER BY o.ts) - 1 AS hb_rank
  FROM obs_hb o
),
obs AS (
  -- writeHourlyHeartbeats copies the PREVIOUS row's status AND data_source
  -- (wait-times.processor.ts ~995) for as long as the Redis lastSeen key is
  -- under 24 h. A heartbeat may confirm a state, never certify it indefinitely.
  SELECT aid, pid, ts, st, is_hb, (hb_rank = (SELECT max_carried_hb FROM params)) AS hb_capped
  FROM obs_rank WHERE hb_rank <= (SELECT max_carried_hb FROM params)
),
seg AS (
  SELECT o.aid, o.pid, o.st, o.is_hb, o.hb_capped, o.ts AS raw_start,
         LEAD(o.ts) OVER w AS next_ts,
         LEAD(o.st) OVER w AS next_st
  FROM obs o WINDOW w AS (PARTITION BY o.aid ORDER BY o.ts)
),
seg_cap AS (
  SELECT s.*, p.win_end, p.as_of,
         s.raw_start AS seg_start,
         LEAST(COALESCE(s.next_ts, p.as_of), s.raw_start + p.max_carry, p.win_end, p.as_of) AS seg_end,
         (COALESCE(s.next_ts, p.as_of) > s.raw_start + p.max_carry) AS carry_broken,
         (s.next_ts IS NULL) AS is_last
  FROM seg s, params p
),
down_seg AS (
  -- LATERAL, not a JOIN: a JOIN to `win` would emit one row per overlapping
  -- window and the duplicate would break the contiguity test below.
  SELECT c.*, COALESCE(x.op_min, 0) AS op_min
  FROM seg_cap c
  LEFT JOIN LATERAL (
    SELECT SUM(EXTRACT(EPOCH FROM (LEAST(c.seg_end, w.closes_at)
                                 - GREATEST(c.seg_start, w.opens_at)))/60.0) AS op_min
    FROM win w WHERE w.park_id = c.pid AND w.opens_at < c.seg_end AND w.closes_at > c.seg_start
  ) x ON TRUE
  WHERE c.st = 'DOWN' AND c.seg_end > c.seg_start
),
spell_mark AS (
  SELECT d.*, CASE WHEN LAG(d.seg_end) OVER w = d.seg_start THEN 0 ELSE 1 END AS is_new
  FROM down_seg d WINDOW w AS (PARTITION BY d.aid ORDER BY d.seg_start)
),
spell_grp AS (
  SELECT m.*, SUM(m.is_new) OVER (PARTITION BY m.aid ORDER BY m.seg_start
                                  ROWS UNBOUNDED PRECEDING) AS spell_no
  FROM spell_mark m
),
spell AS (
  SELECT aid, pid, spell_no,
         MIN(seg_start) AS started_at, MAX(seg_end) AS ended_at,
         SUM(op_min) AS op_min,
         SUM(op_min) FILTER (WHERE NOT is_hb) AS op_min_real,
         count(*) AS rows_in_spell, count(*) FILTER (WHERE is_hb) AS hb_rows,
         (array_agg(next_st      ORDER BY seg_start DESC))[1] AS last_next_st,
         (array_agg(carry_broken ORDER BY seg_start DESC))[1] AS last_carry_broken,
         (array_agg(is_last      ORDER BY seg_start DESC))[1] AS last_is_last,
         (array_agg(hb_capped    ORDER BY seg_start DESC))[1] AS last_hb_capped,
         (array_agg(seg_end      ORDER BY seg_start DESC))[1] AS last_seg_end
  FROM spell_grp GROUP BY aid, pid, spell_no
),
spell_lag AS (
  SELECT s.*, LAG(s.ended_at) OVER w AS prev_end FROM spell s
  WINDOW w AS (PARTITION BY s.aid ORDER BY s.started_at)
),
stitch AS (
  -- Requirement (b): a ride can be down for days. Two spells are ONE outage when
  -- the later resumes within 20 h and NO OPERATING observation lies between.
  SELECT sl.*,
    CASE WHEN sl.prev_end IS NOT NULL
          AND sl.started_at - sl.prev_end <= (SELECT stitch_gap FROM params)
          AND NOT EXISTS (SELECT 1 FROM obs o
                           WHERE o.aid = sl.aid AND o.st = 'OPERATING'
                             AND o.ts > sl.prev_end AND o.ts <= sl.started_at)
         THEN 0 ELSE 1 END AS is_new_outage
  FROM spell_lag sl
),
grp AS (
  SELECT st.*, SUM(st.is_new_outage) OVER (PARTITION BY st.aid ORDER BY st.started_at
                                           ROWS UNBOUNDED PRECEDING) AS outage_no
  FROM stitch st
),
outage AS (
SELECT aid AS "attractionId", pid AS "parkId",
       MIN(started_at) AS "startedAt",
       MAX(ended_at)   AS end_bound,
       ROUND(SUM(op_min))::int      AS "operatingMinutes",
       ROUND(SUM(op_min_real))::int AS "observedOperatingMinutes",
       ROUND(EXTRACT(EPOCH FROM (MAX(ended_at) - MIN(started_at)))/60.0)::int AS "wallMinutes",
       SUM(rows_in_spell)::int AS "rowsInSpell", SUM(hb_rows)::int AS "heartbeatRows",
       CASE
         WHEN (array_agg(last_seg_end ORDER BY started_at DESC))[1] >= (SELECT win_end FROM params)
              THEN 'window_edge'
         WHEN (array_agg(last_is_last ORDER BY started_at DESC))[1]
              AND NOT (array_agg(last_carry_broken ORDER BY started_at DESC))[1] THEN 'ongoing'
         WHEN (array_agg(last_next_st ORDER BY started_at DESC))[1] = 'ABSENT' THEN 'source_absent'
         WHEN (array_agg(last_hb_capped ORDER BY started_at DESC))[1] THEN 'unconfirmed'
         WHEN (array_agg(last_carry_broken ORDER BY started_at DESC))[1] THEN 'gap'
         WHEN (array_agg(last_next_st ORDER BY started_at DESC))[1] = 'OPERATING' THEN 'recovered'
         WHEN (array_agg(last_next_st ORDER BY started_at DESC))[1] = 'REFURBISHMENT' THEN 'reclassified'
         WHEN (array_agg(last_next_st ORDER BY started_at DESC))[1] = 'CLOSED' THEN 'closed'
         ELSE 'gap' END AS "endReason"
FROM grp GROUP BY aid, pid, outage_no
)
SELECT o.*,
       (SELECT count(*)::int FROM win w
         WHERE w.park_id = o."parkId"
           AND w.opens_at < o.end_bound AND w.closes_at > o."startedAt") AS "operatingDays",
       (o."endReason" IN ('recovered','reclassified')) AS "durationUsable",
       ((o.end_bound - o."startedAt") > (SELECT max_run_days FROM params) * INTERVAL '1 day')
         AS "likelyWorksPeriod",
       CASE WHEN o."endReason" = 'ongoing' THEN NULL ELSE o.end_bound END AS "endedAt"
FROM outage o
WHERE o."operatingMinutes" >= (SELECT min_minutes FROM params)
ORDER BY o."attractionId", o."startedAt";

-- Measured on the fixture (park tz Europe/Berlin unless noted):
--   R1  clean            80 min, recovered, operatingDays 1, 1 heartbeat merged
--   R2  multi-day        ONE row 10 Aug 16:00 -> 12 Aug 12:00, wall 2640 min,
--                        operatingMinutes 545, operatingDays 3, recovered
--   R3  open now         endReason 'ongoing', endedAt NULL
--   R3  historic window  endReason 'window_edge', endedAt = win_end
--   R4  5-hour gap       70 min (one carry), 'gap', durationUsable false
--   R5  reconciliation   'source_absent', durationUsable false
--   R6  Wakobato Sunday  no row at all; 0 operating and 0 down minutes that day
--   R7  8 h of heartbeat 310 min, 'unconfirmed', durationUsable false
--   R8  20:00, park shut no row (0 operating minutes)
--   R9  two OPERATING rows on one day: 40 min, not 80
--   R10 retired          no row
--   B1  America/New_York 23:30 -> 00:50 across local midnight: one 80-min row,
--                        operatingDays 1, attributed to the window's own day
--   queue-times-only park: nothing, no wiki_entity_id

-- ===========================================================================
-- STATEMENT 2 - EXPOSURE. Same CTEs down to seg_cap, then a day rollup.
-- The denominator is the ride's own OPERATING minutes, which is what makes the
-- Wakobato case and the 21 schedule-less parks disappear instead of needing a
-- carve-out. Replace the down_seg..end block above with:
-- ===========================================================================
-- , seg_win AS (   -- one row per (segment, window); summing minutes cannot
--                  -- double-count because `win` is already disjoint per park
--   SELECT c.aid, c.pid, w.op_day, c.st, c.is_hb,
--          EXTRACT(EPOCH FROM (LEAST(c.seg_end, w.closes_at)
--                            - GREATEST(c.seg_start, w.opens_at)))/60.0 AS mins
--   FROM seg_cap c
--   JOIN win w ON w.park_id = c.pid AND w.opens_at < c.seg_end AND w.closes_at > c.seg_start
--   WHERE c.seg_end > c.seg_start
-- ),
-- park_open AS (
--   SELECT park_id, op_day, SUM(EXTRACT(EPOCH FROM (closes_at - opens_at))/60.0)
--            AS park_open_minutes
--   FROM win GROUP BY park_id, op_day
-- ),
-- tracked AS (
--   SELECT a.id AS aid, a."parkId" AS pid
--   FROM attractions a JOIN park_tz z ON z.park_id = a."parkId"
--   WHERE a.retired_at IS NULL AND NOT a.open_with_park
-- )
-- SELECT t.aid AS "attractionId", t.pid AS "parkId", po.op_day AS "opDay",
--        ROUND(po.park_open_minutes)::int AS "parkOpenMinutes",
--        ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st='OPERATING'),0))::int
--          AS "operatingMinutes",
--        ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st='DOWN'),0))::int
--          AS "downMinutes",
--        ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st='DOWN' AND NOT sw.is_hb),0))::int
--          AS "downMinutesObserved",
--        ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st='CLOSED'),0))::int
--          AS "closedMinutes",
--        ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st='REFURBISHMENT'),0))::int
--          AS "refurbishmentMinutes",
--        ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st='ABSENT'),0))::int
--          AS "absentMinutes",
--        GREATEST(ROUND(po.park_open_minutes - COALESCE(SUM(sw.mins),0))::int, 0)
--          AS "unobservedMinutes"
-- FROM tracked t
-- JOIN park_open po ON po.park_id = t.pid
-- LEFT JOIN seg_win sw ON sw.aid = t.aid AND sw.op_day = po.op_day
-- GROUP BY t.aid, t.pid, po.op_day, po.park_open_minutes
-- ORDER BY t.pid, t.aid, po.op_day;
--
-- Verified: the past-midnight window (09:00 -> 01:00, 960 min) puts all its
-- minutes on the window's own opDay, and 80 + 80 + 800 = 960 holds. The
-- overlapping-schedule day reports parkOpenMinutes 480, not 660.
-- `outageStarts` is written from statement 1 in the same transaction, so a
-- multi-day outage increments exactly one day.
```
