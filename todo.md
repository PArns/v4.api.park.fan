# TODO

## The nightly duty-cycle denominator reads a table the same job rewrites (measured 2026-09-08, not fixed)

`DowntimeReconstructionProcessor` runs `CLOSURE_GAP_INTERVALS_SQL` at
`downtime-reconstruction.processor.ts:151`, then deletes and rewrites
`attraction_exposure_days` in the transaction that starts at line 176 (the
delete is at line 183). So the statement's
`active` CTE — the denominator of `gap_days / active_days` — reads the rows the
**previous** run wrote, which stop at that run's `asOf`, while `raw_gaps` (the
numerator) reads `queue_data` right up to this run's. Today's gap days are
counted; today's operating day is not.

At a nightly cadence over a 30-day window that is one day in thirty, ~3.3 %, and
it inflates the ratio — the direction that suppresses genuine faults. It is the
mirror image of the slack-day defect this PR removed from the same expression,
and the two do not cancel: one was in the live statement's favour, this one is
against.

Two edges make it worse than the average: a **first run** and a run after the
**400-day retention purge** see an empty exposure table, so `active_days` is 0
for every ride, `active_days < MIN_DAYS_FOR_CYCLE_TEST` passes everything, and
the duty-cycle filter is off entirely for that run.

The obvious fix does not fit, and this is the measurement that says so. Moving
the closure statement into the transaction after the exposure insert would let
it see this run's rows — but **timed 2026-09-08 over all parks with a 30-day
window, `CLOSURE_GAP_INTERVALS_SQL` takes 30.4 s**. Inside the transaction that
is half a minute of `DELETE` locks held on both `attraction_outages` and
`attraction_exposure_days`, every night.

So the real options are a trade rather than a move:

1. **Split the transaction** — write exposure, run the closure query, write
   outages. Costs the atomicity of the two tables against each other; the
   profile step that reads them both runs afterwards, so the window is internal
   to the job, but it is a guarantee being given up rather than a line moved.
2. **Stop reading the table** — the exposure rows for this window are already in
   memory as `exposure` before the transaction opens. The statement reads them
   in two places (`blind_parks`' evidence-hours sum and `active`), so this means
   passing them in rather than joining.
3. **Accept it and say so** — one day in thirty, ~3.3 %, in the direction that
   suppresses faults. Then `MAX_GAP_DAY_SHARE`'s calibration should be read as
   including it, which nothing currently records.

It is left out of the performance PR because it is that decision, not because it
is large.

## Three defects, one root: the closure statements bucket by calendar date, everything else buckets by operating window (2026-09-08, not fixed)

`attraction_exposure_days.op_day` is documented as "the park-local date the
operating WINDOW opened on. Not the calendar date of the minutes themselves. A
park closing at 02:00 would otherwise split its evening across two rows and lose
it from both." The closure statements do exactly that splitting: `raw_gaps`, the
live `cycle` and `early_end` all take `(ts AT TIME ZONE tz)::date`.

Three symptoms follow, and they are one bug:

1. **`gap_days / active_days` divides two different day definitions.** The
   numerator counts calendar days, the denominator counts window days. In a park
   closing after local midnight a 00:30 gap is filed under D+1 while the evening
   it belongs to is filed under D — so the ratio can exceed 1.0, which the
   `cycle` comment already names as the tell of a wrong count, and every ride in
   that park is dropped as a duty cycle.
2. **`early_end` scores the day currently being judged.** `run_readings` covers
   `[$3 - 21d, $3)`, so today is one of its ~21 days: a ride that broke at 14:00
   in a park closing at 22:00 has today's `last_operating` more than an hour
   before today's close, and counts itself as having ended early. On the fourth
   day of an outage it has contributed four self-generated early days, and
   `days` is days-with-published-hours rather than 21 — so the longest and most
   certain outages are the first to cross `MAX_EARLY_END_SHARE` and vanish.
3. **A past-midnight park loses the signal entirely** — Six Flags Qiddiya City
   on all 42 published days, plus four parks on one event night each; the
   measurements are in the section below.

All three are pre-existing, and the fix for all three is the same: attribute a
reading to the operating window that contains it, which is what
`parkOpenWindowCtes()` was written to do. The two blockers are unchanged — its
placeholders are hard-coded `$1..$3`, and switching to window attribution drops
readings outside opening hours, which changes `last_operating` for **every**
park rather than only the past-midnight ones. That needs its own before/after
over the population.

## early_end attributes a reading to its calendar date, not its operating window (measured 2026-09-08, not fixed)

`CURRENT_CLOSURE_GAP_SQL`'s `early_end` buckets each `queue_data` reading by its
own park-local **date**, then compares it against a closing time anchored to the
**opening's** date and rolled past midnight by `normalizedClosingSql`. In a park
that closes after local midnight the two disagree by a day, and the comparison
`last_operating < closes_at - 60 min` is then true for every reading of every
day: `early_days / days` converges on 1.0, exceeds `MAX_EARLY_END_SHARE`, and
the ride is dropped as timetabled. **The closure signal is silently off for that
park.**

Measured over the last 30 days of published hours in the 91 blind parks:

| park | published days | days closing past local midnight |
| --- | ---: | ---: |
| Six Flags Qiddiya City | 42 | **42** |
| Gardaland | 107 | 1 |
| Parc Asterix | 74 | 1 |
| SeaWorld Orlando | 387 | 1 |
| Busch Gardens Tampa | 387 | 1 |

So one blind park is affected systematically and four on a single event night
each.

**It is pre-existing.** The `LATERAL` this was hoisted out of correlated on the
same date equality and used the same normalized close, so the behaviour is
unchanged — which is why the equivalence check passes and why it is not fixed
here.

The fix is `parkOpenWindowCtes()` in `park-open-window.sql.ts`, whose docblock
already names this exact failure ("Anchor a segment to its WINDOW's day, not its
own date … La Ronde does this every day of the season"). It also flattens
overlapping windows and guards a null close, which `park_day_close` does not.
Two things make it a separate change rather than a line:

1. Its parameters are `$1` park filter, `$2`/`$3` window bounds; the live
   statement's are `$1` attractions, `$2` timezone, `$3` as-of, `$4` park. The
   helper needs configurable placeholders before either statement can use it.
2. Joining readings to a window instead of to a date also **drops readings that
   fall outside opening hours**, and `queue_data` is a change log — rides read
   `OPERATING` for hours after a park shuts ([[project_queue_data_last_known_value]]).
   That is more correct, and it changes `last_operating` for **every** park, not
   only the past-midnight ones. It needs its own before/after over the
   population, not a note in a performance PR.

Also parked with it: `park_day_close` is a third hand-rolled copy of "when does
this park's day end" (`park_open` in the same statement is a second), and the
helper is the place all three should meet.

And one more asymmetry to settle at the same time: the live duty-cycle window is
a fixed **21 days** while the nightly runs from `scanStart` to `asOf`
(`DEFAULT_WINDOW_DAYS` 30, wider whenever a running outage pushes the scan
back). `MAX_GAP_DAY_SHARE` is a share, so the same ride with eight gap days
reads 8/21 live and 8/30 nightly and the two can land on opposite sides of the
threshold. The comment claiming "the same window" was corrected on 2026-09-08;
making it true is a decision about the signal, not a rewording, and belongs with
the window-attribution work rather than in front of it.

## The ML feature fetch reads 730 days to use ~300 (measured 2026-09-08, not fixed)

`fetch_recent_wait_times()` (`ml-service/predict.py:218`) pulls **730 days** of
`queue_data` per prediction. Over the three months in `pg_stat_statements` its
two shapes are **41 % of all database execution time** — 2.0 M calls, ~700 000
buffer blocks *per call* to return 622 rows — and after the closure-gap fix it is
the single largest consumer left (83 % of the non-benchmark load in a 180 s
sample).

The widest frame in the statement is `rolling_avg_90d`, `ROWS BETWEEN 2159
PRECEDING`. **That counts rows, not days**, and the comment beside it ("90 days
* 24 hours = 2160 rows") assumes 24 buckets a day. A ride only produces a bucket
for an hour it was open and reporting, so 2160 rows is roughly 180 days of real
data, not 90.

Measured against 8 rides at Europa-Park and Phantasialand, diffing every column
of every row in the last 35 days against the 730-day output:

| lookback | result |
| --- | --- |
| 365 d | identical |
| 300 d | identical |
| 240 d | **differs** (`rolling_avg_90d`) |
| 150 d | **differs** |

**Do not just set it to 300.** Those are two of the densest parks in the
database; a ride with fewer operating hours needs more calendar days to reach
2160 rows, so the safe floor is a property of the sparsest ride in the batch,
not a constant. Before changing it, measure bucket density per ride and derive
the floor from the p1, or replace the row-count frames with `RANGE`-based ones
so the window means what its comment says. This is the trained-model feature
path, so the check is byte-identical output, not "close enough".

## Ride downtime: what is left after the phase 0 run (2026-09-06)

Phase 0 **has been run** against production. The results, the gate that had to be
replaced and the new recovery estimate are in
[ride-downtime §0 and §6a](docs/analytics/ride-downtime.md#0-what-the-measurement-found).
Four of the six original items are done; what follows is what actually remains.

**Nothing historical reaches a reader yet**, and what stands in the way is now
the hand check rather than the thresholds.

### 1. Apply the two `queue_data` columns — needs a go-ahead

This is the one remaining step that writes to production.

```sql
SET lock_timeout = '5s';
ALTER TABLE queue_data ADD COLUMN is_heartbeat boolean NULL;
ALTER TABLE queue_data ADD COLUMN raw_status   text    NULL;
```

**Timed, 2026-09-06:** 139 ms and 62 ms over a purpose-built 258-chunk
compressed hypertable. All chunks stayed compressed and old rows read NULL. The
cost is the catalogue, so it scales with chunk count (257 in production) and not
with the 4.9 GB of data.

**The risk is the `AccessExclusiveLock`, not the runtime.** The statement queues
behind every running query on `queue_data` and blocks everything behind it while
it waits — hence `lock_timeout` and a retry rather than letting it sit. Off-peak,
and not during a Coolify deploy.

**Nullable, never `DEFAULT false`.** `NULL` means "written before the column
existed" and the reader falls back to `lastUpdated = timestamp`; a default would
promote a year of carried heartbeat rows to observations in one statement.

Note that TypeORM `synchronize` will apply these two columns on the first deploy
that carries the entity, so this is a matter of _when_ rather than _whether_ —
which is the reason it was worth timing.

The five new tables (`attraction_outages`, `attraction_exposure_days`,
`attraction_downtime_profiles`, `park_downtime_coverage`,
`downtime_recovery_curves`) come from entity sync and need nothing by hand.

### 2. Remaining checklist

- [ ] **Hand-check twenty rides before anything publishes.** The one item that
      tests the whole chain against reality rather than against itself, and now
      the only thing between the profiles and a reader. Take the rides that clear
      the re-derived gates and verify their reconstructed outages against the
      parks' own channels for those dates. **More than two of twenty disagree and
      nothing proceeds.** Candidates are easy now: 1324 rides clear the event
      floor, and the densest histories are Parque Warner Madrid, Cedar Point,
      Movie Park Germany and Canada's Wonderland.
- [ ] **First reconstruction fill, in stages.** `POST /v1/admin/rebuild-downtime`
      with a window of **30 days at a time**, not 120 in one call. Measured: 6x
      the window costs ~19x the time, 30 days already spills ~700 MB of temp for
      statement 1, and the two statements run under one `Promise.all`. The
      processor logs any exposure day failing the minute invariant — that check
      already passed read-only over 88 814 days with 0 violations, so a warning
      here means the write path, not the arithmetic.
- [ ] **Thirty days after the columns ship: `GET /v1/admin/downtime-erasure`.**
      How much of the DOWN signal `ConflictResolverService` deletes, in rows and
      in the minutes they carried. Still the number that decides whether §6's
      refusal can ever be revisited.
- [ ] **Re-run the recovery curve calibration after a full quarter.** The
      out-of-sample error is 2.55 pp today, on a 120/60 split of 180 days. Worth
      re-running once a year of reconstruction exists, because the curve is
      currently built from a mostly-summer window and the hazard may be seasonal.
- [ ] **`pnpm measure:cls --late`** on a park page with a DOWN ride (frontend).
      The card gained a second `w-full` line and cards share row heights through
      a subgrid.

### 2b. Found by the first nightly run (2026-09-07)

Both fixed in the same push; both are in
[ride-downtime §0](docs/analytics/ride-downtime.md#0-what-the-measurement-found).

- The recovery curves never built: `delete({})` throws in TypeORM, BullMQ
  retried three times, and nothing was logged — so the run reported success
  while the table stayed empty and three full reconstructions ran per night.
- 102 of 182 scheduled parks have never emitted a DOWN row, and read as
  `reports` they would have said "no outage reported". New `never_reports`
  regime with an evidence threshold of 1500 observed operating hours.

- [ ] **Verify after the next nightly run (05:00):** `downtime_recovery_curves`
      is non-empty (expect ~174 rows, 11 pooled), and `park_downtime_coverage`
      shows roughly 100 `reports` / 91 `never_reports` / 16 `not_capable` /
      6 `no_schedule`.

### 2c. The closure signal (2026-09-07)

Outages are now also read from closures, for the 102 parks whose feed never
emits DOWN. Details in
[ride-downtime §0](docs/analytics/ride-downtime.md#0-what-the-measurement-found).

- [ ] **Hand-check the closure signal separately.** It is weaker evidence than a
      reported DOWN and needs its own sample: take twenty from Phantasialand
      (85 in 21 days across 27 rides) and check them against the park's own
      channels. The wording already refuses to say „gemeldet", but a wrong
      interval is still a wrong interval.
- [ ] **Watch the first nightly run with both statements.** The log line now
      reads `N interval(s), M closure gap(s)`. Expect roughly 2600 gaps over a
      21-day window; a much larger number means a filter stopped working.
- [ ] **Check the closure-gap recovery curve once it exists.** Its population
      has no censoring by construction, so if its quartiles come out far from
      the measured 15/20/35 something is wrong with the interval definition
      rather than with the estimator.

### 2d. Open findings from the 2026-09-07 review

Two independent reviews (correctness + system design) over the whole session's
diff. What they found and I fixed the same day: the closure signal being
unreachable, simultaneity counted over the wrong set, the missing live duration
floor, carried heartbeats truncating the population at 65 min, Kaplan-Meier
ignoring `duration_usable`, profiles and coverage mixing both signals, per-day
`outageCount` published ungated for blind parks, and the two blindness gates
disagreeing for 11 parks.

What is still open, roughly by consequence:

- [x] ~~No staleness gate on a stored profile~~ — `MAX_PROFILE_AGE_DAYS = 2`,
      withheld past it, with a spec.
- [x] ~~`OUTAGE_SCAN_START_SQL` unbounded below~~ — floored at twice the
      requested window. 71 open-ended intervals exist that could have pinned it.
- [x] ~~Profiles are still upsert-only and never deleted~~ — `rebuildProfiles`
      now deletes then upserts in one transaction (PF-40). Scoped to the parks
      the rebuild actually produced rows for, not to the whole table: an empty
      result there would erase every profile in the database on one failed
      exposure build.
- [ ] **The nightly DELETE erases history it never rewrites.** It is keyed on
      `parkId` with no ride predicate, but the INSERT population is the
      `tracked` CTE. A ride that leaves `tracked` (merge, retirement, a flip to
      `open_with_park`, a park losing its schedule) loses its in-window rows
      permanently.
- [x] ~~**`last_merged_at` is stamped by one of four merge paths**~~ — half of
      that was already stale when it was written: `ParkMergeService.consolidateEntities`
      and `AttractionMergeService.merge` both stamp today. The two raw
      `DELETE FROM attractions` paths in `parks.service.ts` — the sync-time
      collision merge inside `syncParks` and the ghost merge in
      `repairDuplicates` — now stamp every survivor in the same transaction,
      before the losing row is deleted (PF-42).
- [x] ~~**Both raw merge paths abort before that stamp can commit**~~, and the
      "no FK catches the orphans" half of the bullet above was wrong: `queue_data`,
      `wait_time_predictions`, `prediction_accuracy` and `ml_prediction_anomalies`
      all declare `@ManyToOne(() => Attraction)` with no `onDelete`, so the FK is
      NO ACTION and `DELETE FROM attractions` **raises 23503** rather than leaving
      orphans. On top of that `repairDuplicates` wrote
      `prediction_accuracy."attractionId"`, a column that does not exist (it is
      `attraction_id`) → 42703, and moved 3 of the tables in
      `ATTRACTION_DEPENDENCIES`. Both blocks now go through
      `consolidateMergedAttractions`, which is `applyMergeDependencies` over
      `ATTRACTION_DEPENDENCIES` inside the TimescaleDB decompression bracket —
      the same shape as `ParkMergeService.consolidateEntityData`, which already
      survived the USH cold run (PF-102).
- [x] ~~**The ghost _park_ delete has the same hole one level up**~~, and it cost
      twice: both raw paths finished with `manager.delete(Park, ghostPark.id)`
      without applying `PARK_DEPENDENCIES`, and the merge moved `attractionId`
      and never the denormalised `parkId` beside it. Both now call
      `consolidateMergedPark` immediately before that DELETE — `mergeParks`
      steps 3-4 as `PARK_INLINE_DEPENDENCIES`, then its step 5b as
      `PARK_DEPENDENCIES`, in that order, plus the two that shape cannot carry
      (`park_p50_baselines` is winner-authoritative, `external_entity_mapping`
      wants the `internal_entity_type` filter). The audit behind it, since the
      ticket asked which of `PARK_TABLES_HANDLED_INLINE` were needed: of the ten
      inline tables three were already reparented (attractions, shows,
      restaurants) and the other seven were all needed. `park_occupancy` is the
      only park FK that is NO ACTION, so it is the only one that could abort the
      transaction; the rest cascade, so leaving them out destroyed a park's
      schedule, daily stats, weather, headliner set and seasons inside a
      transaction that reported success. The two the ticket left open decide the
      same way: `attraction_rope_drop` and `attraction_typical_waits` **move**,
      which is what `PARK_DEPENDENCIES` has always said about them — applying it
      before the DELETE is what makes that true here (PF-111).
- [ ] **A ghost park's own URL is not preserved when the raw paths delete it.**
      `mergeParks` inserts a `ParkSlugAlias` for the loser's path before the
      DELETE, so already-indexed URLs redirect instead of 404ing; neither raw
      path does. Out of PF-111, whose acceptance criteria are about the
      dependent rows rather than the loser's own path.
- [ ] **`attraction_ride_profiles` is still unprotected on the _attraction_
      side.** Its park half is fixed — PF-111 added it to `PARK_DEPENDENCIES`
      and to the `PARK_REFERENCING_TABLES` snapshot, so a park merge now carries
      the curated profiles across instead of cascading them. Its `attractionId`
      carries `onDelete: "CASCADE"` too, and there the losing ride's profile is
      still destroyed by the `DELETE FROM attractions` both raw paths and
      `AttractionMergeService.merge` run. It is in neither
      `ATTRACTION_DEPENDENCIES` nor `ATTRACTION_REFERENCING_TABLES`, so the
      guard cannot see it either. Left open rather than guessed at, because the
      answer is not a `MergeStrategy`: `discard` is what happens today and is
      wrong by the file's own rule (the neighbours it would sit beside —
      rope-drop, typical-waits — are `discard` because they are _derived_, and
      this is hand-curated with no feed and no seed); `move` collides, since
      `attractionId` is both the merge column and the primary key. What it wants
      is winner-authoritative — take the loser's row only where the survivor has
      none — the same shape `park_p50_baselines` needs and the same reason it is
      not a dependency.
- [x] ~~**A third park delete in `parks.service.ts` has the whole hole.**~~ The
      priority merge in `syncParks` (`parkRepository.delete(losingPark.id)`,
      guarded by an `isEmpty` count over shows/restaurants/attractions) applied
      neither `PARK_DEPENDENCIES` nor `PARK_INLINE_DEPENDENCIES`, so
      `park_occupancy` and the two attraction baselines raised 23503 and aborted
      the sync run. Worse than the two PF-111 fixed: the path held **no
      transaction**, so the entity moves above it were already committed when the
      DELETE threw, and it left a losing park stripped of its rides and still
      present. PAR-103 moved it into `mergePriorityDuplicateLoser`: one
      transaction, the TimescaleDB decompression bracket, attractions
      partitioned by slug (the unique `(parkId, slug)` makes a blind move a
      23505, so they now move by id and colliding losers go through
      `consolidateMergedAttractions`), and `consolidateMergedPark` immediately
      before the park row goes.
      **And the block is not reachable today**, which nobody had noticed: it is
      entered only when no park carries the incoming `externalId`, and its
      loser is looked up by that same `externalId` from the map whose hit skips
      the branch. The fix is right under either reading and the derivation is
      pinned at the call site, in the method docblock and as a spec case — but
      whether the block should exist at all is PAR-142. Shows and restaurants
      stay blind moves here; that is the next entry.
- [ ] **The blind show and restaurant moves can raise 23505 before any of that
      is reached.** All three raw paths do `UPDATE shows SET "parkId" = …` and
      the same for restaurants, against a unique `(parkId, slug)` on either table
      (`show.entity.ts:33`, `restaurant.entity.ts:33`). Two rows for one park
      from two sources are exactly the case that produces a shared slug, so the
      transaction rolls back before the attraction and park steps run at all.
      `mergeParks.migrateEntities` handles it — match on slug or name,
      consolidate, delete the loser — and neither raw path does. Not PF-111's
      scope: it is a collision decision per entity type, not a dependency list.
- [ ] **A migrated `park_season` can name attractions the same merge deleted.**
      `park_seasons.attraction_ids` is a jsonb array of attraction ids, and
      `PARK_DEPENDENCIES` moves the row onto the survivor. Where the merge
      collided a ride, the id in that array belongs to the deleted loser, and
      `ParkSeasonService` re-validates the stored array on the next edit — so
      the season is carried across intact and then refuses every later change
      with "These attractions are not in this park". The ids would have to be
      rewritten to the survivors, which the dependency list has no way to
      express.
- [x] ~~A park with no published hours is served `not_down_capable`, not
      `no_schedule`~~ — the population query gained a `sched` branch that
      sources the rides of `no_schedule` parks directly from `attractions`
      (PF-40). Only that regime: `not_capable` already resolves correctly
      through the missing-row fallback.
- [ ] **`park_open` in the live closure query re-reads `closingTime` raw**,
      without `normalizedClosingSql()`. Stored history still holds misdated
      closings (a 34-hour day, a 3-year one), and one such row in a blind park
      makes the guard true permanently.
- [x] ~~`longest_started_at` is always written NULL~~ — computed in the same
      aggregate as `longest_minutes` and written beside it (PF-40). Ties go to
      the most recent spell so two rebuilds over unchanged data agree. Still
      rendered nowhere; the column now carries a value to render.
- [ ] **The operating day is keyed off the calendar date** in the closure
      statement, not the window's opening date, so a park closing after midnight
      (La Ronde) drops every gap spanning midnight.
- [ ] Cheap: drop `idx_attraction_outages_ride` (byte-identical to the PK), and
      the unread `@Index` on profiles/curves. `merge-dependencies.spec.ts`'s
      snapshot lists predate all five tables, so that guard passes vacuously.

### 2e. Closed: the hand-run ALTER (2026-09-07)

§1 above asked for a go-ahead on the two `queue_data` columns. It is moot: the
entity shipped, Coolify auto-deploys on push, and `synchronize` applied both
`ALTER`s unattended at boot — without the `lock_timeout` the plan specified. It
was harmless because both columns are nullable with no default (verified: 226 of
257 chunks still compressed). **If anyone later "tidies" `is_heartbeat` to
`default: false`, that 139 ms catalogue edit becomes an operation TimescaleDB
refuses on compressed chunks.**

### 2f. Known asymmetry: the early-end filter is live-only

`MAX_EARLY_END_SHARE` catches a ride whose day habitually ends before the
park's — Futuroscope's cinemas score 100 % against a real fault's 8 % — and it
runs only in `CURRENT_CLOSURE_GAP_SQL`. In the nightly statement the same
comparison (last OPERATING reading vs that day's published close, per ride per
day) does not finish: 61 s over seven days as a lateral, past 110 s over 21 even
hoisted and scoped to candidates.

Safe today because nothing published reads those rows — profiles, coverage and
the recovery curve all filter `signal = 'down'`. What the nightly job stores for
`closed_gap` is a record, not a figure.

- [ ] **Make it affordable before anything publishes `closed_gap` history.** The
      stored rows currently carry cinema noise the live line refuses to show.
      Most likely shape: a per-(attraction, day) "ended early" flag written by
      the reconstruction itself, so the share is a cheap count rather than a
      re-derivation.

### 3. Done, 2026-09-06

- Phase 0 run: 150 132 events / 90 d over 2228 rides in 78 parks; capability
  census 197 of 213 parks down-capable, 192 with a schedule.
- Event floor re-derived: **24 confirmed** (1324 rides clear it, median 31).
- Artefact gate replaced — it correlated with "outage shorter than an hour" at
  r = 0.996 and marked 47 of 78 parks unreadable. It now measures temporal
  resolution, and catches nobody.
- `EXPLAIN (ANALYZE, BUFFERS)` on both statements over 1/7/30/180-day windows.
- Exposure invariant verified read-only: 88 814 days, 0 violations.
- `ALTER` timed against a compressed 258-chunk probe.
- `perPark()` slug-collision fixed (`disneyland-park` is Anaheim _and_ Paris).
- New: the how-much-longer estimate, on the ride card and the ride page.

## Retire the attractions that no longer exist (2026-08-15)

The mechanism exists (`retired_at`, admin endpoints, job exclusions). What is
left is establishing, per attraction, whether it is actually gone.

- [ ] **73 individual retirement candidates** — attractions that had real wait
      times, stopped reporting more than 30 days ago on a date **not shared**
      with others in their park, and are still receiving reconciliation rows.

      Regenerate the list with this — it is the whole definition, and the
          `s.n <= 2` clause is the load-bearing part:

          ```sql
          WITH act AS (
            SELECT "attractionId" AS aid,
                   max(timestamp) FILTER (WHERE status='OPERATING') AS last_op,
                   max(timestamp) AS last_row, max("waitTime") AS max_wait
              FROM queue_data WHERE timestamp > now() - interval '400 days' GROUP BY 1
          ), cand AS (
            SELECT a.id, a.name, a."parkId", p.name AS park,
                   act.last_op::date AS d, act.max_wait
              FROM act JOIN attractions a ON a.id = act.aid
              JOIN parks p ON p.id = a."parkId"
             WHERE act.last_op < now() - interval '30 days'
               AND act.last_row > now() - interval '2 days'
               AND act.max_wait > 0            -- it really had a queue
               AND a.retired_at IS NULL
          ), same_day AS (SELECT "parkId", d, count(*) AS n FROM cand GROUP BY 1,2)
          SELECT c.park, c.name, c.d AS went_silent, c.id
            FROM cand c JOIN same_day s ON s."parkId" = c."parkId" AND s.d = c.d
           WHERE s.n <= 2                      -- individual, not a block
           ORDER BY c.park, c.name;
          ```

          **Research shape that works** (six subagents grouped by operator, since
          causes cluster there): each attraction gets one of PERMANENTLY_CLOSED /
          REFURBISHMENT / SEASONAL / STILL_OPERATING / UNKNOWN, and
          PERMANENTLY_CLOSED requires a **date and a source** or it does not count.
          Tell the agents explicitly that STILL_OPERATING is a normal answer —
          *Pooh's Hunny Hunt*, *Dumbo*, *Enchanted Storybook Castle*, *Marvel Cave*
          and *Shock Wave* are all in the list and are all landmarks of their parks.
          If those come back "retired", the research is wrong, not the parks.

          Traps found while preparing the batch: "Sea World" here is the **Gold
          Coast, Australia** park, not Orlando; `Skyride (Egypt Station)` and
          `Walibi Express Station 2` are **stations** of one ride; Universal
          Beijing's "The Wizarding World of Harry Potter" is a whole **land**; and
          `Coastersaurus - Currently Closed for Maintenance` carries a hint in its
          name that still needs verifying.
          Spread over ~20 parks, at most five each. The scattered dates are what
          separates them from the 67 whose whole block fell silent on one day —
          those are seasonal closures (Wet'n'Wild's 13+9 on 2026-06-29 is the
          Southern-Hemisphere winter, Bellewaerde's 5 on 2026-02-11 the Belgian
          one) and must NOT be retired.
          Each needs a **date and a source**, not just a verdict: `retired_at` wants
          the actual closure date where one is stated, and anything that turns out
          to still exist stays untouched.

- [x] Four already established and retired as the proof batch: Animal Kingdom's
      _Affection Section_ (2026-02-22), _Dino-Sue_ (2026-02-15) and _The
      Animation Experience_ (2026-02-23), Ocean Park's _North Pole Encounter_
      (2026-03-03).
- [ ] **`Expedition Everest - Legend of the Forbidden Mountain Single Rider` is
      not a retirement candidate — it is a data-model error.** It is a queue
      variant of a live roller coaster, not an attraction, so it should be
      merged into its parent or excluded at mapping time. Worth checking whether
      other parks have the same shape.

## Feed-dropped attractions still get marked seasonal (2026-08-15)

A ride that no upstream source reports any more now reads `UNKNOWN` instead of
CLOSED on all three surfaces. What is **not** fixed is what the detector does
with the same rows.

- [ ] `detect-seasonal` reads `current_status = 'CLOSED'` and the OPERATING
      history, both of which still describe the frozen feed. So the
      feed-dropped attractions keep being marked seasonal. **The population is
      142, not the ~140 this used to say** (measured 2026-09-11, PAR-38 — the
      figures are close by coincidence, not agreement): 125 genuinely silent
      rides plus Universal Studios Singapore's 17, whose feed
      moved to `show_live_data` and whose stale `attractions` rows carry the
      same frozen CLOSED history until PAR-159 retires them. The rest of the
      old ~140 were rides that were never silent at all. The months it
      derives are the observation-window artefact **at scale**: all 44
      Europa-Park rides carry the identical list `[1,2,3,4,5,6,12]`, which is
      simply "every month before the feed went silent". In August that reads as
      44 attractions out of season at a park in peak season.
      **(b) is built** — months need `MIN_OBSERVED_DAYS` (330) of watching, on
      attractions and shows alike. **(a) still to do**: the stored artefact
      months must be cleared once the guard is deployed, not before — Step 3 has
      no `is_seasonal = false` filter, so every still-closed candidate is
      re-evaluated on the next 2:30am run and a reset done first would simply be
      rewritten.
- [ ] Consider teaching the detector the same distinction the read path just
      learned: a row written by `system-reconciliation` is not evidence of a
      season. That is probably the cleaner fix than (b) alone.

## Upstream: ThemeParks.wiki dropped whole clusters of attractions

- [x] ~~Ten parks lost a block of attractions from the wiki's **live** feed on a
      single day each~~ — **answered 2026-09-11 against production (PAR-38).**
      The same query over a 270-day window returns **16 park rows (15 real
      parks) and 204 rides**, and they split three ways; the classification, the two checks it takes to
      tell them apart and the per-park verdicts are §5.2a of
      `docs/architecture/attraction-status-and-seasonality.md`. In short:
      **17** at Universal Studios Singapore were recategorised upstream
      (`ATTRACTION` → `SHOW`, ids unchanged, live data still arriving — already
      re-matched into `shows`, the stale `attractions` rows are what is left —
      PAR-159),
      **125** are genuinely silent — 106 of them have a wiki entity and it is
      still intact upstream; the other 19 are Queue-Times-only ids, one of which
      is its own defect (PAR-161) — and **62 were never silent at all** — the wiki or Queue-Times
      keeps writing CLOSED for them and they are simply shut (Wet'n'Wild in the
      southern winter, Traumatica until autumn, Ocean Park). Busch Gardens Tampa
      recovered on its own on 2026-08-17 after 65 days.

      Two claims in the old text were wrong and are worth not repeating:
      "every affected ride lacks a `queue_times_entity_id`" (**84 of the 204
      carry one**, and all nine at Busch Gardens Tampa carried one while they
      were out), and the implied mitigation of broadening Queue-Times matching
      — Queue-Times dropped the same ids the wiki did at Knott's, so there is
      nothing to broaden onto.
- [x] ~~**"Wet'n'Wild" and "Wet 'n' Wild Gold Coast"** both show 13 silenced
      attractions with the same date~~ — **confirmed a duplicate pair on
      2026-09-11**: identical coordinates to seven decimals, same city, same
      `park_type`, 13 attractions each. `GET /v1/admin/duplicate-parks` does not
      see it (`total: 0`): every branch of `findDuplicates` needs a name
      similarity ≥ 0.85 and the pair scores 0.6923 on ` Gold Coast` alone.
      Detector fix is PAR-160, the merge itself stays a separate decision —
      §5.5 of `docs/architecture/attraction-status-and-seasonality.md`.
- [x] **Europa-Park worked through, 2026-09-09** — all 45 of its silenced rides
      researched against the operator's own pages. Ten are free-flow (two of
      them written that day, audit rows `d98a7513-2799-42ae-8bf9-1a1d07d1fc54`
      for _Limerick Castle_ and `ed34c229-1220-4728-ba51-cc10527374ea` for
      _Paul's Playboat_, both without
      months because the operator lists them under all four seasons), 34 are
      operated rides that correctly stay `UNKNOWN`, and one could not be decided.
      Measured with the park `OPERATING`: 10 `OPERATING`, 35 `UNKNOWN`, no
      `CLOSED` — §2.3 is holding. Details and the two research traps:
      `docs/architecture/attraction-status-and-seasonality.md` §5.2.
- [ ] **Two Europa-Park rides have never once reported OPERATING.** _Vintage
      Cars_ (5714 rows) and _Yomi Adventure Trail_ (3636) carry nothing but
      CLOSED across their whole history, while the operator lists both as
      running attractions. They are not part of the feed drop — they have no
      `queue_times_entity_id` either, but they were never reported before it
      happened. Neither §2.3 nor §2.4 covers a ride that reports and only ever
      says closed.
- [ ] **Europa-Park _Rocking Bridge & Chute_ has no operator page.** It is on
      neither the attraction list nor the children's page, and
      `/de/attraktionen/wackelbruecke-mit-wendelrutsche` answers 403. Fan
      sources describe a children's play structure (tower, wobbly bridge,
      spiral slide, max 120 cm), which would be `open_with_park` — but §7 step 1
      rules that out as evidence, and its absence from the list may equally mean
      it no longer stands, which would be `retired_at`. Needs a source either
      way; it reads `UNKNOWN` until then.
- [x] ~~**The three unaccounted-for parks want finding.**~~ Found on 2026-09-11
      (PAR-38): re-running the original 120-day cut returns eight parks, and the
      three the 2026-08-15 table never listed are **Traumatica (7)**,
      **Mid-America Parks (9)** and **Universal Studios Japan (6)**. Seven
      listed plus these three is the "ten".
- [ ] **The other parks have not been swept, and the list of them shrank on
      2026-09-11** (PAR-38, §5.2a of
      `docs/architecture/attraction-status-and-seasonality.md`). Five of the six
      parks this item used to name need no sweep at all: Universal Studios
      Singapore's 17 were recategorised to `SHOW` and still report, both
      Wet'n'Wild rows and Ocean Park were never silent, and Busch Gardens Tampa
      came back by itself on 2026-08-17.

      That leaves **Rulantica (18)** from the original list, and the wider cut
      adds **Mid-America Parks (3)** — between them the only silent rides that
      are operating attractions rather than arcades, museums or out-of-season
      mazes. Six Flags Fiesta Texas (15), Knott's Berry Farm (9) and Universal
      Studios Hollywood (5) are silent too, but a Fright Fest maze out of season
      and an arcade with no queue are not free-flow candidates and a sweep would
      spend a day saying so.

## Free-flow attractions & seasonality (2026-08-15)

**Context:** `open_with_park` was only ever curated for Phantasialand. A sweep
found 76 name-matched candidates; 47 already report OPERATING (the feed handles
them), 4 sit in Hansa-Park where the flag deliberately cannot fire, and 25 were
researched one by one against the operators' own pages. 14 were flagged.

> **"The feed handles them" is not a durable property.** Europa-Park is the
> worked example, and it is the drop already documented in
> `docs/architecture/attraction-status-and-seasonality.md` §5.2 — 2026-06-07,
> 13:18 UTC. What that section counts is the 44 rides that lost the live feed;
> measured the other way, as rides with a long OPERATING history and not one
> OPERATING row since, it is 45, and the two sets are close but not the same
> question. Either way the point for *this* section is the inference, not the
> count: a candidate left unflagged because the feed happened to report it
> OPERATING has nothing holding it there. Our two Europa-Park water playgrounds
> are the proof — blanket OPERATING all winter, blanket CLOSED all summer,
> neither a statement about the playground. Read nothing on either side of that
> timestamp as evidence about a season.

**Seasonal free-flow areas whose park keeps running around them** — the case
`open_with_park` alone cannot express, because the flag says "open whenever the
park is" and these are not:

- [x] **The season gate exists now** — `isFreeFlowOpen` takes `seasonMonths` +
      the park timezone, and the detector no longer owns the months on a
      free-flow row. What is still missing is the months themselves.
- [x] **Curated, 2026-09-09** — Europa-Park _Lítill Island_ `[3–9]` and
      _Water Playground_ `[3–10]`, Everland _Snow playground_ `[12, 1, 2]`,
      Bellewaerde _Snowmen Playground_ `[11, 12, 1]`, all four with
      `open_with_park`. Sources on each audit row.

      The rule that answered "is 3 in or out?" — a month goes in unless the
      operator's season covers no more than a tenth of the days the **park** is
      open that month — is written down once, with its derivation, the
      `schedule_entries` query behind the denominator and the one call in this
      round that is not safe:
      `docs/architecture/attraction-status-and-seasonality.md` §7a.
- [ ] **Season unknown, water-based, park open year-round** — Peppa Pig
      _Muddy Puddles Splash Pad_, Walibi Rhône-Alpes _Exotic Island 3-6_ and
      _7-12_. Confirmed free-flow, but no source states an operating window, and
      a water play area plausibly closes in cold months. Held rather than
      guessed. Re-checked 2026-09-09 against the operators' own pages: neither
      states a window, so the hold stands rather than merely persists. (The
      splash pads at _seasonal parks_ — Water Country USA, Hurricane Harbor
      Arlington — were flagged: the park-status gate does the seasonal work
      there.)

**`season_months` can encode the observation window, not a season:**

- [ ] Phantasialand's Avoras, Berliner Eislaufen and Ice skate hire all have
      their first queue_data row on **2025-12-24** and all derived
      `season_months = [1, 12]`. For the two ice-rink attractions that is
      correct; for Avoras — advertised by the park as open _"ganzjährig"_ — it
      was pure artefact, and it read as out of season all summer.
      **The data alone cannot separate the two cases.** A guard ("only derive
      months once we have observed ≥ ~330 days") would drop the artefact but
      also drop the two correct labels, so it was not shipped. Revisit once
      history spans a full year for these attractions, or gate on observed span
      per attraction rather than globally.

**Attractions marked seasonal with no months at all:**

- [ ] Movie Park Germany's 9 Halloween Horror Fest mazes are correctly seasonal
      but have `season_months = NULL`: their first feed row is 2026-04-17, i.e.
      _after_ the last Halloween, so there is no observed operating month to
      derive from. The detector honestly writes NULL. Consequence:
      `isCurrentlyInSeason` is `null`, the frontend cannot distinguish "closed
      today" from "not in season", and they drag the park's operating count to
      22/39 in August. Do **not** hand-fill October — whether the feed even
      reports the mazes OPERATING during HHF is unproven. After HHF 2026 the
      detector will have real evidence.
- [ ] Related presentation gap: the API already knows `isSeasonal: true`. What a
      client cannot do is tell "out of season" from "no data", because
      `isCurrentlyInSeason` is `null` in both cases.

**Unresolved identities — concluded 2026-09-09, except where noted:**

- [x] Heide Park **"PLAYGROUND"** is **five playgrounds, not an artefact**. The
      upstream entity API returns five distinct coordinate pairs inside the park
      for external ids 15385, 15386, 15387, 15390 and 15391 — it simply gives
      all five the same name. Four of the five rows only appeared on 18/19
      August 2026, so the upstream was backfilling, not duplicating. The
      identity question is closed; whether they should carry `open_with_park`
      is a separate call nobody has made yet.
- [x] Plopsaland De Panne **"The Pirates' Playground"** is **no longer listed by
      the operator**. plopsa.com's sitemap (lastmod 2026-09-06) names
      `bumbas-speeltuin` and `willies-speeltuin` as the park's playgrounds and
      no pirate one in any of the four languages; the old URLs answer "Pagina
      niet gevonden". The pirate theming survives as a ride, a restaurant and a
      shop. **Deliberately not retired**: the evidence is an absence, and a
      retirement is a claim about the world that needs a date and a source.
      Neither exists. Revisit if the operator ever says so.
- [ ] LEGOLAND Korea **"Cole's Rock Climbing"** — still unstated by the
      operator. Re-checked 2026-09-09: the Korea page carries one sentence of
      body text and no restriction block at all. What is new is that LEGOLAND
      Japan types the same-named installation officially as "Play Area
      (Covered)", "Freely climb", height restriction "No limit", target age
      "all" — but that is the same cross-park inference the question already
      stalled on, so the flag stays off. **What would settle it**: a restriction
      line on the Korea page, or the park map's legend.
- [x] Toverland **"Kletterparcours"** — **retired 2026-09-09** with
      `retired_at = 2025-11-02`. Toverland's own blog says goodbye to it on 3
      November 2025, the 2004 high-ropes course made way for a holiday resort,
      and Looopings photographed the cleared site. It was never free-flow
      (harnessed, 140 cm minimum) — flagging it by name pattern would have
      advertised a demolished attraction as permanently open.
- [ ] Movie Park **"Teenage Mutant Ninja Turtles: License to Drive"** — **the
      feed is wrong, not the ride.** Movie Park lists it as a current
      nickelodeon LAND attraction, 121–150 cm, season "All Year". We hold 4737
      rows and every one says CLOSED, across two independent sources
      (themeparks-wiki to 2026-04-23, queue-times since), and ThemeParks.wiki's
      live endpoint now returns an empty `liveData`. It is not free-flow — a
      staffed children's driving school with a height window — so
      `open_with_park` is the wrong tool. The correction that fits is
      `curated_is_seasonal = false`; left unwritten here because it was outside
      the ticket that established this.

**The wider backlog (not yet touched):**

- [ ] The name-based net has a known hole: _Mopti's Monkey Depot_ contains no
      playground vocabulary and would never have matched. The behavioural net —
      attractions that never report OPERATING in a park whose feed demonstrably
      works — returns **453** rows. A sample shows it is dominated by Halloween
      event attractions, winter operations (_Curlingbaan_, _Schaatsbaan_,
      _Tubingbaan_), off-season water areas and genuinely defunct rides, so it
      needs a cheaper triage than per-attraction research before it is useful.

## Ride profiles: the safety nets that went with the seed (2026-08-15)

**Context:** [#163](https://github.com/PArns/v4.api.park.fan/pull/163) removed
`RIDE_PROFILE_SEED`, its spec, the mirrored term-id allowlist and the
`apply-ride-profiles` job. `attraction_ride_profiles` is now the source of truth
and is edited directly. Two things the job did for free are now nobody's job,
and both fail **silently** — that is what makes them worth tracking.

- [x] **Nothing validates term ids** — built: `GET /v1/admin/ride-profile-term-audit`
      diffs every stored id against `park.fan/api/glossary/term-ids` (which
      publishes the ids that actually resolve to a page) and names both the
      broken ids and the rides they shorten.
- [x] **Nothing evicts caches after a curation write** — built:
      `POST /v1/admin/publish-ride-profiles` evicts every park whose profiles
      carry a recent `seeded_at`, revalidates, and queues the post-CDN sweep.
- [x] **Der Audit laeuft jetzt von selbst** — taeglich 06:30 auf der
      manual-metadata-Queue. Er scheitert bewusst NICHT bei einer kaputten ID:
      die IDs sind in Ordnung, bis das Frontend eine Umbenennung deployt, und
      ein jede Nacht roter Job erzieht dazu, ihn zu ignorieren. Eine Warnung,
      die die IDs und die betroffenen Rides nennt, ist das Signal; der Endpoint
      bleibt fuer die Details. Ein unerreichbares Frontend wird als solches
      geloggt und nicht als Kurations-Problem.
- [ ] **`publish-ride-profiles` bleibt ein manueller Aufruf** — und das ist
      richtig so: er gehoert ans Ende einer Kuratier-Sitzung, und die ist
      ohnehin ein menschlicher Moment.

**Curation left deliberately open:**

- [ ] **Der Audit-Cron ist registriert, aber noch nie gelaufen.** `delayed: 1` auf
      der Queue belegt die Registrierung, der Fehlerpfad ist getestet — dass der
      Handler in Produktion durchlaeuft, zeigt erst der erste Lauf um 06:30.
      Danach einmal im API-Log nach `Ride-profile term audit clean` schauen.
- [ ] **Die Bull-Queue heisst weiter `manual-metadata`**, obwohl der Seed dieses
      Namens geloescht ist. Bewusst so gelassen: Bull schluesselt Repeatable-Jobs
      in Redis am Queue-Namen, ein Umbenennen wuerde
      `ride-profile-term-audit-cron` unter dem alten Namen stranden — registriert
      und von niemandem konsumiert. Sauber ginge es nur per Expand/Contract: neue
      Queue registrieren, alten Repeatable entfernen, dann die alte fallenlassen.
      Der Grund steht am Dekorator, die Klasse heisst inzwischen
      `CuratedDataProcessor`.
- [x] **Universal Studios Florida ist zusammengefuehrt.** Der Queue-Times-Datensatz
      (`universal-studios-at-universal-orlando`) ist im Wiki-Datensatz aufgegangen:
      32 Attraktionen und 313 Schedule-Eintraege migriert, der Park steht jetzt bei
      36 Attraktionen ohne Duplikate, alle 11 Ride-Profile erhalten. RCDB 3866 ist
      damit nicht mehr doppelt vergeben.
- [x] **`wait_time_predictions`: die 51.000 doppelten Primaerschluessel sind weg,
      und Hurricane Harbor ist zusammengefuehrt.** Timescale erzwingt den PK auf
      komprimierten Chunks nicht, also kollidierten dort Zeilen beim
      Dekomprimieren — womit JEDE Loeschung scheiterte, die einen betroffenen
      Chunk beruehrte. Der PK haengt an der Hypertable, nicht am Chunk, ein
      chunkweises Reparieren war also nicht moeglich. Entschieden wurde, die
      sieben betroffenen Chunks (2026-05-21 bis 07-09) zu droppen: 13,5 Mio.
      Zeilen Vorhersage-Historie, ~37 % der Tabelle. `drop_chunks` braucht keine
      Dekomprimierung und lief in Sekunden. Die Tabelle steht jetzt bei 23,0 Mio.
      Zeilen, aeltester Chunk 2026-07-09, alle verbliebenen Chunks duplikatfrei.
      Danach ging der Park-Merge ohne Weiteres durch (13 Attraktionen, 173
      Schedule-Eintraege).
- [ ] **Die Ursache der Duplikate ist NICHT gefunden.** Das Fenster war
      abgegrenzt (05-21 bis 07-09) und alles danach ist sauber, das Problem hat
      also von selbst aufgehoert — aber niemand weiss, warum es anfing oder
      warum es endete. Wenn es wiederkommt, faellt es erst auf, wenn wieder eine
      Loeschung scheitert. Ein billiger Waechter waere eine periodische Zaehlung
      doppelter Schluessel pro Chunk; die Query steht im Verlauf dieser Session.
- [ ] **prediction_accuracy fuer 2026-05-21 bis 07-09 hat keine Grundlage mehr.**
      Die Vorhersagen dieses Zeitraums sind geloescht; ausgewertete Kennzahlen,
      die noch darauf verweisen, stehen ohne Beleg da. Pruefen, ob dort etwas
      nachgezogen oder als luecke markiert werden muss.
- [ ] **Traumatica und Europa-Park bleiben getrennt** (bewusste Entscheidung:
      Traumatica ist ein eigenstaendiges Event mit eigenem Ticket, kein
      Duplikat). Folge davon: `matterhorn-blitz` und `pegasus` existieren als
      Attraktion unter beiden Parks und teilen sich je eine RCDB-ID (971, 3403).
      Das ist die einzige zulaessige Ausnahme von der Regel "eine ID, eine
      Attraktion" — dieselbe physische Bahn wird in zwei Veranstaltungen
      gelistet. Beim Duplikat-Check also nicht als Fehler werten.
- [ ] **`curated_may_get_wet` beweist sich erst beim naechsten Detail-Sync.**
      Aktuell stimmt sie ueberall mit `may_get_wet` ueberein, weil der geloeschte
      Seed seine Werte dort schon hineingeschrieben hatte. Die Divergenz — und
      damit der Beweis, dass die Korrekturschicht greift — entsteht erst, wenn
      der Sync Gentings Shot Tower wieder auf `true` setzt. Danach einmal
      pruefen, dass die Ride-Seite weiterhin `false` ausliefert.
- [x] **Runde 5 erledigt — der Cluster-Sweep ueber benannte Inversionen ist
      sauber.** Alle Listen, die eine benannte Inversion enthalten, werden jetzt
      nur noch von Rides geteilt, die auch dieselbe Inversionszahl melden. Was
      die Query noch ausgibt, sind die generischen duennen Listen
      (`lifthill → first-drop → helix → brake-run` und Verwandte, 54 Rides),
      Minenzuege und Holzachterbahnen — die fuehren gar keine benannte
      Inversion, dort kann die Liste nur unvollstaendig sein, nicht falsch.
- [x] **Runde 6 erledigt — die Cluster-Query ist ausgereizt.** Es gibt keine
      geteilte Elementliste mit einer benannten Inversion mehr, deren Rides
      unterschiedliche Inversionszahlen melden. Die verbliebenen Gruppen sind
      echte Klonfamilien (Boomerang 6, SLC 6, Double Loop Corkscrew 5,
      Sky Rocket II 4, Batman 2 …) und ihre Uebereinstimmung ist jetzt ein
      Konsistenz-Signal statt eines Alarms. Als naechstes Instrument braeuchte
      es etwas anderes als diese Query — z. B. Elementlisten, die zur
      veroeffentlichten Inversionszahl nicht aufgehen, ueber den ganzen Bestand
      statt nur ueber Cluster.
- [x] **Die doppelte RCDB-ID 3 ist bereinigt** — nur Gurnee traegt sie noch,
      Santa Clara steht auf NULL. (Nebenbei aufgefallen: La Ronde hat einen
      dritten Demon, ganz ohne ID.)
- [x] **Runde 3 erledigt** — Voltron, Banshee, blue fire, Fury (Bobbejaanland)
      und die zehn Marquee-Namen mit 0 Inversionen geprueft. Vier Fehler
      gefunden und korrigiert; Millennium Force, Taron, Shambhala, Silver Star,
      Kondaa und drei der vier Goliaths waren exakt richtig.
- [ ] **Untamed ist der letzte ungeklaerte RMC.** Steel Vengeance und Iron Gwazi
      trugen beide einen erfundenen `twisted-horseshoe-roll`; Untamed traegt
      ihn auch, und die Arithmetik geht mit ihm auf (5). Aber keine Quelle
      benennt die Figuren ausser der ersten — Wikipedia und RMC nennen dort
      einen „270° Double Inverting Corner Stall", fuer den es keinen Begriff
      gibt. Ohne Beleg nichts entfernt: Verdacht ist kein Nachweis. Braucht
      eine Quelle, die alle fuenf benennt (RMC-Projektseite oder POV-Zaehlung).
- [ ] **Helix' hintere Haelfte ist ungeklaert.** Wikipedia nennt einen Pretzel
      Knot, eine zweite Quelle stattdessen Inside Top Hat + Inline Twist. Beide
      koennen nicht stimmen; die Liste fuehrt deshalb nur die uebereinstimmenden
      Figuren und kommt auf 5 statt der veroeffentlichten 7.
- [ ] **`the-flying-dinosaur-2` ist wieder ein `-2`-Duplikat** — diesmal ein
      B&M Flying Coaster mit 90 min p90, also ein echter Headliner auf einem
      Duplikat-Datensatz.
- [ ] **`sky-loop` fehlt im Glossar.** Abismo (Madrid, RCDB 3185) ist der einzige
      je gebaute Maurer SkyLoop XT 450, und seine Signaturfigur — die
      herzförmige Schleife, in der der Zug sekundenlang kopfüber hängt — hat
      keinen Begriff. Die Elementliste führt jetzt Korkenzieher + Immelmann
      (so benennt die spanische Wikipedia die beiden Überschläge, und nur so
      geht die veröffentlichte 2 auf), aber die Figur selbst fehlt. Gleiche
      Klasse wie `cobra-loop` vor der Ergänzung.
- [ ] **Red Force: Modell umstritten.** Die DB sagt `Accelerator Coaster`,
      Wikipedia `LSM Launch Coaster`. Je eine Quelle, also nichts geändert —
      die Regel „bei Widerspruch nichts schreiben" gilt auch fürs Modell.
      Klärt eine dritte Quelle (Intamin-Projektseite, Ferrari Land), ob der
      Abschuss hydraulisch oder per LSM erfolgt.
- [ ] **Furius Baco: `overbank` ist unbelegt.** Die gefundene Quelle nennt vier
      Helices und keinen Overbank. Eine Quelle reicht mir nicht zum Ändern,
      aber das Element steht auf der Kippe.
- [ ] **26 Attraktions-Duplikate warten, vom Tooling bereits als sicher
      eingestuft** — `POST /v1/admin/merge-duplicate-attractions` findet sie im
      Trockenlauf: **Energylandia 23**, Heide Park 2, Six Flags New England 1.
      Es ist derselbe Cross-Source-Fall wie bei Magic Kingdom: eine Wiki-Zeile
      und eine Queue-Times-Zeile derselben Bahn. Sechs weitere stellt das
      Tooling bewusst zur Handpruefung zurueck, weil die Namen auseinandergehen
      (`Riptide Racer` vs `Riptide`, `Main Train` vs `Choco Chip Creek`) — die
      brauchen ein Urteil, keinen Batch-Lauf.

- [ ] **Hyperia** (Thorpe Park, RCDB 20652) — sources state 2, 3 _and_ 4
      inversions; Wikipedia contradicts itself within one article. The element
      list also calls its Immelmann non-inverting. Needs the park's own spec
      sheet or a POV count before the entry moves.
- [ ] **Zadra** (Energylandia, RCDB 16184) — publishes 3 inversions but the
      curated element list names 4 inverting figures. No source states the
      element order, so neither side can be corrected without inventing data.
- [ ] **Coasters with no RCDB id are unexamined.** Every ride that _has_ an id
      now has a profile (0 remaining). The coverage question is the other
      direction: 6,508 attractions have no profile, and while most are flat rides
      and shows, an unknown number are coasters that never got an id from the
      Wikidata match. Find them by `types`/name heuristics before deciding
      whether it is worth a pass.

**park.fan follow-ups from the same work:**

- [ ] **`scripts/export-glossary-term-ids.mjs` has no consumer.** Its whole
      purpose was writing the API allowlist that #163 deleted. It still runs and
      still prints a useful id list — decide whether to delete it or document it
      as a diffing tool, but do not leave it looking like a required step.
- [ ] **18 named figures have no 3-D player** (42 of 77 `coaster-elements` have
      one). Concepts and brakes legitimately have none; these are real shapes:
      `bowtie, butterfly, cobra-loop, cutback, dive-drop, flying-snake-dive,
    high-five, inline-twist, jojo-roll, norwegian-loop, predrop, pretzel-knot,
    splashdown, stall, stengel-dive, swing-launch, treble-clef, turntable`.
      `inline-twist` and `stall` are the highest-value — they appear most often
      in curated layouts. Build against `lib/three/coaster/elements.ts` and
      verify with `scripts/render-coaster-elements.mjs`, per the three.js
      convention.

**Observed while auditing, not acted on:**

- [x] **Animal Kingdom haelt keine Magic-Kingdom-Rides mehr** — 42 umgehaengt,
      35 Duplikate gemerged, der Park steht bei 24 statt 66 Attraktionen.

## Schedule times: 12-hour-clock rows need a curated override (2026-07-27)

**Context:** `normalizeClosingTime` (PR "repair closing times whose date contradicts
the opening") re-anchors a misdated closing time to the opening's park-local date.
That fixes 178 rows, but 7 of them stay wrong in a way no generic rule can repair —
the source reports `opens 15:00 / closes 12:00`, which is almost certainly a
12-hour-clock error where 12:00 means **midnight**:

| Park                   | Days                               | Reported      | After re-anchoring |
| ---------------------- | ---------------------------------- | ------------- | ------------------ |
| Six Flags Qiddiya City | 2026-04-17/24, 05-01/08/15         | 15:00 → 12:00 | 21 h day           |
| Kings Dominion         | 2026-09-18, 09-25 (Haunt evenings) | 18:00 → 12:00 | 18 h day           |

Those windows are **right during the actual event hours and wrong overnight** — a
strict improvement over the previous state (closing before opening ⇒ the park read
CLOSED all evening), but still not the truth. Kings Dominion's dates are upcoming
Halloween nights, so this has a real audience.

**Why a generic fix is wrong here:** "reinterpret 12:00 as 00:00" would silently
rewrite every legitimate noon closing (water parks and Christmas markets do close at
noon). The distinguishing signal is `closing < opening`, which the normalizer has
already consumed. Guessing further means inventing data.

**Built, 2026-09-11 (PAR-33):** the narrower option — a per-park
`curated_uses_twelve_hour_clock` flag, applied by `correctTwelveHourClockClose`
on the raw pair *before* `normalizeClosingTime`, only when the raw closing is
`< opening` **and** lands on exactly 12:00 park-local. It then reads as midnight
on the following park-local day. The static-file option above is dead:
`src/attractions/data/manual-attraction-metadata.ts` no longer exists, and
curated facts are sibling columns on the entity today.

- [x] The mechanism. `src/common/utils/operating-window.util.ts`, gated in
      `saveScheduleData`; the editor picks the field up from
      `PARK_CURATED_FIELDS` with no frontend change.
- [ ] **The curated value itself, for Six Flags Qiddiya City.** It is an admin
      write and the column only exists in production after this deploy, so it
      cannot be done in the same pass. Until it is written, the five rows below
      stay as they are.

**The population, re-measured against production on 2026-09-11 — it is smaller
than this section says.** Reported were 7 rows in 2 parks; the 12-hour-clock
pattern (closing exactly 12:00 park-local, on a later date than the opening)
now matches **5 rows in 1 park**:

| Park                   | Days                       | Stored        | Status |
| ---------------------- | -------------------------- | ------------- | ------ |
| Six Flags Qiddiya City | 2026-04-17/24, 05-01/08/15 | 15:00 → 12:00 | still wrong, 21 h day |
| Kings Dominion         | 2026-09-18, 09-25          | 18:00 → 00:00 | **correct since the 2026-08-09 sync** |

Kings Dominion's source publishes midnight properly now and
`normalizeClosingTime` rolls it forward on its own; there is nothing to flag
there, and flagging it anyway would be writing a curated fact nothing supports.
The `> interval '16 hours'` query below also returns 106 Fantawild rows that are
a different thing entirely (`00:00 → 23:59`, a near-full-day row), so prefer the
narrower predicate when looking for *this* fault:

```sql
SELECT p.name, s.date,
       (s."openingTime" AT TIME ZONE p.timezone)::time AS opens,
       (s."closingTime" AT TIME ZONE p.timezone)::time AS closes
FROM schedule_entries s JOIN parks p ON p.id = s."parkId"
WHERE (s."closingTime" AT TIME ZONE p.timezone)::time = '12:00:00'
  AND (s."closingTime" AT TIME ZONE p.timezone)::date
      > (s."openingTime" AT TIME ZONE p.timezone)::date;
```

**Not backfilled.** The flag changes what the next sync of that park writes; the
five stored rows are a separate decision.

**And that query finds candidates, not cases.** It reads stored rows, which are
post-normalization: a close stamped `12:00` on the *opening's* date (the fault —
re-anchored and rolled forward) is by then indistinguishable from one stamped
`12:00` on the *next* date (a source publishing an over-long day, stored
verbatim). The flag only fires on the first, and on the second it does nothing
without saying so. Check the raw payload's closing **date** before writing it on
a new park — `docs/admin/curation.md`.

**Also still open from the same sweep:**

- [ ] One `schedule_entries` row with an equal opening and closing (Universal Volcano
      Bay, dated `1970-01-01`). Left untouched by design — rolling it forward would
      invent a 24 h operating day. Decide whether to delete the sentinel row.
- [ ] ~127 duplicate `(parkId, date, scheduleType, attractionId)` groups, unrelated to
      the date repair (which deliberately scoped its dedupe to the days it touched).
      There is **no unique index** on that tuple; adding one would need the duplicates
      resolved first.
- [ ] After the first schedule sync on the new code (daily 15:00 UTC), confirm no
      **new** impossible windows appear — the repair proved the old rows are fixed,
      not that the write path holds:

  ```sql
  SELECT count(*) FROM schedule_entries
  WHERE "openingTime" IS NOT NULL AND "closingTime" IS NOT NULL
    AND ("closingTime" < "openingTime"
         OR "closingTime" > "openingTime" + interval '24 hours');
  ```

## PCN/Shape — deferred model work (gated on a clean board, PR #79 review)

Deliberately NOT shipped in PR #79 — code-side that PR is complete; these are
model/quality experiments that must wait until the shadow boards have matured
1–2 weeks on the fixed scorer (above), because each is judged on that board and
"nothing flips production without a busy/headliner win on clean evidence".
Rough order by ROI; each its own PR. Full rationale in
[docs/ml/pcn-intraday-review.md](docs/ml/pcn-intraday-review.md) (§5–6, §8).

- [x] **Receptive-field bake-off** — done and flipped: `PCN_GWN_LAYERS` defaults
      to **8** in `pcn-service/config.py`, so the served GraphWaveNet sees the
      whole 192-slot context instead of ~1 h. Won on the busy segment and the
      champion swap now serves PCN intraday. _(Note: review §7 table row 6 still
      reads "Default bleibt 2" — that line is stale, the code is the truth.)_
- [ ] **Lead-curve scoring from the stored fan** (review §3 / §7.7) — _partly done:_
      the `pcn_blend` shadow model in `pcn-service/score.py` keeps the persistence
      blend under live A/B (`pcn_forecasts` stays raw on purpose). What remains: the scorer
      currently joins only the freshest origin (≈15-min leads), so the quality of
      the actually-served longer leads (3–12h, rest-of-day) is unmeasured. Join the
      stored 48-slot fan at lead 1h/3h/6h vs actual + persistence baseline. A
      CatBoost head-to-head at long lead additionally needs the design-doc §12.3
      CatBoost co-snapshot (not implemented) — optional.
- [ ] **Feature channels** (review §5b) — DOW shipped; `is_holiday` was measured
      and **rejected** (clean A/B came out flat, so it is not in the channel set —
      don't re-add it without new evidence). Still open, each through the bake-off
      on busy-MAE/bias: `is_school_break`, schedule-relative time (minutes since
      open / to close), weather (the worst-MAE list — Cheetah Hunt / Wolfpack Raft
      Slide / Manta — is a water/outdoor cluster).
- [ ] **KPIs must follow the served model** (review §6a): "Live MAE 8.70" + the drift
      warning (24.58/20) still measure CatBoost-stored, but PCN serves intraday. Point
      the `prediction_accuracy` pipeline at the serving view (incl. PCN override) or
      add a second "served" panel; split the drift monitor by horizon (CatBoost drift
      is now a far-daily concern where it stays the sole level provider).
- [ ] **Shape offline-vs-live reconcile** (review §6b): offline claimed −7.4% busy,
      live board shows Shape losing everywhere (busy −3.9, bias −20). After the scorer
      fix, re-read the board; then check whether the _level_ Shape renders onto
      under-shoots busy days (bias −20 smells like a level, not a curve, error). No
      producer swap to `learned.py` before this is understood.
- [ ] **Cheap experiments from the design doc** (§11.5): Chronos-Bolt zero-shot as a
      foundation baseline (no training, instant comparison number) and TouringPlans
      pretraining seed against the ~6–7-month history gap. Weekend-sized, clear signal.
- [ ] **Per-park training hygiene** (review §5c): no validation split / early-stop
      (fixed 500 steps for a 10-ride park and a 100-ride park alike); one robust
      `_scale` per park (headliner + walk-on share a scale). Hold out the last day,
      early-stop, log per-park final loss so degenerate park models are visible before
      they serve.

## Days past midnight: verify after deploy, then let the frontend drop its hedge (2026-09-05)

The `/plan/day` wrap fix is in this repo (`unfoldedCloseHour`, changelog
"a park that closes after midnight had no plan at all"). Two things can only be
checked once it is live:

- [ ] **Confirm against the parks that wrap.** Six Flags Qiddiya City is the
      sharpest case — its rollup for the night of 2026-09-02 holds 16:00 through
      midnight with 13 rides still measured at hour 24, and the endpoint answered
      `rides: []` for it:

      ```
          curl -s 'https://api.park.fan/v1/parks/asia/saudi-arabia/al-moqbel-palaces/six-flags-qiddiya-city/plan/day?date=2026-09-02' \
            | jq '{ctx: .context | {openHour, closeHour}, rides: (.rides|length), lastHour: (.rides[0].hours|last)}'
          ```

          Expect `openHour: 16`, `closeHour: 0` and hours running past 23. Check a
          forecast day too (Parque Warner Madrid or Cedar Point on 31 October), and
          La Ronde, whose rollup is thin enough that it may still answer with few
          rides for reasons that have nothing to do with midnight.

- [ ] **Tell the frontend the contract is settled.** `estimateFor`
      (`lib/planner/estimate.ts` in the park.fan repo) looks a ride's curve up
      twice — once at the axis hour, once at the wall-clock hour — because
      nothing established which one `hours[].hour` carried on a wrap day. It
      carries the **unfolded** one (24 = midnight), so the second lookup can go,
      and `docs/…/parks-past-midnight.md` there wants the note it asks for.

## ML hourly_agg cache — post-deploy verification & follow-up

**Context:** `fetch_recent_wait_times` (`ml-service/predict.py`, the `WITH hourly_agg ...` query)
is the #1 steady-state DB load. Root cause: `base_time=datetime.now()` (microseconds) flowed
into the cache key → ~0% hit rate. Fix shipped: bucket `end_time` to the cache-TTL window +
raise TTL 2→15 min + evict expired entries on write.

**Baseline (pre-fix, measured 2026-06-03, ~133 min window):**

| fingerprint  | calls/min | ms/min (DB time)             | mean_ms |
| ------------ | --------- | ---------------------------- | ------- |
| 48c290bd     | 26.1      | 7196                         | 275.2   |
| 0f0d8d65     | 3.6       | 1355                         | 373.4   |
| **combined** | **~29.7** | **~8551** (≈14% of one core) | —       |

### Verification protocol (run AFTER deploy)

> **Stale as written (noted 2026-08-15):** the fix itself is long deployed —
> `predict.py` buckets the cache key and runs a 900 s TTL with eviction. Nobody
> ran the before/after comparison, and the 2026-06-03 baseline is now two months
> and several query changes old. Either re-baseline and measure, or close this
> out on the current slow-query log instead of resurrecting the old numbers.

- [ ] Confirm new ml-service container is live (Coolify redeploy done — module-global cache
      only resets on a fresh process, so the fix is NOT active until redeploy).
- [ ] `SELECT pg_stat_statements_reset();` on celestrial Postgres.
- [ ] Let it run ~30–60 min (cover ≥2 of the 15-min prediction crons + on-demand traffic).
- [ ] Re-run the baseline query (calls/min + ms/min for `query LIKE 'WITH hourly_agg%'`) and
      compare against the table above. Expect the on-demand/repeat-park calls to collapse.

Baseline SQL: `pg_stat_statements` joined with `pg_stat_statements_info`, normalize
`calls` and `total_exec_time` by `EXTRACT(EPOCH FROM now()-stats_reset)/60`.

### Decision gate — per-attraction caching (only if still hot)

If `hourly_agg` is still a top load after the fix, the remaining cost is the **single-attraction
path** `getAttractionPredictions` (`src/ml/ml.service.ts`, `attractionIds: [attractionId]`,
attraction-detail pages) which uses a per-single-attraction key and does NOT reuse the
park-level fetch (`predictForPark` → `activeAttractionIds`).

- [ ] If hot: cache the query result **split by attractionId** (safe — window functions are
      `PARTITION BY "attractionId"`, so each attraction's rolling values are independent).
      On read, assemble from per-attraction cache; query only the missing IDs. Then single-
      attraction and park-level paths share entries.
- [ ] If not hot: close this out, no further work.

---

## Remaining refactorings from the 2026-06 codebase review

The low-risk findings were fixed in PR #68 (N+1 batching, parallel weather sync,
shared live-data/pagination/cache-key helpers, dead TTL constants, luxon removal,
ML consistency cleanup, `safeJsonParse`, broken merge/repair cache invalidation).
What follows is the deliberately deferred rest — each item carries behavioral risk
and should be its own PR.

### 1. Tests for ShowsService / RestaurantsService (do this FIRST)

**Why first:** both services have **zero** spec files, and they are the precondition
for item 2 — refactoring untested sync code is how regressions ship.

**How:**

- Mirror the existing patterns in `src/attractions/services/attraction-integration.service.spec.ts`
  and `src/parks/parks.service.spec.ts` (repository mocks via `getRepositoryToken`,
  Redis mock as plain object).
- Priority coverage, in order:
  1. `shouldSaveShowLiveData` / `shouldSaveDiningAvailability` (delta-save contract:
     status change, showtimes/waitTime change, operating-hours change, day rollover
     via `hasDateChangedInTimezone`),
  2. `findBatchCurrentStatusByShows` (stale-showtime skip: OPERATING + lastUpdated > 48h → null;
     `projectShowtimesToToday` projection),
  3. `findTodayOperatingDataByPark` (timezone filter — feed rows across a midnight boundary),
  4. `syncShows` / `syncRestaurants` (upsert behaviour, slug uniqueness, wiki-only park filter).
- Effort: ~1 day. No production code changes needed.

### 2. Generic entity sync (attractions/shows/restaurants)

**Current state:** `syncAttractions`, `syncShows`, `syncRestaurants` share the
walk-parks → fetch-children → filter-type → prefetch-existing → upsert skeleton
(~80 duplicated lines), but differ on purpose:

- attractions: also syncs from Queue-Times (`qt-`) and Wartezeiten (`wz-`) sources,
- shows: batches updates (`toUpdate[]` + `Promise.all`) and inserts separately,
- restaurants: optional `deep` mode (per-entity `getEntity()` with fallback),
  prefetch via `In(apiExternalIds)`.

**How:** template-method base class, NOT full unification:

```ts
abstract class ThemeParksEntitySync<TEntity, TChild> {
  // template: park loop + isThemeParksWikiId() skip + children fetch + prefetch maps
  protected abstract filterChildren(children: EntityChild[]): TChild[];
  protected abstract mapChild(child: TChild, parkId: string): Partial<TEntity>;
  protected abstract persist(toInsert: ..., toUpdate: ...): Promise<void>; // strategies stay per-entity
}
```

Attractions keep their qt-/wz- branches OUTSIDE the template (only the wiki branch
moves in). Don't force `deep` into the template — keep it a restaurants-only hook.
**Prereq:** item 1. Effort: ~1–2 days incl. test updates.

### 3. Queue processor batch-loop helper (NOT a base class)

**Current state:** ~21 processors in `src/queues/processors/` repeat
logger + batch loop (`BATCH_SIZE = 5`) + success/failure counters + duration log.
Redis done-markers and error semantics vary too much for inheritance.

**How:** extract only the uniform part into `src/queues/utils/batch-runner.util.ts`:

```ts
export async function runInBatches<T>(
  items: T[],
  batchSize: number,
  worker: (item: T) => Promise<void>,
): Promise<{ succeeded: number; failed: number }>;
```

Adopt it opportunistically when touching a processor; don't do a big-bang rewrite.
Effort: helper ~1h, adoption incremental.

### 4. `any`-sweep in analytics.service.ts

**Current state:** ~12 `any`s in `src/analytics/analytics.service.ts`, mostly
untyped raw-SQL rows (`getRawMany()` results sorted/mapped with `(a: any, b: any)`).
Smaller offenders: `park-merge.service.ts`, `conflict-resolver.service.ts`,
`open-meteo.client.ts`, `file-logger.util.ts`.

**How:** for each raw query, declare a row type next to it (same pattern as the
`ExistingScheduleRow` type added to `parks.service.ts` in PR #68) and type the
`getRawMany<Row>()` call. Verify each field against the actual SELECT — numeric
columns come back as **strings** from pg (`parseFloat` sites are the tell).
Effort: ~0.5–1 day; mechanical but needs care with pg string-typing.

### 5. Search index bounding (only when the warning fires)

**Current state:** the 4 `loadXxxIndexFromDb` methods in `search.service.ts` are
unbounded full-table reads. There is no active/deleted flag to filter on, so a
naive LIMIT would silently drop entities from search. PR #68 added a size log +
a warning at >16 MB serialized.

**How (when the warning appears in logs):**

- preferred: add an `isSearchable`/popularity-derived flag and filter on it,
- or: split the Redis index into per-continent keys and lazy-load,
- or: switch serialization to msgpack/gzip (last resort, complexity for ~2–3x).

### 6. Python `features.py` vectorization (nice-to-have)

**Current state:** per-park `groupby` loops with `df.loc[idx, ...]` assignments for
timezone-local features (`ml-service/features.py` ~lines 51–114). Affects nightly
training wall-time only, not request latency.

**How:** group rows by timezone (not park), convert once per unique tz via
`df["timestamp"].dt.tz_convert(tz)`, assign back via `.loc[mask]`. Validate by
comparing feature output on a fixed dataset before/after (`verify_features.py`
exists for exactly this).

### 7. Decide: PoC scripts in nf-service

`nf-service/poc_eval.py` and `poc_eval_hourly.py` are standalone eval tools in the
same spirit as `backtest_*.py` / ml-service's `verify_*.py`. Kept for now.
Either document them in a README line each, or delete them. Owner call.
