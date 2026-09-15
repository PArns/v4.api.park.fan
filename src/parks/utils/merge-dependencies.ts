import { Logger } from "@nestjs/common";

export type MergeStrategy =
  "move" | "discard" | "winner-authoritative" | "custom";

export interface MergeDependency {
  /** Table holding rows that point at the entity being merged away. */
  table: string;
  /** Column on that table carrying the entity id. */
  column: string;
  /**
   * `move`    — reparent the loser's rows onto the winner (keeps the data).
   * `discard` — drop them; the winner's own row is authoritative and the
   *             value is derived, so keeping both would be meaningless.
   * `winner-authoritative` — move them only where the winner holds nothing;
   *             otherwise drop them, after logging what is being dropped —
   *             UNLESS the entry declares `richness` below, in which case the
   *             two rows are ranked and the richer one survives.
   *             For a row that is one per entity, hand-written and
   *             reproducible from no feed: `discard` would destroy a curation
   *             the winner has no equivalent of, and `move` cannot be used at
   *             all where the merge column is also the primary key, because
   *             the UPDATE then collides with the winner's own row.
   * `custom`  — none of the three shapes above fits, and `apply` below says
   *             what happens instead. The bar is the table's SHAPE rather than
   *             the awkwardness of its case: a table that names the entity in
   *             more than one column, or carries a constraint across two of
   *             them, cannot be expressed as one column plus a conflict key,
   *             because every statement the three strategies issue names
   *             exactly one column. Declared on exactly one entry
   *             (`attraction_review_marks`, PAR-149).
   */
  strategy: MergeStrategy;
  /**
   * Columns forming a unique key together with `column`. Loser rows whose key
   * already exists on the winner are deleted before the move — otherwise the
   * UPDATE violates the constraint and aborts the whole merge transaction.
   */
  conflictColumns?: string[];
  /**
   * How much a row of this table says, as a number. Only read by
   * `winner-authoritative`, and only where it is declared.
   *
   * Without it that strategy keeps the winner's row whenever the winner holds
   * one — which ranks two curations by which ATTRACTION happened to survive,
   * a property of the merge rather than of the rows. With it, the richer row
   * survives, and a tie keeps the winner's, so nothing is ever moved without a
   * strictly richer row to move.
   *
   * It is still a trade and not a pure gain: the richer row survives WHOLE, so
   * a field only the poorer row had goes with it — a survivor holding a model
   * name and an opening year loses both to a loser holding nine track elements
   * and neither. Under the old rule those two fields survived and the nine
   * elements did not. Merging the two rows field by field would end the trade
   * and is a third curation decision nobody has taken; recorded as PAR-208.
   *
   * Declared on exactly one entry (`attraction_ride_profiles`, PAR-179).
   * Leaving it off everywhere else is the decision, not an omission: for a
   * derived row "richer" says nothing, and for a curated one somebody has to
   * define what more means before a merge may act on it.
   */
  richness?: (row: Record<string, unknown>) => number;
  /**
   * What a `custom` entry does, and the only thing that may write for one.
   *
   * It hangs off the dependency rather than being called beside
   * `applyMergeDependencies` because there are four merge paths and all four
   * apply `ATTRACTION_DEPENDENCIES` — a call next to the list is four places
   * to forget it, and the guard that fails when a table has no strategy would
   * go on passing, because the table is declared either way.
   *
   * It receives the same transaction-bound manager the rest of the pass uses,
   * so a failure rolls the merge back like any other statement. It gets no
   * `dep`: the SQL of such an entry names its columns as literals (that is
   * what makes it custom), so there is nothing on the entry left to read.
   */
  apply?: (
    manager: MergeQueryRunner,
    winnerId: string,
    loserId: string,
  ) => Promise<void>;
}

/**
 * The columns of `attraction_ride_profiles` a person fills in, one point each
 * when set. Physical names, because `rideProfileRichness` is handed a raw row.
 *
 * Absent on purpose: `elements` and `types` (counted by length, not as a flag),
 * `stats` and `stats_updated_at` (imported, see below), `attractionId` /
 * `parkId` / `seeded_at` / `createdAt` / `updatedAt` (bookkeeping, set on every
 * row, so they would add the same constant to both sides of every comparison).
 *
 * A hand-written twin of the entity's column names, so
 * `merge-dependencies.spec.ts` checks every one of them against TypeORM's
 * metadata: a renamed column would otherwise drop out of the score in silence
 * and tilt the ranking towards deleting the row.
 */
export const CURATED_RIDE_PROFILE_FIELDS = [
  "manufacturer_name",
  "manufacturer_term_id",
  "model",
  "opened_year",
  "inversions",
  "curated_stats",
] as const;

/**
 * How much a curated ride profile says: one point per track element, one per
 * ride type, one per curated field that is set.
 *
 * The formula is PAR-179's, read literally — `curated_stats` is one column and
 * therefore one point, not one per measurement inside it. Counting its four
 * measurements separately was the first draft and is a different rule: a row
 * with no track elements at all would then outrank a three-element layout on
 * the strength of its speed, height, length and duration. Which of those two a
 * merge should keep is a curation question, and the decision on the ticket
 * says "Anzahl gesetzter Felder".
 *
 * The point of ranking at all is the stub. `AdminRideProfileService.upsert`
 * creates a row from a manufacturer name alone, with `elements` and `types`
 * empty — a profile scoring 1 against a fully walked-through layout scoring
 * twenty. Ranking by content is what stops the stub from outranking the layout
 * merely by sitting on the surviving row.
 *
 * `stats` and `stats_updated_at` are deliberately NOT counted: they are
 * imported from Wikidata by `RideStatsService`, so counting them would let an
 * automatic import outrank a curation — the exact inversion this function
 * exists to prevent.
 *
 * Takes a raw row (`SELECT *`), so the keys are the physical column names and
 * not the entity's properties.
 */
export function rideProfileRichness(row: Record<string, unknown>): number {
  const listLength = (value: unknown): number =>
    Array.isArray(value) ? value.length : 0;

  let score = listLength(row.elements) + listLength(row.types);

  for (const field of CURATED_RIDE_PROFILE_FIELDS) {
    if (row[field] !== null && row[field] !== undefined) score++;
  }

  return score;
}

/**
 * Rewrites a losing attraction's review marks onto the survivor.
 *
 * The one `custom` entry on any of these lists, because the table names an
 * attraction in TWO columns and holds a constraint across them
 * (`attraction-review-mark.entity.ts`):
 *
 *   - unique `(kind, attraction_id, other_attraction_id)`
 *   - CHECK `other_attraction_id IS NULL OR attraction_id < other_attraction_id`
 *
 * The CHECK is what rules the ordinary strategies out, not the second column
 * alone. A pair fact has no direction, so the pair is stored once in ascending
 * order; rewriting one of the two ids therefore has to restore that order in
 * the SAME statement, and a CHECK is not deferrable. Two declarations of the
 * same table — one per column — would each violate it halfway through.
 *
 * What the rows are: a person's verdict that a detector's candidate is not a
 * case ("these two are different rides", "this one is not retired"). Nothing
 * writes them but a human, and the FK is ON DELETE CASCADE on both columns, so
 * leaving the table off this list means every merge deletes half the verdicts
 * about the losing ride and keeps the other half pointing at a row that is
 * gone. Neither raises an error.
 *
 * Four cases, in the order the statements run:
 *
 *   1. The mark about EXACTLY this pair cannot survive in any form: rewritten
 *      it reads "A is not a duplicate of A" and violates the CHECK. For a
 *      `not_a_duplicate` mark it is also the verdict this merge contradicts,
 *      which is why it is logged with its `reason` before it goes — that
 *      sentence and its URL are the only record that somebody once decided the
 *      opposite.
 *   2. A pair mark whose partner the winner already has a verdict on: the
 *      winner's stands and the loser's is logged and dropped, because after
 *      the merge both name one pair and the unique index allows one.
 *   3. Every other pair mark moves, canonical order restored in the same
 *      statement.
 *   4. A single-attraction mark (`other_attraction_id IS NULL`) moves as it is.
 *      It is never deduped: Postgres treats NULLs in a unique index as
 *      distinct, so two `not_retired` rows on one attraction are a state the
 *      table already permits, and both readers of this table are anti-joins
 *      (`WHERE m.id IS NULL`) that a second row cannot disturb. Dropping one
 *      would destroy a human's reason to keep a row the index does not object
 *      to.
 *
 * Both deletes report through `RETURNING` rather than reading first and
 * deleting second the way `applyWinnerAuthoritative` does. That branch has to
 * read anyway — the read is what it decides on. Here the read would exist only
 * to log, and a line written from `RETURNING` describes rows that really were
 * deleted rather than rows a failing statement left standing.
 *
 * Both are wrapped in a CTE for that, and the wrapper is load-bearing rather
 * than a flourish: TypeORM's postgres driver rewrites the result of a bare
 * DELETE or UPDATE into `[rows, rowCount]` (`PostgresQueryRunner`, `switch
 * (raw.command)`), so `RETURNING *` would arrive as a two-element array whose
 * second element is a number — and every check below would then find two
 * "rows" to warn about on every merge, whether anything was deleted or not.
 * Selecting from the CTE makes the command a SELECT, and the rows come back as
 * rows. `MLService.deleteOldPredictions` wraps its DELETE for the same reason.
 */
export async function mergeAttractionReviewMarks(
  manager: MergeQueryRunner,
  winnerId: string,
  loserId: string,
): Promise<void> {
  // The same refusal `applyMergeDependencies` and `planWinnerAuthoritative`
  // make, and for the sharper reason: with one id on both sides the second
  // DELETE's EXISTS matches every pair mark against ITSELF — the partner is the
  // row's own other half and the winner is the row's own first half — so it
  // would delete every pair verdict the attraction carries. Every caller checks
  // today; this is so the one that stops checking fails instead.
  if (winnerId === loserId) {
    throw new Error(
      `Cannot merge review marks with one id on both sides (${winnerId})`,
    );
  }

  // Almost no ride carries a review mark, so the ordinary merge is one index
  // lookup instead of five statements.
  const touching = asRows(
    await manager.query(
      `SELECT 1 FROM attraction_review_marks
        WHERE attraction_id = $1::uuid OR other_attraction_id = $1::uuid
        LIMIT 1`,
      [loserId],
    ),
  );
  if (touching.length === 0) return;

  const mutual = asRows(
    await manager.query(
      `WITH dropped AS (
         DELETE FROM attraction_review_marks
          WHERE (attraction_id = $1::uuid AND other_attraction_id = $2::uuid)
             OR (attraction_id = $2::uuid AND other_attraction_id = $1::uuid)
          RETURNING *
       ) SELECT * FROM dropped`,
      [winnerId, loserId],
    ),
  );
  if (mutual.length > 0) {
    logger.warn(
      `🗑️  attraction_review_marks: dropping ${mutual.length} mark(s) about ` +
        `${winnerId} and ${loserId} themselves — the merge answers the ` +
        `question they record — ` +
        mutual.map((row) => JSON.stringify(row)).join(" | "),
    );
  }

  // The partner id is the half of the pair that is NOT the loser. Spelled out
  // twice rather than through a LATERAL, so the statement stays one a reader
  // can run by hand against a pair of ids.
  const partner = `CASE WHEN m.attraction_id = $2::uuid
                        THEN m.other_attraction_id ELSE m.attraction_id END`;
  const superseded = asRows(
    await manager.query(
      `WITH dropped AS (
         DELETE FROM attraction_review_marks AS m
          WHERE m.other_attraction_id IS NOT NULL
            AND (m.attraction_id = $2::uuid OR m.other_attraction_id = $2::uuid)
            AND EXISTS (
                  SELECT 1 FROM attraction_review_marks AS w
                   WHERE w.kind = m.kind
                     AND w.attraction_id = LEAST($1::uuid, ${partner})
                     AND w.other_attraction_id = GREATEST($1::uuid, ${partner})
                )
          RETURNING *
       ) SELECT * FROM dropped`,
      [winnerId, loserId],
    ),
  );
  if (superseded.length > 0) {
    logger.warn(
      `🗑️  attraction_review_marks: dropping ${superseded.length} mark(s) ` +
        `whose pair ${winnerId} already carries a verdict on — ` +
        superseded.map((row) => JSON.stringify(row)).join(" | "),
    );
  }

  // Three moves, one per shape, and they cannot overlap: the first leaves no
  // row naming the loser in `other_attraction_id` (it writes GREATEST of the
  // winner and the partner) and touches no row with a NULL partner, and the
  // second cannot produce the loser's id either, because after the delete
  // above no surviving row names both sides.
  await manager.query(
    `UPDATE attraction_review_marks
        SET attraction_id = LEAST($1::uuid, other_attraction_id),
            other_attraction_id = GREATEST($1::uuid, other_attraction_id)
      WHERE attraction_id = $2::uuid AND other_attraction_id IS NOT NULL`,
    [winnerId, loserId],
  );
  await manager.query(
    `UPDATE attraction_review_marks
        SET attraction_id = LEAST(attraction_id, $1::uuid),
            other_attraction_id = GREATEST(attraction_id, $1::uuid)
      WHERE other_attraction_id = $2::uuid`,
    [winnerId, loserId],
  );
  await manager.query(
    `UPDATE attraction_review_marks
        SET attraction_id = $1::uuid
      WHERE attraction_id = $2::uuid AND other_attraction_id IS NULL`,
    [winnerId, loserId],
  );
}

/**
 * Every table referencing `attractions`, with what a merge must do to it.
 *
 * Verified against the live catalog and exercised end-to-end by a rollback-only
 * cold run of the Universal Studios Hollywood merge (29 colliding rides): the
 * sequence below completes with zero queue_data rows lost and zero orphans.
 *
 * The previous hard-coded list covered 7 of these 19. The gaps produced, in
 * order of how the cold run hit them:
 *   1. prediction_accuracy   — unique (attraction_id, target_time) → 23505
 *   2. ml_prediction_anomalies — FK NO ACTION → 23503 on the attraction DELETE
 *   3. attraction_hourly_history / rope_drop / typical_waits / p90 → silent CASCADE
 *   4. pcn/shape/tft/catboost forecasts, day_operating, aggregates → orphans
 *
 * PAR-149 added four more, found by re-deriving the list from the entities
 * rather than from that run: `ride_alerts`, `schedule_entries` and
 * `attraction_review_marks` all cascade, `prediction_lead_snapshots` has no FK.
 * They postdate the cold run and are pinned by `merge-dependencies.spec.ts`.
 */
export const ATTRACTION_DEPENDENCIES: MergeDependency[] = [
  // --- time series: always keep, never collides on its own surrogate key ---
  { table: "queue_data", column: "attractionId", strategy: "move" },
  { table: "forecast_data", column: "attractionId", strategy: "move" },

  // --- keyed history and predictions: dedupe on the key, then keep the rest ---
  {
    table: "wait_time_predictions",
    column: "attractionId",
    strategy: "move",
    conflictColumns: ["predictionType", "createdAt", "predictedTime"],
  },
  {
    table: "prediction_accuracy",
    column: "attraction_id",
    strategy: "move",
    conflictColumns: ["target_time"],
  },
  {
    table: "attraction_hourly_history",
    column: "attractionId",
    strategy: "move",
    conflictColumns: ["date"],
  },
  {
    table: "attraction_day_operating",
    column: "attractionId",
    strategy: "move",
    conflictColumns: ["op_day"],
  },
  {
    table: "attraction_outages",
    column: "attractionId",
    strategy: "move",
    conflictColumns: ["started_at"],
  },
  {
    table: "attraction_exposure_days",
    column: "attractionId",
    strategy: "move",
    conflictColumns: ["op_day"],
  },
  {
    // The per-ride half of a table the park side deliberately keeps out of
    // every dependency list. There the rows are park-level (`attractionId IS
    // NULL`) or per-ride and told apart by that nullable column, which a
    // row-wise `IN` cannot compare — so both raw paths do it by hand with `IS
    // NOT DISTINCT FROM`.
    //
    // None of that applies on this side, which is why the same table can be a
    // plain `move` here: `WHERE "attractionId" = $loser` has already excluded
    // every park-level row, and `(date, scheduleType)` carries no nullable
    // column. The key is the one `mergeParks` uses for the park-level move.
    //
    // Without it the FK — ON DELETE CASCADE, nullable — takes the losing
    // ride's whole schedule with it: its maintenance days, its ticketed
    // evenings, everything `/plan/day` reads to know when that ride runs.
    //
    // On `mergeParks` the rows this saves are then taken by something else,
    // and it is not this entry's bug to fix: step 3 there dedupes the table
    // park-wide on `["date", "scheduleType"]` with no `attractionId`, so a
    // per-ride row is deleted whenever the surviving park holds ANY row of
    // that type that day — usually its own park-level OPERATING row. That is
    // PAR-171, it predates this entry, and the two raw paths already avoid it
    // with `IS NOT DISTINCT FROM`. So this saves the ride's schedule on the
    // attraction-merge path today and on the park path once PAR-171 lands.
    table: "schedule_entries",
    column: "attractionId",
    strategy: "move",
    conflictColumns: ["date", "scheduleType"],
  },
  {
    // No FK at all, so forgetting it leaves orphans rather than raising —
    // the `pcn_forecasts` failure mode one table over. Its PK is the whole
    // key `(attraction_id, target_date, lead_days)`, and one target day
    // accumulates one row per lead distance, so a loser row for a day and a
    // distance the winner already sampled has to go before the move.
    //
    // `move` rather than `discard` although the numbers are model output: the
    // table exists precisely because its rows cannot be recomputed later — a
    // prediction made 60 days out is deleted and rewritten by every nightly
    // run until its target date arrives, and this is the copy that survives to
    // be scored. Discarding them would not lose a derived figure, it would
    // lose the only measurement of how wrong a long-lead forecast was.
    table: "prediction_lead_snapshots",
    column: "attraction_id",
    strategy: "move",
    conflictColumns: ["target_date", "lead_days"],
  },
  {
    // Discarded rather than moved: the profile is an aggregate over a window
    // that the merge has just invalidated, and the survivor is skipped by the
    // reconstruction until `last_merged_at` falls out of range anyway. Carrying
    // the loser's numbers across would publish a figure about a ride that no
    // longer exists in that shape.
    table: "attraction_downtime_profiles",
    column: "attractionId",
    strategy: "discard",
  },
  {
    table: "headliner_attractions",
    column: "attractionId",
    strategy: "move",
    conflictColumns: ["parkId"],
  },

  // --- ML forecast tables: no FK, so silent orphans if forgotten ---
  {
    table: "pcn_forecasts",
    column: "attraction_id",
    strategy: "move",
    conflictColumns: ["target_slot", "origin_slot", "quantile"],
  },
  {
    table: "shape_forecasts",
    column: "attraction_id",
    strategy: "move",
    conflictColumns: ["target_slot", "origin_date"],
  },
  {
    table: "tft_forecasts",
    column: "attraction_id",
    strategy: "move",
    conflictColumns: ["target_date", "forecast_date"],
  },
  {
    table: "catboost_daily_forecasts",
    column: "attraction_id",
    strategy: "move",
    conflictColumns: ["target_date", "forecast_date"],
  },
  { table: "queue_data_aggregates", column: "attractionId", strategy: "move" },
  {
    table: "ml_accuracy_comparisons",
    column: "attractionId",
    strategy: "move",
  },
  // FK is NO ACTION: leaving these behind blocks the DELETE outright.
  {
    table: "ml_prediction_anomalies",
    column: "attraction_id",
    strategy: "move",
  },

  // --- one derived row per attraction: the winner's own is authoritative ---
  {
    table: "attraction_accuracy_stats",
    column: "attraction_id",
    strategy: "discard",
  },
  {
    table: "attraction_p50_baselines",
    column: "attractionId",
    strategy: "discard",
  },
  {
    table: "attraction_p90_baselines",
    column: "attractionId",
    strategy: "discard",
  },
  {
    table: "attraction_rope_drop",
    column: "attractionId",
    strategy: "discard",
  },
  {
    table: "attraction_typical_waits",
    column: "attractionId",
    strategy: "discard",
  },

  // --- curated, one row per ride, reproducible from no feed ---
  {
    // The odd one out on this list, and the reason `winner-authoritative`
    // exists. Its three neighbours above — rope drop, typical waits, the two
    // baselines — are `discard` because they are DERIVED: the nightly jobs
    // rewrite the survivor's row from the queue history that has just moved
    // onto it. This row is not derived from anything: there is no feed and no
    // seed job, only an editor typing the track elements, the ride types and
    // the builder — by hand into the database, or through
    // `AdminRideProfileService.upsert`. A cascade — the FK is ON DELETE
    // CASCADE — takes that with no way back, and no job would rebuild it.
    // Same family as `park_seasons` and `park_slug_aliases` one list over.
    //
    // `move` is not available: `attractionId` is the merge column AND the
    // primary key (`@PrimaryColumn("uuid") attractionId`), so an UPDATE onto a
    // survivor that already has a profile is a PK violation that rolls the
    // whole merge back. `conflictColumns` cannot express it either — that
    // dedupe deletes the LOSER's row whenever the key collides, which here is
    // every single time the survivor has a profile, i.e. `discard` wearing a
    // different hat.
    //
    // So: the loser's profile is inherited into an empty cell, and where the
    // survivor already holds one the two are RANKED — `richness` below — and
    // whichever says less is logged and dropped. A tie keeps the survivor's.
    // The log line is the same bargain `logDroppedCuration` strikes for the
    // curated park columns, and for the same reason: a hand-written value that
    // ceases to exist should at least leave a line somebody can find.
    //
    // Ranking is PAR-179's correction to PAR-105, which decided the survivor's
    // profile always wins. That rule ranked two curations by which ATTRACTION
    // survived, and `AdminRideProfileService.upsert` creates a row from a
    // manufacturer name alone — so a one-field stub on the survivor beat a
    // fourteen-element layout on the loser, and the layout was gone. The same
    // rule also let the order of an unordered SELECT decide, wherever one merge
    // folds two losers into one survivor (`ParkMergeService.migrateEntities`
    // matches by slug OR name, and a name is not unique inside a park): the
    // first loser's row was inherited, and the second was then measured against
    // THAT row. Ranking by content answers both, because it does not depend on
    // who survived or on who arrived first.
    //
    // This is the only entry on any of these lists that declares `richness`,
    // and per PAR-179 it stays that way: "the winner's row wins" remains the
    // rule everywhere else.
    //
    // The park-side twin of this entry (`PARK_DEPENDENCIES`, column `parkId`,
    // `move`) is a different job and both are needed: that one carries the
    // denormalised parkId of a profile whose ride has already moved, this one
    // decides what happens when the ride itself is merged away. Every park
    // path runs the attraction step first, so they compose in that order.
    //
    // Postdates the cold run named above — it is pinned by
    // `merge-dependencies.spec.ts` rather than by a production rehearsal.
    table: "attraction_ride_profiles",
    column: "attractionId",
    strategy: "winner-authoritative",
    richness: rideProfileRichness,
  },

  // --- rows a person set by hand, one of them a stranger's ---
  {
    // The attraction-side twin of `show_follows`, named as such in that
    // entry's comment since PAR-104 and missing from this list until PAR-149:
    // same ON DELETE CASCADE, same unique key on the subscriber, same owner —
    // somebody outside this project who would simply never hear from us again.
    // A merge deleted their armed alert inside a transaction that then
    // reported success.
    //
    // Deduping on the subscriber is what the unique index asks for: one alert
    // per subscriber per ride. Which of two rows survives is arbitrary where
    // they differ — `thresholdMinutes` may be 20 on one and 40 on the other,
    // and the winner's wins — and that is the same open product question
    // `show_follows` records as PAR-151 rather than a second one. In every
    // branch the subscriber keeps a working alert for the ride they asked
    // about, which is what the CASCADE took away.
    table: "ride_alerts",
    column: "attractionId",
    strategy: "move",
    conflictColumns: ["subscriptionId"],
  },
  {
    // Two attraction columns and a CHECK across them, so no combination of
    // `column` and `conflictColumns` can express it — see
    // `mergeAttractionReviewMarks` for the four cases and why each is what it
    // is. The entry still names a table and a column because the guard reads
    // `table` and `ParkMergeService`'s identifier allowlist reads `column`;
    // the statements themselves name their columns as literals.
    table: "attraction_review_marks",
    column: "attraction_id",
    strategy: "custom",
    apply: mergeAttractionReviewMarks,
  },
];

/**
 * Every table referencing `shows`, with what a merge must do to it.
 *
 * A show collides for the same reason a ride does, and more often: the unique
 * index is `(parkId, slug)` (`show.entity.ts`), and two park rows from two
 * sources describe one park, so a shared slug is the normal case rather than
 * the exception. The blind `UPDATE shows SET "parkId"` both raw paths in
 * `parks.service.ts` used to issue therefore raised **23505** and rolled the
 * whole merge back before the attraction and park steps ran at all.
 *
 * Resolving the collision means deleting the losing show, and the three tables
 * below fail in three different ways when that happens without them — the same
 * spread the attraction list above was written against:
 *
 *   - `show_live_data`       FK CASCADE   → the whole showtime history of the
 *                            losing row disappears inside a transaction that
 *                            then reports success. It is the only record of
 *                            what that show ever played.
 *   - `show_follows`         FK CASCADE   → somebody's push reminder for that
 *                            show is deleted with it. Nothing tells them; the
 *                            notification simply never arrives.
 *   - `show_schedule_patterns` no FK at all → the rows survive pointing at a
 *                            show that is gone.
 *
 * All three merge paths apply this list: the two raw ones in
 * `parks.service.ts`, and `ParkMergeService.consolidateEntityData`, which used
 * to apply a dependency list to attractions only and end on "Note: Add
 * show/restaurant specific consolidation if needed" (PAR-150). It answers that
 * comment with this list rather than a second one: two different answers to one
 * question are worse than the open question.
 */
export const SHOW_DEPENDENCIES: MergeDependency[] = [
  {
    // The show's own time series, and a hypertable — the caller must have
    // lifted `timescaledb.max_tuples_decompressed_per_dml_transaction`, which
    // both raw paths do at the top of the merge transaction. PK is
    // `(id, timestamp)`, its own surrogate id, so changing `showId` cannot
    // collide and no snapshot is ever dropped.
    table: "show_live_data",
    column: "showId",
    strategy: "move",
  },
  {
    // The projected weekday programme (`ShowSchedulePattern`). PK is
    // `(show_id, weekday)`, so the winner's own row for a weekday wins and the
    // loser's remaining weekdays fill the gaps.
    //
    // `move` rather than `discard` although `rebuildSchedulePatterns` replaces
    // the table wholesale every night: the rebuild reads `show_live_data`,
    // which has just moved onto the survivor, so it would reach the same answer
    // — but not before tomorrow. Moving the rows costs one statement and keeps
    // `/plan/day` projecting the show's Saturday in the meantime.
    table: "show_schedule_patterns",
    column: "show_id",
    strategy: "move",
    conflictColumns: ["weekday"],
  },
  {
    // A visitor's reminder — set by hand, like `park_seasons` and
    // `attraction_ride_profiles` on the park list, and the only one of the
    // three whose owner is a stranger who would simply never hear from us
    // again. Unique on `(subscriptionId, showId)`, so a subscriber who
    // followed both rows keeps the winner's: the unique index allows one
    // reminder per subscriber per show, and a second row would be a second
    // notification for the same performance.
    //
    // Which of the two survives is arbitrary where they mean different things
    // — `startTime` null is "whichever is next" (the card's bell) and a set one
    // names a single performance (a showtime badge), and the winner's row wins
    // either way. Recorded as PAR-151 rather than decided here: before this
    // list existed the CASCADE deleted the losing row outright, so the
    // subscriber now keeps a working reminder for the same show in every
    // branch, and picking between them is a product question.
    //
    // `ride_alerts` is the attraction-side twin of this row, with the same
    // CASCADE and the same unique `(subscriptionId, attractionId)`. It was off
    // `ATTRACTION_DEPENDENCIES` until PAR-149 and is on it now, deduped on the
    // same column, so the two sides finally decide the same way.
    table: "show_follows",
    column: "showId",
    strategy: "move",
    conflictColumns: ["subscriptionId"],
  },
];

/**
 * Every table referencing `restaurants`, with what a merge must do to it.
 *
 * The same unique `(parkId, slug)` as shows (`restaurant.entity.ts`) and the
 * same 23505, with one dependent table rather than three: nobody follows a
 * restaurant and nothing projects its week.
 */
export const RESTAURANT_DEPENDENCIES: MergeDependency[] = [
  {
    // PK `(id, timestamp)`, hypertable, FK CASCADE — exactly `show_live_data`'s
    // case one table over.
    table: "restaurant_live_data",
    column: "restaurantId",
    strategy: "move",
  },
];

/**
 * The two child entities a park merge reparents wholesale, each with the
 * dependency list of its own losing rows.
 *
 * A closed constant rather than a parameter, because the table name is
 * interpolated into SQL: a caller that takes it from the outside has to prove
 * it is a bare identifier, and a caller that iterates this cannot be handed
 * anything else. Attractions are deliberately absent — they need land columns
 * merged and `last_merged_at` stamped before the losing row goes, which is
 * neither of these two entities' case.
 */
export const PARK_CHILD_ENTITIES = [
  { table: "shows", dependencies: SHOW_DEPENDENCIES },
  { table: "restaurants", dependencies: RESTAURANT_DEPENDENCIES },
] as const;

/**
 * Tables from `known` that no show merge strategy is declared for.
 *
 * `external_entity_mapping` is excluded the same way it is on the attraction
 * side: it is keyed on `internal_entity_id` across every entity type at once
 * and is moved by the caller, before the dependencies.
 */
export function showTablesMissingFrom(known: string[]): string[] {
  const declared = new Set([
    ...SHOW_DEPENDENCIES.map((d) => d.table),
    "external_entity_mapping",
  ]);
  return known.filter((table) => !declared.has(table)).sort();
}

/** The restaurant counterpart of `showTablesMissingFrom`. */
export function restaurantTablesMissingFrom(known: string[]): string[] {
  const declared = new Set([
    ...RESTAURANT_DEPENDENCIES.map((d) => d.table),
    "external_entity_mapping",
  ]);
  return known.filter((table) => !declared.has(table)).sort();
}

/**
 * Park-scoped tables the merge must handle beyond the nine it already migrates.
 *
 * `park_slug_aliases` is the one with teeth: its FK is ON DELETE CASCADE, and
 * those rows are what keep already-indexed URLs resolving after a rename. A
 * merge that ignores them destroys that history silently.
 *
 * The `attraction_*` entries carry a denormalised parkId alongside their
 * attractionId. Reparenting an attraction leaves that column pointing at the
 * deleted park, so they need updating even though the attraction row itself
 * moved correctly.
 */
export const PARK_DEPENDENCIES: MergeDependency[] = [
  {
    table: "park_slug_aliases",
    column: "parkId",
    strategy: "move",
    // All four slugs, because the unique index is all four
    // (`park-slug-alias.entity.ts`) and it does not include `parkId` — a path
    // is unambiguous across the whole table, not per park. On `slug` alone the
    // dedupe DELETE read across the other three and dropped ghost rows that
    // could never have collided: `disneyland-park` is Anaheim AND Paris, so a
    // ghost holding `north-america/united-states/anaheim/disneyland-park` lost
    // its redirect to a winner holding the Paris path, silently and for good —
    // these rows have no feed and no way back. Written out, the key is inert:
    // two rows cannot share a path, so the DELETE matches nothing, and the
    // UPDATE cannot violate the index either because it changes `parkId` and
    // the indexed tuple stays put. That is the point. The narrower key was the
    // only thing here that could destroy a row, and it is the one column of the
    // four that repeats across cities.
    conflictColumns: ["continentSlug", "countrySlug", "citySlug", "slug"],
  },
  {
    // The same "curated, cascade-deleted, irreplaceable" case as the aliases
    // above. Nothing syncs a season — every row is a person reading a park's
    // own calendar — and the FK is ON DELETE CASCADE, so leaving it off this
    // list means a merge deletes it inside the transaction and reports
    // success. `park_id`, not `parkId`: the column name goes into raw SQL and
    // this table has no camelCase one. No conflictColumns — the table has no
    // unique key, and two overlapping seasons after a merge are a curation
    // question, not a constraint violation.
    table: "park_seasons",
    column: "park_id",
    strategy: "move",
  },
  { table: "park_p90_baselines", column: "parkId", strategy: "discard" },
  {
    table: "weather_warnings",
    column: "parkId",
    strategy: "move",
    conflictColumns: ["alertId"],
  },
  {
    // The third of the "curated, cascade-deleted, irreplaceable" cases, and
    // the one that had gone unnoticed because the merge used to abort before
    // reaching the park DELETE. Same lifecycle as `park_seasons`: no feed and
    // no seed job — the rows ARE the source of truth, written only by a person
    // (straight into the database, or through `AdminRideProfileService`), so a
    // cascade takes a ride's track elements, its ride types and its builder
    // with no way back and nothing would rebuild them. The
    // parkId beside the attractionId is the denormalised one, so it has to
    // move for every ride the merge reparents.
    table: "attraction_ride_profiles",
    column: "parkId",
    strategy: "move",
  },
  { table: "attraction_p50_baselines", column: "parkId", strategy: "move" },
  { table: "attraction_p90_baselines", column: "parkId", strategy: "move" },
  { table: "attraction_rope_drop", column: "parkId", strategy: "move" },
  { table: "attraction_typical_waits", column: "parkId", strategy: "move" },
  { table: "attraction_day_operating", column: "parkId", strategy: "move" },
  { table: "attraction_hourly_history", column: "parkId", strategy: "move" },
  { table: "attraction_outages", column: "parkId", strategy: "move" },
  { table: "attraction_exposure_days", column: "parkId", strategy: "move" },
  { table: "attraction_downtime_profiles", column: "parkId", strategy: "move" },
  { table: "park_downtime_coverage", column: "parkId", strategy: "discard" },
  // `park_id`, not `parkId`: this table is written by raw SQL and its column is
  // snake_case. Discard rather than move — a curve is an aggregate over the
  // loser's intervals, and those move to the winner and are recomputed the same
  // night. Moving the curve would keep a row derived from a population that no
  // longer exists.
  { table: "downtime_recovery_curves", column: "park_id", strategy: "discard" },
  { table: "queue_data_aggregates", column: "parkId", strategy: "move" },
  { table: "ml_accuracy_comparisons", column: "parkId", strategy: "move" },
  { table: "ml_prediction_anomalies", column: "park_id", strategy: "move" },
];

/**
 * Tables from `known` that no merge strategy is declared for. Used by the test
 * suite to fail loudly when a new attraction-referencing table appears.
 */
export function attractionTablesMissingFrom(known: string[]): string[] {
  const declared = new Set(ATTRACTION_DEPENDENCIES.map((d) => d.table));
  return known.filter((table) => !declared.has(table)).sort();
}

/**
 * Park-scoped tables `mergeParks` migrates by hand, before it reaches
 * `PARK_DEPENDENCIES`.
 *
 * Listed here only so `parkTablesMissingFrom` can tell "handled elsewhere"
 * apart from "forgotten". Keep it in step with steps 2-5 of `mergeParks`.
 */
export const PARK_TABLES_HANDLED_INLINE = [
  "attractions",
  "shows",
  "restaurants",
  "park_daily_stats",
  "schedule_entries",
  "park_p50_baselines",
  "park_occupancy",
  "headliner_attractions",
  "weather_data",
  "external_entity_mapping",
] as const;

/**
 * The park-scoped tables of `PARK_TABLES_HANDLED_INLINE` that a merge path has
 * to migrate on its own, expressed as dependencies so the two raw paths in
 * `parks.service.ts` can hand them to `applyMergeDependencies` instead of
 * hand-rolling six more UPDATEs. `mergeParks` keeps its own `migrateTableData`
 * calls; this list is the same set of decisions, in the vocabulary the raw
 * paths already speak.
 *
 * Five of the ten inline tables are deliberately absent:
 *   - `attractions`, `shows`, `restaurants` — both raw paths already reparent
 *     them, and all three need the collision handling that precedes this
 *     (`ATTRACTION_DEPENDENCIES`, `SHOW_DEPENDENCIES`,
 *     `RESTAURANT_DEPENDENCIES`), because all three carry a unique
 *     `(parkId, slug)`.
 *   - `park_p50_baselines` — winner-authoritative, hand-rolled by both callers
 *     (`migrateTableData(..., null)` in `mergeParks`, an explicit SELECT and
 *     branch in `consolidateMergedPark`) since before there was a
 *     `winner-authoritative` strategy to declare instead. The two are the same
 *     rule, so folding it in is a simplification and not a fix — and it is a
 *     change to the park half, which PAR-105 kept out of scope. Recorded as
 *     PAR-178.
 *   - `external_entity_mapping` — keyed on `internal_entity_id` for every
 *     entity type at once, so it wants the `internal_entity_type = 'park'`
 *     filter a bare dependency cannot carry.
 *   - `schedule_entries` — its rows are park-level OR attraction-level, and
 *     the difference is a nullable column. `applyMergeDependencies` compares
 *     conflict keys with a row-wise `IN`, and a NULL inside one of those makes
 *     the comparison NULL rather than true, so no key that mentions
 *     `attractionId` can dedupe a park-level row and no key that omits it can
 *     spare a per-ride one. The caller uses `IS NOT DISTINCT FROM` instead.
 *
 * Whether a table is here decides one of two failure modes, both real:
 * `park_occupancy` is the only one whose FK is NO ACTION (`ManyToOne(() =>
 * Park)` with no `onDelete`, `park-occupancy.entity.ts`), so leaving it out
 * raises 23503 on the park DELETE and rolls the merge back. The other four
 * cascade, so leaving them out destroys them inside a transaction that then
 * reports success — a park's whole schedule, its daily stats, its weather and
 * its headliner set.
 */
export const PARK_INLINE_DEPENDENCIES: MergeDependency[] = [
  {
    table: "park_daily_stats",
    column: "parkId",
    strategy: "move",
    conflictColumns: ["date"],
  },
  {
    // The one that stops the DELETE: FK NO ACTION. Its PK is (id, timestamp),
    // so the parkId move cannot collide on its own key — the dedupe is about
    // meaning rather than constraints, and matches `mergeParks`: two occupancy
    // readings for one park at one instant are not two facts.
    table: "park_occupancy",
    column: "parkId",
    strategy: "move",
    conflictColumns: ["timestamp"],
  },
  {
    // PK is (parkId, attractionId), so this move CAN collide: a ride that
    // collided at the attraction level already had its row reparented onto the
    // survivor by `ATTRACTION_DEPENDENCIES`, and now carries the ghost's
    // parkId beside the survivor's attractionId.
    table: "headliner_attractions",
    column: "parkId",
    strategy: "move",
    conflictColumns: ["attractionId"],
  },
  {
    table: "weather_data",
    column: "parkId",
    strategy: "move",
    conflictColumns: ["date"],
  },
];

/**
 * Park-referencing tables nothing in the merge accounts for.
 *
 * The attraction side has had this guard from the start; the park side did not,
 * which is how `park_seasons` — hand-written, cascade-deleted, reproducible
 * from no feed — was destroyed by every merge without a log line. The point is
 * not this one table but the next one: a new park-scoped entity fails the suite
 * until somebody has decided whether it moves, is discarded, or is handled by
 * hand.
 */
export function parkTablesMissingFrom(known: string[]): string[] {
  const declared = new Set<string>([
    ...PARK_DEPENDENCIES.map((d) => d.table),
    ...PARK_TABLES_HANDLED_INLINE,
  ]);
  return known.filter((table) => !declared.has(table)).sort();
}

/**
 * Named for the file rather than for a service: every merge path calls in here,
 * and a line about a curation that has just ceased to exist has to be findable
 * without knowing which of the four paths issued it.
 */
const logger = new Logger("MergeDependencies");

/** Table and column names are interpolated into SQL, so they must be bare names. */
const SAFE_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function assertSafeIdentifier(value: string): void {
  if (!SAFE_IDENTIFIER.test(value)) {
    throw new Error(`Unsafe SQL identifier in merge dependency: "${value}"`);
  }
}

export interface MergeQueryRunner {
  query: (sql: string, params?: unknown[]) => Promise<unknown>;
}

/** A `SELECT` through the minimal manager interface, as rows. */
function asRows(result: unknown): Array<Record<string, unknown>> {
  return Array.isArray(result)
    ? (result as Array<Record<string, unknown>>)
    : [];
}

/**
 * What a `winner-authoritative` entry would do to one pair of entities.
 *
 * `dropped` is the whole point of the type: those rows are hand-written, have
 * no feed behind them and no job that would rebuild them, so both the merge
 * (as a log line) and the preview (as a warning to the admin about to press
 * the button) have to be able to name them before they cease to exist.
 */
export interface WinnerAuthoritativeDecision {
  /**
   * `nothing`     — the loser holds no row; the winner's, if any, stands.
   * `inherit`     — the winner holds none, so the loser's row moves across.
   * `keep-winner` — both hold rows and the winner's says at least as much.
   * `take-loser`  — both hold rows and the loser's says more (`richness`).
   */
  action: "nothing" | "inherit" | "keep-winner" | "take-loser";
  /** The rows that would cease to exist. Empty unless something is dropped. */
  dropped: Array<Record<string, unknown>>;
  /** Which entity loses them. Null where nothing is dropped. */
  droppedFrom: "winner" | "loser" | null;
}

/** The richest single row of a side, or 0 for a side holding none. */
function topRichness(
  rows: Array<Record<string, unknown>>,
  richness: (row: Record<string, unknown>) => number,
): number {
  return rows.reduce((best, row) => Math.max(best, richness(row)), 0);
}

/**
 * Decides a `winner-authoritative` entry from the rows alone — no database, no
 * side effects.
 *
 * Split out so `previewMerge` can report the same outcome the merge will
 * produce instead of deriving a second answer from the same table. Two
 * derivations of one rule drift, and the one that drifts unnoticed is the
 * preview, because nobody checks a rehearsal against the act.
 *
 * Without `dep.richness` this is exactly the old rule: the winner's row wins
 * whenever the winner holds one. With it, the richer row wins and a TIE still
 * keeps the winner's, so nothing moves without a strictly richer row to move.
 * It is a trade rather than a pure gain — the richer row survives whole, and a
 * field only the poorer one carried goes with it (PAR-208).
 *
 * Written for a table holding one row per entity. Several rows are handled
 * (they move together, or they are dropped together, and a side is ranked by
 * its richest row), but "the winner's row wins" stops meaning anything obvious
 * once there are many, so a new entry using this strategy should be
 * one-per-entity in fact and not only in the common case.
 */
export function decideWinnerAuthoritative(
  dep: MergeDependency,
  losingRows: Array<Record<string, unknown>>,
  winningRows: Array<Record<string, unknown>>,
): WinnerAuthoritativeDecision {
  // Nothing to inherit, and nothing to lose either.
  if (losingRows.length === 0) {
    return { action: "nothing", dropped: [], droppedFrom: null };
  }

  if (winningRows.length === 0) {
    return { action: "inherit", dropped: [], droppedFrom: null };
  }

  if (
    dep.richness &&
    topRichness(losingRows, dep.richness) >
      topRichness(winningRows, dep.richness)
  ) {
    return {
      action: "take-loser",
      dropped: winningRows,
      droppedFrom: "winner",
    };
  }

  return { action: "keep-winner", dropped: losingRows, droppedFrom: "loser" };
}

/**
 * Reads both sides of a `winner-authoritative` entry and decides, without
 * writing anything.
 *
 * The read is the point: deciding on counts alone would leave the DELETE
 * indistinguishable from `discard` — the values gone, and the only record that
 * they existed gone with them.
 *
 * The winner's side is read in full only where the entry ranks rows. Without
 * `richness` the branch decides on the winner holding ANY row, and a
 * `SELECT 1 … LIMIT 1` says that in one index lookup.
 *
 * Both sides are read fresh on every call, which matters where one merge folds
 * SEVERAL losers into one winner: `ParkMergeService.migrateEntities` matches a
 * loser to a winner by slug OR name, and a name is not unique inside a park, so
 * two losers can arrive at the same survivor. The second one is then measured
 * against whatever is standing on the winner — possibly the first loser's row,
 * inherited a moment ago. Under `richness` that is still the right comparison,
 * because it asks what the row says rather than who brought it.
 */
export async function planWinnerAuthoritative(
  manager: MergeQueryRunner,
  dep: MergeDependency,
  winnerId: string,
  loserId: string,
): Promise<WinnerAuthoritativeDecision> {
  // The same refusal `applyMergeDependencies` makes, because this is a second
  // exported way into the same read path and the guard there was written for
  // exactly that reason ("cheaper to refuse than to be careful"). With one id
  // on both sides the two SELECTs return the SAME row, and a caller acting on
  // the answer would log and delete a curation over a merge that is not one.
  if (winnerId === loserId) {
    throw new Error(
      `Cannot plan a merge dependency with one id on both sides (${winnerId})`,
    );
  }

  assertSafeIdentifier(dep.table);
  assertSafeIdentifier(dep.column);

  const losing = asRows(
    await manager.query(
      `SELECT * FROM ${dep.table} WHERE "${dep.column}" = $1`,
      [loserId],
    ),
  );
  // Leaving early keeps the ordinary merge — the overwhelming majority, since
  // almost no ride carries a curated profile — at one statement instead of
  // three, and stops the log from announcing a loss that did not happen.
  if (losing.length === 0) {
    return { action: "nothing", dropped: [], droppedFrom: null };
  }

  const winning = asRows(
    await manager.query(
      dep.richness
        ? `SELECT * FROM ${dep.table} WHERE "${dep.column}" = $1`
        : `SELECT 1 FROM ${dep.table} WHERE "${dep.column}" = $1 LIMIT 1`,
      [winnerId],
    ),
  );

  return decideWinnerAuthoritative(dep, losing, winning);
}

/**
 * Inherits the loser's row where the winner has none, ranks the two where both
 * hold one, and says what it drops either way.
 *
 * Three statements at most in the common case, four where the loser's row is
 * the richer one and the winner's has to be deleted before the move: the merge
 * column is the primary key, so the UPDATE would otherwise collide with the row
 * it is replacing.
 *
 * The warning is written inside the caller's transaction, so a merge that
 * rolls back further down leaves a line about a row that still exists. The
 * same is true of `logDroppedCuration`, and the trade is the same one: a log
 * line that is occasionally too pessimistic beats one that is never written
 * because the statement it describes threw.
 */
async function applyWinnerAuthoritative(
  manager: MergeQueryRunner,
  dep: MergeDependency,
  winnerId: string,
  loserId: string,
): Promise<void> {
  const decision = await planWinnerAuthoritative(
    manager,
    dep,
    winnerId,
    loserId,
  );

  if (decision.action === "nothing") return;

  if (decision.action === "inherit") {
    await manager.query(
      `UPDATE ${dep.table} SET "${dep.column}" = $1 WHERE "${dep.column}" = $2`,
      [winnerId, loserId],
    );
    return;
  }

  // Before the DELETE, not after: this is the only trace the row leaves.
  //
  // Two branches, two sentences, because the ids mean different things in them.
  // `keep-winner` says "the row already on" rather than "its own row": where a
  // merge folds several losers into one winner, the row standing there may be
  // the first loser's, inherited a moment ago, and naming it as the survivor's
  // would tell a reader the survivor had a curation of its own — the one thing
  // they would check before deciding whether the dropped one is worth typing
  // back in. `take-loser` cannot borrow that wording: the row it keeps sits on
  // the LOSING attraction, which is deleted seconds later, so a reader looking
  // it up under that id would find nothing. It names the destination instead.
  const lost = decision.action === "take-loser" ? winnerId : loserId;
  const outcome =
    decision.action === "take-loser"
      ? `moving the richer row from ${loserId} onto ${winnerId} and dropping ${winnerId}'s own`
      : `keeping the row already on ${winnerId} and dropping ${loserId}'s`;
  logger.warn(
    `🗑️  ${dep.table}: ${outcome} — ` +
      decision.dropped.map((row) => JSON.stringify(row)).join(" | "),
  );

  await manager.query(`DELETE FROM ${dep.table} WHERE "${dep.column}" = $1`, [
    lost,
  ]);

  // Only now is the key free. `take-loser` is the branch that needs it: the
  // merge column is the primary key here, so the row cannot be moved onto an
  // id that is still occupied.
  if (decision.action === "take-loser") {
    await manager.query(
      `UPDATE ${dep.table} SET "${dep.column}" = $1 WHERE "${dep.column}" = $2`,
      [winnerId, loserId],
    );
  }
}

/**
 * Moves or discards every dependent row of a losing entity, in one pass.
 *
 * Rows whose unique key already exists on the winner are deleted first. Both
 * entities describe the same real ride or park, so a same-key row is a
 * duplicate observation rather than information — and without the delete the
 * UPDATE trips the constraint and rolls the entire merge back. Everything
 * else is reparented, so no time series is lost.
 *
 * A `winner-authoritative` entry is the third case and reads the table before
 * it writes to it — see `applyWinnerAuthoritative`. A `custom` entry is the
 * fourth and hands the pair to its own function, for a table whose shape none
 * of the three statements above can address.
 *
 * Callers must already hold a transaction, and for TimescaleDB tables must
 * have lifted `timescaledb.max_tuples_decompressed_per_dml_transaction`.
 */
export async function applyMergeDependencies(
  manager: MergeQueryRunner,
  dependencies: MergeDependency[],
  winnerId: string,
  loserId: string,
): Promise<void> {
  // One id for both sides is not a no-op here, it is a wipe: `discard` deletes
  // the winner's own rows, a conflict key matches every row against itself so
  // the dedupe DELETE empties the table for that entity, and the branch below
  // reads one row as both sides and drops it. Every caller checks this today
  // and `parks.service.ts` says in as many words that the check is one edit
  // away from not being there. Cheaper to refuse than to be careful.
  if (winnerId === loserId) {
    throw new Error(
      `Cannot apply merge dependencies with one id on both sides (${winnerId})`,
    );
  }

  for (const dep of dependencies) {
    assertSafeIdentifier(dep.table);
    assertSafeIdentifier(dep.column);
    dep.conflictColumns?.forEach(assertSafeIdentifier);
    // A key on a winner-authoritative entry would be read by nothing: that
    // branch decides on the winner having any row at all, not on a matching
    // one. Declaring both is somebody expecting a dedupe that never runs, and
    // the type cannot say so — `strategy` and `conflictColumns` are fields of
    // one interface because half the callers read `conflictColumns` off a list
    // they have not narrowed.
    if (
      dep.strategy === "winner-authoritative" &&
      dep.conflictColumns?.length
    ) {
      throw new Error(
        `Merge dependency "${dep.table}" declares conflictColumns with the ` +
          `winner-authoritative strategy, which ignores them`,
      );
    }
    // The mirror of the check above, and the more expensive mistake of the
    // two: a `richness` on a `move` or `discard` entry is somebody believing
    // their curation is being ranked while the row is reparented or deleted
    // without ever being read.
    if (dep.richness && dep.strategy !== "winner-authoritative") {
      throw new Error(
        `Merge dependency "${dep.table}" declares richness with the ` +
          `${dep.strategy} strategy, which never reads it`,
      );
    }
    // Both directions, because both are silent. A `custom` entry without a
    // function is a table declared to the guard and handled by nothing — the
    // exact failure this whole file exists to make impossible — and an `apply`
    // on any other strategy is a function nothing calls, beside a table that
    // is being moved or deleted by the generic path instead.
    if (dep.strategy === "custom" && !dep.apply) {
      throw new Error(
        `Merge dependency "${dep.table}" declares the custom strategy ` +
          `without an apply function, so nothing would handle it`,
      );
    }
    if (dep.apply && dep.strategy !== "custom") {
      throw new Error(
        `Merge dependency "${dep.table}" declares apply with the ` +
          `${dep.strategy} strategy, which never calls it`,
      );
    }
    if (dep.strategy === "custom" && dep.conflictColumns?.length) {
      throw new Error(
        `Merge dependency "${dep.table}" declares conflictColumns with the ` +
          `custom strategy, which never reads them`,
      );
    }
  }

  for (const dep of dependencies) {
    if (dep.strategy === "discard") {
      await manager.query(
        `DELETE FROM ${dep.table} WHERE "${dep.column}" = $1`,
        [loserId],
      );
      continue;
    }

    if (dep.strategy === "winner-authoritative") {
      await applyWinnerAuthoritative(manager, dep, winnerId, loserId);
      continue;
    }

    if (dep.strategy === "custom") {
      // Non-null by the check above, which runs over the whole list before the
      // first statement.
      await dep.apply!(manager, winnerId, loserId);
      continue;
    }

    if (dep.conflictColumns?.length) {
      const keyList = dep.conflictColumns.map((c) => `"${c}"`).join(", ");
      await manager.query(
        `DELETE FROM ${dep.table} WHERE "${dep.column}" = $1 AND (${keyList}) IN ` +
          `(SELECT ${keyList} FROM ${dep.table} WHERE "${dep.column}" = $2)`,
        [loserId, winnerId],
      );
    }

    await manager.query(
      `UPDATE ${dep.table} SET "${dep.column}" = $1 WHERE "${dep.column}" = $2`,
      [winnerId, loserId],
    );
  }
}
