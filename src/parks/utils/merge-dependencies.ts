import { Logger } from "@nestjs/common";

export type MergeStrategy = "move" | "discard" | "winner-authoritative";

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
   *             otherwise drop them, after logging what is being dropped.
   *             For a row that is one per entity, hand-written and
   *             reproducible from no feed: `discard` would destroy a curation
   *             the winner has no equivalent of, and `move` cannot be used at
   *             all where the merge column is also the primary key, because
   *             the UPDATE then collides with the winner's own row.
   */
  strategy: MergeStrategy;
  /**
   * Columns forming a unique key together with `column`. Loser rows whose key
   * already exists on the winner are deleted before the move — otherwise the
   * UPDATE violates the constraint and aborts the whole merge transaction.
   */
  conflictColumns?: string[];
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
    // So: the survivor's own profile always wins, the loser's is inherited
    // only into an empty cell, and where one is dropped `applyMergeDependencies`
    // logs its contents first — the same bargain `logDroppedCuration` strikes
    // for the curated park columns, and for the same reason: a hand-written
    // value that ceases to exist should at least leave a line somebody can
    // find. Which of two competing profiles survives is a curation question
    // and is decided here by "the winner's", per PAR-105.
    //
    // That decision is not free, and the log line is where it is paid for:
    // `AdminRideProfileService.upsert` will create a row from a manufacturer
    // name alone, with `elements` and `types` empty, so a stub on the survivor
    // outranks a fully walked-through layout on the loser. Rarely — it needs
    // two rows for one ride in one park and a profile on BOTH — and the
    // alternative (rank the rows by how much they say) is a second curation
    // decision that is not this list's to make. Recorded as PAR-179.
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
    // CASCADE and the same unique `(subscriptionId, attractionId)`, and it is
    // NOT on `ATTRACTION_DEPENDENCIES` — see PAR-149.
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

interface MergeQueryRunner {
  query: (sql: string, params?: unknown[]) => Promise<unknown>;
}

/** A `SELECT` through the minimal manager interface, as rows. */
function asRows(result: unknown): Array<Record<string, unknown>> {
  return Array.isArray(result)
    ? (result as Array<Record<string, unknown>>)
    : [];
}

/**
 * Inherits the loser's row only where the winner has none, and says what it
 * drops when it cannot.
 *
 * Three statements at most, and the first one is the point: reading the losing
 * rows before deciding is what makes the log line possible. Without it the
 * DELETE is indistinguishable from `discard` — the values are gone and the only
 * record that they existed went with them.
 *
 * Written for a table holding one row per entity. It handles several rows
 * correctly (all of them move, or all of them are logged and dropped), but
 * "the winner's row wins" stops meaning anything obvious once there are many,
 * so a new entry using this strategy should be one-per-entity in fact and not
 * only in the common case.
 *
 * The warning is written inside the caller's transaction, so a merge that
 * rolls back further down leaves a line about a row that still exists. The
 * same is true of `logDroppedCuration`, and the trade is the same one: a log
 * line that is occasionally too pessimistic beats one that is never written
 * because the statement it describes threw.
 *
 * "The winner's own row" is read fresh each call, and where one merge folds
 * SEVERAL losers into one winner that matters: `ParkMergeService
 * .migrateEntities` matches a loser to a winner by slug OR name, and a name is
 * not unique inside a park, so two losers can arrive at the same survivor. The
 * first one's row is inherited, and the second is then measured against THAT
 * row rather than against anything the winner brought. Which of the two
 * curations survives is decided by the order of an unordered SELECT, so it is
 * arbitrary — better than the cascade that used to take both, worse than a
 * rule, and the reason the drop is logged rather than only counted. Ranking
 * the rows by what they say instead is the open question on PAR-179.
 */
async function applyWinnerAuthoritative(
  manager: MergeQueryRunner,
  dep: MergeDependency,
  winnerId: string,
  loserId: string,
): Promise<void> {
  const losing = asRows(
    await manager.query(
      `SELECT * FROM ${dep.table} WHERE "${dep.column}" = $1`,
      [loserId],
    ),
  );
  // Nothing to inherit, and nothing to lose either. Leaving early keeps the
  // ordinary merge — the overwhelming majority, since almost no ride carries a
  // curated profile — at one statement instead of three, and stops the log
  // from announcing a loss that did not happen.
  if (losing.length === 0) return;

  const winnerHolds = asRows(
    await manager.query(
      `SELECT 1 FROM ${dep.table} WHERE "${dep.column}" = $1 LIMIT 1`,
      [winnerId],
    ),
  );

  if (winnerHolds.length === 0) {
    await manager.query(
      `UPDATE ${dep.table} SET "${dep.column}" = $1 WHERE "${dep.column}" = $2`,
      [winnerId, loserId],
    );
    return;
  }

  // Before the DELETE, not after: this is the only trace the row leaves.
  logger.warn(
    `🗑️  ${dep.table}: keeping ${winnerId}'s own row and dropping ${loserId}'s — ` +
      losing.map((row) => JSON.stringify(row)).join(" | "),
  );
  await manager.query(`DELETE FROM ${dep.table} WHERE "${dep.column}" = $1`, [
    loserId,
  ]);
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
 * it writes to it — see `applyWinnerAuthoritative`.
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
