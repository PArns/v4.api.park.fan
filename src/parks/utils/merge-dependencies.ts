export type MergeStrategy = "move" | "discard";

export interface MergeDependency {
  /** Table holding rows that point at the entity being merged away. */
  table: string;
  /** Column on that table carrying the entity id. */
  column: string;
  /**
   * `move`    — reparent the loser's rows onto the winner (keeps the data).
   * `discard` — drop them; the winner's own row is authoritative and the
   *             value is derived, so keeping both would be meaningless.
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
];

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
    // reaching the park DELETE. Same lifecycle as `park_seasons`: no feed, no
    // seed job, no writer in this codebase — the rows ARE the source of truth
    // and are edited straight into the database, so a cascade takes a ride's
    // track elements, its ride types and its builder with no way back. The
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
 *     them, and attractions need the collision handling that precedes this.
 *   - `park_p50_baselines` — winner-authoritative rather than move-or-discard
 *     (`migrateTableData(..., null)`), which is not a `MergeStrategy`. Its
 *     caller does it by hand, and says why.
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

/** Table and column names are interpolated into SQL, so they must be bare names. */
const SAFE_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function assertSafeIdentifier(value: string): void {
  if (!SAFE_IDENTIFIER.test(value)) {
    throw new Error(`Unsafe SQL identifier in merge dependency: "${value}"`);
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
 * Callers must already hold a transaction, and for TimescaleDB tables must
 * have lifted `timescaledb.max_tuples_decompressed_per_dml_transaction`.
 */
export async function applyMergeDependencies(
  manager: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
  dependencies: MergeDependency[],
  winnerId: string,
  loserId: string,
): Promise<void> {
  for (const dep of dependencies) {
    assertSafeIdentifier(dep.table);
    assertSafeIdentifier(dep.column);
    dep.conflictColumns?.forEach(assertSafeIdentifier);
  }

  for (const dep of dependencies) {
    if (dep.strategy === "discard") {
      await manager.query(
        `DELETE FROM ${dep.table} WHERE "${dep.column}" = $1`,
        [loserId],
      );
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
