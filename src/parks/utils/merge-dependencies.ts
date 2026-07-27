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
    conflictColumns: ["slug"],
  },
  { table: "park_p90_baselines", column: "parkId", strategy: "discard" },
  {
    table: "weather_warnings",
    column: "parkId",
    strategy: "move",
    conflictColumns: ["alertId"],
  },
  { table: "attraction_p50_baselines", column: "parkId", strategy: "move" },
  { table: "attraction_p90_baselines", column: "parkId", strategy: "move" },
  { table: "attraction_rope_drop", column: "parkId", strategy: "move" },
  { table: "attraction_typical_waits", column: "parkId", strategy: "move" },
  { table: "attraction_day_operating", column: "parkId", strategy: "move" },
  { table: "attraction_hourly_history", column: "parkId", strategy: "move" },
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
