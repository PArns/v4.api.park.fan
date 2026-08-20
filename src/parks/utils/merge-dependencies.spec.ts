import {
  ATTRACTION_DEPENDENCIES,
  PARK_DEPENDENCIES,
  attractionTablesMissingFrom,
  parkTablesMissingFrom,
  applyMergeDependencies,
} from "./merge-dependencies";

/**
 * A merge deletes the losing row, so every table pointing at it must be dealt
 * with first. Missing one has three failure modes, all of which a cold run
 * against production actually produced:
 *
 *   - FK NO ACTION  → the DELETE throws and the whole merge rolls back
 *                     (ml_prediction_anomalies did exactly this)
 *   - FK CASCADE    → the rows vanish silently (attraction_hourly_history)
 *   - no FK at all  → the rows survive pointing at a row that is gone
 *                     (pcn_forecasts: 3.4M orphans across the -2 duplicates)
 *
 * And any table with a unique key on (entity, time) needs its colliding loser
 * rows dropped BEFORE the move, or the UPDATE violates the constraint — which
 * is how the real merge died on prediction_accuracy's uq_pa_attraction_target.
 */
describe("merge dependency tables", () => {
  /**
   * Snapshot of every table referencing attractions, taken from the live
   * catalog on 2026-07-27. If a new one appears, this test fails and whoever
   * added it has to declare a merge strategy.
   */
  const ATTRACTION_REFERENCING_TABLES = [
    "attraction_accuracy_stats",
    "attraction_day_operating",
    "attraction_hourly_history",
    "attraction_p50_baselines",
    "attraction_p90_baselines",
    "attraction_rope_drop",
    "attraction_typical_waits",
    "catboost_daily_forecasts",
    "forecast_data",
    "headliner_attractions",
    "ml_accuracy_comparisons",
    "ml_prediction_anomalies",
    "pcn_forecasts",
    "prediction_accuracy",
    "queue_data",
    "queue_data_aggregates",
    "shape_forecasts",
    "tft_forecasts",
    "wait_time_predictions",
  ];

  it("declares a strategy for every table that references an attraction", () => {
    expect(attractionTablesMissingFrom(ATTRACTION_REFERENCING_TABLES)).toEqual(
      [],
    );
  });

  it("never lists the same table twice", () => {
    const tables = ATTRACTION_DEPENDENCIES.map((d) => d.table);
    expect(tables).toHaveLength(new Set(tables).size);
  });

  /**
   * The park-side counterpart, and it did not exist until a merge ate a set of
   * hand-researched seasons. Same snapshot rule as the attraction list above.
   */
  const PARK_REFERENCING_TABLES = [
    "attraction_day_operating",
    "attraction_hourly_history",
    "attraction_p50_baselines",
    "attraction_p90_baselines",
    "attraction_rope_drop",
    "attraction_typical_waits",
    "attractions",
    "external_entity_mapping",
    "headliner_attractions",
    "ml_accuracy_comparisons",
    "ml_prediction_anomalies",
    "park_daily_stats",
    "park_occupancy",
    "park_p50_baselines",
    "park_p90_baselines",
    "park_seasons",
    "park_slug_aliases",
    "queue_data_aggregates",
    "restaurants",
    "schedule_entries",
    "shows",
    "weather_data",
    "weather_warnings",
  ];

  it("declares a strategy for every table that references a park", () => {
    expect(parkTablesMissingFrom(PARK_REFERENCING_TABLES)).toEqual([]);
  });

  it("reparents park_seasons instead of letting the CASCADE eat them", () => {
    // A season is written by a person reading a park's calendar and exists in
    // no feed. The FK is ON DELETE CASCADE, so an undeclared table is not an
    // error at merge time — it is a silent deletion inside the transaction.
    const seasons = PARK_DEPENDENCIES.find((d) => d.table === "park_seasons");
    expect(seasons?.strategy).toBe("move");
    // The physical column. `parkId` is only the TypeScript property, and this
    // name is interpolated straight into SQL.
    expect(seasons?.column).toBe("park_id");
    expect(seasons?.conflictColumns).toBeUndefined();
  });

  it("moves queue data rather than discarding it", () => {
    const queueData = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "queue_data",
    );

    expect(queueData?.strategy).toBe("move");
    // queue_data's PK is (id, timestamp) — its own surrogate id, so changing
    // attractionId can never collide and no observation is ever dropped.
    expect(queueData?.conflictColumns).toBeUndefined();
  });

  it("drops colliding rows before moving prediction_accuracy", () => {
    const predictionAccuracy = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "prediction_accuracy",
    );

    expect(predictionAccuracy?.strategy).toBe("move");
    expect(predictionAccuracy?.conflictColumns).toEqual(["target_time"]);
  });

  it("declares conflict columns for every keyed forecast table", () => {
    const forecastTables = [
      "pcn_forecasts",
      "shape_forecasts",
      "tft_forecasts",
      "catboost_daily_forecasts",
    ];

    for (const table of forecastTables) {
      const dependency = ATTRACTION_DEPENDENCIES.find((d) => d.table === table);
      expect(dependency?.strategy).toBe("move");
      expect(dependency?.conflictColumns?.length).toBeGreaterThan(0);
    }
  });

  it("discards only single-row-per-attraction baselines", () => {
    const discarded = ATTRACTION_DEPENDENCIES.filter(
      (d) => d.strategy === "discard",
    ).map((d) => d.table);

    expect(discarded.sort()).toEqual(
      [
        "attraction_accuracy_stats",
        "attraction_p50_baselines",
        "attraction_p90_baselines",
        "attraction_rope_drop",
        "attraction_typical_waits",
      ].sort(),
    );
  });

  it("reparents park_slug_aliases instead of letting the CASCADE eat them", () => {
    const aliases = PARK_DEPENDENCIES.find(
      (d) => d.table === "park_slug_aliases",
    );

    // These keep already-indexed URLs alive; the FK is ON DELETE CASCADE, so
    // leaving them out of the merge destroys them without a trace.
    expect(aliases?.strategy).toBe("move");
    expect(aliases?.conflictColumns).toEqual(["slug"]);
  });

  it("uses safe SQL identifiers everywhere", () => {
    const identifier = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

    for (const dependency of [
      ...ATTRACTION_DEPENDENCIES,
      ...PARK_DEPENDENCIES,
    ]) {
      expect(dependency.table).toMatch(identifier);
      expect(dependency.column).toMatch(identifier);
      for (const col of dependency.conflictColumns ?? []) {
        expect(col).toMatch(identifier);
      }
    }
  });
});

describe("applyMergeDependencies", () => {
  const manager = { query: jest.fn().mockResolvedValue([]) };

  beforeEach(() => jest.clearAllMocks());

  it("refuses identifiers that are not plain SQL names", async () => {
    await expect(
      applyMergeDependencies(
        manager,
        [
          {
            table: "queue_data; DROP TABLE parks",
            column: "attractionId",
            strategy: "move",
          },
        ],
        "w",
        "l",
      ),
    ).rejects.toThrow(/identifier/i);

    expect(manager.query).not.toHaveBeenCalled();
  });

  it("deletes colliding rows before moving the rest", async () => {
    await applyMergeDependencies(
      manager,
      [
        {
          table: "prediction_accuracy",
          column: "attraction_id",
          strategy: "move",
          conflictColumns: ["target_time"],
        },
      ],
      "winner-id",
      "loser-id",
    );

    const [firstSql] = manager.query.mock.calls[0];
    const [secondSql] = manager.query.mock.calls[1];

    expect(firstSql).toMatch(/^DELETE FROM prediction_accuracy/);
    expect(secondSql).toMatch(/^UPDATE prediction_accuracy/);
  });

  it("issues no delete for a table that cannot collide", async () => {
    await applyMergeDependencies(
      manager,
      [{ table: "queue_data", column: "attractionId", strategy: "move" }],
      "winner-id",
      "loser-id",
    );

    expect(manager.query).toHaveBeenCalledTimes(1);
    expect(manager.query.mock.calls[0][0]).toMatch(/^UPDATE queue_data/);
  });
});
