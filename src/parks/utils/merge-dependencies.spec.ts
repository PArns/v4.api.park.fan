import { Logger } from "@nestjs/common";
import {
  ATTRACTION_DEPENDENCIES,
  PARK_CHILD_ENTITIES,
  PARK_DEPENDENCIES,
  PARK_INLINE_DEPENDENCIES,
  PARK_TABLES_HANDLED_INLINE,
  RESTAURANT_DEPENDENCIES,
  SHOW_DEPENDENCIES,
  applyMergeDependencies,
  attractionTablesMissingFrom,
  parkTablesMissingFrom,
  restaurantTablesMissingFrom,
  showTablesMissingFrom,
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
   *
   * `attraction_ride_profiles` is younger than that snapshot and was therefore
   * invisible to the very guard that exists to catch it — every attraction
   * merge cascaded the curated profile away in silence until PAR-105. Added
   * here in the same commit that declares its strategy.
   */
  const ATTRACTION_REFERENCING_TABLES = [
    "attraction_accuracy_stats",
    "attraction_day_operating",
    "attraction_hourly_history",
    "attraction_p50_baselines",
    "attraction_p90_baselines",
    "attraction_ride_profiles",
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
    "attraction_ride_profiles",
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

  /**
   * `PARK_INLINE_DEPENDENCIES` is the same set of decisions as steps 3-4 of
   * `mergeParks`, for the two raw paths in `parks.service.ts`. The two lists
   * are separate objects and would drift silently, so this pins the one
   * relationship that matters: every inline table is either declared here or
   * named as one of the four the callers handle themselves.
   */
  const PARK_TABLES_HANDLED_BY_THE_CALLER = [
    // Reparented by both raw paths before the park step; attractions need
    // collision handling first.
    "attractions",
    "shows",
    "restaurants",
    // Winner-authoritative, hand-rolled by both callers since before there was
    // a strategy of that name to declare instead (PAR-178).
    "park_p50_baselines",
    // Needs the internal_entity_type filter a bare dependency cannot carry.
    "external_entity_mapping",
    // Park-level and per-ride rows in one table, told apart by a nullable
    // column a row-wise IN cannot compare.
    "schedule_entries",
  ];

  it("covers every inline park table either as a dependency or as the caller's job", () => {
    const covered = new Set([
      ...PARK_INLINE_DEPENDENCIES.map((d) => d.table),
      ...PARK_TABLES_HANDLED_BY_THE_CALLER,
    ]);
    expect(
      PARK_TABLES_HANDLED_INLINE.filter((table) => !covered.has(table)),
    ).toEqual([]);
    // And nothing here that the inline list does not know about — a table
    // declared twice under two strategies is worse than one declared nowhere.
    expect(
      PARK_INLINE_DEPENDENCIES.map((d) => d.table).filter(
        (table) =>
          !(PARK_TABLES_HANDLED_INLINE as readonly string[]).includes(table),
      ),
    ).toEqual([]);
    const both = PARK_INLINE_DEPENDENCIES.map((d) => d.table).filter((table) =>
      PARK_DEPENDENCIES.some((d) => d.table === table),
    );
    expect(both).toEqual([]);
  });

  it("moves park_occupancy — the one park FK that is NO ACTION", () => {
    // `park-occupancy.entity.ts` declares `@ManyToOne(() => Park)` with no
    // `onDelete`. Every other inline table cascades, so forgetting one of those
    // destroys rows quietly; forgetting this one raises 23503 and rolls the
    // whole merge back, taking the sync run with it.
    const occupancy = PARK_INLINE_DEPENDENCIES.find(
      (d) => d.table === "park_occupancy",
    );
    expect(occupancy?.strategy).toBe("move");
    expect(occupancy?.column).toBe("parkId");
  });

  it("dedupes headliner_attractions on attractionId before moving the parkId", () => {
    // Its PK is (parkId, attractionId), and a ride that collided at the
    // attraction level already carries the survivor's attractionId beside the
    // ghost's parkId. Without the dedupe that move is a PK violation.
    const headliners = PARK_INLINE_DEPENDENCIES.find(
      (d) => d.table === "headliner_attractions",
    );
    expect(headliners?.strategy).toBe("move");
    expect(headliners?.conflictColumns).toEqual(["attractionId"]);
  });

  it("moves the curated ride profiles with the park rather than cascading them", () => {
    // Third of the same family as park_seasons and park_slug_aliases: hand
    // written, ON DELETE CASCADE, and reproducible from no feed — the only
    // writers of attraction_ride_profiles are a person at the database and a
    // person in the admin. It went unnoticed because the merge used to abort
    // before it ever reached the park DELETE.
    const profiles = PARK_DEPENDENCIES.find(
      (d) => d.table === "attraction_ride_profiles",
    );
    expect(profiles?.strategy).toBe("move");
    expect(profiles?.column).toBe("parkId");
    // PK is attractionId alone, so moving the parkId cannot collide.
    expect(profiles?.conflictColumns).toBeUndefined();
  });

  it("keeps schedule_entries out of the dependency lists", () => {
    // Its rows are park-level or per-ride, told apart by a nullable column, and
    // `applyMergeDependencies` compares conflict keys with a row-wise IN — NULL
    // there is NULL, never true. Whichever key it were given would be wrong for
    // half the table, so the caller compares the three columns itself.
    for (const list of [PARK_DEPENDENCIES, PARK_INLINE_DEPENDENCIES]) {
      expect(list.find((d) => d.table === "schedule_entries")).toBeUndefined();
    }
  });

  it("moves the rope-drop and typical-wait rows with the park rather than cascading them", () => {
    // Both hang off the park with ON DELETE CASCADE, so a merge that deletes
    // the ghost before applying PARK_DEPENDENCIES destroys the published
    // numbers of every ride that just moved across — no error, no log line.
    for (const table of ["attraction_rope_drop", "attraction_typical_waits"]) {
      const dep = PARK_DEPENDENCIES.find((d) => d.table === table);
      expect(dep?.strategy).toBe("move");
      expect(dep?.column).toBe("parkId");
      // Their PK is attractionId alone, so moving the parkId cannot collide.
      expect(dep?.conflictColumns).toBeUndefined();
    }
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

  it("inherits a curated ride profile rather than discarding or moving it", () => {
    // The decision on PAR-105, and the reason the strategy exists at all.
    //
    // Its neighbours on the discard list are derived — the nightly jobs rewrite
    // the survivor's rope drop and typical waits from the queue history that
    // just moved onto it. A ride profile is written by a person and by nothing
    // else, so `discard` deletes a curation with no way back; and `move` cannot
    // be used at all, because `attractionId` is the primary key and the UPDATE
    // would collide with the survivor's own row.
    const profiles = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "attraction_ride_profiles",
    );

    expect(profiles?.strategy).toBe("winner-authoritative");
    expect(profiles?.column).toBe("attractionId");
    // No conflictColumns: that dedupe deletes the LOSER's row whenever the key
    // collides, which for a PK-keyed table is every time the winner has a row —
    // `discard` under another name, and the opposite of what was decided.
    expect(profiles?.conflictColumns).toBeUndefined();
  });

  it("keeps the attraction and the park entry for ride profiles, on different columns", () => {
    // Two entries, one table, and both are needed: the park list carries the
    // denormalised parkId of a profile whose ride has already been reparented,
    // this one decides what happens when the ride itself is merged away. Every
    // park path runs the attraction step first, so they compose in that order.
    const onAttraction = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "attraction_ride_profiles",
    );
    const onPark = PARK_DEPENDENCIES.find(
      (d) => d.table === "attraction_ride_profiles",
    );

    expect(onAttraction?.column).toBe("attractionId");
    expect(onPark?.column).toBe("parkId");
    expect(onPark?.strategy).toBe("move");
  });

  it("discards only single-row-per-attraction baselines", () => {
    const discarded = ATTRACTION_DEPENDENCIES.filter(
      (d) => d.strategy === "discard",
    ).map((d) => d.table);

    expect(discarded.sort()).toEqual(
      [
        "attraction_accuracy_stats",
        // An aggregate over a window the merge has just invalidated. Carrying
        // the loser's numbers across would publish a figure about a ride that
        // no longer exists in that shape; the reconstruction rewrites the
        // survivor's row once `last_merged_at` falls out of range.
        "attraction_downtime_profiles",
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
    // The whole path, because the unique index is the whole path and carries no
    // `parkId`. On `slug` alone the dedupe DELETE reached across the other
    // three: `disneyland-park` is Anaheim AND Paris, and a merge would have
    // dropped the ghost's redirect over a winner row that shares nothing but
    // the last segment. Widened, it matches only rows that genuinely could not
    // move — which under a table-wide unique index is none.
    expect(aliases?.conflictColumns).toEqual([
      "continentSlug",
      "countrySlug",
      "citySlug",
      "slug",
    ]);
  });

  /**
   * The show side, and it is the park side's problem rather than the
   * attraction side's twin: `shows` carries a unique `(parkId, slug)`, so the
   * losing row cannot simply be reparented and has to be deleted — at which
   * point these three decide whether anything survives it.
   *
   * Snapshot from the entities on 2026-09-11. `external_entity_mapping` is
   * moved by the caller before the dependencies and is excluded by the guard
   * itself, exactly as on the attraction side.
   */
  const SHOW_REFERENCING_TABLES = [
    "external_entity_mapping",
    "show_follows",
    "show_live_data",
    "show_schedule_patterns",
  ];

  const RESTAURANT_REFERENCING_TABLES = [
    "external_entity_mapping",
    "restaurant_live_data",
  ];

  it("declares a strategy for every table that references a show or a restaurant", () => {
    expect(showTablesMissingFrom(SHOW_REFERENCING_TABLES)).toEqual([]);
    expect(restaurantTablesMissingFrom(RESTAURANT_REFERENCING_TABLES)).toEqual(
      [],
    );
  });

  it("keeps a losing show's history, its projected week and its followers", () => {
    // Three tables, three different failure modes, and only the third is
    // visible without a database: show_live_data and show_follows are ON
    // DELETE CASCADE, so forgetting them destroys a show's entire showtime
    // history and somebody's push reminder inside a transaction that then
    // reports success. show_schedule_patterns has no FK at all and would be
    // left pointing at a row that is gone.
    const byTable = new Map(SHOW_DEPENDENCIES.map((d) => [d.table, d]));

    // PK (id, timestamp) — its own surrogate id, so no observation is dropped.
    expect(byTable.get("show_live_data")).toMatchObject({
      column: "showId",
      strategy: "move",
    });
    expect(byTable.get("show_live_data")?.conflictColumns).toBeUndefined();

    // PK (show_id, weekday): the survivor's own Saturday wins, the loser's
    // remaining weekdays fill the gaps.
    expect(byTable.get("show_schedule_patterns")).toMatchObject({
      // `show_id`, not `showId`: the name goes into raw SQL and this table has
      // no camelCase column.
      column: "show_id",
      strategy: "move",
      conflictColumns: ["weekday"],
    });

    // Unique (subscriptionId, showId). The only row on any of these lists a
    // person set by hand, and one reminder per show is what they asked for.
    expect(byTable.get("show_follows")).toMatchObject({
      column: "showId",
      strategy: "move",
      conflictColumns: ["subscriptionId"],
    });

    // Nothing about a show is derived-and-replaceable, so nothing is discarded.
    expect(SHOW_DEPENDENCIES.filter((d) => d.strategy === "discard")).toEqual(
      [],
    );
  });

  it("moves a losing restaurant's live data rather than cascading it", () => {
    expect(RESTAURANT_DEPENDENCIES).toEqual([
      {
        table: "restaurant_live_data",
        column: "restaurantId",
        strategy: "move",
      },
    ]);
  });

  it("pairs each park child entity with its own dependency list", () => {
    // The table name is interpolated into SQL by `migrateParkChildEntities`,
    // so it comes from this closed set rather than from a parameter. Pairing
    // it with the wrong list would move a show's rows for a restaurant.
    expect(PARK_CHILD_ENTITIES.map((e) => e.table)).toEqual([
      "shows",
      "restaurants",
    ]);
    expect(
      PARK_CHILD_ENTITIES.find((e) => e.table === "shows")?.dependencies,
    ).toBe(SHOW_DEPENDENCIES);
    expect(
      PARK_CHILD_ENTITIES.find((e) => e.table === "restaurants")?.dependencies,
    ).toBe(RESTAURANT_DEPENDENCIES);
  });

  it("uses safe SQL identifiers everywhere", () => {
    const identifier = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

    for (const table of PARK_CHILD_ENTITIES.map((e) => e.table)) {
      expect(table).toMatch(identifier);
    }

    for (const dependency of [
      ...ATTRACTION_DEPENDENCIES,
      ...PARK_DEPENDENCIES,
      ...PARK_INLINE_DEPENDENCIES,
      ...SHOW_DEPENDENCIES,
      ...RESTAURANT_DEPENDENCIES,
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

  it("refuses one id on both sides, before any statement", async () => {
    // Not a no-op: `discard` would delete the winner's own rows, a conflict key
    // matches every row against itself so the dedupe DELETE would empty the
    // table for that entity, and the winner-authoritative branch would read one
    // row as both sides and drop it. Every caller guards today; this is so that
    // the one that stops guarding fails loudly instead of quietly.
    await expect(
      applyMergeDependencies(
        manager,
        ATTRACTION_DEPENDENCIES,
        "same-id",
        "same-id",
      ),
    ).rejects.toThrow(/both sides/i);

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

  it("dedupes park_slug_aliases on the whole path, so a shared last segment keeps its redirect", async () => {
    // The one entry here whose unique key spans four columns. On `slug` alone
    // the DELETE below read `("slug") IN (SELECT "slug" …)` and dropped every
    // ghost alias whose last segment the winner happened to use somewhere else
    // in the world — `disneyland-park` is Anaheim and Paris, and the row it
    // deleted is a redirect with no feed behind it.
    const aliases = PARK_DEPENDENCIES.find(
      (d) => d.table === "park_slug_aliases",
    )!;

    await applyMergeDependencies(manager, [aliases], "winner-id", "loser-id");

    const [deleteSql, deleteParams] = manager.query.mock.calls[0];
    expect(deleteSql).toMatch(/^DELETE FROM park_slug_aliases/);
    expect(deleteParams).toEqual(["loser-id", "winner-id"]);
    for (const column of ["continentSlug", "countrySlug", "citySlug", "slug"]) {
      expect(deleteSql).toContain(`"${column}"`);
    }
    // And the move still happens — a narrower key would have been visible here
    // as a missing statement, not as a wrong one.
    expect(manager.query.mock.calls[1][0]).toMatch(/^UPDATE park_slug_aliases/);
  });

  /**
   * The winner-authoritative branch, which is the only one that READS before it
   * writes — and has to, because the log line it owes is the sole record of a
   * hand-written row that is about to cease to exist.
   */
  describe("winner-authoritative", () => {
    const profiles = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "attraction_ride_profiles",
    )!;

    // Restored here rather than at the end of the one case that spies: a
    // failing assertion never reaches the line after it, and a `Logger` left
    // mocked would silence every warning for the rest of the file.
    afterEach(() => jest.restoreAllMocks());

    it("refuses an entry that declares a conflict key it would ignore", async () => {
      // The branch decides on the winner holding ANY row, so a key here is
      // read by nothing — somebody would be expecting a dedupe that never
      // runs. Thrown before the first statement, like the identifier check.
      await expect(
        applyMergeDependencies(
          manager,
          [
            {
              table: "attraction_ride_profiles",
              column: "attractionId",
              strategy: "winner-authoritative",
              conflictColumns: ["parkId"],
            },
          ],
          "winner-id",
          "loser-id",
        ),
      ).rejects.toThrow(/conflictColumns/);

      expect(manager.query).not.toHaveBeenCalled();
    });

    it("touches nothing when the loser has no row", async () => {
      manager.query.mockResolvedValueOnce([]);

      await applyMergeDependencies(
        manager,
        [profiles],
        "winner-id",
        "loser-id",
      );

      // One SELECT and no write at all: the overwhelming majority of merges,
      // since almost no ride carries a curated profile.
      expect(manager.query).toHaveBeenCalledTimes(1);
      expect(manager.query.mock.calls[0][0]).toMatch(
        /^SELECT \* FROM attraction_ride_profiles/,
      );
    });

    it("moves the loser's row when the winner has none", async () => {
      manager.query
        .mockResolvedValueOnce([{ attractionId: "loser-id", elements: [] }])
        .mockResolvedValueOnce([]);

      await applyMergeDependencies(
        manager,
        [profiles],
        "winner-id",
        "loser-id",
      );

      const [, , thirdSql, ...rest] = manager.query.mock.calls.map(
        ([sql]: [string]) => sql,
      );
      expect(rest).toEqual([]);
      expect(thirdSql).toMatch(/^UPDATE attraction_ride_profiles/);
      expect(manager.query.mock.calls[2][1]).toEqual(["winner-id", "loser-id"]);
    });

    it("drops the loser's row when the winner has one, naming it first", async () => {
      const losing = {
        attractionId: "loser-id",
        elements: ["lifthill", "vertical-loop"],
        types: ["launch-coaster"],
      };
      manager.query.mockResolvedValueOnce([losing]).mockResolvedValueOnce([{}]);
      const warn = jest
        .spyOn(Logger.prototype, "warn")
        .mockImplementation(() => undefined);

      await applyMergeDependencies(
        manager,
        [profiles],
        "winner-id",
        "loser-id",
      );

      const [, , thirdSql] = manager.query.mock.calls.map(
        ([sql]: [string]) => sql,
      );
      expect(thirdSql).toMatch(/^DELETE FROM attraction_ride_profiles/);
      expect(manager.query.mock.calls[2][1]).toEqual(["loser-id"]);

      // The assertion that matters is not that something was logged but that
      // the log carries the row: a warning saying only "a profile was dropped"
      // is as unrecoverable as no warning at all.
      expect(warn).toHaveBeenCalledTimes(1);
      const line = warn.mock.calls[0][0] as string;
      expect(line).toContain("vertical-loop");
      expect(line).toContain("launch-coaster");

      // And it is written BEFORE the DELETE. A log line after a statement that
      // throws is a log line that never happens.
      expect(warn.mock.invocationCallOrder[0]).toBeLessThan(
        manager.query.mock.invocationCallOrder[2],
      );
    });
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
