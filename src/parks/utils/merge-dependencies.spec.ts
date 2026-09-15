import { Logger } from "@nestjs/common";
import { getMetadataArgsStorage } from "typeorm";
import { AttractionRideProfile } from "../../attractions/entities/attraction-ride-profile.entity";
import { AttractionReviewMark } from "../../attractions/entities/attraction-review-mark.entity";
import {
  ATTRACTION_DEPENDENCIES,
  CURATED_RIDE_PROFILE_FIELDS,
  PARK_CHILD_ENTITIES,
  PARK_DEPENDENCIES,
  PARK_INLINE_DEPENDENCIES,
  PARK_TABLES_HANDLED_INLINE,
  RESTAURANT_DEPENDENCIES,
  SHOW_DEPENDENCIES,
  applyMergeDependencies,
  attractionTablesMissingFrom,
  decideWinnerAuthoritative,
  mergeAttractionReviewMarks,
  migrateScheduleEntries,
  parkTablesMissingFrom,
  planWinnerAuthoritative,
  rideProfileRichness,
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
   * Snapshot of every table referencing attractions, re-derived from the
   * entities on 2026-09-15 (PAR-149). If a new one appears, this test fails and
   * whoever added it has to declare a merge strategy.
   *
   * The previous snapshot was taken from the live catalog on 2026-07-27 and had
   * gone stale in BOTH directions: three tables were declared but unlisted
   * (`attraction_outages`, `attraction_exposure_days`,
   * `attraction_downtime_profiles`), so the guard was comparing against a list
   * shorter than the one it exists to protect, and four were listed nowhere —
   * `ride_alerts`, `schedule_entries`, `attraction_review_marks` and
   * `prediction_lead_snapshots`. `attraction_ride_profiles` had already been
   * added by hand for the same reason under PAR-105: it was younger than the
   * catalog run and therefore invisible to the very guard meant to catch it.
   *
   * Derived from the entities rather than from a database, because that is
   * what a cloud run can read — every `*.entity.ts` carrying an attraction id
   * column, plus the four forecast tables the ML sub-services create in raw SQL
   * with no entity and no FK (`pcn`/`shape` also create three `*_comparisons`
   * tables, which are keyed by segment and hold no attraction id).
   *
   * `park_seasons` is deliberately absent although it names attractions: its
   * `attraction_ids` is a jsonb array with no foreign key, so a merge leaves
   * dead ids inside the array rather than deleting or orphaning a row, and no
   * column-and-key dependency can express that. PAR-238.
   */
  const ATTRACTION_REFERENCING_TABLES = [
    "attraction_accuracy_stats",
    "attraction_day_operating",
    "attraction_downtime_profiles",
    "attraction_exposure_days",
    "attraction_hourly_history",
    "attraction_outages",
    "attraction_p50_baselines",
    "attraction_p90_baselines",
    "attraction_review_marks",
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
    "prediction_lead_snapshots",
    "queue_data",
    "queue_data_aggregates",
    "ride_alerts",
    "schedule_entries",
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

  it("lists every table it declares a strategy for", () => {
    // The other direction, and the one that had rotted: the guard above asks
    // whether every table in the snapshot has a strategy, so a table that was
    // DECLARED and never listed stayed invisible to it. Three were
    // (attraction_outages, attraction_exposure_days,
    // attraction_downtime_profiles), which is how a list meant to protect 27
    // tables spent a month protecting 20 — the data is corrected above, and
    // without this the same rot starts again with the next entry somebody adds
    // to the list and forgets here.
    const declared = ATTRACTION_DEPENDENCIES.map((d) => d.table);
    expect(
      declared.filter(
        (table) => !ATTRACTION_REFERENCING_TABLES.includes(table),
      ),
    ).toEqual([]);
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

  it("keeps schedule_entries out of the PARK dependency lists and moves it on the attraction side", () => {
    // The park side: its rows are park-level or per-ride, told apart by a
    // nullable column, and `applyMergeDependencies` compares conflict keys with
    // a row-wise IN — NULL there is NULL, never true. Whichever key it were
    // given would be wrong for half the table, so the caller compares the three
    // columns itself.
    for (const list of [PARK_DEPENDENCIES, PARK_INLINE_DEPENDENCIES]) {
      expect(list.find((d) => d.table === "schedule_entries")).toBeUndefined();
    }

    // The attraction side is the same table and not the same problem (PAR-149),
    // which is why this assertion sits next to the one above rather than
    // somewhere it could contradict it: `WHERE "attractionId" = $loser` has
    // already excluded every park-level row, so the key never meets a NULL.
    // Without the entry the FK — ON DELETE CASCADE — deletes whatever per-ride
    // rows the table holds, inside a transaction that then reports success. How
    // many that is, is not established: no write path in this repo sets the
    // column, so the declaration is right on an empty set (two no-ops) and on a
    // non-empty one alike.
    const onAttraction = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "schedule_entries",
    );
    expect(onAttraction).toMatchObject({
      column: "attractionId",
      strategy: "move",
      conflictColumns: ["date", "scheduleType"],
    });
    // And the key holds no nullable column — the whole reason this side may use
    // one. `attractionId` is the nullable one and it is the merge column, not
    // part of the key.
    expect(onAttraction?.conflictColumns).not.toContain("attractionId");
  });

  it("keeps a visitor's ride alert, exactly as the show side keeps their show reminder", () => {
    // AK 4 of PAR-149, and the assertion is a comparison rather than a list of
    // values: `ride_alerts` was named as `show_follows`'s twin in that entry's
    // comment from the day the show list was written, and was missing from this
    // list for as long. Both are ON DELETE CASCADE, both are unique on the
    // subscriber, and the owner of both rows is a stranger who would never hear
    // from us again.
    const alerts = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "ride_alerts",
    );
    const follows = SHOW_DEPENDENCIES.find((d) => d.table === "show_follows");

    expect(alerts?.strategy).toBe(follows?.strategy);
    expect(alerts?.conflictColumns).toEqual(follows?.conflictColumns);
    // Spelled out too, so a change that broke BOTH halves could not pass by
    // keeping them equal to each other.
    expect(alerts).toMatchObject({
      column: "attractionId",
      strategy: "move",
      conflictColumns: ["subscriptionId"],
    });
  });

  it("moves the lead snapshots rather than discarding them", () => {
    // No FK, so forgetting it leaves orphans rather than raising. `move` and
    // not `discard` although the numbers are model output: the table exists
    // because its rows CANNOT be recomputed — every nightly run deletes and
    // rewrites its own daily predictions, and this is the copy that survives to
    // be scored against what actually happened.
    const snapshots = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "prediction_lead_snapshots",
    );

    expect(snapshots).toMatchObject({
      // The physical column: this table has no camelCase one.
      column: "attraction_id",
      strategy: "move",
      // The rest of its PK (attraction_id, target_date, lead_days). One target
      // day accumulates one row per lead distance, so the loser's row for a
      // distance the winner already sampled has to go before the move.
      conflictColumns: ["target_date", "lead_days"],
    });
  });

  it("hands attraction_review_marks to a function of its own", () => {
    // The only `custom` entry anywhere, and the bar is the table's shape: two
    // attraction columns plus a CHECK across them, so no combination of
    // `column` and `conflictColumns` can express it. The three generic
    // statements each name exactly one column.
    const marks = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "attraction_review_marks",
    );

    expect(marks?.strategy).toBe("custom");
    expect(marks?.apply).toBe(mergeAttractionReviewMarks);
    // Same shape of assertion as the one that pins `richness` to one entry:
    // an escape hatch is only bounded while somebody counts its users.
    const custom = [
      ...ATTRACTION_DEPENDENCIES,
      ...PARK_DEPENDENCIES,
      ...PARK_INLINE_DEPENDENCIES,
      ...SHOW_DEPENDENCIES,
      ...RESTAURANT_DEPENDENCIES,
    ].filter((d) => d.strategy === "custom" || d.apply);
    expect(custom).toEqual([marks]);
  });

  it("names the two attraction columns review marks really has", () => {
    // `mergeAttractionReviewMarks` writes its column names as literals — that
    // is what makes it custom — so nothing in the type system connects them to
    // the entity. A rename would leave five statements addressing columns that
    // are not there, and no test here builds a row that would notice.
    const columns = new Set(
      getMetadataArgsStorage()
        .filterColumns(AttractionReviewMark)
        .map((column) => column.options.name ?? column.propertyName),
    );

    // The counter-check first: a lookup that resolved nothing would pass
    // everything below.
    expect(columns.size).toBe(7);
    expect(columns).not.toContain("attractionId");

    expect(columns).toContain("attraction_id");
    expect(columns).toContain("other_attraction_id");
    // Read by the dedupe, which asks whether the winner already holds a verdict
    // OF THE SAME KIND about the same pair.
    expect(columns).toContain("kind");
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

  it("ranks two competing ride profiles, and ranks nothing else", () => {
    // PAR-179: where both sides hold a curated profile the richer one wins,
    // because "the winner's row wins" ranked them by which attraction happened
    // to survive. Patrick's decision names the scope as well as the rule — the
    // exception is for this one table — so the second assertion is half of it.
    const profiles = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "attraction_ride_profiles",
    );
    expect(profiles?.richness).toBe(rideProfileRichness);

    const ranked = [
      ...ATTRACTION_DEPENDENCIES,
      ...PARK_DEPENDENCIES,
      ...PARK_INLINE_DEPENDENCIES,
      ...SHOW_DEPENDENCIES,
      ...RESTAURANT_DEPENDENCIES,
    ].filter((d) => d.richness);
    expect(ranked).toEqual([profiles]);
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

    it("drops the loser's row when the winner's says more, naming it first", async () => {
      const losing = {
        attractionId: "loser-id",
        elements: ["lifthill"],
        types: ["launch-coaster"],
      };
      manager.query.mockResolvedValueOnce([losing]).mockResolvedValueOnce([
        {
          attractionId: "winner-id",
          elements: ["lifthill", "vertical-loop", "zero-g-roll"],
          types: ["launch-coaster"],
        },
      ]);
      const warn = jest
        .spyOn(Logger.prototype, "warn")
        .mockImplementation(() => undefined);

      await applyMergeDependencies(
        manager,
        [profiles],
        "winner-id",
        "loser-id",
      );

      const [, , thirdSql, ...rest] = manager.query.mock.calls.map(
        ([sql]: [string]) => sql,
      );
      expect(thirdSql).toMatch(/^DELETE FROM attraction_ride_profiles/);
      expect(manager.query.mock.calls[2][1]).toEqual(["loser-id"]);
      // No move afterwards: the winner keeps the row it already had.
      expect(rest).toEqual([]);

      // The assertion that matters is not that something was logged but that
      // the log carries the row: a warning saying only "a profile was dropped"
      // is as unrecoverable as no warning at all.
      expect(warn).toHaveBeenCalledTimes(1);
      const line = warn.mock.calls[0][0] as string;
      expect(line).toContain("lifthill");
      expect(line).toContain("launch-coaster");

      // And it is written BEFORE the DELETE. A log line after a statement that
      // throws is a log line that never happens.
      expect(warn.mock.invocationCallOrder[0]).toBeLessThan(
        manager.query.mock.invocationCallOrder[2],
      );
    });

    it("keeps the winner's row on a tie", async () => {
      // PAR-179 decided the ranking and this boundary with it: equal richness
      // keeps the survivor's row, so nothing moves without a strictly richer
      // row to move — a single-field row on each side is the same curation
      // twice, and swapping one for the other would buy nothing and cost the
      // field they disagree on.
      manager.query
        .mockResolvedValueOnce([{ attractionId: "loser-id", model: "Blitz" }])
        .mockResolvedValueOnce([
          { attractionId: "winner-id", model: "Infinity" },
        ]);
      jest.spyOn(Logger.prototype, "warn").mockImplementation(() => undefined);

      await applyMergeDependencies(
        manager,
        [profiles],
        "winner-id",
        "loser-id",
      );

      const [, , thirdSql, ...rest] = manager.query.mock.calls.map(
        ([sql]: [string]) => sql,
      );
      expect(thirdSql).toMatch(/^DELETE FROM attraction_ride_profiles/);
      expect(manager.query.mock.calls[2][1]).toEqual(["loser-id"]);
      expect(rest).toEqual([]);
    });

    it("takes the loser's row when it says more, deleting the winner's first", async () => {
      // The stub, which is what PAR-179 is about: `AdminRideProfileService
      // .upsert` writes a row from a manufacturer name alone, and under the old
      // rule that one field beat a fourteen-element layout purely because it sat
      // on the surviving attraction.
      const losing = {
        attractionId: "loser-id",
        elements: ["lifthill", "vertical-loop", "zero-g-roll"],
        types: ["launch-coaster"],
      };
      manager.query.mockResolvedValueOnce([losing]).mockResolvedValueOnce([
        {
          attractionId: "winner-id",
          elements: [],
          types: [],
          manufacturer_name: "Mack Rides",
        },
      ]);
      const warn = jest
        .spyOn(Logger.prototype, "warn")
        .mockImplementation(() => undefined);

      await applyMergeDependencies(
        manager,
        [profiles],
        "winner-id",
        "loser-id",
      );

      const [, , thirdSql, fourthSql, ...rest] = manager.query.mock.calls.map(
        ([sql]: [string]) => sql,
      );
      expect(rest).toEqual([]);

      // The winner's stub goes first. `attractionId` is the primary key, so the
      // move below cannot land on an id that is still occupied — the UPDATE
      // would raise 23505 and roll the whole merge back.
      expect(thirdSql).toMatch(/^DELETE FROM attraction_ride_profiles/);
      expect(manager.query.mock.calls[2][1]).toEqual(["winner-id"]);
      expect(fourthSql).toMatch(/^UPDATE attraction_ride_profiles/);
      expect(manager.query.mock.calls[3][1]).toEqual(["winner-id", "loser-id"]);

      // The dropped row is the WINNER's here, and the line has to carry it for
      // the same reason as above: it is hand-written and nothing rebuilds it.
      expect(warn).toHaveBeenCalledTimes(1);
      const line = warn.mock.calls[0][0] as string;
      expect(line).toContain("Mack Rides");
      expect(warn.mock.invocationCallOrder[0]).toBeLessThan(
        manager.query.mock.invocationCallOrder[2],
      );
    });

    it("reads the winner's row in full only where the entry ranks rows", async () => {
      // Two shapes of the same probe, and the difference is not cosmetic: the
      // ranking needs the row's contents, an entry without `richness` needs
      // only to know whether one exists, and a table with no index-only answer
      // should not be made to produce one for nothing.
      manager.query
        .mockResolvedValueOnce([{ attractionId: "loser-id" }])
        .mockResolvedValueOnce([]);
      await applyMergeDependencies(
        manager,
        [{ ...profiles, richness: undefined }],
        "winner-id",
        "loser-id",
      );
      expect(manager.query.mock.calls[1][0]).toMatch(
        /^SELECT 1 FROM attraction_ride_profiles .* LIMIT 1$/,
      );

      manager.query.mockClear();
      manager.query
        .mockResolvedValueOnce([{ attractionId: "loser-id" }])
        .mockResolvedValueOnce([]);
      await applyMergeDependencies(
        manager,
        [profiles],
        "winner-id",
        "loser-id",
      );
      expect(manager.query.mock.calls[1][0]).toMatch(
        /^SELECT \* FROM attraction_ride_profiles/,
      );
    });

    it("refuses richness on a strategy that never reads it", async () => {
      // The mirror of the conflictColumns check, and the more expensive of the
      // two: here somebody believes their curation is being ranked while the
      // row is reparented or deleted without ever being read.
      await expect(
        applyMergeDependencies(
          manager,
          [
            {
              table: "attraction_ride_profiles",
              column: "attractionId",
              strategy: "discard",
              richness: rideProfileRichness,
            },
          ],
          "winner-id",
          "loser-id",
        ),
      ).rejects.toThrow(/richness/);

      expect(manager.query).not.toHaveBeenCalled();
    });
  });

  /**
   * The ranking itself, away from any database. It is a curation decision
   * expressed as arithmetic (PAR-179), so what it counts is the assertion.
   */
  describe("rideProfileRichness", () => {
    it("counts elements, types and every curated field that is set", () => {
      expect(
        rideProfileRichness({
          elements: ["lifthill", "first-drop", "vertical-loop"],
          types: ["launch-coaster", "terrain-coaster"],
          manufacturer_name: "Mack Rides",
          manufacturer_term_id: "mack-rides",
          model: "Blitz Coaster",
          opened_year: 2016,
          inversions: 0,
          curated_stats: {
            topSpeedKmh: 100,
            heightM: null,
            lengthM: 1200,
            durationSeconds: null,
          },
        }),
      ).toBe(3 + 2 + 6);
    });

    it("counts curated_stats as one field, however many measurements it holds", () => {
      // The ticket says "Anzahl gesetzter Felder", and `curated_stats` is one
      // column. Counting its four measurements separately is a different rule:
      // a row with no track elements would outrank a three-element layout on
      // the strength of its speed, height, length and duration, and which of
      // those two a merge keeps is a curation question rather than arithmetic.
      const allFour = {
        topSpeedKmh: 100,
        heightM: 40,
        lengthM: 1200,
        durationSeconds: 90,
      };
      expect(rideProfileRichness({ curated_stats: allFour })).toBe(1);
      expect(rideProfileRichness({ curated_stats: { topSpeedKmh: 100 } })).toBe(
        1,
      );
      expect(
        rideProfileRichness({
          elements: ["lifthill", "first-drop", "vertical-loop"],
        }),
      ).toBeGreaterThan(rideProfileRichness({ curated_stats: allFour }));
    });

    it("names columns that exist on the entity", () => {
      // A hand-written twin of the physical column names, and nothing in the
      // type system joins the two halves: every spec here builds its rows as
      // literals, so a renamed column would drop out of the score in silence —
      // no type error, no red test — and tilt the ranking towards deleting the
      // row. `elements` and `types` are checked too although they are counted
      // by length rather than as a flag.
      const columns = new Set(
        getMetadataArgsStorage()
          .filterColumns(AttractionRideProfile)
          .map((column) => column.options.name ?? column.propertyName),
      );

      // The counter-check first: a lookup that resolved nothing would pass
      // every assertion below and guard exactly nothing.
      expect(columns.size).toBe(15);
      expect(columns).not.toContain("manufacturerName");

      for (const field of [
        ...CURATED_RIDE_PROFILE_FIELDS,
        "elements",
        "types",
      ]) {
        expect(columns).toContain(field);
      }
      // And the two the function must NOT count are real columns as well, so
      // their absence from the list above is a decision and not a typo.
      expect(columns).toContain("stats");
      expect(columns).toContain("stats_updated_at");
    });

    it("scores an empty row 0 and a manufacturer-only stub 1", () => {
      // The two ends of the case this exists for: `AdminRideProfileService
      // .upsert` creates the stub from a manufacturer name alone, with both
      // arrays empty.
      expect(rideProfileRichness({ elements: [], types: [] })).toBe(0);
      expect(
        rideProfileRichness({
          elements: [],
          types: [],
          manufacturer_name: "Mack Rides",
        }),
      ).toBe(1);
    });

    it("counts inversions: 0 as stated, not as missing", () => {
      // A curated zero says "this coaster has no inversions", which is a fact
      // somebody looked up. A falsy check would read it as an empty cell and
      // rank a row below one that never answered the question.
      expect(rideProfileRichness({ inversions: 0 })).toBe(1);
      expect(rideProfileRichness({ inversions: null })).toBe(0);
      expect(rideProfileRichness({})).toBe(0);
    });

    it("ignores the Wikidata-imported stats", () => {
      // `stats` is written by RideStatsService, not by a person. Counting it
      // would let an import outrank a curation, which is the inversion this
      // function exists to prevent — and it arrives on rows nobody has touched.
      expect(
        rideProfileRichness({
          elements: [],
          types: [],
          stats: {
            topSpeedKmh: 100,
            heightM: 40,
            lengthM: 1200,
            durationSeconds: 90,
            source: "wikidata",
            sourceId: "Q319081",
          },
          stats_updated_at: new Date(),
        }),
      ).toBe(0);
    });

    it("ignores the bookkeeping columns every row carries", () => {
      // They are set on both sides of every comparison, so counting them would
      // add the same constant twice and change nothing — except on a row whose
      // `parkId` the park step has not reached yet, where it would.
      expect(
        rideProfileRichness({
          attractionId: "a",
          parkId: "p",
          seeded_at: new Date(),
          createdAt: new Date(),
          updatedAt: new Date(),
        }),
      ).toBe(0);
    });

    it("survives a row whose json columns are absent or not arrays", () => {
      // The rows come from `SELECT *` through a raw query, so nothing in the
      // type system stands between the driver and this function.
      expect(rideProfileRichness({ elements: null, types: undefined })).toBe(0);
      expect(rideProfileRichness({ curated_stats: null })).toBe(0);
    });
  });

  /**
   * The decision, without the database. `previewMerge` reports from exactly
   * this function, which is what keeps a rehearsal from drifting from the act.
   */
  describe("decideWinnerAuthoritative", () => {
    const profiles = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "attraction_ride_profiles",
    )!;

    it("names the side that loses its row, in both directions", () => {
      const rich = { elements: ["lifthill", "vertical-loop"], types: [] };
      const stub = { elements: [], types: [], manufacturer_name: "Mack" };

      expect(decideWinnerAuthoritative(profiles, [rich], [stub])).toEqual({
        action: "take-loser",
        dropped: [stub],
        droppedFrom: "winner",
      });
      expect(decideWinnerAuthoritative(profiles, [stub], [rich])).toEqual({
        action: "keep-winner",
        dropped: [stub],
        droppedFrom: "loser",
      });
    });

    it("drops nothing where only one side holds a row", () => {
      const row = { elements: ["lifthill"], types: [] };

      expect(decideWinnerAuthoritative(profiles, [], [row])).toEqual({
        action: "nothing",
        dropped: [],
        droppedFrom: null,
      });
      expect(decideWinnerAuthoritative(profiles, [row], [])).toEqual({
        action: "inherit",
        dropped: [],
        droppedFrom: null,
      });
    });

    it("refuses one id on both sides, like applyMergeDependencies does", async () => {
      // `planWinnerAuthoritative` is a second exported way into the same read
      // path, and with one id on both sides the two SELECTs return the SAME
      // row — which the caller would then log and delete over a merge that is
      // not one. `previewMerge` happens to check first today; the guard in
      // `applyMergeDependencies` exists precisely so that does not have to be
      // true of every future caller.
      const manager = { query: jest.fn().mockResolvedValue([]) };

      await expect(
        planWinnerAuthoritative(manager, profiles, "same-id", "same-id"),
      ).rejects.toThrow(/both sides/);

      expect(manager.query).not.toHaveBeenCalled();
    });

    it("keeps the winner's row for an entry that declares no ranking", () => {
      // The old rule, unchanged, for every entry that does not opt in. PAR-179
      // is an exception for one table and has to stay one.
      const plain = { ...profiles, richness: undefined };
      const rich = { elements: ["lifthill", "vertical-loop"], types: [] };

      expect(decideWinnerAuthoritative(plain, [rich], [{}])).toEqual({
        action: "keep-winner",
        dropped: [rich],
        droppedFrom: "loser",
      });
    });
  });

  /**
   * The custom branch. Its SQL was run against a throwaway PostgreSQL 16 with
   * the CHECK and the unique index rebuilt from the entity, in both uuid
   * orientations, plus a counter-check with the entry removed — a CHECK
   * violation is invisible to every assertion below, because a mock manager
   * accepts any statement. What these pin is the part that outlives the run:
   * which statements are issued, in what order, and that a dropped verdict is
   * named before it ceases to exist.
   */
  describe("custom", () => {
    const marks = ATTRACTION_DEPENDENCIES.find(
      (d) => d.table === "attraction_review_marks",
    )!;

    afterEach(() => jest.restoreAllMocks());

    it("refuses a custom entry with no function to call", async () => {
      // A table declared to the guard and handled by nothing is the exact
      // failure this file exists to prevent, and it is silent: the guard passes
      // because the table is listed.
      await expect(
        applyMergeDependencies(
          manager,
          [
            {
              table: "attraction_review_marks",
              column: "attraction_id",
              strategy: "custom",
            },
          ],
          "winner-id",
          "loser-id",
        ),
      ).rejects.toThrow(/apply/);

      expect(manager.query).not.toHaveBeenCalled();
    });

    it("refuses a function on a strategy that never calls it", async () => {
      const apply = jest.fn();
      await expect(
        applyMergeDependencies(
          manager,
          [
            {
              table: "queue_data",
              column: "attractionId",
              strategy: "move",
              apply,
            },
          ],
          "winner-id",
          "loser-id",
        ),
      ).rejects.toThrow(/apply/);

      expect(apply).not.toHaveBeenCalled();
      expect(manager.query).not.toHaveBeenCalled();
    });

    it("refuses a conflict key the custom branch would ignore", async () => {
      await expect(
        applyMergeDependencies(
          manager,
          [{ ...marks, conflictColumns: ["kind"] }],
          "winner-id",
          "loser-id",
        ),
      ).rejects.toThrow(/conflictColumns/);

      expect(manager.query).not.toHaveBeenCalled();
    });

    it("touches nothing when the loser carries no mark", async () => {
      manager.query.mockResolvedValueOnce([]);

      await applyMergeDependencies(manager, [marks], "winner-id", "loser-id");

      // One index lookup and no write: almost no ride carries a review mark,
      // so this is what a merge costs in practice.
      expect(manager.query).toHaveBeenCalledTimes(1);
      expect(manager.query.mock.calls[0][0]).toMatch(
        /^SELECT 1 FROM attraction_review_marks/,
      );
      expect(manager.query.mock.calls[0][1]).toEqual(["loser-id"]);
    });

    it("drops the pair's own mark and both kinds of superseded row, then moves the rest", async () => {
      const mutual = {
        kind: "not_a_duplicate",
        attraction_id: "winner-id",
        other_attraction_id: "loser-id",
        reason: "cedar creek is a lazy river",
      };
      const superseded = {
        kind: "not_a_duplicate",
        attraction_id: "loser-id",
        other_attraction_id: "other-id",
        reason: "kondaala is the kids' ride",
      };
      manager.query
        .mockResolvedValueOnce([{ "?column?": 1 }])
        .mockResolvedValueOnce([mutual])
        .mockResolvedValueOnce([superseded]);
      const warn = jest
        .spyOn(Logger.prototype, "warn")
        .mockImplementation(() => undefined);

      await applyMergeDependencies(manager, [marks], "winner-id", "loser-id");

      const sql = manager.query.mock.calls.map(([s]: [string]) =>
        s.replace(/\s+/g, " ").trim(),
      );
      expect(sql).toHaveLength(6);
      expect(sql[1]).toContain("DELETE FROM attraction_review_marks WHERE");
      expect(sql[2]).toContain("DELETE FROM attraction_review_marks AS m");
      // Three moves, one per shape. The order is what keeps them from meeting:
      // the first leaves no row naming the loser in `other_attraction_id`, and
      // the third only ever sees rows with no partner at all.
      expect(sql[3]).toContain(
        "WHERE attraction_id = $2::uuid AND other_attraction_id IS NOT NULL",
      );
      expect(sql[4]).toContain("WHERE other_attraction_id = $2::uuid");
      expect(sql[5]).toContain(
        "WHERE attraction_id = $2::uuid AND other_attraction_id IS NULL",
      );
      // Every rewrite restores the canonical order in the same statement: the
      // CHECK is not deferrable, so a move and a later swap is a merge that
      // rolls back.
      for (const statement of [sql[3], sql[4]]) {
        expect(statement).toContain("LEAST(");
        expect(statement).toContain("GREATEST(");
      }

      // Both deletes report through RETURNING, and the log carries the reason
      // rather than a count: that sentence and its URL are the only record that
      // somebody once decided the opposite of what this merge is doing.
      expect(sql[1]).toContain("RETURNING *");
      expect(sql[2]).toContain("RETURNING *");
      expect(warn).toHaveBeenCalledTimes(2);
      expect(warn.mock.calls[0][0]).toContain("cedar creek is a lazy river");
      expect(warn.mock.calls[1][0]).toContain("kondaala is the kids' ride");
    });

    it("reads its deletes as rows, not as TypeORM's [rows, rowCount]", async () => {
      // The bug this pins was in the first version and invisible to every
      // assertion above, because a mock answers whatever it is told to.
      // TypeORM's postgres driver rewrites the result of a bare DELETE or
      // UPDATE into `[rows, rowCount]` (`PostgresQueryRunner`, `switch
      // (raw.command)`) — so `DELETE … RETURNING *` came back as a two-element
      // array whose second element is a number, `dropped.length` was 2 on every
      // merge that got past the gate, and the warning meant to be the only
      // record of a lost verdict announced two rows and printed `[] | 0`.
      //
      // Wrapping the DELETE in a CTE and selecting from it makes the command a
      // SELECT, which is the shape the code reads. Asserted on the SQL rather
      // than by feeding the mock a driver result, because the driver is what
      // decides the shape and a mock cannot be made to disagree with itself.
      manager.query
        .mockResolvedValueOnce([{ "?column?": 1 }])
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([]);

      await mergeAttractionReviewMarks(manager, "winner-id", "loser-id");

      const [, first, second] = manager.query.mock.calls.map(([s]: [string]) =>
        s.replace(/\s+/g, " ").trim(),
      );
      for (const statement of [first, second]) {
        expect(statement).toMatch(/^WITH dropped AS \(\s*DELETE FROM/);
        expect(statement).toMatch(/SELECT \* FROM dropped$/);
      }
    });

    it("refuses one id on both sides, like applyMergeDependencies does", async () => {
      // Sharper here than on the generic path: with one id on both sides the
      // second DELETE's EXISTS matches every pair mark against itself, so it
      // would delete every pair verdict the attraction carries. Unreachable
      // through `applyMergeDependencies`, which refuses first — and this
      // function is exported, so it has its own way in.
      await expect(
        mergeAttractionReviewMarks(manager, "same-id", "same-id"),
      ).rejects.toThrow(/both sides/i);

      expect(manager.query).not.toHaveBeenCalled();
    });

    it("says nothing when it drops nothing", async () => {
      // A merge that only moves marks across is not a merge that lost one, and
      // a warning about every ride with a review mark would train the reader to
      // scroll past the line that matters.
      manager.query
        .mockResolvedValueOnce([{ "?column?": 1 }])
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([]);
      const warn = jest
        .spyOn(Logger.prototype, "warn")
        .mockImplementation(() => undefined);

      await mergeAttractionReviewMarks(manager, "winner-id", "loser-id");

      expect(manager.query).toHaveBeenCalledTimes(6);
      expect(warn).not.toHaveBeenCalled();
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

describe("migrateScheduleEntries", () => {
  const manager = { query: jest.fn().mockResolvedValue([]) };

  beforeEach(() => jest.clearAllMocks());

  it("names all three columns and compares the nullable ride with IS NOT DISTINCT FROM", async () => {
    await migrateScheduleEntries(manager, "winner-park", "loser-park");

    const [deleteSql, deleteParams] = manager.query.mock.calls[0] as [
      string,
      unknown[],
    ];
    expect(deleteSql).toMatch(/^\s*DELETE FROM schedule_entries/);
    expect(deleteSql).toMatch(/"date"/);
    expect(deleteSql).toMatch(/"scheduleType"/);
    expect(deleteSql).toMatch(/"attractionId"\s+IS NOT DISTINCT FROM/);
    expect(deleteParams).toEqual(["winner-park", "loser-park"]);

    const [updateSql, updateParams] = manager.query.mock.calls[1] as [
      string,
      unknown[],
    ];
    expect(updateSql).toMatch(/^\s*UPDATE schedule_entries SET "parkId"/);
    expect(updateParams).toEqual(["winner-park", "loser-park"]);
  });

  it("reports the number of rows it reparented", async () => {
    // TypeORM hands a raw UPDATE back as [rows, affectedCount], and the count
    // is what `mergeParks` reports as `migratedScheduleEntries`.
    manager.query
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([[], 7] as unknown as never);

    await expect(
      migrateScheduleEntries(manager, "winner-park", "loser-park"),
    ).resolves.toBe(7);
  });

  it("refuses one park id on both sides instead of emptying its schedule", async () => {
    // The EXISTS would match every row against itself, so the DELETE is a wipe
    // rather than a no-op — the same reason `applyMergeDependencies` refuses.
    await expect(
      migrateScheduleEntries(manager, "same-park", "same-park"),
    ).rejects.toThrow(/both sides/i);

    expect(manager.query).not.toHaveBeenCalled();
  });
});
