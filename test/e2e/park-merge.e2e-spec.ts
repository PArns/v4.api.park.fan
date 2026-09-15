import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import { ParksModule } from "../../src/parks/parks.module";
import { ParksService } from "../../src/parks/parks.service";
import { ParkMergeService } from "../../src/parks/services/park-merge.service";
import { getDatabaseConfig } from "../../src/config/database.config";
import {
  ATTRACTION_DEPENDENCIES,
  PARK_DEPENDENCIES,
  PARK_INLINE_DEPENDENCIES,
  PARK_TABLES_HANDLED_INLINE,
  RESTAURANT_DEPENDENCIES,
  SHOW_DEPENDENCIES,
} from "../../src/parks/utils/merge-dependencies";

/**
 * The merge transaction against a real Postgres.
 *
 * Unit tests record what `repairDuplicates` asks a manager to do. That proves a
 * statement is ISSUED, never that it COMMITS — which is how three faults lived
 * in this path for as long as they did: a column name that does not exist
 * (42703 on `prediction_accuracy."attractionId"`), a NO ACTION foreign key that
 * blocks the attraction DELETE (23503 on `ml_prediction_anomalies`), and a
 * unique key that the reparenting UPDATE walks straight into (23505). Every one
 * of them is invisible to a recorded manager and fatal to a real transaction,
 * and all three roll the whole merge back — including the `last_merged_at`
 * stamp both raw paths write and neither could ever commit.
 *
 * So the assertions here are deliberately about state AFTER the call returns,
 * not about calls made during it. If the transaction rolls back, the ghost park
 * is still there and every expectation below fails.
 *
 * The seed is built from the faults rather than from a tidy example: each table
 * is here because a specific error class hangs off it, named at its insert.
 */
describe("Park merge (E2E)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let parksService: ParksService;
  let parkMergeService: ParkMergeService;

  const WINNER_PARK = "11111111-1111-4111-8111-111111111111";
  const GHOST_PARK = "22222222-2222-4222-8222-222222222222";
  const WINNER_ATTR = "33333333-3333-4333-8333-333333333333";
  const GHOST_ATTR = "44444444-4444-4444-8444-444444444444";
  const LONELY_ATTR = "55555555-5555-4555-8555-555555555555";

  beforeAll(async () => {
    const dbConfig = getDatabaseConfig();

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true, envFilePath: ".env.test" }),
        TypeOrmModule.forRoot({
          type: "postgres",
          host: dbConfig.host,
          port: dbConfig.port,
          username: dbConfig.username,
          password: dbConfig.password,
          database: dbConfig.database,
          entities: [__dirname + "/../../src/**/*.entity{.ts,.js}"],
          synchronize: true,
          logging: false,
        }),
        ParksModule,
      ],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();

    dataSource = app.get(DataSource);
    parksService = app.get(ParksService);
    parkMergeService = app.get(ParkMergeService);
  });

  afterAll(async () => {
    await app.close();
  });

  async function count(sql: string, params: unknown[] = []): Promise<number> {
    const rows = await dataSource.query(sql, params);
    return Number(rows[0].c);
  }

  /**
   * Two parks sharing one `queue_times_entity_id` — the split-brain state
   * `repairDuplicates` looks for. The winner carries a `wiki_entity_id` and the
   * ghost does not, which is the preference rule that picks the survivor.
   */
  async function seedCollidingParks(): Promise<void> {
    const park = (id: string, name: string, wiki: string | null) =>
      dataSource.query(
        `INSERT INTO parks (id, "externalId", name, slug, timezone, "wiki_entity_id", "queue_times_entity_id", "createdAt")
         VALUES ($1, $2, $3, $4, 'Europe/Berlin', $5, 'qt-777', NOW())`,
        [id, `ext-${name}`, name, name, wiki],
      );
    await park(WINNER_PARK, "winner-park", "wiki-winner");
    await park(GHOST_PARK, "ghost-park", null);

    const attraction = (
      id: string,
      parkId: string,
      slug: string,
      externalId: string,
    ) =>
      dataSource.query(
        `INSERT INTO attractions (id, "externalId", name, slug, "parkId", "createdAt", "updatedAt")
         VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
        [id, externalId, slug, slug, parkId],
      );
    // Same slug on both sides: the collision the merge has to resolve.
    await attraction(WINNER_ATTR, WINNER_PARK, "taron", "ext-taron-wiki");
    await attraction(GHOST_ATTR, GHOST_PARK, "taron", "ext-taron-qt");
    // No counterpart on the winner: this one only has to be reparented.
    await attraction(LONELY_ATTR, GHOST_PARK, "black-mamba", "ext-mamba-qt");

    // queue_data — the hypertable. Its key is a surrogate, so nothing here can
    // collide; what is at stake is the time series surviving the merge at all.
    const queueRow = (attractionId: string, minutesAgo: number, wait: number) =>
      dataSource.query(
        `INSERT INTO queue_data (id, "attractionId", "queueType", status, "waitTime", timestamp, "data_source")
         VALUES (gen_random_uuid(), $1, 'STANDBY', 'OPERATING', $2, NOW() - ($3 || ' minutes')::interval, 'queue-times')`,
        [attractionId, wait, minutesAgo],
      );
    for (let i = 1; i <= 5; i++) await queueRow(GHOST_ATTR, i * 10, 20 + i);
    for (let i = 1; i <= 3; i++) await queueRow(WINNER_ATTR, i * 10, 40 + i);
    for (let i = 1; i <= 2; i++) await queueRow(LONELY_ATTR, i * 10, 15 + i);

    // ml_prediction_anomalies — FK NO ACTION on attractions, and a denormalised
    // park_id beside it. This is the table the attraction DELETE died on (23503),
    // and the park_id is a second move that is easy to forget.
    const anomaly = (attractionId: string, parkId: string, wait: number) =>
      dataSource.query(
        `INSERT INTO ml_prediction_anomalies
           (id, attraction_id, park_id, anomaly_type, severity, predicted_time,
            predicted_wait_time, anomaly_score, model_version, detected_at, "createdAt")
         VALUES (gen_random_uuid(), $1, $2, 'large_error', 'high', NOW(), $3, 91.5, 'v1', NOW(), NOW())`,
        [attractionId, parkId, wait],
      );
    for (let i = 1; i <= 4; i++) await anomaly(GHOST_ATTR, GHOST_PARK, i);
    await anomaly(WINNER_ATTR, WINNER_PARK, 99);
    await anomaly(LONELY_ATTR, GHOST_PARK, 7);

    // prediction_accuracy — unique (attraction_id, target_time). One ghost row
    // shares a target_time with a winner row, so the reparenting UPDATE raises
    // 23505 unless the duplicate is deleted first; the other must survive the
    // move. `date_trunc` so both sides land on the identical instant.
    const accuracy = (attractionId: string, minutesAgo: number) =>
      dataSource.query(
        `INSERT INTO prediction_accuracy
           (id, attraction_id, prediction_time, target_time, predicted_wait_time,
            actual_wait_time, absolute_error, "modelVersion", "predictionType", "createdAt")
         VALUES (gen_random_uuid(), $1, NOW(),
                 date_trunc('minute', NOW()) - ($2 || ' minutes')::interval,
                 30, 35, 5, 'v1', 'hourly', NOW())`,
        [attractionId, minutesAgo],
      );
    await accuracy(GHOST_ATTR, 60);
    await accuracy(WINNER_ATTR, 60); // same target_time → collides
    await accuracy(GHOST_ATTR, 120); // no counterpart → must move

    // pcn_forecasts — one of the four tables no entity owns, and the widest
    // conflict key in the list (four columns). Same shape as above: one row
    // colliding, one row moving.
    const pcnForecast = (attractionId: string, quantile: number) =>
      dataSource.query(
        `INSERT INTO pcn_forecasts (attraction_id, target_slot, origin_slot, quantile, predicted_wait, model_version)
         VALUES ($1, '2026-09-11 12:00', '2026-09-10 12:00', $2, 33.0, 'v1')`,
        [attractionId, quantile],
      );
    await pcnForecast(GHOST_ATTR, 0.5);
    await pcnForecast(WINNER_ATTR, 0.5); // identical key → collides
    await pcnForecast(GHOST_ATTR, 0.8); // no counterpart → must move

    // attraction_p50_baselines — `discard` on the attraction level, `move` on
    // the park level. The order between the two is load-bearing: the ghost
    // ride's row has to be gone before the survivor's parkId is rewritten,
    // or the two contend for the same primary key.
    const baseline = (attractionId: string, parkId: string, value: number) =>
      dataSource.query(
        `INSERT INTO attraction_p50_baselines
           ("attractionId", "parkId", "p50Baseline", "sampleCount", "distinctDays", confidence, "calculatedAt")
         VALUES ($1, $2, $3, 100, 20, 'high', NOW())`,
        [attractionId, parkId, value],
      );
    await baseline(GHOST_ATTR, GHOST_PARK, 12);
    await baseline(WINNER_ATTR, WINNER_PARK, 34);
    await baseline(LONELY_ATTR, GHOST_PARK, 56);

    // park_occupancy — FK NO ACTION on parks, so this is where the park DELETE
    // raises 23503 once the attraction level stops raising it first.
    await dataSource.query(
      `INSERT INTO park_occupancy
         (id, "parkId", timestamp, "occupancyScore", trend, "comparedToTypical",
          "baseline95thPercentile", "currentAvgWait", "activeAttractions", "totalAttractions")
       VALUES (gen_random_uuid(), $1, date_trunc('minute', NOW()), 42, 'stable', 3, 55.0, 21.5, 2, 3)`,
      [GHOST_PARK],
    );

    // park_seasons — ON DELETE CASCADE and curated by hand. Nothing regenerates
    // it, so a merge that forgets it destroys the row inside a transaction that
    // then reports success.
    await dataSource.query(
      `INSERT INTO park_seasons
         (id, park_id, kind, name, start_date, end_date, status, created_at, updated_at)
       VALUES (gen_random_uuid(), $1, 'event', 'Wintertraum', '2026-11-14', '2027-01-10',
               'announced', NOW(), NOW())`,
      [GHOST_PARK],
    );
  }

  /**
   * The guard that keeps this suite honest as the merge grows.
   *
   * Every table the merge touches has to exist in the test schema, whether an
   * entity owns it or `ml-forecast-tables.ts` creates it. A new dependency on a
   * table with neither would not fail here as a missing test — it would fail
   * inside the merge transaction as 42P01 and read like a bug in the merge,
   * which is exactly the confusion that left this path uncovered.
   */
  it("has every merge table in the test schema", async () => {
    const tables = [
      ...new Set([
        ...ATTRACTION_DEPENDENCIES.map((d) => d.table),
        ...PARK_DEPENDENCIES.map((d) => d.table),
        ...PARK_INLINE_DEPENDENCIES.map((d) => d.table),
        ...PARK_TABLES_HANDLED_INLINE,
        // The child-entity lists `mergeParks` hands to the same
        // `applyMergeDependencies`. They were missing here while the only
        // covered path was `repairDuplicates`, which never reaches them.
        ...SHOW_DEPENDENCIES.map((d) => d.table),
        ...RESTAURANT_DEPENDENCIES.map((d) => d.table),
      ]),
    ].sort();

    const missing: string[] = [];
    for (const table of tables) {
      const rows = await dataSource.query(
        `SELECT to_regclass($1) IS NOT NULL AS present`,
        [`public.${table}`],
      );
      if (!rows[0].present) missing.push(table);
    }

    expect(missing).toEqual([]);
  });

  /**
   * And in the right SHAPE, for the three the merge writes time series to.
   *
   * `create_hypertable` in `global-setup.ts` only warns when it fails, so a
   * table that quietly stayed a plain one would leave every case below passing
   * against a schema production does not have — including the compressed-chunk
   * case, whose own premise assertion is the only other thing that would
   * notice.
   */
  it("has the merge's time-series tables as hypertables", async () => {
    const rows = await dataSource.query(
      `SELECT hypertable_name FROM timescaledb_information.hypertables
       WHERE hypertable_name = ANY($1::text[]) ORDER BY hypertable_name`,
      [["queue_data", "restaurant_live_data", "show_live_data"]],
    );

    expect(
      rows.map((r: { hypertable_name: string }) => r.hypertable_name),
    ).toEqual(["queue_data", "restaurant_live_data", "show_live_data"]);
  });

  it("commits: no 23503/23505/42703, time series on the winner, ghost park gone", async () => {
    await seedCollidingParks();

    const before = {
      queueGhost: await count(
        `SELECT count(*) c FROM queue_data WHERE "attractionId" = $1`,
        [GHOST_ATTR],
      ),
      queueWinner: await count(
        `SELECT count(*) c FROM queue_data WHERE "attractionId" = $1`,
        [WINNER_ATTR],
      ),
      anomaliesGhost: await count(
        `SELECT count(*) c FROM ml_prediction_anomalies WHERE attraction_id = $1`,
        [GHOST_ATTR],
      ),
    };
    // The seed is what the assertions below are measured against, so it is
    // asserted too: a silently empty seed would make every count trivially true.
    expect(before.queueGhost).toBe(5);
    expect(before.queueWinner).toBe(3);
    expect(before.anomaliesGhost).toBe(4);

    // The run itself. If anything raises, the transaction rolls back and every
    // expectation below fails — which is the point of asserting on state.
    await parksService.repairDuplicates();

    // 1 · It committed: the ghost park and the ghost ride are gone.
    expect(
      await count(`SELECT count(*) c FROM parks WHERE id = $1`, [GHOST_PARK]),
    ).toBe(0);
    expect(
      await count(`SELECT count(*) c FROM attractions WHERE id = $1`, [
        GHOST_ATTR,
      ]),
    ).toBe(0);

    // 2 · The ghost ride's time series is on the winner, and nothing is orphaned.
    expect(
      await count(
        `SELECT count(*) c FROM queue_data WHERE "attractionId" = $1`,
        [WINNER_ATTR],
      ),
    ).toBe(before.queueGhost + before.queueWinner);
    expect(
      await count(
        `SELECT count(*) c FROM queue_data q
         WHERE NOT EXISTS (SELECT 1 FROM attractions a WHERE a.id = q."attractionId")`,
      ),
    ).toBe(0);

    // 3 · FK NO ACTION, both columns: attraction_id moved, and so did the
    //     denormalised park_id — including the one on the ride that merely
    //     changed parks.
    expect(
      await count(
        `SELECT count(*) c FROM ml_prediction_anomalies WHERE attraction_id = $1`,
        [WINNER_ATTR],
      ),
    ).toBe(5); // 4 ghost + 1 the winner already had
    expect(
      await count(
        `SELECT count(*) c FROM ml_prediction_anomalies WHERE park_id = $1`,
        [GHOST_PARK],
      ),
    ).toBe(0);
    expect(
      await count(
        `SELECT count(*) c FROM ml_prediction_anomalies WHERE park_id = $1`,
        [WINNER_PARK],
      ),
    ).toBe(6); // 4 + 1 + the reparented ride's own

    // 4 · conflictColumns dedupe: the colliding row was dropped, the other moved.
    expect(
      await count(
        `SELECT count(*) c FROM prediction_accuracy WHERE attraction_id = $1`,
        [WINNER_ATTR],
      ),
    ).toBe(2); // the winner's own + the ghost row that did not collide

    // 5 · Same rule on a four-column key, in a table no entity owns.
    expect(
      await count(
        `SELECT count(*) c FROM pcn_forecasts WHERE attraction_id = $1`,
        [WINNER_ATTR],
      ),
    ).toBe(2);

    // 6 · discard-then-move: the ghost ride's baseline is gone, the winner's
    //     survives and now points at the winning park.
    expect(
      await count(
        `SELECT count(*) c FROM attraction_p50_baselines WHERE "attractionId" = $1`,
        [WINNER_ATTR],
      ),
    ).toBe(1);
    expect(
      await count(
        `SELECT count(*) c FROM attraction_p50_baselines WHERE "parkId" = $1`,
        [GHOST_PARK],
      ),
    ).toBe(0);

    // 7 · The stamp that could never commit while the transaction kept aborting.
    const [winner] = await dataSource.query(
      `SELECT "last_merged_at" FROM attractions WHERE id = $1`,
      [WINNER_ATTR],
    );
    expect(winner.last_merged_at).not.toBeNull();

    // 8 · The non-colliding ride was reparented rather than merged away.
    const [lonely] = await dataSource.query(
      `SELECT "parkId" FROM attractions WHERE id = $1`,
      [LONELY_ATTR],
    );
    expect(lonely.parkId).toBe(WINNER_PARK);

    // 9 · The curated season and the occupancy reading survived the park DELETE
    //     — the cascade and the NO ACTION foreign key on the park level.
    expect(
      await count(`SELECT count(*) c FROM park_seasons WHERE park_id = $1`, [
        WINNER_PARK,
      ]),
    ).toBe(1);
    expect(
      await count(`SELECT count(*) c FROM park_occupancy WHERE "parkId" = $1`, [
        WINNER_PARK,
      ]),
    ).toBe(1);
  });

  /**
   * `ParkMergeService.mergeParks` — the OTHER merge path, and the one a person
   * triggers.
   *
   * The suite above drives `ParksService.repairDuplicates()`, one of the two
   * raw paths. `mergeParks` is a separate implementation behind
   * `POST /v1/admin/parks/merge` and `ParkRepairService`, and until PAR-172 it
   * had no end-to-end case at all — which is how PAR-150 could ship a fix for
   * "a colliding show is deleted with its dependent rows still pointing at it"
   * with nothing but a recorded manager behind it.
   *
   * Shows and restaurants rather than rides on purpose: the attraction side is
   * covered above, and the four tables PAR-150 added to the path
   * (`show_live_data`, `show_schedule_patterns`, `show_follows`,
   * `restaurant_live_data`) hang off the two entity types nothing else here
   * touches. Two of them are hypertables, which is why `test/global-setup.ts`
   * now converts them.
   */
  describe("ParkMergeService.mergeParks", () => {
    const MERGE_WINNER = "66666666-6666-4666-8666-666666666666";
    const MERGE_LOSER = "77777777-7777-4777-8777-777777777777";
    const WINNER_SHOW = "88888888-8888-4888-8888-888888888888";
    const LOSER_SHOW = "99999999-9999-4999-8999-999999999999";
    const LONELY_SHOW = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const WINNER_REST = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const LOSER_REST = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    const LONELY_REST = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
    const SUB_BOTH = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
    const SUB_LOSER = "ffffffff-ffff-4fff-8fff-ffffffffffff";

    /**
     * Two parks, each with a show and a restaurant of the same name, plus one
     * of each that only the loser has.
     *
     * Same name AND same slug: `migrateEntities` matches on either, and the
     * unique `(parkId, slug)` on both tables is what a plain reparenting UPDATE
     * would walk into (23505). The lonely pair is the control — it must come
     * out reparented, not merged away, so a test that passes by deleting
     * everything fails.
     */
    async function seedShowAndRestaurantCollision(): Promise<void> {
      const park = (id: string, name: string) =>
        dataSource.query(
          `INSERT INTO parks (id, "externalId", name, slug, timezone, "createdAt")
           VALUES ($1, $2, $3, $4, 'Europe/Berlin', NOW())`,
          [id, `ext-${name}`, name, name],
        );
      await park(MERGE_WINNER, "merge-winner");
      await park(MERGE_LOSER, "merge-loser");

      const show = (id: string, parkId: string, name: string, slug: string) =>
        dataSource.query(
          `INSERT INTO shows (id, "externalId", name, slug, "parkId", "createdAt", "updatedAt")
           VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
          [id, `ext-show-${id.slice(0, 8)}`, name, slug, parkId],
        );
      // The show pair collides on SLUG.
      await show(WINNER_SHOW, MERGE_WINNER, "Fantasmic", "fantasmic");
      await show(LOSER_SHOW, MERGE_LOSER, "Fantasmic!", "fantasmic");
      await show(LONELY_SHOW, MERGE_LOSER, "Lion King", "lion-king");

      const restaurant = (
        id: string,
        parkId: string,
        name: string,
        slug: string,
      ) =>
        dataSource.query(
          `INSERT INTO restaurants (id, "externalId", name, slug, "parkId", "createdAt", "updatedAt")
           VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
          [id, `ext-rest-${id.slice(0, 8)}`, name, slug, parkId],
        );
      // The restaurant pair collides on NAME and on nothing else — the second
      // of `migrateEntities`'s two lookups, which a pair sharing both would
      // never reach. Two feeds spelling the same place differently enough to
      // slug apart ("Bistro du Parc" / "Bistro Du-Parc") is the ordinary way
      // this happens.
      await restaurant(WINNER_REST, MERGE_WINNER, "Bistro du Parc", "bistro");
      await restaurant(
        LOSER_REST,
        MERGE_LOSER,
        "Bistro du Parc",
        "bistro-du-parc",
      );
      await restaurant(LONELY_REST, MERGE_LOSER, "Taverne", "taverne");

      // show_live_data — hypertable, surrogate `(id, timestamp)` key, FK
      // CASCADE. The whole showtime history of the losing show, and the rows
      // that were destroyed before PAR-150: the CASCADE fires on the DELETE in
      // `migrateEntities` unless they have moved first.
      const showLive = (showId: string, hoursAgo: number) =>
        dataSource.query(
          `INSERT INTO show_live_data (id, "showId", status, showtimes, timestamp)
           VALUES (gen_random_uuid(), $1, 'OPERATING',
                   '[{"type":"Performance","startTime":"2026-09-15T18:00:00Z"}]'::jsonb,
                   NOW() - ($2 || ' hours')::interval)`,
          [showId, hoursAgo],
        );
      for (let i = 1; i <= 5; i++) await showLive(LOSER_SHOW, i);
      for (let i = 1; i <= 2; i++) await showLive(WINNER_SHOW, i);
      for (let i = 1; i <= 3; i++) await showLive(LONELY_SHOW, i);

      // show_schedule_patterns — PK `(show_id, weekday)`, so the loser's
      // Saturday collides with the winner's and its Wednesday does not. The
      // dedupe DELETE has to drop exactly the first.
      const pattern = (showId: string, weekday: number, times: string[]) =>
        dataSource.query(
          `INSERT INTO show_schedule_patterns
             (show_id, weekday, times, observed_days, last_observed_on, computed_at)
           VALUES ($1, $2, $3::jsonb, 4, '2026-09-12', NOW())`,
          [showId, weekday, JSON.stringify(times)],
        );
      await pattern(WINNER_SHOW, 6, ["12:30", "14:30"]);
      await pattern(LOSER_SHOW, 6, ["19:00"]); // same weekday → collides
      await pattern(LOSER_SHOW, 3, ["15:45"]); // no counterpart → must move

      // show_follows — unique `(subscriptionId, showId)`, CASCADE on both
      // sides. A subscriber who followed both rows keeps one reminder; a
      // subscriber who only followed the losing show keeps theirs, moved.
      const subscription = (id: string, endpoint: string) =>
        dataSource.query(
          `INSERT INTO push_subscriptions (id, endpoint, p256dh, auth)
           VALUES ($1, $2, 'p256dh-key', 'auth-secret')`,
          [id, endpoint],
        );
      await subscription(SUB_BOTH, "https://push.example/both");
      await subscription(SUB_LOSER, "https://push.example/loser");

      const follow = (subscriptionId: string, showId: string) =>
        dataSource.query(
          `INSERT INTO show_follows (id, "subscriptionId", "showId", "createdAt", "updatedAt")
           VALUES (gen_random_uuid(), $1, $2, NOW(), NOW())`,
          [subscriptionId, showId],
        );
      await follow(SUB_BOTH, WINNER_SHOW);
      await follow(SUB_BOTH, LOSER_SHOW); // same subscriber → collides
      await follow(SUB_LOSER, LOSER_SHOW); // no counterpart → must move

      // restaurant_live_data — the restaurant side's only dependency, same
      // shape as show_live_data one table over.
      const restLive = (restaurantId: string, hoursAgo: number) =>
        dataSource.query(
          `INSERT INTO restaurant_live_data (id, "restaurantId", status, timestamp)
           VALUES (gen_random_uuid(), $1, 'OPERATING', NOW() - ($2 || ' hours')::interval)`,
          [restaurantId, hoursAgo],
        );
      for (let i = 1; i <= 4; i++) await restLive(LOSER_REST, i);
      await restLive(WINNER_REST, 9);
      for (let i = 1; i <= 2; i++) await restLive(LONELY_REST, i);

      // external_entity_mapping — moved by `consolidateEntityData` before the
      // dependency lists, and the one table keyed across every entity type.
      const mapping = (internalId: string, externalId: string) =>
        dataSource.query(
          `INSERT INTO external_entity_mapping
             (internal_entity_id, internal_entity_type, external_source, external_entity_id, created_at)
           VALUES ($1, 'show', 'queue-times', $2, NOW())`,
          [internalId, externalId],
        );
      await mapping(LOSER_SHOW, "qt-show-loser");
      await mapping(WINNER_SHOW, "qt-show-winner");
    }

    it("moves a colliding show's and restaurant's dependent rows to the survivor", async () => {
      await seedShowAndRestaurantCollision();

      // The seed itself is asserted: a silently empty one would make every
      // count below trivially true.
      expect(
        await count(
          `SELECT count(*) c FROM show_live_data WHERE "showId" = $1`,
          [LOSER_SHOW],
        ),
      ).toBe(5);
      expect(
        await count(
          `SELECT count(*) c FROM restaurant_live_data WHERE "restaurantId" = $1`,
          [LOSER_REST],
        ),
      ).toBe(4);

      const result = await parkMergeService.mergeParks(
        MERGE_WINNER,
        MERGE_LOSER,
      );

      // 1 · It committed. Both loser rows counted, the loser park is gone, and
      //     the colliding child rows with it.
      expect(result.success).toBe(true);
      expect(result.migratedShows).toBe(2);
      expect(result.migratedRestaurants).toBe(2);
      expect(
        await count(`SELECT count(*) c FROM parks WHERE id = $1`, [
          MERGE_LOSER,
        ]),
      ).toBe(0);
      expect(
        await count(`SELECT count(*) c FROM shows WHERE id = $1`, [LOSER_SHOW]),
      ).toBe(0);
      expect(
        await count(`SELECT count(*) c FROM restaurants WHERE id = $1`, [
          LOSER_REST,
        ]),
      ).toBe(0);

      // 2 · The losing show's history is on the survivor, not deleted by the
      //     CASCADE. This is the assertion PAR-150 exists for.
      expect(
        await count(
          `SELECT count(*) c FROM show_live_data WHERE "showId" = $1`,
          [WINNER_SHOW],
        ),
      ).toBe(7); // 5 from the loser + the winner's own 2
      expect(
        await count(
          `SELECT count(*) c FROM restaurant_live_data WHERE "restaurantId" = $1`,
          [WINNER_REST],
        ),
      ).toBe(5); // 4 + 1

      // 3 · No orphans anywhere on either side.
      expect(
        await count(
          `SELECT count(*) c FROM show_live_data l
           WHERE NOT EXISTS (SELECT 1 FROM shows s WHERE s.id = l."showId")`,
        ),
      ).toBe(0);
      expect(
        await count(
          `SELECT count(*) c FROM restaurant_live_data l
           WHERE NOT EXISTS (SELECT 1 FROM restaurants r WHERE r.id = l."restaurantId")`,
        ),
      ).toBe(0);
      expect(
        await count(
          `SELECT count(*) c FROM show_follows f
           WHERE NOT EXISTS (SELECT 1 FROM shows s WHERE s.id = f."showId")`,
        ),
      ).toBe(0);
      expect(
        await count(
          `SELECT count(*) c FROM show_schedule_patterns p
           WHERE NOT EXISTS (SELECT 1 FROM shows s WHERE s.id = p.show_id)`,
        ),
      ).toBe(0);

      // 4 · conflictColumns on a composite key: the loser's Saturday was
      //     dropped against the winner's, its Wednesday moved. The winner's own
      //     Saturday is untouched — it is the row that wins, so its times are
      //     the ones that must still be there.
      const patterns = await dataSource.query(
        `SELECT weekday, times FROM show_schedule_patterns
         WHERE show_id = $1 ORDER BY weekday`,
        [WINNER_SHOW],
      );
      expect(patterns.map((p: { weekday: number }) => p.weekday)).toEqual([
        3, 6,
      ]);
      expect(patterns[1].times).toEqual(["12:30", "14:30"]);

      // 5 · Same rule on the reminder: one row per subscriber, both
      //     subscribers still have one, and nobody got a second.
      const followers = await dataSource.query(
        `SELECT "subscriptionId" FROM show_follows WHERE "showId" = $1 ORDER BY "subscriptionId"`,
        [WINNER_SHOW],
      );
      expect(
        followers.map((f: { subscriptionId: string }) => f.subscriptionId),
      ).toEqual([SUB_BOTH, SUB_LOSER]);

      // 6 · The mapping moved, and the winner kept its own.
      expect(
        await count(
          `SELECT count(*) c FROM external_entity_mapping WHERE internal_entity_id = $1`,
          [WINNER_SHOW],
        ),
      ).toBe(2);

      // 7 · The control: no counterpart on the winner, so these were
      //     reparented rather than merged away, and kept their own rows.
      const [lonelyShow] = await dataSource.query(
        `SELECT "parkId" FROM shows WHERE id = $1`,
        [LONELY_SHOW],
      );
      expect(lonelyShow.parkId).toBe(MERGE_WINNER);
      const [lonelyRest] = await dataSource.query(
        `SELECT "parkId" FROM restaurants WHERE id = $1`,
        [LONELY_REST],
      );
      expect(lonelyRest.parkId).toBe(MERGE_WINNER);
      expect(
        await count(
          `SELECT count(*) c FROM show_live_data WHERE "showId" = $1`,
          [LONELY_SHOW],
        ),
      ).toBe(3);
      expect(
        await count(
          `SELECT count(*) c FROM restaurant_live_data WHERE "restaurantId" = $1`,
          [LONELY_REST],
        ),
      ).toBe(2);
    });

    /**
     * The same merge with the losing show's history in a COMPRESSED chunk.
     *
     * What this proves: `applyMergeDependencies` rewrites `showId` on rows that
     * live in a compressed chunk, rather than failing the whole transaction the
     * way a compressed chunk refuses DML on older TimescaleDB. Production keeps
     * `show_live_data` compressed — `timescaledb_information.hypertables` says
     * `compression_enabled = t` with 235 compressed chunks on 2026-09-15 — so
     * an uncompressed test schema answers a question production does not ask.
     *
     * Compression is configured WITHOUT a `segmentby`, because that is what
     * production has: `timescaledb_information.compression_settings` is empty
     * for this table, and `TimescaleInitService.enableCompression` passes no
     * `segmentBy` for it. The difference is not cosmetic. Segmenting by
     * `showId` — the very column the UPDATE rewrites — would let TimescaleDB
     * decompress only the losing show's segment; without one it has to
     * decompress every batch the matching rows sit in, which is the harder case
     * and the one production runs.
     *
     * What it does NOT prove: that lifting
     * `timescaledb.max_tuples_decompressed_per_dml_transaction` in step 0 of
     * `mergeParks` is load-bearing. The cap is 100000 tuples and this seed is
     * five rows — a fixture large enough to reach the cap would cost more run
     * time than the suite has. That one stays a reading of the code.
     */
    it("moves a compressed show history to the survivor", async () => {
      await seedShowAndRestaurantCollision();

      // Whether compression is already on depends on which file ran first:
      // every suite that boots `AppModule` starts `TimescaleInitService`, which
      // enables it and adds a policy — and only does so successfully now that
      // `global-setup.ts` makes this a hypertable at all. So the setting is
      // read rather than assumed, and only put back if this test is the one
      // that turned it on. Hard-coding `compress = false` afterwards would
      // silently disable compression for every later file and orphan that
      // policy.
      const enabled = async (): Promise<boolean> => {
        const rows = await dataSource.query(
          `SELECT compression_enabled FROM timescaledb_information.hypertables
           WHERE hypertable_name = 'show_live_data'`,
        );
        return rows[0]?.compression_enabled === true;
      };

      const wasCompressed = await enabled();
      if (!wasCompressed) {
        await dataSource.query(
          `ALTER TABLE show_live_data SET (timescaledb.compress)`,
        );
      }

      let failed = false;
      try {
        await dataSource.query(
          `SELECT compress_chunk(c, if_not_compressed => true)
           FROM show_chunks('show_live_data') c`,
        );

        // The premise of the test, asserted rather than assumed: with nothing
        // compressed this case would be the previous one under a longer name
        // (G-72). Both halves are needed — "at least one compressed" would hold
        // while the losing show's rows sat in a second, uncompressed chunk,
        // which is what a chunk boundary crossed mid-seed produces.
        expect(
          await count(
            `SELECT count(*) c FROM timescaledb_information.chunks
             WHERE hypertable_name = 'show_live_data' AND is_compressed`,
          ),
        ).toBeGreaterThan(0);
        expect(
          await count(
            `SELECT count(*) c FROM timescaledb_information.chunks
             WHERE hypertable_name = 'show_live_data' AND NOT is_compressed`,
          ),
        ).toBe(0);

        await parkMergeService.mergeParks(MERGE_WINNER, MERGE_LOSER);

        expect(
          await count(
            `SELECT count(*) c FROM show_live_data WHERE "showId" = $1`,
            [WINNER_SHOW],
          ),
        ).toBe(7);
        expect(
          await count(
            `SELECT count(*) c FROM show_live_data l
             WHERE NOT EXISTS (SELECT 1 FROM shows s WHERE s.id = l."showId")`,
          ),
        ).toBe(0);
      } catch (error) {
        failed = true;
        throw error;
      } finally {
        try {
          await dataSource.query(
            `SELECT decompress_chunk(c, if_compressed => true)
             FROM show_chunks('show_live_data') c`,
          );
          if (!wasCompressed) {
            await dataSource.query(
              `ALTER TABLE show_live_data SET (timescaledb.compress = false)`,
            );
          }
        } catch (error) {
          // Swallowed ONLY when the body already threw: a merge that dies on
          // the compressed chunk is what this case is for, and a cleanup
          // statement failing afterwards must not replace that error with its
          // own. On the success path it is rethrown instead — a restore that
          // silently fails hands every later file a compression state this
          // test invented, which is the leak the `wasCompressed` read exists
          // to prevent.
          if (!failed) throw error;
          console.warn(
            "Could not restore show_live_data compression state:",
            error instanceof Error ? error.message : String(error),
          );
        }
      }
    });

    it("refuses a self-merge without touching a row", async () => {
      await seedShowAndRestaurantCollision();

      await expect(
        parkMergeService.mergeParks(MERGE_WINNER, MERGE_WINNER),
      ).rejects.toThrow(/into itself/);

      // The guard is in front of the transaction, so nothing should have run.
      // Asserted against the winner's own rows rather than against the loser's:
      // one id on both sides is what turns every dedupe DELETE into a wipe of
      // the surviving side, so those are the counts that would fall.
      expect(
        await count(`SELECT count(*) c FROM parks WHERE id = $1`, [
          MERGE_WINNER,
        ]),
      ).toBe(1);
      expect(
        await count(`SELECT count(*) c FROM shows WHERE "parkId" = $1`, [
          MERGE_WINNER,
        ]),
      ).toBe(1);
      expect(
        await count(
          `SELECT count(*) c FROM show_live_data WHERE "showId" = $1`,
          [WINNER_SHOW],
        ),
      ).toBe(2);
      expect(
        await count(
          `SELECT count(*) c FROM show_schedule_patterns WHERE show_id = $1`,
          [WINNER_SHOW],
        ),
      ).toBe(1);
      expect(
        await count(`SELECT count(*) c FROM show_follows WHERE "showId" = $1`, [
          WINNER_SHOW,
        ]),
      ).toBe(1);
      expect(
        await count(
          `SELECT count(*) c FROM restaurant_live_data WHERE "restaurantId" = $1`,
          [WINNER_REST],
        ),
      ).toBe(1);

      // And the loser side. Both blocks of counts are defensive rather than
      // sharp, and it is worth saying which is which: the guard throws BEFORE
      // `dataSource.transaction` opens, and even without it
      // `applyMergeDependencies` refuses one id on both sides and rolls the
      // whole thing back — so no placement of the guard makes these numbers
      // move. What they do catch is a future step that writes outside the
      // transaction. The assertion with teeth here is the throw itself.
      expect(
        await count(`SELECT count(*) c FROM parks WHERE id = $1`, [
          MERGE_LOSER,
        ]),
      ).toBe(1);
      expect(
        await count(`SELECT count(*) c FROM shows WHERE "parkId" = $1`, [
          MERGE_LOSER,
        ]),
      ).toBe(2);
      expect(
        await count(`SELECT count(*) c FROM restaurants WHERE "parkId" = $1`, [
          MERGE_LOSER,
        ]),
      ).toBe(2);
      expect(
        await count(
          `SELECT count(*) c FROM show_live_data WHERE "showId" = $1`,
          [LOSER_SHOW],
        ),
      ).toBe(5);
      expect(
        await count(
          `SELECT count(*) c FROM restaurant_live_data WHERE "restaurantId" = $1`,
          [LOSER_REST],
        ),
      ).toBe(4);
    });
  });
});
