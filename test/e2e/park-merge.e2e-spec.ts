import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import { ParksModule } from "../../src/parks/parks.module";
import { ParksService } from "../../src/parks/parks.service";
import { getDatabaseConfig } from "../../src/config/database.config";
import {
  ATTRACTION_DEPENDENCIES,
  PARK_DEPENDENCIES,
  PARK_INLINE_DEPENDENCIES,
  PARK_TABLES_HANDLED_INLINE,
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
});
