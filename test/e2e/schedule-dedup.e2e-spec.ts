import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import { ParksModule } from "../../src/parks/parks.module";
import { ParksService } from "../../src/parks/parks.service";
import { getDatabaseConfig } from "../../src/config/database.config";
import { crossTypeConflictSql } from "../../src/parks/utils/schedule-dedup.sql";
import {
  insertScheduleEntries,
  scheduleOf,
  ScheduleSeed,
} from "../helpers/schedule-entries";

/**
 * `cleanupDuplicateScheduleEntries` and its per-park twin against a real
 * Postgres.
 *
 * The unit specs read the generated SQL. What the statements depend on is
 * semantics only a database has: `PARTITION BY` puts every NULL `attractionId`
 * into ONE partition, which is what dedupes the park-level rows together, and
 * the `IS NOT DISTINCT FROM` in the phase-2 `EXISTS` is what keeps those rows
 * in the candidate set at all — a row-wise `IN` over the nullable column is
 * NULL for every one of them (PAR-246).
 */
describe("schedule dedup (E2E)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let parksService: ParksService;

  const PARK = "11111111-1111-4111-8111-111111111111";
  const OTHER_PARK = "22222222-2222-4222-8222-222222222222";
  const RIDE_ONE = "33333333-3333-4333-8333-333333333333";
  const RIDE_TWO = "44444444-4444-4444-8444-444444444444";
  const OTHER_RIDE_ONE = "55555555-5555-4555-8555-555555555555";
  const OTHER_RIDE_TWO = "66666666-6666-4666-8666-666666666666";
  const DAY = "2026-09-20";

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

  async function insertPark(id: string, slug: string): Promise<void> {
    await dataSource.query(
      `INSERT INTO parks (id, "externalId", name, slug, timezone, "createdAt")
       VALUES ($1, $2, $2, $2, 'Europe/Berlin', NOW())`,
      [id, slug],
    );
  }

  async function insertRide(id: string, parkId: string, slug: string) {
    await dataSource.query(
      `INSERT INTO attractions (id, "externalId", name, slug, "parkId", "createdAt", "updatedAt")
       VALUES ($1, $2, $2, $2, $3, NOW(), NOW())`,
      [id, slug, parkId],
    );
  }

  /**
   * One park day with every shape the key has to keep apart: the opening
   * hours, a gap-filled UNKNOWN placeholder beside them, a ride stated twice
   * and a ride stated as two day statuses.
   */
  const dayOf = (parkId: string, rideOne: string, rideTwo: string) => {
    const row = (
      attractionId: string | null,
      scheduleType: string,
      description: string,
      updatedAt: string,
    ): ScheduleSeed => ({
      parkId,
      attractionId,
      date: DAY,
      scheduleType,
      description,
      updatedAt,
    });
    return [
      row(null, "OPERATING", "hours", "2026-09-01 10:00+00"),
      row(null, "UNKNOWN", "Gap-filled", "2026-09-02 10:00+00"),
      row(rideOne, "OPERATING", "ride one older", "2026-09-01 10:00+00"),
      row(rideOne, "OPERATING", "ride one newer", "2026-09-02 10:00+00"),
      row(rideTwo, "OPERATING", "ride two open", "2026-09-01 10:00+00"),
      row(rideTwo, "CLOSED", "ride two closed", "2026-09-02 10:00+00"),
    ];
  };

  /** The hours and one row per ride; ride two's CLOSED is newer and still loses. */
  const cleanedDayOf = (rideOne: string, rideTwo: string) =>
    [
      `${DAY} OPERATING park hours`,
      `${DAY} OPERATING ${rideOne} ride one newer`,
      `${DAY} OPERATING ${rideTwo} ride two open`,
    ].sort();

  async function seedTwoParks(): Promise<void> {
    await insertPark(PARK, "dedup-park");
    await insertPark(OTHER_PARK, "dedup-other-park");
    await insertRide(RIDE_ONE, PARK, "ride-one");
    await insertRide(RIDE_TWO, PARK, "ride-two");
    await insertRide(OTHER_RIDE_ONE, OTHER_PARK, "other-ride-one");
    await insertRide(OTHER_RIDE_TWO, OTHER_PARK, "other-ride-two");
    await insertScheduleEntries(dataSource, [
      ...dayOf(PARK, RIDE_ONE, RIDE_TWO),
      ...dayOf(OTHER_PARK, OTHER_RIDE_ONE, OTHER_RIDE_TWO),
    ]);
    // The seed, asserted (📚 G-72).
    expect(await scheduleOf(dataSource, PARK)).toHaveLength(6);
    expect(await scheduleOf(dataSource, OTHER_PARK)).toHaveLength(6);
  }

  it("keeps the park's hours and one row per ride, and a second run deletes nothing", async () => {
    await seedTwoParks();

    // Three per park: the placeholder, ride one's older copy, ride two's CLOSED.
    expect(await parksService.cleanupDuplicateScheduleEntries()).toBe(6);

    expect(await scheduleOf(dataSource, PARK)).toEqual(
      cleanedDayOf(RIDE_ONE, RIDE_TWO),
    );
    expect(await scheduleOf(dataSource, OTHER_PARK)).toEqual(
      cleanedDayOf(OTHER_RIDE_ONE, OTHER_RIDE_TWO),
    );

    expect(await parksService.cleanupDuplicateScheduleEntries()).toBe(0);
  });

  it("the per-park twin cleans its park and leaves the other one alone", async () => {
    await seedTwoParks();
    const forPark = (parkId: string): Promise<number> =>
      parksService["cleanupDuplicateScheduleEntriesForPark"](parkId);

    expect(await forPark(PARK)).toBe(3);

    expect(await scheduleOf(dataSource, PARK)).toEqual(
      cleanedDayOf(RIDE_ONE, RIDE_TWO),
    );
    expect(await scheduleOf(dataSource, OTHER_PARK)).toEqual(
      dayOf(OTHER_PARK, OTHER_RIDE_ONE, OTHER_RIDE_TWO)
        .map((r) =>
          [
            r.date,
            r.scheduleType,
            r.attractionId ?? "park",
            r.description,
          ].join(" "),
        )
        .sort(),
    );

    expect(await forPark(PARK)).toBe(0);
  });

  /**
   * Phase 2 against a filled table. Its pre-filter is a correlated `EXISTS`
   * whose `IS NOT DISTINCT FROM` allows neither a hash nor a merge join, so the
   * question is whether the nested loop stays cheap at production size —
   * `schedule_entries` held 125 663 rows when PAR-1005 counted them. The fill is
   * twice that: 60 parks, 365 days, the opening hours and ten rides each, plus
   * one CLOSED row per park day so the phase has 21 900 losers to delete.
   *
   * The bound is loose on purpose: it catches a plan that turns quadratic, not
   * a slow laptop.
   */
  it("runs phase 2 over a filled table in well under the bound", async () => {
    await dataSource.query(
      `INSERT INTO parks (id, "externalId", name, slug, timezone, "createdAt")
       SELECT md5('park' || p)::uuid, 'fill-' || p, 'fill-' || p, 'fill-' || p, 'Europe/Berlin', NOW()
         FROM generate_series(1, 60) p`,
    );
    await dataSource.query(
      `INSERT INTO attractions (id, "externalId", name, slug, "parkId", "createdAt", "updatedAt")
       SELECT md5('ride' || p || '-' || r)::uuid, 'fill-' || p || '-' || r, 'r' || r, 'r' || r,
              md5('park' || p)::uuid, NOW(), NOW()
         FROM generate_series(1, 60) p, generate_series(1, 10) r`,
    );
    await dataSource.query(
      `INSERT INTO schedule_entries (id, "parkId", "attractionId", date, "scheduleType", "createdAt", "updatedAt")
       SELECT gen_random_uuid(), md5('park' || p)::uuid,
              CASE WHEN r = 0 THEN NULL ELSE md5('ride' || p || '-' || r)::uuid END,
              DATE '2026-01-01' + d, 'OPERATING', NOW(), NOW()
         FROM generate_series(1, 60) p, generate_series(0, 10) r, generate_series(0, 364) d`,
    );
    await dataSource.query(
      `INSERT INTO schedule_entries (id, "parkId", "attractionId", date, "scheduleType", description, "createdAt", "updatedAt")
       SELECT gen_random_uuid(), md5('park' || p)::uuid, NULL, DATE '2026-01-01' + d,
              'CLOSED', 'Gap-filled', NOW(), NOW()
         FROM generate_series(1, 60) p, generate_series(0, 364) d`,
    );
    await dataSource.query(`ANALYZE schedule_entries`);
    expect(
      await dataSource
        .query(`SELECT count(*)::int AS c FROM schedule_entries`)
        .then((r) => r[0].c),
    ).toBe(60 * 365 * 12);

    const started = process.hrtime.bigint();
    const result = await dataSource.query(crossTypeConflictSql("global"));
    const ms = Number(process.hrtime.bigint() - started) / 1e6;
    console.log(
      `phase 2 over ${60 * 365 * 12} rows: ${ms.toFixed(0)} ms, ${result[1]} deleted`,
    );

    expect(result[1]).toBe(60 * 365);
    expect(ms).toBeLessThan(30_000);
  });
});
