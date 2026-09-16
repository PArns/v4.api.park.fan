import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import { getDatabaseConfig } from "../../src/config/database.config";
import { LIVE_STATS_SQL } from "../../src/discovery/discovery.service";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { QueueData } from "../../src/queue-data/entities/queue-data.entity";
import {
  LiveStatus,
  QueueType,
} from "../../src/external-apis/themeparks/themeparks.types";
import { seedMinimalTestData, clearTestData } from "../helpers/seed-test-data";
import { randomUUID } from "node:crypto";

/**
 * PAR-233. `LIVE_STATS_SQL` is the geo listing's counter, and it counted
 * retired rides: on 2026-09-16 production served `attractionCount: 35` for
 * Universal Studios Singapore while the park's own page served 18 — the
 * difference was exactly its 17 retired rows.
 *
 * The query is raw SQL with no parameters, so it is run here against a real
 * database rather than asserted as a string. That is the only thing that
 * catches a predicate put in a syntactically legal place but the wrong one,
 * and the only thing that proves the two attraction reads inside it (the
 * `total_attractions` subquery and the `latest_attraction_data` CTE) both
 * still parse.
 */
describe("LIVE_STATS_SQL — retired attractions (E2E)", () => {
  let app: INestApplication;
  let dataSource: DataSource;

  type LiveStatsRow = {
    id: string;
    total_attractions: number;
    operating_conf_count: number;
    explicitly_closed_count: number;
  };

  async function statsFor(parkId: string): Promise<LiveStatsRow> {
    const rows: LiveStatsRow[] = await dataSource.query(LIVE_STATS_SQL);
    const row = rows.find((r) => r.id === parkId);
    // The query LEFT JOINs from `parks`, so every seeded park has a row even
    // with no rides at all. A missing row means the shape changed, not that
    // the count is zero.
    expect(row).toBeDefined();
    return row!;
  }

  async function totalFor(parkId: string): Promise<number> {
    return Number((await statsFor(parkId)).total_attractions);
  }

  /**
   * A reading inside the CTE's 30-minute window. That window is the only way
   * into `latest_attraction_data`, and therefore the only way to reach the
   * predicate behind `closedAttractions`.
   */
  async function addReading(attractionId: string, status: LiveStatus) {
    // Built here rather than via `test/fixtures/queue-data.fixtures.ts`:
    // importing that file pulls in type errors it already carries on main,
    // and this spec has no business fixing them.
    const row = new QueueData();
    Object.assign(row, {
      id: randomUUID(),
      attractionId,
      queueType: QueueType.STANDBY,
      status,
      waitTime: status === LiveStatus.OPERATING ? 30 : 0,
      timestamp: new Date(),
      lastUpdated: new Date(),
    });
    await dataSource.getRepository(QueueData).save(row);
  }

  async function retire(attractionId: string) {
    const result = await dataSource
      .getRepository(Attraction)
      .update({ id: attractionId }, { retiredAt: new Date() });
    expect(result.affected).toBe(1);
  }

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
      ],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
  });

  afterAll(async () => {
    await app.close();
  });

  afterEach(async () => {
    await clearTestData(app);
  });

  it("runs against a real database", async () => {
    // The cheapest guard there is: the whole statement parses and returns the
    // columns the service reads. A predicate in the wrong clause fails here
    // and nowhere else in the suite.
    const rows: LiveStatsRow[] = await dataSource.query(LIVE_STATS_SQL);
    expect(Array.isArray(rows)).toBe(true);
  });

  it("stops counting an attraction once it is retired", async () => {
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const parkAttractions = seeded.attractions.filter(
      (a) => a.parkId === park.id,
    );
    expect(parkAttractions.length).toBeGreaterThan(1);

    // Reachable first (G-44): the park's rides are in the count before any of
    // them is retired, so the assertion below is about the predicate and not
    // about an empty table.
    expect(await totalFor(park.id)).toBe(parkAttractions.length);

    const retired = await dataSource
      .getRepository(Attraction)
      .update({ id: parkAttractions[0].id }, { retiredAt: new Date() });
    expect(retired.affected).toBe(1);

    expect(await totalFor(park.id)).toBe(parkAttractions.length - 1);
  });

  /**
   * `total_attractions` and the live counters are two separate reads of
   * `attractions`, and a spec that only covers the first leaves the second
   * free to drop its predicate without turning anything red. These two cases
   * exist because that is exactly what the first draft of this file did.
   *
   * Getting in needs a reading inside the CTE's 30-minute window, which is
   * also the real-world shape of the bug: a just-retired ride keeps its last
   * reading, and with it its place in the counts, until that window slides
   * past — and `writeHourlyHeartbeats` can push it back in, because that job
   * reads every attraction without a `retiredAt` predicate (PAR-295).
   *
   * `explicitly_closed_count` is the column with a reader: `hydrateStructure`
   * serves it as `closedAttractions` and derives `operatingAttractions` from
   * `totalAttractions - explicitlyClosedCount`. `operating_conf_count` is
   * asserted here as well because it is the CTE's other output, but it is
   * parsed into `ParkLiveStats` and never read (noted on PAR-296).
   */
  it("stops counting a just-retired ride as operating", async () => {
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const [operating, stillRunning] = seeded.attractions.filter(
      (a) => a.parkId === park.id,
    );
    await addReading(operating.id, LiveStatus.OPERATING);
    await addReading(stillRunning.id, LiveStatus.OPERATING);

    // Reachable (G-44): both rides are inside the window and counted.
    expect(Number((await statsFor(park.id)).operating_conf_count)).toBe(2);

    await retire(operating.id);

    const after = await statsFor(park.id);
    // The control is this 1 rather than a 0: the neighbour's reading is still
    // inside the window, so the drop came from the predicate and not from the
    // window sliding shut under both rides.
    expect(Number(after.operating_conf_count)).toBe(1);
    // And the subquery moved too — a different read of the same table, so it
    // is asserted separately rather than as evidence about the CTE.
    expect(Number(after.total_attractions)).toBe(
      seeded.attractions.filter((a) => a.parkId === park.id).length - 1,
    );
  });

  it("stops counting a just-retired ride as closed", async () => {
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const [closed, alsoClosed] = seeded.attractions.filter(
      (a) => a.parkId === park.id,
    );
    await addReading(closed.id, LiveStatus.CLOSED);
    await addReading(alsoClosed.id, LiveStatus.CLOSED);

    expect(Number((await statsFor(park.id)).explicitly_closed_count)).toBe(2);

    await retire(closed.id);

    expect(Number((await statsFor(park.id)).explicitly_closed_count)).toBe(1);
  });

  it("leaves the other park's count alone", async () => {
    // The counter is grouped per park; retiring a ride in one must not move
    // the neighbour, which is the failure mode of a predicate that lands in
    // the outer query instead of the correlated subquery.
    const seeded = await seedMinimalTestData(app);
    const [park, otherPark] = seeded.parks;
    const otherBefore = await totalFor(otherPark.id);

    await dataSource
      .getRepository(Attraction)
      .update(
        { id: seeded.attractions.find((a) => a.parkId === park.id)!.id },
        { retiredAt: new Date() },
      );

    expect(await totalFor(otherPark.id)).toBe(otherBefore);
  });
});
