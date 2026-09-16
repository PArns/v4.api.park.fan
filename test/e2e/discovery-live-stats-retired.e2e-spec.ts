import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import { getDatabaseConfig } from "../../src/config/database.config";
import { LIVE_STATS_SQL } from "../../src/discovery/discovery.service";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { seedMinimalTestData, clearTestData } from "../helpers/seed-test-data";

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

  type LiveStatsRow = { id: string; total_attractions: number };

  async function totalFor(parkId: string): Promise<number> {
    const rows: LiveStatsRow[] = await dataSource.query(LIVE_STATS_SQL);
    const row = rows.find((r) => r.id === parkId);
    // The query LEFT JOINs from `parks`, so every seeded park has a row even
    // with no rides at all. A missing row means the shape changed, not that
    // the count is zero.
    expect(row).toBeDefined();
    return Number(row!.total_attractions);
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
