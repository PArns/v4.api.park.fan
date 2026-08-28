import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import type { Redis } from "ioredis";
import { ParksModule } from "../../src/parks/parks.module";
import { DestinationsModule } from "../../src/destinations/destinations.module";
import { getDatabaseConfig } from "../../src/config/database.config";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { createTestPark } from "../fixtures/park.fixtures";
import { createTestAttraction } from "../fixtures/attraction.fixtures";
import { seedMinimalTestData, clearTestData } from "../helpers/seed-test-data";

describe("ParksController (E2E)", () => {
  let app: INestApplication;

  beforeAll(async () => {
    const dbConfig = getDatabaseConfig();

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({
          isGlobal: true,
          envFilePath: ".env.test",
        }),
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
        DestinationsModule,
      ],
    }).compile();

    app = moduleFixture.createNestApplication();

    // Apply global pipes (same as production)
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
      }),
    );

    // Apply global prefix (same as production)
    app.setGlobalPrefix("v1");

    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  afterEach(async () => {
    // Clean data after each test
    await clearTestData(app);
  });

  // The fixture parks live at north-america/united-states/orlando; a park is only
  // addressable through that geographic path, not by slug alone.
  const GEO = "/v1/parks/north-america/united-states/orlando";

  describe("GET /v1/parks", () => {
    it("returns an empty page when no parks exist", async () => {
      const { body } = await request(app.getHttpServer())
        .get("/v1/parks")
        .expect(200);

      expect(body.data).toEqual([]);
      expect(body.pagination).toMatchObject({ total: 0, totalPages: 0 });
    });

    it("returns every park with its pagination envelope", async () => {
      await seedMinimalTestData(app);

      const { body } = await request(app.getHttpServer())
        .get("/v1/parks")
        .expect(200);

      expect(body.data).toHaveLength(2);
      expect(body.pagination).toMatchObject({ page: 1, total: 2 });
      expect(body.data[0]).toHaveProperty("id");
      expect(body.data[0]).toHaveProperty("name");
      expect(body.data[0]).toHaveProperty("slug");
      expect(body.data[0].name).toContain("Test");
    });
  });

  describe(`GET ${GEO}/:parkSlug`, () => {
    it("404s when the park does not exist", () => {
      return request(app.getHttpServer())
        .get(`${GEO}/non-existent-park`)
        .expect(404);
    });

    it("returns the park addressed by its geographic path", async () => {
      const { parks } = await seedMinimalTestData(app);
      const testPark = parks[0];

      const { body } = await request(app.getHttpServer())
        .get(`${GEO}/${testPark.slug}`)
        .expect(200);

      expect(body).toMatchObject({
        id: testPark.id,
        name: testPark.name,
        slug: testPark.slug,
        timezone: testPark.timezone,
      });
    });
  });

  /**
   * The first half hour of a park's day, which used to have its own truth.
   *
   * A queue row is written when a value changes plus an hourly heartbeat, so a ride
   * that has been shut for months carries a reading from before the gates opened —
   * and the query that fetches "current status" used to start counting at today's
   * opening time, which left every such ride with nothing at all. An open park with
   * no reading means an optimistically OPERATING ride, so between 09:00 and the poll
   * that landed at 09:23 Phantasialand served all 40 of its attractions as running,
   * an ice rink in August among them, while the source had said CLOSED all night.
   */
  describe(`GET ${GEO}/:parkSlug — a ride whose feed has not changed since before opening`, () => {
    const SLUG = "test-just-opened-park";

    it("serves the source's last word instead of an optimistic OPERATING", async () => {
      const dataSource = app.get(DataSource);
      // The park response caches under the request URL and under the park id;
      // both would answer with a previous test's park otherwise.
      await app.get<Redis>(REDIS_CLIENT).flushdb();

      const park = await dataSource.getRepository(Park).save(
        createTestPark({
          externalId: "test-park-just-opened",
          name: "Test Just Opened Park",
          slug: SLUG,
          // Park-local "today" has to be the day the schedule row is dated on,
          // and only UTC guarantees that at every hour the suite might run.
          timezone: "UTC",
        }),
      );

      const attraction = await dataSource.getRepository(Attraction).save(
        createTestAttraction(park.id, {
          externalId: "test-attr-ice-rink",
          name: "Test Ice Rink",
          slug: "test-ice-rink",
        }),
      );

      // Gates opened ten minutes ago.
      await dataSource.query(
        `INSERT INTO schedule_entries (id, "parkId", date, "scheduleType", "openingTime", "closingTime")
         VALUES (gen_random_uuid(), $1, CURRENT_DATE, 'OPERATING',
                 NOW() - INTERVAL '10 minutes', NOW() + INTERVAL '8 hours')`,
        [park.id],
      );

      // The source's last word, from before that: closed, as it has been all night.
      await dataSource.query(
        `INSERT INTO queue_data (id, "attractionId", "queueType", status, "waitTime", timestamp, data_source)
         VALUES (gen_random_uuid(), $1, 'STANDBY', 'CLOSED', 0, NOW() - INTERVAL '45 minutes', 'themeparks-wiki')`,
        [attraction.id],
      );

      const { body } = await request(app.getHttpServer())
        .get(`${GEO}/${SLUG}`)
        .expect(200);

      expect(body.status).toBe("OPERATING");
      const ride = body.attractions.find(
        (a: { id: string }) => a.id === attraction.id,
      );
      expect(ride).toBeDefined();
      expect(ride.status).toBe("CLOSED");
      expect(ride.effectiveStatus).toBe("CLOSED");
    });
  });
});
