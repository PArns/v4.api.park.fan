import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { AttractionsModule } from "../../src/attractions/attractions.module";
import { ParksModule } from "../../src/parks/parks.module";
import { QueueDataModule } from "../../src/queue-data/queue-data.module";
import { AnalyticsModule } from "../../src/analytics/analytics.module";
import { MLModule } from "../../src/ml/ml.module";
import { RedisModule, REDIS_CLIENT } from "../../src/common/redis/redis.module";
import { getDatabaseConfig } from "../../src/config/database.config";
import { seedMinimalTestData } from "../helpers/seed-test-data";
import { DataSource } from "typeorm";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import type { Redis } from "ioredis";

/**
 * Attractions are served under the park's geographic path — there is no flat
 * /v1/attractions collection. The fixture parks live at
 * north-america/united-states/orlando.
 */
describe("Park attractions (E2E)", () => {
  let app: INestApplication;
  let redis: Redis;

  const GEO = "/v1/parks/north-america/united-states/orlando";

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
        RedisModule,
        AttractionsModule,
        ParksModule,
        QueueDataModule,
        AnalyticsModule,
        MLModule,
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

    redis = app.get(REDIS_CLIENT);
  });

  afterAll(async () => {
    await app.close();
  });

  // The shared setup truncates every table after each test, so each one seeds its
  // own data. The responses are cached for five minutes, so the cache has to go
  // with them or a test reads the previous test's park.
  beforeEach(async () => {
    await redis.flushdb();
  });

  describe(`GET ${GEO}/:parkSlug/attractions`, () => {
    it("returns every attraction of the park with its pagination envelope", async () => {
      const { parks } = await seedMinimalTestData(app);
      const park = parks[0];

      const { body } = await request(app.getHttpServer())
        .get(`${GEO}/${park.slug}/attractions`)
        .expect(200);

      expect(body.data).toHaveLength(5);
      expect(body.pagination).toMatchObject({
        page: 1,
        total: 5,
        totalPages: 1,
        hasNext: false,
        hasPrevious: false,
      });
      expect(body.data[0]).toHaveProperty("id");
      expect(body.data[0]).toHaveProperty("name");
      expect(body.data[0]).toHaveProperty("slug");
    });

    it("scopes the list to the park in the path", async () => {
      const { parks } = await seedMinimalTestData(app);

      const [first, second] = await Promise.all([
        request(app.getHttpServer())
          .get(`${GEO}/${parks[0].slug}/attractions`)
          .expect(200),
        request(app.getHttpServer())
          .get(`${GEO}/${parks[1].slug}/attractions`)
          .expect(200),
      ]);

      const firstSlugs = first.body.data.map((a: { slug: string }) => a.slug);
      const secondSlugs = second.body.data.map((a: { slug: string }) => a.slug);

      expect(firstSlugs).toHaveLength(5);
      expect(secondSlugs).toHaveLength(5);
      expect(firstSlugs.filter((s: string) => secondSlugs.includes(s))).toEqual(
        [],
      );
    });

    it("404s for a park that does not exist", async () => {
      await seedMinimalTestData(app);

      await request(app.getHttpServer())
        .get(`${GEO}/no-such-park/attractions`)
        .expect(404);
    });
  });

  describe(`GET ${GEO}/:parkSlug/attractions/:attractionSlug`, () => {
    it("returns the attraction with its statistics block", async () => {
      const { parks, attractions } = await seedMinimalTestData(app);
      const park = parks[0];
      const attraction = attractions.find((a) => a.parkId === park.id)!;

      const { body } = await request(app.getHttpServer())
        .get(`${GEO}/${park.slug}/attractions/${attraction.slug}`)
        .expect(200);

      expect(body).toMatchObject({
        id: attraction.id,
        name: attraction.name,
        slug: attraction.slug,
      });
      expect(body).toHaveProperty("statistics");
    });

    it("404s for an attraction that does not exist", async () => {
      const { parks } = await seedMinimalTestData(app);

      await request(app.getHttpServer())
        .get(`${GEO}/${parks[0].slug}/attractions/non-existent-attraction`)
        .expect(404);
    });

    /**
     * The park list has excluded retired attractions since PAR-159; PAR-233
     * added search, the favorites list and the geo count. This is the half of
     * the promise that pulls the other way, and the one a filter one clause
     * too broad would quietly break:
     * `retiredAt` says the row "keeps answering here so its history stays
     * readable", and a 404 would turn a ride that existed into one that never
     * did.
     */
    it("still answers for a retired attraction while the park list drops it", async () => {
      const { parks, attractions } = await seedMinimalTestData(app);
      const park = parks[0];
      const retired = attractions.find((a) => a.parkId === park.id)!;

      // Reachable in both places first, so neither assertion below can pass
      // for the wrong reason (G-44).
      await request(app.getHttpServer())
        .get(`${GEO}/${park.slug}/attractions/${retired.slug}`)
        .expect(200);
      const before = await request(app.getHttpServer())
        .get(`${GEO}/${park.slug}/attractions`)
        .expect(200);
      expect(before.body.data.map((a: { slug: string }) => a.slug)).toContain(
        retired.slug,
      );

      const updated = await app
        .get(DataSource)
        .getRepository(Attraction)
        .update({ id: retired.id }, { retiredAt: new Date() });
      expect(updated.affected).toBe(1);
      await redis.flushdb();

      // Gone from the list that describes the park as it is today ...
      const after = await request(app.getHttpServer())
        .get(`${GEO}/${park.slug}/attractions`)
        .expect(200);
      const slugs = after.body.data.map((a: { slug: string }) => a.slug);
      expect(slugs).not.toContain(retired.slug);
      expect(slugs).toHaveLength(4);

      // ... and still readable on its own route, with its retirement stated.
      const { body } = await request(app.getHttpServer())
        .get(`${GEO}/${park.slug}/attractions/${retired.slug}`)
        .expect(200);
      expect(body.id).toBe(retired.id);
      expect(body.retiredAt).not.toBeNull();
    });
  });
});
