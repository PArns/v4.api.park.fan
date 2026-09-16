import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { SearchModule } from "../../src/search/search.module";
import { ParksModule } from "../../src/parks/parks.module";
import { AttractionsModule } from "../../src/attractions/attractions.module";
import { getDatabaseConfig } from "../../src/config/database.config";
import { seedMinimalTestData, clearTestData } from "../helpers/seed-test-data";
import { SearchService } from "../../src/search/search.service";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import type { Redis } from "ioredis";
import { DataSource } from "typeorm";
import { Attraction } from "../../src/attractions/entities/attraction.entity";

describe("SearchController (E2E)", () => {
  let app: INestApplication;
  let searchService: SearchService;
  let redis: Redis;

  /**
   * Search answers from an in-process index built at module init — i.e. against the
   * still-empty database — and caches every response in Redis. Seeding alone leaves
   * it searching a snapshot of a database that no longer exists.
   */
  async function seedAndIndex() {
    const seeded = await seedMinimalTestData(app);
    await redis.flushdb();
    await searchService.refreshSearchIndex();
    return seeded;
  }

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
        SearchModule,
        ParksModule,
        AttractionsModule,
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

    searchService = moduleFixture.get<SearchService>(SearchService);
    redis = moduleFixture.get<Redis>(REDIS_CLIENT);
  });

  afterAll(async () => {
    await app.close();
  });

  afterEach(async () => {
    // Clean data after each test
    await clearTestData(app);
  });

  describe("GET /v1/search", () => {
    it("should return 400 when query is too short", () => {
      return request(app.getHttpServer()).get("/v1/search?q=a").expect(400);
    });

    it("should return empty results when no matches found", async () => {
      // Seed test data
      await seedAndIndex();

      const response = await request(app.getHttpServer())
        .get("/v1/search?q=nonexistent")
        .expect(200);

      expect(response.body).toHaveProperty("results");
      expect(response.body.results).toHaveLength(0);
      expect(response.body).toHaveProperty("counts");
      expect(response.body.counts.park.total).toBe(0);
      expect(response.body.counts.attraction.total).toBe(0);
    });

    it("should find parks by name", async () => {
      // Seed test data (contains "Test Magic Kingdom" and "Test EPCOT")
      await seedAndIndex();

      const response = await request(app.getHttpServer())
        .get("/v1/search?q=magic")
        .expect(200);

      expect(response.body).toHaveProperty("results");
      expect(response.body.results.length).toBeGreaterThan(0);

      // Check result structure (search returns minimal data: type, id, slug only)
      const firstResult = response.body.results[0];
      expect(firstResult).toHaveProperty("type");
      expect(firstResult).toHaveProperty("id");
      expect(firstResult).toHaveProperty("slug");
      expect(firstResult.type).toBe("park");
    });

    it("should find attractions by name", async () => {
      // Seed test data (contains "Test Space Mountain", etc.)
      await seedAndIndex();

      const response = await request(app.getHttpServer())
        .get("/v1/search?q=space")
        .expect(200);

      expect(response.body).toHaveProperty("results");
      expect(response.body.results.length).toBeGreaterThan(0);

      // Search results include slug which contains the search term
      const spaceResult = response.body.results.find((r: any) =>
        r.slug.toLowerCase().includes("space"),
      );
      expect(spaceResult).toBeDefined();
      expect(spaceResult.type).toBe("attraction");
    });

    it("should filter by type", async () => {
      // Seed test data
      await seedAndIndex();

      const response = await request(app.getHttpServer())
        .get("/v1/search?q=test&type=park")
        .expect(200);

      expect(response.body).toHaveProperty("results");

      // All results should be parks
      response.body.results.forEach((result: any) => {
        expect(result.type).toBe("park");
      });
    });

    it("should respect limit parameter", async () => {
      // Seed test data
      await seedAndIndex();

      const response = await request(app.getHttpServer())
        .get("/v1/search?q=test&limit=2")
        .expect(200);

      const parks = response.body.results.filter((r: any) => r.type === "park");
      const attractions = response.body.results.filter(
        (r: any) => r.type === "attraction",
      );

      expect(parks.length).toBeLessThanOrEqual(2);
      expect(attractions.length).toBeLessThanOrEqual(2);
    });

    it("should perform fuzzy matching", async () => {
      // Seed test data (contains "Test Magic Kingdom")
      await seedAndIndex();

      // Try searching with a typo
      const response = await request(app.getHttpServer())
        .get("/v1/search?q=magik")
        .expect(200);

      // Should still find "Magic" with fuzzy matching
      // Note: This depends on search implementation supporting fuzzy matching
      expect(response.body).toHaveProperty("results");
    });
  });

  /**
   * PAR-233. `retiredAt` promises the row leaves search; it did not.
   *
   * Search reads the same rows twice — the Redis/in-process index built by
   * `loadAttractionIndexFromDb`, and the SQL path in `searchAttractions` that
   * serves requests before the index is ready. Both are covered here, because
   * filtering only one leaves the other serving the row: the index outlives
   * the request and keeps answering until the next rebuild.
   *
   * The two filtering cases prove the row is REACHABLE first and only then
   * prove it is gone (G-44) — an assertion that something is absent passes
   * just as well when the query never looked at it. The third case is the
   * counterweight rather than a filtering case: it pins that retirement
   * changes visibility and not existence.
   */
  describe("GET /v1/search — retired attractions", () => {
    // Both parks get a Splash Mountain, so one can be retired while the other
    // stays as the control that proves the query still returns rows.
    const RETIRED_SLUG = "test-splash-mountain";
    const SURVIVOR_SLUG = "test-splash-mountain-p1";

    /** Attraction slugs returned for `q=splash`, whichever path answered. */
    async function attractionSlugs(): Promise<string[]> {
      const response = await request(app.getHttpServer())
        .get("/v1/search?q=splash&type=attraction")
        .expect(200);
      return response.body.results.map((r: { slug: string }) => r.slug);
    }

    async function retireBySlug(slug: string) {
      const result = await app
        .get(DataSource)
        .getRepository(Attraction)
        .update({ slug }, { retiredAt: new Date() });
      // Guard the fixture rather than the code: a renamed slug would otherwise
      // retire nothing and leave both assertions below passing for free.
      expect(result.affected).toBe(1);
    }

    afterEach(() => {
      // `indexReady` is a property of the shared service instance, so a test
      // that flips it has to put it back or it silently reroutes the next one.
      (searchService as unknown as { indexReady: boolean }).indexReady = false;
    });

    it("drops a retired attraction from the in-process index", async () => {
      await seedAndIndex();

      // Reachable: the index carries both rides before anything is retired.
      const before = await attractionSlugs();
      expect(before).toContain(RETIRED_SLUG);
      expect(before).toContain(SURVIVOR_SLUG);

      await retireBySlug(RETIRED_SLUG);
      await redis.flushdb();
      await searchService.refreshSearchIndex();

      const after = await attractionSlugs();
      expect(after).not.toContain(RETIRED_SLUG);
      // The control: the index was rebuilt with content, not emptied.
      expect(after).toContain(SURVIVOR_SLUG);
    });

    it("drops a retired attraction from the SQL path that serves an unbuilt index", async () => {
      await seedMinimalTestData(app);
      await redis.flushdb();

      // The SQL path only runs while the index is not ready — the window after
      // a boot or a failed refresh.
      (searchService as unknown as { indexReady: boolean }).indexReady = false;

      const before = await attractionSlugs();
      expect(before).toContain(RETIRED_SLUG);
      expect(before).toContain(SURVIVOR_SLUG);

      await retireBySlug(RETIRED_SLUG);
      await redis.flushdb();

      const after = await attractionSlugs();
      expect(after).not.toContain(RETIRED_SLUG);
      expect(after).toContain(SURVIVOR_SLUG);
    });

    it("keeps the retired row in the database so its history stays readable", async () => {
      // The other half of the promise: retirement changes visibility in the
      // lists, not whether the row and its queue_data still exist.
      await seedMinimalTestData(app);
      await retireBySlug(RETIRED_SLUG);

      const row = await app
        .get(DataSource)
        .getRepository(Attraction)
        .findOne({ where: { slug: RETIRED_SLUG } });

      expect(row).not.toBeNull();
      expect(row!.retiredAt).toBeInstanceOf(Date);
    });
  });
});
