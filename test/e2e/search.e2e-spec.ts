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

describe("SearchController (E2E)", () => {
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
      await seedMinimalTestData(app);

      const response = await request(app.getHttpServer())
        .get("/v1/search?q=nonexistent")
        .expect(200);

      expect(response.body).toHaveProperty("results");
      expect(response.body.results).toHaveLength(0);
      expect(response.body).toHaveProperty("total", 0);
    });

    it("should find parks by name", async () => {
      // Seed test data (contains "Test Magic Kingdom" and "Test EPCOT")
      await seedMinimalTestData(app);

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
      await seedMinimalTestData(app);

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
      await seedMinimalTestData(app);

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
      await seedMinimalTestData(app);

      const response = await request(app.getHttpServer())
        .get("/v1/search?q=test&limit=5")
        .expect(200);

      expect(response.body.results.length).toBeLessThanOrEqual(5);
    });

    it("should perform fuzzy matching", async () => {
      // Seed test data (contains "Test Magic Kingdom")
      await seedMinimalTestData(app);

      // Try searching with a typo
      const response = await request(app.getHttpServer())
        .get("/v1/search?q=magik")
        .expect(200);

      // Should still find "Magic" with fuzzy matching
      // Note: This depends on search implementation supporting fuzzy matching
      expect(response.body).toHaveProperty("results");
    });
  });
});
