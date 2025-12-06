import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { AnalyticsModule } from "../../src/analytics/analytics.module";
import { getDatabaseConfig } from "../../src/config/database.config";
import { RedisModule } from "../../src/common/redis/redis.module";
import { QueueDataModule } from "../../src/queue-data/queue-data.module";
import { ParksModule } from "../../src/parks/parks.module";

describe("AnalyticsController (E2E)", () => {
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
        RedisModule,
        QueueDataModule,
        ParksModule,
        AnalyticsModule,
      ],
    }).compile();

    app = moduleFixture.createNestApplication();

    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
      }),
    );

    app.setGlobalPrefix("v1");

    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  describe("GET /v1/analytics/realtime", () => {
    it("should return comprehensive global statistics with valid data", async () => {
      const { body } = await request(app.getHttpServer())
        .get("/v1/analytics/realtime")
        .expect(200);

      // Validate root structure
      expect(body).toHaveProperty("counts");
      expect(body).toHaveProperty("mostCrowdedPark");
      expect(body).toHaveProperty("leastCrowdedPark");
      expect(body).toHaveProperty("longestWaitRide");
      expect(body).toHaveProperty("shortestWaitRide");
      expect(body).toHaveProperty("lastUpdated");

      // Validate counts object structure and types
      expect(body.counts).toMatchObject({
        openParks: expect.any(Number),
        closedParks: expect.any(Number),
        parks: expect.any(Number),
        attractions: expect.any(Number),
        shows: expect.any(Number),
        restaurants: expect.any(Number),
        queueDataRecords: expect.any(Number),
        weatherDataRecords: expect.any(Number),
        scheduleEntries: expect.any(Number),
        restaurantLiveDataRecords: expect.any(Number),
        showLiveDataRecords: expect.any(Number),
        waitTimePredictions: expect.any(Number),
      });

      // Validate count logic
      expect(body.counts.parks).toBe(
        body.counts.openParks + body.counts.closedParks,
      );
      expect(body.counts.openParks).toBeGreaterThanOrEqual(0);
      expect(body.counts.closedParks).toBeGreaterThanOrEqual(0);

      // Validate all counts are non-negative
      Object.values(body.counts).forEach((count) => {
        expect(count).toBeGreaterThanOrEqual(0);
      });

      // Validate park stats if present
      if (body.mostCrowdedPark) {
        expect(body.mostCrowdedPark).toMatchObject({
          id: expect.any(String),
          name: expect.any(String),
          slug: expect.any(String),
          averageWaitTime: expect.any(Number),
          url: expect.any(String),
          crowdLevel: expect.any(String),
        });

        // URL should start with /v1/parks/
        expect(body.mostCrowdedPark.url).toMatch(/^\/v1\/parks\//);

        // crowdLevel should be valid
        expect([
          "very_low",
          "low",
          "normal",
          "high",
          "very_high",
          "extreme",
        ]).toContain(body.mostCrowdedPark.crowdLevel);

        // averageWaitTime should be reasonable (0-300 minutes)
        expect(body.mostCrowdedPark.averageWaitTime).toBeGreaterThanOrEqual(0);
        expect(body.mostCrowdedPark.averageWaitTime).toBeLessThanOrEqual(300);
      }

      if (body.leastCrowdedPark) {
        expect(body.leastCrowdedPark).toMatchObject({
          id: expect.any(String),
          name: expect.any(String),
          slug: expect.any(String),
          averageWaitTime: expect.any(Number),
          url: expect.any(String),
          crowdLevel: expect.any(String),
        });

        expect(body.leastCrowdedPark.url).toMatch(/^\/v1\/parks\//);
        expect([
          "very_low",
          "low",
          "normal",
          "high",
          "very_high",
          "extreme",
        ]).toContain(body.leastCrowdedPark.crowdLevel);
      }

      // Validate ride stats if present
      if (body.longestWaitRide) {
        expect(body.longestWaitRide).toMatchObject({
          id: expect.any(String),
          name: expect.any(String),
          slug: expect.any(String),
          parkName: expect.any(String),
          parkSlug: expect.any(String),
          waitTime: expect.any(Number),
          url: expect.any(String),
          crowdLevel: expect.any(String),
        });

        // URL should match /v1/parks/:slug/attractions/:slug format
        expect(body.longestWaitRide.url).toMatch(
          /^\/v1\/parks\/[^/]+\/attractions\/[^/]+$/,
        );

        expect([
          "very_low",
          "low",
          "normal",
          "high",
          "very_high",
          "extreme",
        ]).toContain(body.longestWaitRide.crowdLevel);

        // waitTime should be reasonable (0-500 minutes)
        expect(body.longestWaitRide.waitTime).toBeGreaterThanOrEqual(0);
        expect(body.longestWaitRide.waitTime).toBeLessThanOrEqual(500);
      }

      if (body.shortestWaitRide) {
        expect(body.shortestWaitRide).toMatchObject({
          id: expect.any(String),
          name: expect.any(String),
          slug: expect.any(String),
          parkName: expect.any(String),
          parkSlug: expect.any(String),
          waitTime: expect.any(Number),
          url: expect.any(String),
          crowdLevel: expect.any(String),
        });

        expect(body.shortestWaitRide.url).toMatch(
          /^\/v1\/parks\/[^/]+\/attractions\/[^/]+$/,
        );

        expect([
          "very_low",
          "low",
          "normal",
          "high",
          "very_high",
          "extreme",
        ]).toContain(body.shortestWaitRide.crowdLevel);
      }

      // Validate lastUpdated is a valid ISO timestamp
      expect(new Date(body.lastUpdated).toISOString()).toBe(body.lastUpdated);

      // If both parks exist, most crowded should have >= wait time
      if (body.mostCrowdedPark && body.leastCrowdedPark) {
        expect(body.mostCrowdedPark.averageWaitTime).toBeGreaterThanOrEqual(
          body.leastCrowdedPark.averageWaitTime,
        );
      }

      // If both rides exist, longest should have >= wait time
      if (body.longestWaitRide && body.shortestWaitRide) {
        expect(body.longestWaitRide.waitTime).toBeGreaterThanOrEqual(
          body.shortestWaitRide.waitTime,
        );
      }
    });

    it("should handle empty data gracefully", async () => {
      const { body } = await request(app.getHttpServer())
        .get("/v1/analytics/realtime")
        .expect(200);

      // Even with no data, structure should be valid
      expect(body).toHaveProperty("counts");
      expect(body).toHaveProperty("lastUpdated");

      // Counts can be 0 but should exist
      expect(typeof body.counts.parks).toBe("number");
      expect(typeof body.counts.attractions).toBe("number");
    });
  });
});
