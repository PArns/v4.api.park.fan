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
import { getDatabaseConfig } from "../../src/config/database.config";
import { seedMinimalTestData, clearTestData } from "../helpers/seed-test-data";

describe("AttractionsController (E2E)", () => {
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
  });

  afterAll(async () => {
    await app.close();
  });

  afterEach(async () => {
    // Clean data after each test
    await clearTestData(app);
  });

  describe("GET /v1/attractions", () => {
    it("should return empty array when no attractions exist", () => {
      return request(app.getHttpServer())
        .get("/v1/attractions")
        .expect(200)
        .expect([]);
    });

    it("should return all attractions when attractions exist", async () => {
      // Seed test data
      await seedMinimalTestData(app);

      const response = await request(app.getHttpServer())
        .get("/v1/attractions")
        .expect(200);

      expect(response.body).toHaveLength(10); // 2 parks * 5 attractions each
      expect(response.body[0]).toHaveProperty("id");
      expect(response.body[0]).toHaveProperty("name");
      expect(response.body[0]).toHaveProperty("slug");
      expect(response.body[0]).toHaveProperty("park");
      expect(response.body[0].park).toHaveProperty("id");
      expect(response.body[0].name).toContain("Test");
    });

    it("should filter attractions by park", async () => {
      // Seed test data
      const { parks } = await seedMinimalTestData(app);
      const firstPark = parks[0];

      const response = await request(app.getHttpServer())
        .get(`/v1/attractions?park=${firstPark.slug}`)
        .expect(200);

      expect(response.body).toHaveLength(5);
      response.body.forEach((attraction: any) => {
        expect(attraction.park.id).toBe(firstPark.id);
      });
    });
  });

  describe("GET /v1/attractions/:slug", () => {
    it("should return 404 when attraction does not exist", () => {
      return request(app.getHttpServer())
        .get("/v1/attractions/non-existent-attraction")
        .expect(404);
    });

    it("should return attraction by slug", async () => {
      // Seed test data
      const { attractions } = await seedMinimalTestData(app);
      const testAttraction = attractions[0];

      const response = await request(app.getHttpServer())
        .get(`/v1/attractions/${testAttraction.slug}`)
        .expect(200);

      expect(response.body).toHaveProperty("id", testAttraction.id);
      expect(response.body).toHaveProperty("name", testAttraction.name);
      expect(response.body).toHaveProperty("slug", testAttraction.slug);
      expect(response.body).toHaveProperty("park");
      expect(response.body.park.id).toBe(testAttraction.parkId);

      // Check that statistics and prediction accuracy exist (always present)
      expect(response.body).toHaveProperty("statistics");
      expect(response.body).toHaveProperty("predictionAccuracy");

      // Note: queues, forecasts, and predictions are only included if data exists
      // Since test fixtures don't have queue data, these fields won't be present
    });
  });
});
