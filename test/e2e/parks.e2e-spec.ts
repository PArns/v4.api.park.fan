import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { ParksModule } from "../../src/parks/parks.module";
import { DestinationsModule } from "../../src/destinations/destinations.module";
import { getDatabaseConfig } from "../../src/config/database.config";
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

  describe("GET /v1/parks", () => {
    it("should return empty array when no parks exist", () => {
      return request(app.getHttpServer())
        .get("/v1/parks")
        .expect(200)
        .expect([]);
    });

    it("should return all parks when parks exist", async () => {
      // Seed test data
      const { parks } = await seedMinimalTestData(app);

      const response = await request(app.getHttpServer())
        .get("/v1/parks")
        .expect(200);

      expect(response.body).toHaveLength(2);
      expect(response.body[0]).toHaveProperty("id");
      expect(response.body[0]).toHaveProperty("name");
      expect(response.body[0]).toHaveProperty("slug");
      expect(response.body[0].name).toContain("Test");
    });
  });

  describe("GET /v1/parks/:slug", () => {
    it("should return 404 when park does not exist", () => {
      return request(app.getHttpServer())
        .get("/v1/parks/non-existent-park")
        .expect(404);
    });

    it("should return park by slug", async () => {
      // Seed test data
      const { parks } = await seedMinimalTestData(app);
      const testPark = parks[0];

      const response = await request(app.getHttpServer())
        .get(`/v1/parks/${testPark.slug}`)
        .expect(200);

      expect(response.body).toHaveProperty("id", testPark.id);
      expect(response.body).toHaveProperty("name", testPark.name);
      expect(response.body).toHaveProperty("slug", testPark.slug);
      expect(response.body).toHaveProperty("timezone", testPark.timezone);
    });
  });
});
