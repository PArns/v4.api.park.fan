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
});
