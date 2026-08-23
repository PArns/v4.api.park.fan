import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { ParkSeason } from "../../src/parks/entities/park-season.entity";
import { AdminAuthService } from "../../src/admin/auth/admin-auth.service";
import { AdminSessionStore } from "../../src/admin/auth/admin-session.store";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import type { Redis } from "ioredis";

/**
 * The numbers behind the dashboard tiles.
 *
 * Worth a test precisely because they are counts: a tile is believed at a
 * glance and nobody re-derives it. "Curated" here has to mean the same thing
 * the editor's badge means — at least one hand-written column carries a value —
 * or the dashboard and the row it links to disagree about the same park.
 */
describe("Admin overview (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let redis: Redis;
  let token: string;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    app.setGlobalPrefix("v1");
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
        transformOptions: { enableImplicitConversion: true },
      }),
    );
    await app.init();

    dataSource = app.get(DataSource);
    redis = app.get(REDIS_CLIENT);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await redis.flushdb();

    const parkRepo = dataSource.getRepository(Park);
    const base = {
      latitude: 50.8,
      longitude: 6.88,
      timezone: "Europe/Berlin",
      continent: "Europe",
      continentSlug: "europe",
      country: "Germany",
      countrySlug: "germany",
      countryCode: "DE",
      city: "Brühl",
      citySlug: "bruehl",
    };
    const curatedPark = await parkRepo.save(
      parkRepo.create({
        ...base,
        externalId: "e2e-overview-1",
        name: "Phantasialand",
        slug: "phantasialand",
        curatedPhone: "+49 2232 36200",
      }),
    );
    await parkRepo.save(
      parkRepo.create({
        ...base,
        externalId: "e2e-overview-2",
        name: "Toverland",
        slug: "toverland",
      }),
    );

    const attractionRepo = dataSource.getRepository(Attraction);
    await attractionRepo.save([
      attractionRepo.create({
        externalId: "e2e-a1",
        name: "Taron",
        slug: "taron",
        parkId: curatedPark.id,
        curatedName: "Taron",
      }),
      attractionRepo.create({
        externalId: "e2e-a2",
        name: "Black Mamba",
        slug: "black-mamba",
        parkId: curatedPark.id,
        isSeasonal: true,
      }),
      attractionRepo.create({
        externalId: "e2e-a3",
        name: "Winterbahn",
        slug: "winterbahn",
        parkId: curatedPark.id,
        isSeasonal: true,
        seasonMonths: [12, 1],
      }),
      // Retired rows are not backlog and must not be counted as any.
      attractionRepo.create({
        externalId: "e2e-a4",
        name: "Abgerissen",
        slug: "abgerissen",
        parkId: curatedPark.id,
        retiredAt: new Date("2026-01-01T00:00:00Z"),
      }),
    ]);

    const seasonRepo = dataSource.getRepository(ParkSeason);
    const heute = new Date().toISOString().slice(0, 10);
    await seasonRepo.save([
      seasonRepo.create({
        parkId: curatedPark.id,
        kind: "halloween",
        startDate: heute,
        endDate: heute,
        status: "confirmed",
      }),
      seasonRepo.create({
        parkId: curatedPark.id,
        kind: "christmas",
        startDate: "2099-12-01",
        endDate: "2099-12-31",
        status: "expected",
      }),
    ]);

    const account = await app.get(AdminAuthService).create({
      email: "overview-e2e@example.invalid",
      role: "owner",
      password: "a-long-enough-password",
    });
    await dataSource.query(
      `UPDATE admin_users SET must_change_password = false WHERE id = $1`,
      [account.id],
    );
    const issued = await app.get(AdminSessionStore).create({
      userId: account.id,
      email: account.email,
      displayName: account.displayName,
      role: "owner",
      ip: null,
      userAgent: "e2e",
      mustChangePassword: false,
      mustEnrolTotp: false,
    });
    token = issued.token;
  });

  it("counts coverage the way the editor's badge does", async () => {
    const response = await request(app.getHttpServer())
      .get("/v1/admin/content/overview")
      .set("Authorization", `Bearer ${token}`);

    expect(response.status).toBe(200);
    expect(response.body.parks).toEqual({
      total: 2,
      curated: 1,
      withSeasons: 1,
    });
    expect(response.body.attractions).toMatchObject({
      // The retired one is not in any of these.
      total: 3,
      curated: 1,
      seasonal: 2,
      // One seasonal ride has months, the other has none.
      seasonalWithoutMonths: 1,
    });
  });

  it("separates a season running today from one still ahead", async () => {
    const response = await request(app.getHttpServer())
      .get("/v1/admin/content/overview")
      .set("Authorization", `Bearer ${token}`);

    expect(response.body.seasons).toEqual({
      total: 2,
      running: 1,
      upcoming: 1,
    });
  });

  it("reports the curation work of the last thirty days, by day", async () => {
    const response = await request(app.getHttpServer())
      .get("/v1/admin/content/overview")
      .set("Authorization", `Bearer ${token}`);

    expect(response.body.curations).toEqual({
      last30Days: 0,
      perDay: [],
    });
  });

  it("refuses without a session", async () => {
    await request(app.getHttpServer())
      .get("/v1/admin/content/overview")
      .expect(401);
  });
});
