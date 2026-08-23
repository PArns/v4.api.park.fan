import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { ParkSeason } from "../../src/parks/entities/park-season.entity";
import { AdminAuthService } from "../../src/admin/auth/admin-auth.service";
import { AdminSessionStore } from "../../src/admin/auth/admin-session.store";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import type { Redis } from "ioredis";

/**
 * Creating a season, over HTTP, through everything.
 *
 * Every piece of this path had been checked on its own — the route answers, the
 * DTO accepts the editor's payload, the service writes the row against
 * production data, the frontend proxy forwards it unchanged — and creating a
 * season still did not work. What none of those covered is the request as it
 * actually arrives: the guard resolving a real session, the global pipe with
 * `forbidNonWhitelisted` and implicit conversion, the audit interceptor, and
 * the publish that runs after the write.
 *
 * So this test is the one that asks the whole question. The pipe is configured
 * exactly as `main.ts` does it, because a pipe that differs from production is
 * a test that agrees with itself.
 */
describe("Admin seasons (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let redis: Redis;
  let token: string;
  let parkId: string;

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
    const park = await parkRepo.save(
      parkRepo.create({
        externalId: "e2e-seasons-park",
        name: "Phantasialand",
        slug: "phantasialand",
        latitude: 50.7986,
        longitude: 6.8792,
        timezone: "Europe/Berlin",
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Brühl",
        citySlug: "bruehl",
      }),
    );
    parkId = park.id;

    const auth = app.get(AdminAuthService);
    const account = await auth.create({
      email: "seasons-e2e@example.invalid",
      role: "owner",
      password: "a-long-enough-password",
    });
    // Created accounts owe a password change, and the guard lets such a session
    // reach nothing else — which is correct, and not what this test is about.
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

  /** Exactly what the season dialog sends when nothing optional is filled in. */
  const editorPayload = {
    kind: "halloween",
    name: "Halloween Nights",
    startDate: "2026-10-03",
    endDate: "2026-11-01",
    dates: null,
    status: "announced",
    separateTicket: false,
    priceFrom: null,
    priceCurrency: null,
    opensAt: null,
    closesAt: null,
    url: null,
    sourceUrl: null,
    note: null,
  };

  it("creates a season from the editor's payload", async () => {
    const response = await request(app.getHttpServer())
      .post(`/v1/admin/content/parks/${parkId}/seasons`)
      .set("Authorization", `Bearer ${token}`)
      .send(editorPayload);

    expect(response.status).toBe(201);
    expect(response.body).toMatchObject({
      kind: "halloween",
      startDate: "2026-10-03",
      endDate: "2026-11-01",
    });

    const rows = await dataSource.getRepository(ParkSeason).find();
    expect(rows).toHaveLength(1);
  });

  it("creates one with individual dates and a price", async () => {
    const response = await request(app.getHttpServer())
      .post(`/v1/admin/content/parks/${parkId}/seasons`)
      .set("Authorization", `Bearer ${token}`)
      .send({
        ...editorPayload,
        kind: "special_event",
        name: "Spooky Days",
        startDate: "2026-10-14",
        endDate: "2026-10-21",
        dates: ["2026-10-14", "2026-10-15", "2026-10-19"],
        status: "confirmed",
        separateTicket: true,
        priceFrom: 53.5,
        priceCurrency: "EUR",
        opensAt: "18:00",
        closesAt: "23:00",
      });

    expect(response.status).toBe(201);
    expect(response.body.dates).toEqual([
      "2026-10-14",
      "2026-10-15",
      "2026-10-19",
    ]);
  });

  it("says which field is wrong rather than failing opaquely", async () => {
    const response = await request(app.getHttpServer())
      .post(`/v1/admin/content/parks/${parkId}/seasons`)
      .set("Authorization", `Bearer ${token}`)
      .send({ ...editorPayload, endDate: "2026-09-01" });

    expect(response.status).toBe(400);
    expect(JSON.stringify(response.body)).toMatch(/endDate/i);
  });

  it("refuses without a session", async () => {
    const response = await request(app.getHttpServer())
      .post(`/v1/admin/content/parks/${parkId}/seasons`)
      .send(editorPayload);

    expect(response.status).toBe(401);
  });
});
