import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import {
  GLOBAL_PREFIX,
  GLOBAL_PREFIX_OPTIONS,
} from "../../src/common/swagger/swagger-config";

/**
 * A visitor's account against Postgres and Redis: registering signs it in,
 * its synced state is its own and written field by field, it reaches nothing
 * of the developer API or the admin, and it deletes itself with its password.
 */
describe("Visitor accounts (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let address = 0;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    app.setGlobalPrefix(GLOBAL_PREFIX, GLOBAL_PREFIX_OPTIONS);
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
  });

  afterAll(async () => {
    await app.close();
  });

  function call(
    method: "get" | "post" | "put" | "delete",
    path: string,
    session?: string,
  ) {
    address += 1;
    const req = request(app.getHttpServer())
      [method](path)
      .set("cf-connecting-ip", `198.51.100.${address}`);
    return session ? req.set("Authorization", `Bearer ${session}`) : req;
  }

  const PARK = "e119ba74-0e0e-49f2-bf33-d4db83722b32";
  const RIDE = "0b6f5c1e-2a8d-4d7e-9a43-5d3c8f1e2b10";

  it("registers, syncs its own state, reaches nothing else and deletes itself", async () => {
    const registered = await call("post", "/v1/account/register")
      .send({ email: "fan@e2e.test", password: "a-long-enough-password" })
      .expect(201);
    expect(registered.body.user.role).toBe("user");
    const session = registered.body.token as string;

    const empty = await call("get", "/v1/account", session).expect(200);
    expect(empty.body.data.favorites.parks).toEqual([]);

    await call("put", "/v1/account/data", session)
      .send({
        favorites: {
          parks: [PARK, PARK],
          attractions: [RIDE],
          shows: [],
          restaurants: [],
        },
        temperatureUnit: "F",
      })
      .expect(200);
    await call("put", "/v1/account/data", session)
      .send({ riddenRides: [RIDE] })
      .expect(200);
    await call("put", "/v1/account/data", session)
      .send({ riddenRides: ["not-a-uuid"] })
      .expect(400);
    const synced = await call("get", "/v1/account", session).expect(200);
    expect(synced.body.data).toMatchObject({
      favorites: { parks: [PARK], attractions: [RIDE] },
      riddenRides: [RIDE],
      temperatureUnit: "F",
    });

    await call("get", "/v1/developer/overview", session).expect(403);
    await call("get", "/v1/admin/auth/users", session).expect(403);

    await call("delete", "/v1/account", session)
      .send({ password: "wrong-password-here" })
      .expect(401);
    const deleted = await call("delete", "/v1/account", session)
      .send({ password: "a-long-enough-password" })
      .expect(200);
    expect(deleted.body.erased).toMatchObject({ accountData: 1 });
    const left = await dataSource.query(
      `SELECT
         (SELECT count(*)::int FROM admin_users WHERE email = 'fan@e2e.test') AS users,
         (SELECT count(*)::int FROM account_data WHERE user_id = $1::uuid) AS data`,
      [registered.body.user.id],
    );
    expect(left[0]).toEqual({ users: 0, data: 0 });
    await call("get", "/v1/account", session).expect(401);
  });

  it("refuses a taken address and lets no staff account delete itself", async () => {
    await call("post", "/v1/account/register")
      .send({ email: "taken@e2e.test", password: "a-long-enough-password" })
      .expect(201);
    const again = await call("post", "/v1/account/register")
      .send({ email: "taken@e2e.test", password: "a-long-enough-password" })
      .expect(409);
    expect(again.body.error).toBe("account-exists");

    const staff = await call("post", "/v1/account/register")
      .send({ email: "staff@e2e.test", password: "a-long-enough-password" })
      .expect(201);
    await dataSource.query(
      `UPDATE admin_users SET role = 'editor' WHERE email = 'staff@e2e.test'`,
    );
    const login = await call("post", "/v1/admin/auth/login")
      .send({ email: "staff@e2e.test", password: "a-long-enough-password" })
      .expect(200);
    await call("delete", "/v1/account", login.body.token as string)
      .send({ password: "a-long-enough-password" })
      .expect(403);
    expect(staff.body.user.role).toBe("user");
  });
});
