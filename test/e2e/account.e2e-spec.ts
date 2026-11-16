import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { AuthAttemptsService } from "../../src/admin/auth/auth-attempts.service";
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
    process.env.ACCOUNT_REGISTRATION = "open";
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
    delete process.env.ACCOUNT_REGISTRATION;
    await app.close();
  });

  function call(
    method: "get" | "post" | "put" | "patch" | "delete",
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

  it("refuses a registration while registration is closed", async () => {
    delete process.env.ACCOUNT_REGISTRATION;
    try {
      const refused = await call("post", "/v1/account/register")
        .send({ email: "early@e2e.test", password: "a-long-enough-password" })
        .expect(403);
      expect(refused.body.error).toBe("registration-closed");
      const [today] = await app.get(AuthAttemptsService).lastDays(1);
      expect(today.counts["register-user:closed"]).toBeGreaterThanOrEqual(1);
    } finally {
      process.env.ACCOUNT_REGISTRATION = "open";
    }
  });

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

  it("keeps a member's own storage, bounded, and erases it with the account", async () => {
    const registered = await call("post", "/v1/account/register")
      .send({ email: "store@e2e.test", password: "a-long-enough-password" })
      .expect(201);
    const session = registered.body.token as string;

    await call("get", "/v1/account/storage/notes", session).expect(404);
    await call("put", "/v1/account/storage/notes", session)
      .send({ value: { text: "Taron first", parks: [PARK] } })
      .expect(200);
    const read = await call("get", "/v1/account/storage/notes", session).expect(
      200,
    );
    expect(read.body.value).toEqual({ text: "Taron first", parks: [PARK] });
    const listed = await call("get", "/v1/account/storage", session).expect(
      200,
    );
    expect(listed.body.items.map((item: { key: string }) => item.key)).toEqual([
      "notes",
    ]);

    await call("put", "/v1/account/storage/Bad Key", session)
      .send({ value: 1 })
      .expect(400);
    await call("put", "/v1/account/storage/big", session)
      .send({ value: "x".repeat(70 * 1024) })
      .expect(413);
    await call("put", "/v1/account/storage/empty", session)
      .send({})
      .expect(400);

    const other = await call("post", "/v1/account/register")
      .send({
        email: "other-store@e2e.test",
        password: "a-long-enough-password",
      })
      .expect(201);
    await call(
      "get",
      "/v1/account/storage/notes",
      other.body.token as string,
    ).expect(404);

    await call("delete", "/v1/account/storage/notes", session).expect(204);
    await call("put", "/v1/account/storage/keep", session)
      .send({ value: true })
      .expect(200);
    const deleted = await call("delete", "/v1/account", session)
      .send({ password: "a-long-enough-password" })
      .expect(200);
    expect(deleted.body.erased).toMatchObject({ accountStorage: 1 });
  });

  it("manages its own account: name, export, password, sessions", async () => {
    const registered = await call("post", "/v1/account/register")
      .send({ email: "manage@e2e.test", password: "a-long-enough-password" })
      .expect(201);
    const session = registered.body.token as string;

    const renamed = await call("patch", "/v1/account/profile", session)
      .send({ displayName: "  Coaster Fan  " })
      .expect(200);
    expect(renamed.body.account.displayName).toBe("Coaster Fan");
    const me = await call("get", "/v1/account", session).expect(200);
    expect(me.body.account).toMatchObject({
      displayName: "Coaster Fan",
      totpEnabled: false,
    });
    await call("patch", "/v1/account/profile", session)
      .send({ displayName: "" })
      .expect(400);

    await call("put", "/v1/account/storage/notes", session)
      .send({ value: { text: "Taron first" } })
      .expect(200);
    const exported = await call("get", "/v1/account/export", session).expect(
      200,
    );
    expect(exported.headers["content-disposition"]).toContain("attachment");
    expect(exported.body.account.email).toBe("manage@e2e.test");
    expect(exported.body.storage).toEqual([
      expect.objectContaining({ key: "notes", value: { text: "Taron first" } }),
    ]);

    await call("post", "/v1/admin/auth/change-password", session)
      .send({
        currentPassword: "wrong-password-here",
        newPassword: "another-long-password",
      })
      .expect(401);
    const changed = await call(
      "post",
      "/v1/admin/auth/change-password",
      session,
    )
      .send({
        currentPassword: "a-long-enough-password",
        newPassword: "another-long-password",
      })
      .expect(200);
    const current = (changed.body.token as string | undefined) ?? session;

    const listed = await call("get", "/v1/admin/auth/sessions", current).expect(
      200,
    );
    expect(
      listed.body.sessions.some((s: { current: boolean }) => s.current),
    ).toBe(true);
    await call("post", "/v1/admin/auth/sessions/revoke-all", current).expect(
      200,
    );
    await call("get", "/v1/account", current).expect(401);
  });
});
