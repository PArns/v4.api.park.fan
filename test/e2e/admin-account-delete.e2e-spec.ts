import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { AdminAuthService } from "../../src/admin/auth/admin-auth.service";
import { AdminSessionStore } from "../../src/admin/auth/admin-session.store";
import type { AdminRole } from "../../src/admin/auth/entities/admin-user.entity";
import { ApiKeyService } from "../../src/api-keys/api-key.service";
import {
  GLOBAL_PREFIX,
  GLOBAL_PREFIX_OPTIONS,
} from "../../src/common/swagger/swagger-config";

/**
 * Deleting an account for good, against Postgres and Redis: a developer goes
 * with its sessions, keys and requests, and its key stops opening anything at
 * once; nobody deletes their own account, and only
 * an owner deletes at all.
 */
describe("Admin account deletion (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let address = 0;
  const PASSWORD = "a-long-enough-password";

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
    method: "get" | "post" | "patch" | "delete",
    path: string,
    session: string,
  ) {
    address += 1;
    return request(app.getHttpServer())
      [method](path)
      .set("cf-connecting-ip", `198.51.100.${address}`)
      .set("Authorization", `Bearer ${session}`);
  }

  async function account(email: string, role: AdminRole) {
    const created = await app.get(AdminAuthService).create({
      email,
      role,
      password: PASSWORD,
    });
    await dataSource.query(
      `UPDATE admin_users SET must_change_password = false WHERE id = $1::uuid`,
      [created.id],
    );
    const { token } = await app.get(AdminSessionStore).create({
      userId: created.id,
      email: created.email,
      displayName: created.displayName,
      role,
      ip: null,
      userAgent: "e2e",
      mustChangePassword: false,
      mustEnrolTotp: false,
    });
    return { id: created.id, session: token };
  }

  const REQUEST = {
    projectName: "Fan site",
    kind: "website",
    platforms: ["web"],
    description:
      "A fan site that shows the wait times of three parks below each article.",
    source: "open-source",
    repository: "https://github.com/example/fan",
    revenue: ["none"],
    permissions: ["live.schedule"],
    volume: "under-1k",
    serverSide: true,
    attributionUrl: "https://fan.example/",
    locale: "en",
    terms: true,
  };

  it("deletes a developer with its sessions, keys and requests", async () => {
    const owner = await account("owner-delete@e2e.test", "owner");
    const dev = await account("dev-delete@e2e.test", "developer");

    const filed = await call("post", "/v1/developer/requests", dev.session)
      .send(REQUEST)
      .expect(201);
    await call("post", "/v1/developer/requests", dev.session)
      .send({ ...REQUEST, projectName: "Second" })
      .expect(201);
    await call(
      "post",
      `/v1/admin/api-key-requests/${filed.body.item.id}/approve`,
      owner.session,
    )
      .send({ permissions: ["live.schedule"] })
      .expect(200);
    const overview = await call(
      "get",
      "/v1/developer/overview",
      dev.session,
    ).expect(200);
    const keyId = overview.body.keys[0].id as string;
    const picked = await call(
      "post",
      `/v1/developer/keys/${keyId}/rotate`,
      dev.session,
    )
      .send({ graceHours: 0 })
      .expect(200);
    const secret = picked.body.key as string;
    expect((await app.get(ApiKeyService).lookup(secret)).status).toBe("active");

    const deleted = await call(
      "delete",
      `/v1/admin/auth/users/${dev.id}`,
      owner.session,
    ).expect(200);
    expect(deleted.body.erased).toEqual({ apiKeys: 1, keyRequests: 2 });

    const counts = await dataSource.query(
      `SELECT
         (SELECT count(*)::int FROM admin_users WHERE id = $1::uuid) AS users,
         (SELECT count(*)::int FROM api_keys WHERE owner_user_id = $1::uuid) AS keys,
         (SELECT count(*)::int FROM api_key_requests WHERE user_id = $1::uuid) AS requests`,
      [dev.id],
    );
    expect(counts[0]).toEqual({ users: 0, keys: 0, requests: 0 });
    expect((await app.get(ApiKeyService).lookup(secret)).status).not.toBe(
      "active",
    );
    await call("get", "/v1/developer/overview", dev.session).expect(401);

    const audit = await dataSource.query(
      `SELECT action FROM admin_audit_log WHERE entity_id = $1 ORDER BY created_at DESC LIMIT 1`,
      [dev.id],
    );
    expect(audit[0]?.action).toBe("user.delete");
  });

  it("refuses one's own account and anyone but an owner, and lets an owner delete another", async () => {
    const owner = await account("owner-a@e2e.test", "owner");
    const other = await account("owner-b@e2e.test", "owner");
    const editor = await account("editor-delete@e2e.test", "editor");

    await call(
      "delete",
      `/v1/admin/auth/users/${owner.id}`,
      owner.session,
    ).expect(400);
    await call(
      "delete",
      `/v1/admin/auth/users/${other.id}`,
      editor.session,
    ).expect(403);
    await call(
      "delete",
      `/v1/admin/auth/users/${other.id}`,
      owner.session,
    ).expect(200);
    await call(
      "delete",
      `/v1/admin/auth/users/${other.id}`,
      owner.session,
    ).expect(404);
  });

  it("demotes an owner while another stays, and refuses to demote the last one", async () => {
    const keep = await account("owner-keep@e2e.test", "owner");
    const demoted = await account("owner-demote@e2e.test", "owner");
    await call("patch", `/v1/admin/auth/users/${demoted.id}`, keep.session)
      .send({ role: "editor" })
      .expect(200);
    await dataSource.query(
      `UPDATE admin_users SET is_active = false WHERE role = 'owner' AND id != $1::uuid`,
      [keep.id],
    );
    const helper = await account("owner-helper@e2e.test", "owner");
    await dataSource.query(
      `UPDATE admin_users SET role = 'viewer' WHERE id = $1::uuid`,
      [helper.id],
    );
    await call("patch", `/v1/admin/auth/users/${keep.id}`, keep.session)
      .send({ role: "editor" })
      .expect(400);
  });
});
