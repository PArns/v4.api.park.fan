import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { AdminAuthService } from "../../src/admin/auth/admin-auth.service";
import { AdminSessionStore } from "../../src/admin/auth/admin-session.store";
import type { AdminRole } from "../../src/admin/auth/entities/admin-user.entity";
import { s256Challenge } from "../../src/admin/oauth/pkce.util";
import {
  GLOBAL_PREFIX,
  GLOBAL_PREFIX_OPTIONS,
} from "../../src/common/swagger/swagger-config";
import {
  getApiPublicUrl,
  getFrontendUrl,
  getMcpResourceUrl,
} from "../../src/config/oauth.config";

/**
 * The admin MCP through everything: the guard, the real token tables, Redis
 * for pending requests and codes, the OAuth endpoints and the MCP transport.
 * What the unit specs cannot show is the SQL — the joins that refuse a
 * revoked token or an inactive account, the conditional UPDATE that lets one
 * of two parallel refreshes win — so these run against Postgres.
 */
describe("Admin MCP and OAuth (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let address = 0;

  const VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
  const REDIRECT = "http://127.0.0.1:53682/callback";

  beforeAll(async () => {
    delete process.env.ADMIN_LEGACY_PASS;
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
    delete process.env.ADMIN_LEGACY_PASS;
    await app.close();
  });

  /** Requests of one test from one address, so the per-address limits do not add up across tests. */
  function from() {
    address += 1;
    const ip = `198.51.100.${address}`;
    const server = app.getHttpServer();
    return {
      get: (path: string) =>
        request(server).get(path).set("cf-connecting-ip", ip),
      post: (path: string) =>
        request(server).post(path).set("cf-connecting-ip", ip),
      patch: (path: string) =>
        request(server).patch(path).set("cf-connecting-ip", ip),
      delete: (path: string) =>
        request(server).delete(path).set("cf-connecting-ip", ip),
    };
  }

  async function account(email: string, role: AdminRole) {
    const created = await app.get(AdminAuthService).create({
      email,
      role,
      password: "a-long-enough-password",
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

  type Client = ReturnType<typeof from>;

  async function register(client: Client): Promise<string> {
    const response = await client
      .post("/v1/oauth/register")
      .send({ client_name: "Claude Code", redirect_uris: [REDIRECT] })
      .expect(201);
    return response.body.client_id as string;
  }

  /** authorize → consent (approve with the session) → the code at the redirect. */
  async function authorizeAndApprove(
    client: Client,
    clientId: string,
    session: string,
  ) {
    const authorize = await client
      .get("/v1/oauth/authorize")
      .query({
        response_type: "code",
        client_id: clientId,
        redirect_uri: REDIRECT,
        code_challenge: s256Challenge(VERIFIER),
        code_challenge_method: "S256",
        state: "e2e-state",
        scope: "admin:read",
        resource: getMcpResourceUrl(),
      })
      .expect(302);
    expect(authorize.headers["cache-control"]).toBe("no-store");
    const consent = new URL(authorize.headers.location);
    expect(`${consent.origin}${consent.pathname}`).toBe(
      `${getFrontendUrl()}/admin/oauth/consent`,
    );
    const id = consent.searchParams.get("request")!;

    const shown = await client
      .get(`/v1/admin/oauth/requests/${id}`)
      .set("Authorization", `Bearer ${session}`)
      .expect(200);
    expect(shown.body).toMatchObject({
      clientId,
      clientName: "Claude Code",
      redirectHost: "127.0.0.1:53682",
      scope: "admin:read",
    });

    const approved = await client
      .post(`/v1/admin/oauth/requests/${id}/approve`)
      .set("Authorization", `Bearer ${session}`)
      .expect(200);
    const callback = new URL(approved.body.redirectTo);
    expect(`${callback.origin}${callback.pathname}`).toBe(REDIRECT);
    expect(callback.searchParams.get("state")).toBe("e2e-state");
    return callback.searchParams.get("code")!;
  }

  function exchange(client: Client, clientId: string, code: string) {
    return client.post("/v1/oauth/token").type("form").send({
      grant_type: "authorization_code",
      code,
      client_id: clientId,
      redirect_uri: REDIRECT,
      code_verifier: VERIFIER,
    });
  }

  async function grant(client: Client, session: string) {
    const clientId = await register(client);
    const code = await authorizeAndApprove(client, clientId, session);
    const tokens = await exchange(client, clientId, code).then((r) => {
      expect(r.status).toBe(200);
      return r.body as { access_token: string; refresh_token: string };
    });
    return { clientId, code, ...tokens };
  }

  function mcp(client: Client, token: string | null, body: object) {
    const call = client
      .post("/v1/mcp")
      .set("Accept", "application/json, text/event-stream")
      .set("Content-Type", "application/json");
    if (token) call.set("Authorization", `Bearer ${token}`);
    return call.send(body);
  }

  const toolsList = { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} };
  const whoami = {
    jsonrpc: "2.0",
    id: 2,
    method: "tools/call",
    params: { name: "whoami", arguments: {} },
  };

  it("serves the discovery documents at the host root", async () => {
    const client = from();
    const resource = await client
      .get("/.well-known/oauth-protected-resource/v1/mcp")
      .expect(200);
    expect(resource.body).toEqual({
      resource: getMcpResourceUrl(),
      authorization_servers: [getApiPublicUrl()],
      scopes_supported: ["admin:read"],
      bearer_methods_supported: ["header"],
      resource_name: "park.fan admin",
    });
    await client.get("/.well-known/oauth-protected-resource").expect(200);
    const server = await client
      .get("/.well-known/oauth-authorization-server")
      .expect(200);
    expect(server.body).toMatchObject({
      issuer: getApiPublicUrl(),
      authorization_endpoint: `${getApiPublicUrl()}/v1/oauth/authorize`,
      token_endpoint: `${getApiPublicUrl()}/v1/oauth/token`,
      registration_endpoint: `${getApiPublicUrl()}/v1/oauth/register`,
      revocation_endpoint: `${getApiPublicUrl()}/v1/oauth/revoke`,
      code_challenge_methods_supported: ["S256"],
      token_endpoint_auth_methods_supported: ["none"],
    });
    await client.get("/v1/.well-known/oauth-authorization-server").expect(404);
  });

  it("challenges an unauthenticated MCP request with the resource metadata", async () => {
    const response = await mcp(from(), null, toolsList).expect(401);
    expect(response.headers["www-authenticate"]).toContain(
      `resource_metadata="${getApiPublicUrl()}/.well-known/oauth-protected-resource/v1/mcp"`,
    );
  });

  it("runs the whole OAuth flow into a read-only MCP session", async () => {
    const client = from();
    const owner = await account("owner-flow@example.invalid", "owner");
    const { access_token } = await grant(client, owner.session);
    expect(access_token).toMatch(/^pfo_/);

    const list = await mcp(client, access_token, toolsList).expect(200);
    const names = list.body.result.tools.map(
      (tool: { name: string }) => tool.name,
    );
    expect(names).toEqual(
      expect.arrayContaining(["whoami", "get_park", "audit_log"]),
    );
    for (const tool of list.body.result.tools) {
      expect(tool.annotations.readOnlyHint).toBe(true);
    }

    const me = await mcp(client, access_token, whoami).expect(200);
    expect(JSON.parse(me.body.result.content[0].text)).toMatchObject({
      email: "owner-flow@example.invalid",
      // An owner's grant is still read-only.
      effectiveRole: "viewer",
      credential: { kind: "oauth", client: "Claude Code", scope: "admin:read" },
    });

    const rows = await dataSource.query(
      `SELECT t.kind, t.role_cap, s.kind AS secret, length(s.token_hash) AS hash_length
         FROM admin_api_tokens t JOIN admin_api_token_secrets s ON s.token_id = t.id
        ORDER BY s.kind`,
    );
    expect(rows).toEqual([
      { kind: "oauth", role_cap: "viewer", secret: "access", hash_length: 64 },
      { kind: "oauth", role_cap: "viewer", secret: "refresh", hash_length: 64 },
    ]);
    const audit = await dataSource.query(
      `SELECT action FROM admin_audit_log WHERE action LIKE 'auth.oauth.%'`,
    );
    expect(audit).toEqual([{ action: "auth.oauth.grant" }]);
  });

  it("refuses a session, the shared pass and a refresh token on /v1/mcp", async () => {
    process.env.ADMIN_LEGACY_PASS = "a-long-shared-secret-value";
    const client = from();
    const owner = await account("owner-refuse@example.invalid", "owner");
    const { refresh_token } = await grant(client, owner.session);

    await mcp(client, owner.session, toolsList).expect(401);
    await client
      .post("/v1/mcp?pass=a-long-shared-secret-value")
      .set("Accept", "application/json, text/event-stream")
      .send(toolsList)
      .expect(401);
    await mcp(client, refresh_token, toolsList).expect(401);
    delete process.env.ADMIN_LEGACY_PASS;
  });

  it("keeps the token and consent endpoints closed to the shared pass and to tokens", async () => {
    process.env.ADMIN_LEGACY_PASS = "a-long-shared-secret-value";
    const client = from();
    const owner = await account("owner-closed@example.invalid", "owner");
    const { access_token } = await grant(client, owner.session);

    await client
      .get("/v1/admin/auth/tokens?pass=a-long-shared-secret-value")
      .expect(403);
    await client
      .post(
        "/v1/admin/oauth/requests/whatever/approve?pass=a-long-shared-secret-value",
      )
      .expect(403);
    await client
      .get("/v1/admin/auth/tokens")
      .set("Authorization", `Bearer ${access_token}`)
      .expect(401);
    await client
      .post("/v1/admin/auth/tokens")
      .set("Authorization", `Bearer ${access_token}`)
      .send({ name: "escalation" })
      .expect(401);
    await client
      .get("/v1/admin/system-health")
      .set("Authorization", `Bearer ${access_token}`)
      .expect(401);
    delete process.env.ADMIN_LEGACY_PASS;
  });

  it("revokes the grant when an authorization code is replayed", async () => {
    const client = from();
    const owner = await account("owner-replay@example.invalid", "owner");
    const { clientId, code, access_token } = await grant(client, owner.session);

    const replay = await exchange(client, clientId, code).expect(400);
    expect(replay.body.error).toBe("invalid_grant");
    await mcp(client, access_token, toolsList).expect(401);
  });

  it("rotates refresh tokens and revokes the grant when a rotated one comes back", async () => {
    const client = from();
    const owner = await account("owner-rotate@example.invalid", "owner");
    const first = await grant(client, owner.session);

    const rotated = await client
      .post("/v1/oauth/token")
      .type("form")
      .send({
        grant_type: "refresh_token",
        refresh_token: first.refresh_token,
        client_id: first.clientId,
      })
      .expect(200);
    expect(rotated.body.refresh_token).not.toBe(first.refresh_token);
    expect(rotated.headers["cache-control"]).toBe("no-store");
    await mcp(client, rotated.body.access_token, toolsList).expect(200);

    // Past the race window, as a thief's replay would be.
    await dataSource.query(
      `UPDATE admin_api_token_secrets SET consumed_at = now() - interval '1 minute'
        WHERE consumed_at IS NOT NULL`,
    );
    const reuse = await client
      .post("/v1/oauth/token")
      .type("form")
      .send({
        grant_type: "refresh_token",
        refresh_token: first.refresh_token,
        client_id: first.clientId,
      })
      .expect(400);
    expect(reuse.body.error).toBe("invalid_grant");
    await mcp(client, rotated.body.access_token, toolsList).expect(401);
    const [grantRow] = await dataSource.query(
      `SELECT revoked_reason FROM admin_api_tokens`,
    );
    expect(grantRow.revoked_reason).toBe("refresh-reuse");
  });

  it("lets exactly one of two parallel refreshes win, and keeps the grant", async () => {
    const client = from();
    const owner = await account("owner-race@example.invalid", "owner");
    const first = await grant(client, owner.session);
    const body = {
      grant_type: "refresh_token",
      refresh_token: first.refresh_token,
      client_id: first.clientId,
    };
    const [a, b] = await Promise.all([
      client.post("/v1/oauth/token").type("form").send(body),
      client.post("/v1/oauth/token").type("form").send(body),
    ]);
    expect([a.status, b.status].sort()).toEqual([200, 400]);
    const winner = a.status === 200 ? a : b;
    await mcp(client, winner.body.access_token, toolsList).expect(200);
  });

  it("refuses a refresh token presented by another client", async () => {
    const client = from();
    const owner = await account("owner-client@example.invalid", "owner");
    const first = await grant(client, owner.session);
    const otherClient = await register(client);
    await client
      .post("/v1/oauth/token")
      .type("form")
      .send({
        grant_type: "refresh_token",
        refresh_token: first.refresh_token,
        client_id: otherClient,
      })
      .expect(400);
  });

  it("creates, uses, lists and revokes a personal access token", async () => {
    const client = from();
    const editor = await account("editor-pat@example.invalid", "editor");

    const created = await client
      .post("/v1/admin/auth/tokens")
      .set("Authorization", `Bearer ${editor.session}`)
      .send({ name: "Laptop", expiresInDays: 30 })
      .expect(201);
    expect(created.body.token).toMatch(/^pfa_[\w-]{43}$/);
    expect(created.body.item).toMatchObject({
      kind: "pat",
      name: "Laptop",
      clientName: null,
      prefix: created.body.token.slice(0, 8),
      scope: "admin:read",
      lastUsedAt: null,
    });
    const pat = created.body.token as string;

    const me = await mcp(client, pat, whoami).expect(200);
    expect(JSON.parse(me.body.result.content[0].text)).toMatchObject({
      effectiveRole: "viewer",
      credential: { kind: "pat" },
    });

    const listed = await client
      .get("/v1/admin/auth/tokens")
      .set("Authorization", `Bearer ${editor.session}`)
      .expect(200);
    expect(listed.body.items).toHaveLength(1);
    expect(listed.body.items[0].lastUsedAt).not.toBeNull();
    expect(JSON.stringify(listed.body)).not.toContain(pat);

    const stored = await dataSource.query(
      `SELECT token_hash FROM admin_api_token_secrets`,
    );
    expect(stored[0].token_hash).not.toContain(pat.slice(4));

    await client
      .delete(`/v1/admin/auth/tokens/${created.body.item.id}`)
      .set("Authorization", `Bearer ${editor.session}`)
      .expect(204);
    await mcp(client, pat, toolsList).expect(401);
    const audit = await dataSource.query(
      `SELECT action FROM admin_audit_log WHERE action LIKE 'auth.token.%' ORDER BY created_at`,
    );
    expect(audit.map((row: { action: string }) => row.action)).toEqual([
      "auth.token.create",
      "auth.token.revoke",
    ]);
  });

  it("refuses an expired token", async () => {
    const client = from();
    const viewer = await account("viewer-expired@example.invalid", "viewer");
    const created = await client
      .post("/v1/admin/auth/tokens")
      .set("Authorization", `Bearer ${viewer.session}`)
      .send({ name: "Short", expiresInDays: 30 })
      .expect(201);
    await mcp(client, created.body.token, toolsList).expect(200);
    await dataSource.query(
      `UPDATE admin_api_token_secrets SET expires_at = now() - interval '1 second'`,
    );
    await mcp(client, created.body.token, toolsList).expect(401);
  });

  it("cuts a deactivated account off at the next request", async () => {
    const client = from();
    const owner = await account("owner-deact@example.invalid", "owner");
    const viewer = await account("viewer-deact@example.invalid", "viewer");
    const created = await client
      .post("/v1/admin/auth/tokens")
      .set("Authorization", `Bearer ${viewer.session}`)
      .send({ name: "Script" })
      .expect(201);
    await mcp(client, created.body.token, toolsList).expect(200);

    // Directly in the table: the request-time check, without any revocation.
    await dataSource.query(
      `UPDATE admin_users SET is_active = false WHERE id = $1::uuid`,
      [viewer.id],
    );
    await mcp(client, created.body.token, toolsList).expect(401);
    await dataSource.query(
      `UPDATE admin_users SET is_active = true WHERE id = $1::uuid`,
      [viewer.id],
    );
    await mcp(client, created.body.token, toolsList).expect(200);

    // Through the admin: the token is revoked for good, reactivation or not.
    await client
      .patch(`/v1/admin/auth/users/${viewer.id}`)
      .set("Authorization", `Bearer ${owner.session}`)
      .send({ isActive: false })
      .expect(200);
    await dataSource.query(
      `UPDATE admin_users SET is_active = true WHERE id = $1::uuid`,
      [viewer.id],
    );
    await mcp(client, created.body.token, toolsList).expect(401);
  });

  it("ends tokens and grants with sign-out-everywhere", async () => {
    const client = from();
    const viewer = await account("viewer-signout@example.invalid", "viewer");
    const { access_token } = await grant(client, viewer.session);
    await mcp(client, access_token, toolsList).expect(200);
    await client
      .post("/v1/admin/auth/sessions/revoke-all")
      .set("Authorization", `Bearer ${viewer.session}`)
      .expect(200);
    await mcp(client, access_token, toolsList).expect(401);
  });

  it("lets an owner list and revoke another account's grant, and nobody else", async () => {
    const client = from();
    const owner = await account("owner-admin@example.invalid", "owner");
    const viewer = await account("viewer-other@example.invalid", "viewer");
    const editor = await account("editor-other@example.invalid", "editor");
    const { access_token } = await grant(client, viewer.session);

    const listed = await client
      .get(`/v1/admin/auth/tokens?userId=${viewer.id}`)
      .set("Authorization", `Bearer ${owner.session}`)
      .expect(200);
    expect(listed.body.items).toHaveLength(1);
    expect(listed.body.items[0]).toMatchObject({
      kind: "oauth",
      name: null,
      clientName: "Claude Code",
    });
    await client
      .get(`/v1/admin/auth/tokens?userId=${viewer.id}`)
      .set("Authorization", `Bearer ${editor.session}`)
      .expect(403);
    await client
      .delete(`/v1/admin/auth/tokens/${listed.body.items[0].id}`)
      .set("Authorization", `Bearer ${editor.session}`)
      .expect(404);
    await client
      .delete(`/v1/admin/auth/tokens/${listed.body.items[0].id}`)
      .set("Authorization", `Bearer ${owner.session}`)
      .expect(204);
    await mcp(client, access_token, toolsList).expect(401);
  });

  it("revokes a grant through RFC 7009 only for its own client", async () => {
    const client = from();
    const viewer = await account("viewer-revoke@example.invalid", "viewer");
    const { clientId, access_token, refresh_token } = await grant(
      client,
      viewer.session,
    );
    const stranger = await register(client);
    await client
      .post("/v1/oauth/revoke")
      .type("form")
      .send({ token: refresh_token, client_id: stranger })
      .expect(200);
    await mcp(client, access_token, toolsList).expect(200);
    await client
      .post("/v1/oauth/revoke")
      .type("form")
      .send({ token: refresh_token, client_id: clientId })
      .expect(200);
    await mcp(client, access_token, toolsList).expect(401);
  });

  it("never redirects to a URI it cannot vouch for", async () => {
    const client = from();
    await client
      .post("/v1/oauth/register")
      .send({ client_name: "Evil", redirect_uris: ["https://evil.example/cb"] })
      .expect(400)
      .expect((response) =>
        expect(response.body.error).toBe("invalid_redirect_uri"),
      );

    const unknown = await client
      .get("/v1/oauth/authorize")
      .query({
        client_id: "pfc_nobody",
        redirect_uri: "https://evil.example/cb",
        response_type: "code",
      })
      .expect(400);
    expect(unknown.headers["content-type"]).toContain("text/html");
    expect(unknown.headers["content-security-policy"]).toContain(
      "default-src 'none'",
    );
    expect(unknown.headers.location).toBeUndefined();

    const clientId = await register(client);
    const foreign = await client
      .get("/v1/oauth/authorize")
      .query({
        client_id: clientId,
        redirect_uri: "https://evil.example/cb",
        response_type: "code",
      })
      .expect(400);
    expect(foreign.headers.location).toBeUndefined();
  });

  it("answers a denial with access_denied at the client's redirect", async () => {
    const client = from();
    const viewer = await account("viewer-deny@example.invalid", "viewer");
    const clientId = await register(client);
    const authorize = await client
      .get("/v1/oauth/authorize")
      .query({
        response_type: "code",
        client_id: clientId,
        redirect_uri: REDIRECT,
        code_challenge: s256Challenge(VERIFIER),
        code_challenge_method: "S256",
        state: "deny-state",
      })
      .expect(302);
    const id = new URL(authorize.headers.location).searchParams.get("request");
    const denied = await client
      .post(`/v1/admin/oauth/requests/${id}/deny`)
      .set("Authorization", `Bearer ${viewer.session}`)
      .expect(200);
    const target = new URL(denied.body.redirectTo);
    expect(target.searchParams.get("error")).toBe("access_denied");
    expect(target.searchParams.get("state")).toBe("deny-state");
    await client
      .post(`/v1/admin/oauth/requests/${id}/approve`)
      .set("Authorization", `Bearer ${viewer.session}`)
      .expect(404);
  });
});
