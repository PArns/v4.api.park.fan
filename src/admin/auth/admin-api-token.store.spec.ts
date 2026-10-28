import { AdminApiTokenStore } from "./admin-api-token.store";
import { lesserRole } from "./entities/admin-user.entity";
import { hashToken } from "./token-hash.util";

const USER = "11111111-1111-4111-8111-111111111111";

function row(overrides: Record<string, unknown> = {}) {
  return {
    id: "22222222-2222-4222-8222-222222222222",
    kind: "pat",
    client_name: null,
    scope: "admin:read",
    role_cap: "viewer",
    last_used_at: new Date(),
    user_id: USER,
    email: "you@park.fan",
    display_name: "You",
    role: "owner",
    is_active: true,
    must_change_password: false,
    totp_enabled: true,
    ...overrides,
  };
}

function build(rows: unknown[] = []) {
  const secrets = { query: jest.fn(async (): Promise<unknown> => rows) };
  const tokens = { query: jest.fn(async (): Promise<unknown> => [[], 0]) };
  const store = new AdminApiTokenStore(tokens as never, secrets as never);
  return { store, secrets, tokens };
}

describe("lesserRole", () => {
  it("caps a role at the token's", () => {
    expect(lesserRole("owner", "viewer")).toBe("viewer");
    expect(lesserRole("viewer", "owner")).toBe("viewer");
    expect(lesserRole("editor", "author")).toBe("author");
    expect(lesserRole("editor", "editor")).toBe("editor");
  });

  it("reads an unknown value as viewer, never as more", () => {
    expect(lesserRole("owner", "admin" as never)).toBe("viewer");
    expect(lesserRole("superuser" as never, "owner")).toBe("viewer");
  });
});

describe("AdminApiTokenStore.resolvePrincipal", () => {
  const previous = process.env.ADMIN_REQUIRE_TOTP;
  afterEach(() => {
    if (previous === undefined) delete process.env.ADMIN_REQUIRE_TOTP;
    else process.env.ADMIN_REQUIRE_TOTP = previous;
  });

  it("caps an owner's token at viewer", async () => {
    const { store, secrets } = build([row()]);
    const principal = await store.resolvePrincipal(
      "pfa_secret",
      "198.51.100.7",
    );
    expect(principal).toMatchObject({
      userId: USER,
      role: "viewer",
      legacy: false,
      sessionToken: null,
      ip: "198.51.100.7",
      apiToken: { kind: "pat", scope: "admin:read" },
    });
    // Looked up by its hash; the token itself never reaches the database.
    expect(secrets.query).toHaveBeenCalledWith(expect.any(String), [
      hashToken("pfa_secret"),
    ]);
  });

  it("never lifts a viewer account above viewer", async () => {
    const { store } = build([row({ role: "viewer", role_cap: "owner" })]);
    expect((await store.resolvePrincipal("pfo_x", null))?.role).toBe("viewer");
  });

  it("refuses a deactivated account at request time", async () => {
    const { store } = build([row({ is_active: false })]);
    expect(await store.resolvePrincipal("pfa_secret", null)).toBeNull();
  });

  it("refuses an unknown, expired or revoked token (no row)", async () => {
    const { store } = build([]);
    expect(await store.resolvePrincipal("pfa_secret", null)).toBeNull();
  });

  it("never looks up a value without a token prefix", async () => {
    const { store, secrets } = build([row()]);
    expect(await store.resolvePrincipal("a-session-token", null)).toBeNull();
    expect(await store.resolvePrincipal("pfr_refresh-token", null)).toBeNull();
    expect(secrets.query).not.toHaveBeenCalled();
  });

  it("carries the enrolment debt when two-factor became mandatory", async () => {
    process.env.ADMIN_REQUIRE_TOTP = "true";
    const { store } = build([row({ totp_enabled: false })]);
    expect(
      (await store.resolvePrincipal("pfa_secret", null))?.mustEnrolTotp,
    ).toBe(true);
  });

  it("records use at most once a minute", async () => {
    const recent = build([row({ last_used_at: new Date() })]);
    await recent.store.resolvePrincipal("pfa_secret", null);
    expect(recent.tokens.query).not.toHaveBeenCalled();

    const stale = build([
      row({ last_used_at: new Date(Date.now() - 120_000) }),
    ]);
    await stale.store.resolvePrincipal("pfa_secret", null);
    expect(stale.tokens.query).toHaveBeenCalledWith(
      expect.stringContaining("SET last_used_at = now()"),
      [row().id],
    );
  });
});

describe("AdminApiTokenStore.rotateRefreshToken", () => {
  it("refuses an access token or a PAT presented as a refresh token", async () => {
    const { store, secrets } = build();
    expect(await store.rotateRefreshToken("pfo_access", "pfc_x")).toEqual({
      outcome: "invalid",
    });
    expect(await store.rotateRefreshToken("pfa_pat", "pfc_x")).toEqual({
      outcome: "invalid",
    });
    expect(secrets.query).not.toHaveBeenCalled();
  });

  it("revokes the grant when a token used outside the grace window comes back", async () => {
    const { store, secrets, tokens } = build();
    secrets.query
      .mockResolvedValueOnce([[], 0])
      .mockResolvedValueOnce([{ token_id: row().id, reused: true }])
      .mockResolvedValue([[], 0]);
    tokens.query.mockResolvedValueOnce([
      [
        {
          id: row().id,
          user_id: USER,
          kind: "oauth",
          name: null,
          client_name: "Claude",
          prefix: "pfo_abcd",
        },
      ],
      1,
    ]);
    expect(await store.rotateRefreshToken("pfr_old", "pfc_x")).toEqual({
      outcome: "reused",
      tokenId: row().id,
    });
    expect(tokens.query).toHaveBeenCalledWith(
      expect.stringContaining("SET revoked_at = now()"),
      [row().id, null, "refresh-reuse"],
    );
  });

  it("treats a token used a moment ago as a race, not a theft", async () => {
    const { store, secrets, tokens } = build();
    secrets.query
      .mockResolvedValueOnce([[], 0])
      .mockResolvedValueOnce([{ token_id: row().id, reused: false }]);
    expect(await store.rotateRefreshToken("pfr_old", "pfc_x")).toEqual({
      outcome: "invalid",
    });
    expect(tokens.query).not.toHaveBeenCalled();
  });
});

describe("AdminApiTokenStore.revoke", () => {
  it("binds nothing that is not a uuid", async () => {
    const { store, tokens } = build();
    expect(await store.revoke("not-a-uuid", null, "user")).toBeNull();
    expect(await store.revoke(row().id, "nope", "user")).toBeNull();
    expect(await store.revokeAllForUser("nope", "user")).toBe(0);
    expect(tokens.query).not.toHaveBeenCalled();
  });
});
