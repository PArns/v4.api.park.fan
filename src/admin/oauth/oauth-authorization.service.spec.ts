import { NotFoundException } from "@nestjs/common";
import { RedisMock } from "../../../test/mocks/redis.mock";
import { OAuthAuthorizationService } from "./oauth-authorization.service";
import { OAuthError } from "./oauth-error";
import { s256Challenge } from "./pkce.util";
import type { AdminPrincipal } from "../auth/admin-principal";

const VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const CHALLENGE = s256Challenge(VERIFIER);
const REDIRECT = "http://localhost:33418/callback";
const CLIENT = {
  clientId: "pfc_client",
  clientName: "Claude Code",
  redirectUris: [REDIRECT, "https://claude.ai/api/mcp/auth_callback"],
};
const ADMIN: AdminPrincipal = {
  userId: "11111111-1111-4111-8111-111111111111",
  email: "you@park.fan",
  displayName: "You",
  role: "owner",
  sessionToken: "session",
  legacy: false,
  ip: null,
  mustChangePassword: false,
  mustEnrolTotp: false,
};

function build() {
  const redis = new RedisMock();
  const clients = {
    find: jest.fn(async (id: unknown) =>
      id === CLIENT.clientId ? CLIENT : null,
    ),
    markUsed: jest.fn(async () => undefined),
  };
  const tokens = {
    issueOAuthGrant: jest.fn(async () => ({
      tokenId: "grant-1",
      accessToken: "pfo_access",
      refreshToken: "pfr_refresh",
      expiresIn: 3600,
      scope: "admin:read" as const,
    })),
    revoke: jest.fn(async () => null),
    rotateRefreshToken: jest.fn(),
    revokeByValue: jest.fn(async () => true),
  };
  const accounts = {
    findById: jest.fn(async () => ({
      id: ADMIN.userId,
      email: ADMIN.email,
      isActive: true,
    })),
  };
  const audit = { record: jest.fn(async () => null) };
  const service = new OAuthAuthorizationService(
    redis as never,
    clients as never,
    tokens as never,
    accounts as never,
    audit as never,
  );
  return { service, redis, clients, tokens, accounts, audit };
}

const validRequest = {
  response_type: "code",
  client_id: CLIENT.clientId,
  redirect_uri: REDIRECT,
  code_challenge: CHALLENGE,
  code_challenge_method: "S256",
  state: "st-1",
  scope: "admin:read",
  resource: "https://api.park.fan/v1/mcp",
};

async function expectOAuthError(promise: Promise<unknown>, code: string) {
  await expect(promise).rejects.toBeInstanceOf(OAuthError);
  await promise.catch((error: OAuthError) => expect(error.code).toBe(code));
}

describe("OAuthAuthorizationService", () => {
  const env = process.env;
  beforeEach(() => {
    process.env = {
      ...env,
      API_PUBLIC_URL: "https://api.park.fan",
      FRONTEND_URL: "https://park.fan",
    };
    delete process.env.OAUTH_REDIRECT_HOSTS;
  });
  afterAll(() => {
    process.env = env;
  });

  async function approvedCode(
    service: OAuthAuthorizationService,
    request: Record<string, unknown> = validRequest,
  ): Promise<string> {
    const outcome = await service.authorize(request);
    if (outcome.kind !== "redirect") throw new Error("expected a redirect");
    const id = new URL(outcome.location).searchParams.get("request")!;
    const { redirectTo } = await service.approve(id, ADMIN);
    return new URL(redirectTo).searchParams.get("code")!;
  }

  describe("authorize", () => {
    it("parks a valid request and sends the browser to the consent page", async () => {
      const { service } = build();
      const outcome = await service.authorize(validRequest);
      expect(outcome.kind).toBe("redirect");
      const location = new URL((outcome as { location: string }).location);
      expect(`${location.origin}${location.pathname}`).toBe(
        "https://park.fan/admin/oauth/consent",
      );
      const summary = await service.describe(
        location.searchParams.get("request")!,
      );
      expect(summary).toMatchObject({
        clientId: CLIENT.clientId,
        clientName: "Claude Code",
        redirectHost: "localhost:33418",
        scope: "admin:read",
      });
    });

    it("shows a page, never a redirect, for an unknown client", async () => {
      const { service } = build();
      expect(
        await service.authorize({ ...validRequest, client_id: "pfc_other" }),
      ).toEqual({ kind: "error-page", message: "unknown-client" });
    });

    it("shows a page for a redirect URI the client never registered", async () => {
      const { service } = build();
      expect(
        await service.authorize({
          ...validRequest,
          redirect_uri: "https://evil.example/callback",
        }),
      ).toEqual({ kind: "error-page", message: "invalid-redirect" });
    });

    it("shows a page when the allowed hosts narrowed after registration", async () => {
      process.env.OAUTH_REDIRECT_HOSTS = "claude.com";
      const { service } = build();
      expect(
        await service.authorize({
          ...validRequest,
          redirect_uri: "https://claude.ai/api/mcp/auth_callback",
        }),
      ).toEqual({ kind: "error-page", message: "invalid-redirect" });
    });

    it("shows a page for a repeated redirect_uri", async () => {
      const { service } = build();
      expect(
        await service.authorize({
          ...validRequest,
          redirect_uri: [REDIRECT, REDIRECT],
        }),
      ).toEqual({ kind: "error-page", message: "invalid-request" });
    });

    it.each([
      ["no PKCE", { code_challenge: undefined }, "invalid_request"],
      ["plain PKCE", { code_challenge_method: "plain" }, "invalid_request"],
      ["a malformed challenge", { code_challenge: "short" }, "invalid_request"],
      [
        "another response type",
        { response_type: "token" },
        "unsupported_response_type",
      ],
      [
        "another resource",
        { resource: "https://api.park.fan/v1/parks" },
        "invalid_target",
      ],
    ])(
      "sends %s back to the client as an error",
      async (_label, patch, error) => {
        const { service } = build();
        const outcome = await service.authorize({ ...validRequest, ...patch });
        expect(outcome.kind).toBe("redirect");
        const location = new URL((outcome as { location: string }).location);
        expect(`${location.origin}${location.pathname}`).toBe(
          "http://localhost:33418/callback",
        );
        expect(location.searchParams.get("error")).toBe(error);
        expect(location.searchParams.get("state")).toBe("st-1");
        expect(location.searchParams.get("iss")).toBe("https://api.park.fan");
      },
    );

    it("accepts the resource with a trailing slash", async () => {
      const { service } = build();
      const outcome = await service.authorize({
        ...validRequest,
        resource: "https://api.park.fan/v1/mcp/",
      });
      expect((outcome as { location: string }).location).toContain(
        "/admin/oauth/consent",
      );
    });

    it("refuses a long run of slashes in linear time", async () => {
      const { service } = build();
      const started = Date.now();
      const outcome = await service.authorize({
        ...validRequest,
        resource: `https://api.park.fan/v1${"/".repeat(100_000)}x`,
      });
      expect(Date.now() - started).toBeLessThan(500);
      expect(
        new URL((outcome as { location: string }).location).searchParams.get(
          "error",
        ),
      ).toBe("invalid_target");
    });
  });

  describe("consent", () => {
    it("answers a request once", async () => {
      const { service } = build();
      const outcome = await service.authorize(validRequest);
      const id = new URL(
        (outcome as { location: string }).location,
      ).searchParams.get("request")!;
      await service.approve(id, ADMIN);
      await expect(service.approve(id, ADMIN)).rejects.toBeInstanceOf(
        NotFoundException,
      );
      await expect(service.deny(id)).rejects.toBeInstanceOf(NotFoundException);
    });

    it("hands the code to the registered redirect with state and issuer, and audits it", async () => {
      const { service, audit, clients } = build();
      const outcome = await service.authorize(validRequest);
      const id = new URL(
        (outcome as { location: string }).location,
      ).searchParams.get("request")!;
      const { redirectTo } = await service.approve(id, ADMIN);
      const url = new URL(redirectTo);
      expect(`${url.origin}${url.pathname}`).toBe(REDIRECT);
      expect(url.searchParams.get("code")).toMatch(/^[\w-]{43}$/);
      expect(url.searchParams.get("state")).toBe("st-1");
      expect(url.searchParams.get("iss")).toBe("https://api.park.fan");
      expect(audit.record).toHaveBeenCalledWith(
        expect.objectContaining({
          action: "auth.oauth.grant",
          entityId: CLIENT.clientId,
        }),
      );
      expect(clients.markUsed).toHaveBeenCalledWith(CLIENT.clientId);
    });

    it("sends a denial back as access_denied", async () => {
      const { service } = build();
      const outcome = await service.authorize(validRequest);
      const id = new URL(
        (outcome as { location: string }).location,
      ).searchParams.get("request")!;
      const url = new URL((await service.deny(id)).redirectTo);
      expect(url.searchParams.get("error")).toBe("access_denied");
      expect(url.searchParams.get("state")).toBe("st-1");
      expect(url.searchParams.get("code")).toBeNull();
    });

    it("refuses a principal without an account", async () => {
      const { service } = build();
      const outcome = await service.authorize(validRequest);
      const id = new URL(
        (outcome as { location: string }).location,
      ).searchParams.get("request")!;
      await expect(
        service.approve(id, { ...ADMIN, userId: null, legacy: true }),
      ).rejects.toBeInstanceOf(NotFoundException);
    });
  });

  describe("code exchange", () => {
    const exchange = (code: string, patch: Record<string, unknown> = {}) => ({
      grant_type: "authorization_code",
      code,
      client_id: CLIENT.clientId,
      redirect_uri: REDIRECT,
      code_verifier: VERIFIER,
      ...patch,
    });

    it("issues a grant for the right verifier", async () => {
      const { service, tokens } = build();
      const code = await approvedCode(service);
      expect(await service.exchangeCode(exchange(code))).toEqual({
        access_token: "pfo_access",
        token_type: "Bearer",
        expires_in: 3600,
        refresh_token: "pfr_refresh",
        scope: "admin:read",
      });
      expect(tokens.issueOAuthGrant).toHaveBeenCalledWith({
        userId: ADMIN.userId,
        clientId: CLIENT.clientId,
        clientName: "Claude Code",
        scope: "admin:read",
      });
    });

    it("burns the code on a wrong verifier", async () => {
      const { service } = build();
      const code = await approvedCode(service);
      await expectOAuthError(
        service.exchangeCode(exchange(code, { code_verifier: "x".repeat(43) })),
        "invalid_grant",
      );
      await expectOAuthError(
        service.exchangeCode(exchange(code)),
        "invalid_grant",
      );
    });

    it("refuses an exchange without a verifier", async () => {
      const { service } = build();
      const code = await approvedCode(service);
      await expectOAuthError(
        service.exchangeCode(exchange(code, { code_verifier: undefined })),
        "invalid_grant",
      );
    });

    it("refuses another redirect URI than the request named", async () => {
      const { service } = build();
      const code = await approvedCode(service);
      await expectOAuthError(
        service.exchangeCode(
          exchange(code, {
            redirect_uri: "https://claude.ai/api/mcp/auth_callback",
          }),
        ),
        "invalid_grant",
      );
    });

    it("refuses another client", async () => {
      const { service, clients } = build();
      const code = await approvedCode(service);
      clients.find.mockResolvedValueOnce({ ...CLIENT, clientId: "pfc_other" });
      await expectOAuthError(
        service.exchangeCode(exchange(code, { client_id: "pfc_other" })),
        "invalid_grant",
      );
    });

    it("answers invalid_client for an unknown client", async () => {
      const { service } = build();
      const code = await approvedCode(service);
      const attempt = service.exchangeCode(
        exchange(code, { client_id: "pfc_unknown" }),
      );
      await expectOAuthError(attempt, "invalid_client");
      await attempt.catch((error: OAuthError) =>
        expect(error.status).toBe(401),
      );
    });

    it("revokes the grant when the code comes back (RFC 6749 §4.1.2)", async () => {
      const { service, tokens } = build();
      const code = await approvedCode(service);
      await service.exchangeCode(exchange(code));
      await expectOAuthError(
        service.exchangeCode(exchange(code)),
        "invalid_grant",
      );
      expect(tokens.revoke).toHaveBeenCalledWith(
        "grant-1",
        null,
        "code-replay",
      );
    });

    it("expires a code after a minute", async () => {
      const { service, redis } = build();
      const code = await approvedCode(service);
      const now = Date.now();
      redis.now = () => now + 61_000;
      await expectOAuthError(
        service.exchangeCode(exchange(code)),
        "invalid_grant",
      );
    });

    it("refuses a code for an account deactivated since the approval", async () => {
      const { service, accounts, tokens } = build();
      const code = await approvedCode(service);
      accounts.findById.mockResolvedValueOnce({
        id: ADMIN.userId,
        email: ADMIN.email,
        isActive: false,
      });
      await expectOAuthError(
        service.exchangeCode(exchange(code)),
        "invalid_grant",
      );
      expect(tokens.issueOAuthGrant).not.toHaveBeenCalled();
    });

    it("refuses a repeated parameter", async () => {
      const { service } = build();
      const code = await approvedCode(service);
      await expectOAuthError(
        service.exchangeCode(exchange(code, { code: [code, code] })),
        "invalid_request",
      );
    });
  });

  describe("refresh", () => {
    it("maps a reused refresh token to invalid_grant", async () => {
      const { service, tokens } = build();
      tokens.rotateRefreshToken.mockResolvedValueOnce({
        outcome: "reused",
        tokenId: "g",
      });
      await expectOAuthError(
        service.refresh({ refresh_token: "pfr_x", client_id: CLIENT.clientId }),
        "invalid_grant",
      );
    });

    it("returns the rotated pair", async () => {
      const { service, tokens } = build();
      tokens.rotateRefreshToken.mockResolvedValueOnce({
        outcome: "ok",
        userId: ADMIN.userId,
        tokens: {
          tokenId: "g",
          accessToken: "pfo_new",
          refreshToken: "pfr_new",
          expiresIn: 3600,
          scope: "admin:read",
        },
      });
      expect(
        await service.refresh({
          refresh_token: "pfr_x",
          client_id: CLIENT.clientId,
        }),
      ).toMatchObject({ access_token: "pfo_new", refresh_token: "pfr_new" });
      expect(tokens.rotateRefreshToken).toHaveBeenCalledWith(
        "pfr_x",
        CLIENT.clientId,
      );
    });

    it("needs the client id", async () => {
      const { service } = build();
      await expectOAuthError(
        service.refresh({ refresh_token: "pfr_x" }),
        "invalid_request",
      );
    });
  });
});
