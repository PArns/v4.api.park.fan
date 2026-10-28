import { Inject, Injectable, Logger, NotFoundException } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { withoutTrailingSlashes } from "../../common/utils/url.util";
import {
  checkRateLimit,
  hashRateLimitKey,
} from "../../common/redis/rate-limit.util";
import {
  getApiPublicUrl,
  getFrontendUrl,
  getMcpResourceUrl,
  getOAuthRedirectPolicy,
} from "../../config/oauth.config";
import {
  AdminApiTokenStore,
  GrantRefusedError,
} from "../auth/admin-api-token.store";
import type { IssuedOAuthTokens } from "../auth/admin-api-token.store";
import { AdminAuthService } from "../auth/admin-auth.service";
import { AdminAuditService } from "../auth/admin-audit.service";
import type { AdminPrincipal } from "../auth/admin-principal";
import {
  DEFAULT_ADMIN_TOKEN_SCOPE,
  type AdminTokenScope,
} from "../auth/admin-api-token.constants";
import { generateToken, hashToken } from "../auth/token-hash.util";
import { OAuthClientService } from "./oauth-client.service";
import { OAuthError } from "./oauth-error";
import type { AuthorizeErrorMessage } from "./oauth-error-page";
import { isValidCodeChallenge, verifyPkceS256 } from "./pkce.util";
import { checkRedirectUri, isRegisteredRedirect } from "./redirect-uri.util";

/** How long the consent page has before the request is gone. */
export const PENDING_REQUEST_TTL_SECONDS = 10 * 60;

/** How long an authorization code may wait for its exchange. */
export const AUTHORIZATION_CODE_TTL_SECONDS = 60;

/** How long a used code is remembered, so a replay revokes what it bought. */
const USED_CODE_MEMORY_SECONDS = 10 * 60;

const MAX_STATE_LENGTH = 1024;

/**
 * Pending requests are written to Redis before anybody has signed in, so
 * strangers can create them: per address bucket (IPv4, IPv6 /64) and per
 * client, at most this many in {@link PENDING_REQUEST_TTL_SECONDS}.
 */
export const MAX_PENDING_PER_ADDRESS = 30;
export const MAX_PENDING_PER_CLIENT = 30;

const REQUEST_PREFIX = "admin:oauth:request:";
const CODE_PREFIX = "admin:oauth:code:";
const USED_CODE_PREFIX = "admin:oauth:code-used:";

interface PendingRequest {
  id: string;
  clientId: string;
  clientName: string;
  redirectUri: string;
  /** Whether the request named its redirect URI; the exchange must then repeat it. */
  redirectUriGiven: boolean;
  codeChallenge: string;
  state: string | null;
  scope: AdminTokenScope;
  /** Epoch ms the request was parked; absent on one parked before it was stored. */
  createdAt?: number;
  expiresAt: number;
}

interface StoredCode {
  /** Epoch ms of the approval; a sign-out-everywhere after it voids the code. */
  approvedAt: number;
  clientId: string;
  redirectUri: string;
  redirectUriGiven: boolean;
  codeChallenge: string;
  userId: string;
  scope: AdminTokenScope;
}

/** What `/v1/oauth/authorize` does with the browser. */
export type AuthorizeOutcome =
  | { kind: "redirect"; location: string }
  | { kind: "error-page"; message: AuthorizeErrorMessage };

/** `GET /v1/admin/oauth/requests/:id`, what the consent page shows. */
export interface PendingRequestSummary {
  id: string;
  clientId: string;
  clientName: string;
  redirectHost: string;
  scope: string;
  /** When the client started the request: the consent page shows its age and warns on an old one. */
  createdAt: string;
  expiresAt: string;
}

/** The RFC 6749 §5.1 token response. */
export interface TokenResponse {
  access_token: string;
  token_type: "Bearer";
  expires_in: number;
  refresh_token: string;
  scope: string;
}

type Params = Record<string, unknown>;

/**
 * One parameter of a request. Repeating a parameter is an error in OAuth
 * (RFC 6749 §3.1); `qs` hands a repeat over as an array, and an object for
 * `a[b]=c`.
 */
function single(params: Params, name: string): string | undefined | null {
  const value = params[name];
  if (value === undefined) return undefined;
  return typeof value === "string" ? value : null;
}

function requireField(params: Params, name: string): string {
  const value = single(params, name);
  if (value === null) {
    throw new OAuthError("invalid_request", `${name} was given more than once`);
  }
  if (!value) throw new OAuthError("invalid_request", `${name} is required`);
  return value;
}

function optionalField(params: Params, name: string): string | undefined {
  const value = single(params, name);
  if (value === null) {
    throw new OAuthError("invalid_request", `${name} was given more than once`);
  }
  return value || undefined;
}

/** RFC 8707: the only resource tokens are issued for is the MCP endpoint. */
function isMcpResource(value: string): boolean {
  return withoutTrailingSlashes(value) === getMcpResourceUrl();
}

/**
 * The authorization server: the authorize redirect, the consent the admin
 * gives on park.fan, and the code and refresh exchanges.
 *
 * The admin never logs in here. `/v1/oauth/authorize` parks the request in
 * Redis and sends the browser to the frontend's consent page, which runs
 * behind the admin login and calls `approve` with the session. That keeps one
 * login (with its Turnstile, lockout and second factor) instead of two.
 */
@Injectable()
export class OAuthAuthorizationService {
  private readonly logger = new Logger(OAuthAuthorizationService.name);

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly clients: OAuthClientService,
    private readonly tokens: AdminApiTokenStore,
    private readonly accounts: AdminAuthService,
    private readonly audit: AdminAuditService,
  ) {}

  /**
   * Validate an authorization request and park it for the consent page.
   *
   * Until the client and its redirect URI are known good, every failure is a
   * page here: an error sent to an unchecked URI is an open redirect. After
   * that, failures go back to the client as RFC 6749 §4.1.2.1 errors.
   */
  async authorize(
    params: Params,
    addressBucket: string | null = null,
  ): Promise<AuthorizeOutcome> {
    if (
      addressBucket &&
      !(await this.withinLimit(
        `admin:oauth:authorize:${hashRateLimitKey(addressBucket)}`,
        MAX_PENDING_PER_ADDRESS,
      ))
    ) {
      return { kind: "error-page", message: "rate-limited" };
    }

    const clientId = single(params, "client_id");
    const client = clientId ? await this.clients.find(clientId) : null;
    if (!client) return { kind: "error-page", message: "unknown-client" };

    const requested = single(params, "redirect_uri");
    if (requested === null) {
      return { kind: "error-page", message: "invalid-request" };
    }
    let redirectUri: string;
    if (requested) {
      if (!isRegisteredRedirect(requested, client.redirectUris)) {
        return { kind: "error-page", message: "invalid-redirect" };
      }
      redirectUri = requested;
    } else if (client.redirectUris.length === 1) {
      redirectUri = client.redirectUris[0];
    } else {
      return { kind: "error-page", message: "invalid-request" };
    }
    // The allowed hosts may have narrowed since the client registered.
    if (!checkRedirectUri(redirectUri, getOAuthRedirectPolicy()).ok) {
      return { kind: "error-page", message: "invalid-redirect" };
    }

    const rawState = single(params, "state");
    const state = typeof rawState === "string" && rawState ? rawState : null;
    const fail = (code: string, description: string): AuthorizeOutcome => ({
      kind: "redirect",
      location: this.errorRedirect(redirectUri, code, description, state),
    });

    if (rawState === null || (state && state.length > MAX_STATE_LENGTH)) {
      return fail("invalid_request", "state is malformed");
    }
    const responseType = single(params, "response_type");
    if (responseType !== "code") {
      return fail(
        "unsupported_response_type",
        "Only response_type=code is supported",
      );
    }
    const challenge = single(params, "code_challenge");
    if (
      single(params, "code_challenge_method") !== "S256" ||
      !isValidCodeChallenge(challenge)
    ) {
      return fail(
        "invalid_request",
        "PKCE with code_challenge_method=S256 is required",
      );
    }
    const resource = single(params, "resource");
    if (resource === null || (resource && !isMcpResource(resource))) {
      return fail(
        "invalid_target",
        `Tokens are issued for ${getMcpResourceUrl()} only`,
      );
    }
    if (single(params, "scope") === null) {
      return fail("invalid_scope", "scope was given more than once");
    }

    if (
      !(await this.withinLimit(
        `admin:oauth:pending:${client.clientId}`,
        MAX_PENDING_PER_CLIENT,
      ))
    ) {
      return fail(
        "temporarily_unavailable",
        "Too many open authorization requests for this client; try again in a few minutes",
      );
    }

    const id = generateToken();
    const now = Date.now();
    const pending: PendingRequest = {
      id,
      clientId: client.clientId,
      clientName: client.clientName,
      redirectUri,
      redirectUriGiven: Boolean(requested),
      codeChallenge: challenge,
      state,
      // v1 has one scope. Whatever else was asked for is not granted, which
      // RFC 6749 §3.3 allows; the token response names what was.
      scope: DEFAULT_ADMIN_TOKEN_SCOPE,
      createdAt: now,
      expiresAt: now + PENDING_REQUEST_TTL_SECONDS * 1000,
    };
    await this.redis.set(
      REQUEST_PREFIX + hashToken(id),
      JSON.stringify(pending),
      "EX",
      PENDING_REQUEST_TTL_SECONDS,
    );

    const consent = new URL(`${getFrontendUrl()}/admin/oauth/consent`);
    consent.searchParams.set("request", id);
    return { kind: "redirect", location: consent.toString() };
  }

  async describe(id: string): Promise<PendingRequestSummary> {
    const pending = await this.readPending(id, false);
    return {
      id: pending.id,
      clientId: pending.clientId,
      clientName: pending.clientName,
      redirectHost: new URL(pending.redirectUri).host,
      scope: pending.scope,
      // A request parked before `createdAt` was stored has its start in its
      // expiry: the lifetime is a constant.
      createdAt: new Date(
        pending.createdAt ??
          pending.expiresAt - PENDING_REQUEST_TTL_SECONDS * 1000,
      ).toISOString(),
      expiresAt: new Date(pending.expiresAt).toISOString(),
    };
  }

  /** The admin said yes: a single-use code, bound to the account, for the client. */
  async approve(
    id: string,
    admin: AdminPrincipal,
  ): Promise<{ redirectTo: string }> {
    if (!admin.userId) throw new NotFoundException("No such pending request");
    const pending = await this.readPending(id, true);
    const client = await this.clients.find(pending.clientId);
    if (!client)
      throw new NotFoundException("The client is no longer registered");

    const code = generateToken();
    const stored: StoredCode = {
      approvedAt: Date.now(),
      clientId: pending.clientId,
      redirectUri: pending.redirectUri,
      redirectUriGiven: pending.redirectUriGiven,
      codeChallenge: pending.codeChallenge,
      userId: admin.userId,
      scope: pending.scope,
    };
    await this.redis.set(
      CODE_PREFIX + hashToken(code),
      JSON.stringify(stored),
      "EX",
      AUTHORIZATION_CODE_TTL_SECONDS,
    );
    await this.clients.markUsed(client.clientId);

    await this.audit.record({
      actor: admin,
      action: "auth.oauth.grant",
      entityType: "oauth_client",
      entityId: client.clientId,
      entityLabel: client.clientName,
      after: {
        redirectHost: new URL(pending.redirectUri).host,
        scope: pending.scope,
      },
    });

    const target = new URL(pending.redirectUri);
    target.searchParams.set("code", code);
    if (pending.state) target.searchParams.set("state", pending.state);
    target.searchParams.set("iss", getApiPublicUrl());
    return { redirectTo: target.toString() };
  }

  async deny(id: string): Promise<{ redirectTo: string }> {
    const pending = await this.readPending(id, true);
    return {
      redirectTo: this.errorRedirect(
        pending.redirectUri,
        "access_denied",
        "The administrator declined the request",
        pending.state,
      ),
    };
  }

  /** `grant_type=authorization_code` (RFC 6749 §4.1.3, PKCE per RFC 7636 §4.6). */
  async exchangeCode(params: Params): Promise<TokenResponse> {
    const code = requireField(params, "code");
    const clientId = requireField(params, "client_id");
    const redirectUri = optionalField(params, "redirect_uri");
    const verifier = optionalField(params, "code_verifier");
    const resource = optionalField(params, "resource");

    const client = await this.clients.find(clientId);
    if (!client) {
      throw new OAuthError("invalid_client", "Unknown client", 401);
    }

    const codeHash = hashToken(code);
    const raw = await this.redis.getdel(CODE_PREFIX + codeHash);
    if (!raw) {
      const grantId = await this.redis.get(USED_CODE_PREFIX + codeHash);
      if (grantId) {
        // RFC 6749 §4.1.2: a code used twice revokes what it was used for.
        await this.tokens.revoke(grantId, null, "code-replay");
        this.logger.warn(
          `🔁 An authorization code was presented twice; revoked OAuth grant ${grantId}`,
        );
      }
      throw new OAuthError(
        "invalid_grant",
        "The authorization code is invalid, expired or already used",
      );
    }
    const stored = JSON.parse(raw) as StoredCode;

    if (stored.clientId !== client.clientId) {
      throw new OAuthError(
        "invalid_grant",
        "The code was issued to another client",
      );
    }
    const redirectMatches = stored.redirectUriGiven
      ? redirectUri === stored.redirectUri
      : redirectUri === undefined || redirectUri === stored.redirectUri;
    if (!redirectMatches) {
      throw new OAuthError(
        "invalid_grant",
        "redirect_uri does not match the authorization request",
      );
    }
    if (!verifyPkceS256(verifier, stored.codeChallenge)) {
      throw new OAuthError("invalid_grant", "PKCE verification failed");
    }
    if (resource !== undefined && !isMcpResource(resource)) {
      throw new OAuthError(
        "invalid_target",
        `Tokens are issued for ${getMcpResourceUrl()} only`,
      );
    }
    const account = await this.accounts.findById(stored.userId);
    if (!account?.isActive) {
      throw new OAuthError("invalid_grant", "The account is not active");
    }

    let issued: IssuedOAuthTokens;
    try {
      issued = await this.tokens.issueOAuthGrant({
        userId: stored.userId,
        clientId: client.clientId,
        clientName: client.clientName,
        scope: stored.scope,
        approvedAt: new Date(stored.approvedAt ?? 0),
      });
    } catch (error) {
      if (error instanceof GrantRefusedError) {
        throw new OAuthError("invalid_grant", error.message);
      }
      throw error;
    }
    await this.redis.set(
      USED_CODE_PREFIX + codeHash,
      issued.tokenId,
      "EX",
      USED_CODE_MEMORY_SECONDS,
    );
    this.logger.log(
      `🔑 OAuth grant ${issued.tokenId} issued to "${client.clientName}" for ${account.email}`,
    );
    return tokenResponse(issued);
  }

  /** `grant_type=refresh_token`, rotating (OAuth 2.1 §4.3.1). */
  async refresh(params: Params): Promise<TokenResponse> {
    const refreshToken = requireField(params, "refresh_token");
    const clientId = requireField(params, "client_id");
    const client = await this.clients.find(clientId);
    if (!client) {
      throw new OAuthError("invalid_client", "Unknown client", 401);
    }
    const resource = optionalField(params, "resource");
    if (resource !== undefined && !isMcpResource(resource)) {
      throw new OAuthError(
        "invalid_target",
        `Tokens are issued for ${getMcpResourceUrl()} only`,
      );
    }

    const outcome = await this.tokens.rotateRefreshToken(
      refreshToken,
      client.clientId,
    );
    if (outcome.outcome !== "ok") {
      throw new OAuthError(
        "invalid_grant",
        outcome.outcome === "reused"
          ? "The refresh token was already used; the grant is revoked"
          : "The refresh token is invalid or expired",
      );
    }
    return tokenResponse(outcome.tokens);
  }

  /** RFC 7009. Always succeeds for a well-formed request, known token or not. */
  async revoke(params: Params): Promise<void> {
    const token = requireField(params, "token");
    const clientId = requireField(params, "client_id");
    await this.tokens.revokeByValue(token, clientId);
  }

  private async withinLimit(key: string, max: number): Promise<boolean> {
    const verdict = await checkRateLimit({
      redis: this.redis,
      logger: this.logger,
      label: "OAuth authorize limiter",
      key,
      max,
      windowSeconds: PENDING_REQUEST_TTL_SECONDS,
    });
    return verdict.allowed;
  }

  private async readPending(
    id: string,
    consume: boolean,
  ): Promise<PendingRequest> {
    const key = REQUEST_PREFIX + hashToken(id);
    const raw = consume
      ? await this.redis.getdel(key)
      : await this.redis.get(key);
    if (!raw) {
      throw new NotFoundException(
        "No such pending request — it expired or was already answered",
      );
    }
    return JSON.parse(raw) as PendingRequest;
  }

  private errorRedirect(
    redirectUri: string,
    error: string,
    description: string,
    state: string | null,
  ): string {
    const target = new URL(redirectUri);
    target.searchParams.set("error", error);
    target.searchParams.set("error_description", description);
    if (state) target.searchParams.set("state", state);
    target.searchParams.set("iss", getApiPublicUrl());
    return target.toString();
  }
}

function tokenResponse(issued: IssuedOAuthTokens): TokenResponse {
  return {
    access_token: issued.accessToken,
    token_type: "Bearer",
    expires_in: issued.expiresIn,
    refresh_token: issued.refreshToken,
    scope: issued.scope,
  };
}
