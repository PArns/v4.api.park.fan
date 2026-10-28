import { BadRequestException, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { EntityManager, Repository } from "typeorm";
import { AdminApiToken } from "./entities/admin-api-token.entity";
import { AdminApiTokenSecret } from "./entities/admin-api-token-secret.entity";
import { lesserRole, type AdminRole } from "./entities/admin-user.entity";
import type { AdminPrincipal } from "./admin-principal";
import {
  ACCESS_TOKEN_TTL_SECONDS,
  ADMIN_TOKEN_SCOPES,
  DEFAULT_ADMIN_TOKEN_SCOPE,
  DISPLAY_PREFIX_LENGTH,
  OAUTH_ACCESS_PREFIX,
  OAUTH_REFRESH_PREFIX,
  PAT_PREFIX,
  REFRESH_TOKEN_TTL_SECONDS,
  looksLikeApiToken,
  type AdminApiTokenKind,
  type AdminTokenScope,
} from "./admin-api-token.constants";
import { generateToken, hashToken } from "./token-hash.util";
import { isTotpRequired } from "../../config/admin-auth.config";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";

/** One row of `GET /v1/admin/auth/tokens`. */
export interface AdminApiTokenItem {
  id: string;
  kind: AdminApiTokenKind;
  name: string | null;
  clientName: string | null;
  prefix: string;
  scope: string;
  createdAt: string;
  lastUsedAt: string | null;
  expiresAt: string | null;
}

export interface IssuedOAuthTokens {
  tokenId: string;
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  scope: AdminTokenScope;
}

export type RefreshOutcome =
  | { outcome: "ok"; tokens: IssuedOAuthTokens; userId: string }
  | { outcome: "invalid" }
  | { outcome: "reused"; tokenId: string };

/** Enough PATs for every machine a person owns; a bound on what a leak can mint. */
export const MAX_ACTIVE_PATS_PER_USER = 25;

/** Each `/mcp` login makes a new grant; the oldest beyond this are revoked. */
export const MAX_ACTIVE_GRANTS_PER_USER = 20;

/**
 * Two processes of one client sharing a refresh token race to rotate it. The
 * loser presents a token consumed a moment ago; within this window that is a
 * race and answers `invalid_grant`, after it the grant is treated as stolen.
 */
export const REFRESH_REUSE_GRACE_SECONDS = 10;

const LAST_USED_RESOLUTION_MS = 60_000;
const PURGE_INTERVAL_MS = 60 * 60 * 1000;

/** An exchange for an account that signed out everywhere after the approval. */
export class GrantRefusedError extends Error {
  constructor() {
    super("The account signed out everywhere after this code was approved");
  }
}

/**
 * Lock the account's row for the rest of the transaction and read its
 * sign-out cut-off. `signOutEverywhere` writes the cut-off on the same row, so
 * a token is either issued before it (and revoked with the rest) or after it.
 */
async function lockAccount(
  manager: EntityManager,
  userId: string,
): Promise<Date | null> {
  const rows = (await manager.query(
    `SELECT tokens_revoked_at FROM admin_users WHERE id = $1::uuid FOR UPDATE`,
    [userId],
  )) as Array<{ tokens_revoked_at: Date | null }>;
  if (rows.length === 0) throw new GrantRefusedError();
  return rows[0].tokens_revoked_at ? new Date(rows[0].tokens_revoked_at) : null;
}

/** Lets each row of an UPDATE … RETURNING through TypeORM's `[rows, count]`. */
function returnedRows<T>(result: unknown): T[] {
  if (Array.isArray(result) && Array.isArray(result[0]))
    return result[0] as T[];
  return Array.isArray(result) ? (result as T[]) : [];
}

function displayPrefix(token: string): string {
  return token.slice(0, DISPLAY_PREFIX_LENGTH);
}

function iso(value: Date | string | null): string | null {
  return value ? new Date(value).toISOString() : null;
}

export function toTokenItem(row: AdminApiToken): AdminApiTokenItem {
  return {
    id: row.id,
    kind: row.kind,
    name: row.name,
    clientName: row.clientName,
    prefix: row.prefix,
    scope: row.scope,
    createdAt: new Date(row.createdAt).toISOString(),
    lastUsedAt: iso(row.lastUsedAt),
    expiresAt: iso(row.expiresAt),
  };
}

/**
 * Personal access tokens and OAuth grants, in Postgres.
 *
 * Postgres rather than Redis, where sessions live: a token outlives any
 * session by weeks, and a Redis flush must not sign every MCP client out or,
 * worse, forget that a refresh token was already used. Every bearer lookup
 * joins `admin_users`, so a deactivated account is refused on its next
 * request, revoked or not.
 */
@Injectable()
export class AdminApiTokenStore {
  private readonly logger = new Logger(AdminApiTokenStore.name);
  private lastPurgeAt = 0;

  constructor(
    @InjectRepository(AdminApiToken)
    private readonly tokens: Repository<AdminApiToken>,
    @InjectRepository(AdminApiTokenSecret)
    private readonly secrets: Repository<AdminApiTokenSecret>,
  ) {}

  /** A new PAT, returned in clear exactly once. */
  async createPersonalToken(
    userId: string,
    name: string,
    expiresInDays: number | null,
  ): Promise<{ token: string; item: AdminApiTokenItem }> {
    const token = generateToken(PAT_PREFIX);
    const expiresAt = expiresInDays
      ? new Date(Date.now() + expiresInDays * 24 * 60 * 60 * 1000)
      : null;
    const scope = DEFAULT_ADMIN_TOKEN_SCOPE;

    const row = await this.tokens.manager.transaction(async (manager) => {
      // Counted under the account's row lock, so two parallel creations
      // cannot both pass the cap.
      await lockAccount(manager, userId);
      const [{ active }] = (await manager.query(
        `SELECT count(*)::int AS active FROM admin_api_tokens
          WHERE user_id = $1::uuid AND kind = 'pat' AND revoked_at IS NULL
            AND (expires_at IS NULL OR expires_at > now())`,
        [userId],
      )) as Array<{ active: number }>;
      if (active >= MAX_ACTIVE_PATS_PER_USER) {
        throw new BadRequestException(
          `At most ${MAX_ACTIVE_PATS_PER_USER} active tokens per account — revoke one first`,
        );
      }
      const saved = await manager.save(
        manager.create(AdminApiToken, {
          userId,
          kind: "pat",
          name,
          clientId: null,
          clientName: null,
          prefix: displayPrefix(token),
          scope,
          roleCap: ADMIN_TOKEN_SCOPES[scope],
          lastUsedAt: null,
          expiresAt,
          revokedAt: null,
          revokedReason: null,
        }),
      );
      await manager.insert(AdminApiTokenSecret, {
        tokenHash: hashToken(token),
        tokenId: saved.id,
        kind: "pat",
        expiresAt,
        consumedAt: null,
      });
      return saved;
    });

    return { token, item: toTokenItem(row) };
  }

  /**
   * A new grant for an approved, exchanged authorization code. Refused with
   * {@link GrantRefusedError} when the account signed out everywhere after
   * the approval: the code was meant to die with its sessions.
   */
  async issueOAuthGrant(input: {
    userId: string;
    clientId: string;
    clientName: string;
    scope: AdminTokenScope;
    approvedAt: Date;
  }): Promise<IssuedOAuthTokens> {
    const access = generateToken(OAUTH_ACCESS_PREFIX);
    const refresh = generateToken(OAUTH_REFRESH_PREFIX);
    const refreshExpiresAt = this.refreshExpiry();

    const row = await this.tokens.manager.transaction(async (manager) => {
      const cutoff = await lockAccount(manager, input.userId);
      if (cutoff && input.approvedAt.getTime() <= cutoff.getTime()) {
        throw new GrantRefusedError();
      }
      const saved = await manager.save(
        manager.create(AdminApiToken, {
          userId: input.userId,
          kind: "oauth",
          name: null,
          clientId: input.clientId,
          clientName: input.clientName,
          prefix: displayPrefix(access),
          scope: input.scope,
          roleCap: ADMIN_TOKEN_SCOPES[input.scope],
          lastUsedAt: null,
          expiresAt: refreshExpiresAt,
          revokedAt: null,
          revokedReason: null,
        }),
      );
      await manager.insert(AdminApiTokenSecret, [
        this.accessSecret(access, saved.id),
        this.refreshSecret(refresh, saved.id, refreshExpiresAt),
      ]);
      return saved;
    });

    await this.pruneGrants(input.userId);
    this.purgeExpiredSoon();

    return {
      tokenId: row.id,
      accessToken: access,
      refreshToken: refresh,
      expiresIn: ACCESS_TOKEN_TTL_SECONDS,
      scope: input.scope,
    };
  }

  /**
   * Exchange a refresh token for a new pair (OAuth 2.1 rotation).
   *
   * The token is claimed with a conditional UPDATE, so of two parallel
   * exchanges exactly one wins. Presenting a token that was already exchanged
   * revokes the whole grant: either the client or a thief holds a copy, and
   * the server cannot tell which.
   */
  async rotateRefreshToken(
    refreshToken: string,
    clientId: string,
  ): Promise<RefreshOutcome> {
    if (!refreshToken.startsWith(OAUTH_REFRESH_PREFIX)) {
      return { outcome: "invalid" };
    }
    const hash = hashToken(refreshToken);

    // The claim and the new secrets are one transaction: a failure between
    // them must not leave the token consumed, or the client's retry would
    // look like a reuse and revoke a healthy grant.
    const access = generateToken(OAUTH_ACCESS_PREFIX);
    const refresh = generateToken(OAUTH_REFRESH_PREFIX);
    const refreshExpiresAt = this.refreshExpiry();
    const grant = await this.tokens.manager.transaction(async (manager) => {
      const claimed = returnedRows<{
        token_id: string;
        user_id: string;
        scope: string;
      }>(
        await manager.query(
          `UPDATE admin_api_token_secrets s
              SET consumed_at = now()
             FROM admin_api_tokens t
            WHERE s.token_hash = $1
              AND s.kind = 'refresh'
              AND s.consumed_at IS NULL
              AND s.expires_at > now()
              AND t.id = s.token_id
              AND t.revoked_at IS NULL
              AND t.client_id = $2
              AND EXISTS (
                SELECT 1 FROM admin_users u
                 WHERE u.id = t.user_id AND u.is_active
                   AND (u.tokens_revoked_at IS NULL OR t.created_at > u.tokens_revoked_at)
              )
          RETURNING t.id AS token_id, t.user_id, t.scope`,
          [hash, clientId],
        ),
      );
      const found = claimed[0];
      if (!found) return null;
      await manager.insert(AdminApiTokenSecret, [
        this.accessSecret(access, found.token_id),
        this.refreshSecret(refresh, found.token_id, refreshExpiresAt),
      ]);
      await manager.update(
        AdminApiToken,
        { id: found.token_id },
        { prefix: displayPrefix(access), expiresAt: refreshExpiresAt },
      );
      return found;
    });

    if (!grant) {
      const [seen] = (await this.secrets.query(
        `SELECT s.token_id,
                s.consumed_at < now() - make_interval(secs => $2) AS reused
           FROM admin_api_token_secrets s
           JOIN admin_api_tokens t ON t.id = s.token_id
          WHERE s.token_hash = $1
            AND s.kind = 'refresh'
            AND s.consumed_at IS NOT NULL
            AND t.revoked_at IS NULL`,
        [hash, REFRESH_REUSE_GRACE_SECONDS],
      )) as Array<{ token_id: string; reused: boolean }>;
      if (seen?.reused) {
        await this.revoke(seen.token_id, null, "refresh-reuse");
        this.logger.warn(
          `🔁 A rotated refresh token was presented again; revoked OAuth grant ${seen.token_id}`,
        );
        return { outcome: "reused", tokenId: seen.token_id };
      }
      return { outcome: "invalid" };
    }

    const scope = (
      grant.scope in ADMIN_TOKEN_SCOPES
        ? grant.scope
        : DEFAULT_ADMIN_TOKEN_SCOPE
    ) as AdminTokenScope;

    this.purgeExpiredSoon();

    return {
      outcome: "ok",
      userId: grant.user_id,
      tokens: {
        tokenId: grant.token_id,
        accessToken: access,
        refreshToken: refresh,
        expiresIn: ACCESS_TOKEN_TTL_SECONDS,
        scope,
      },
    };
  }

  /**
   * The principal behind a PAT or an OAuth access token, or null.
   *
   * Null for anything not valid right now: unknown, expired, revoked, a
   * refresh token, an inactive account. The role is the lesser of the
   * account's and the token's cap.
   */
  async resolvePrincipal(
    token: string,
    ip: string | null,
  ): Promise<AdminPrincipal | null> {
    if (!looksLikeApiToken(token)) return null;

    const [row] = (await this.secrets.query(
      `SELECT t.id, t.kind, t.client_name, t.scope, t.role_cap, t.last_used_at,
              u.id AS user_id, u.email, u.display_name, u.role, u.is_active,
              u.must_change_password, u.totp_enabled
         FROM admin_api_token_secrets s
         JOIN admin_api_tokens t ON t.id = s.token_id
         JOIN admin_users u ON u.id = t.user_id
        WHERE s.token_hash = $1
          AND s.kind IN ('pat', 'access')
          AND (s.expires_at IS NULL OR s.expires_at > now())
          AND t.revoked_at IS NULL
          AND (t.expires_at IS NULL OR t.expires_at > now())
          AND (u.tokens_revoked_at IS NULL OR t.created_at > u.tokens_revoked_at)`,
      [hashToken(token)],
    )) as Array<{
      id: string;
      kind: AdminApiTokenKind;
      client_name: string | null;
      scope: string;
      role_cap: AdminRole;
      last_used_at: Date | null;
      user_id: string;
      email: string;
      display_name: string;
      role: AdminRole;
      is_active: boolean;
      must_change_password: boolean;
      totp_enabled: boolean;
    }>;
    if (!row || !row.is_active) return null;

    this.touch(row.id, row.last_used_at);

    return {
      userId: row.user_id,
      email: row.email,
      displayName: row.display_name,
      role: lesserRole(row.role, row.role_cap),
      sessionToken: null,
      legacy: false,
      ip,
      mustChangePassword: row.must_change_password,
      mustEnrolTotp: isTotpRequired() && !row.totp_enabled,
      apiToken: {
        id: row.id,
        kind: row.kind,
        clientName: row.client_name,
        scope: row.scope,
      },
    };
  }

  /** Active tokens and grants of one account, newest first. */
  async list(userId: string): Promise<AdminApiTokenItem[]> {
    const rows = await this.activeQuery(userId)
      .orderBy("t.createdAt", "DESC")
      .getMany();
    return rows.map(toTokenItem);
  }

  /**
   * Revoke one token or grant and drop its secrets. `userId` limits it to that
   * account's tokens; null revokes whoever's it is (an owner, or the server).
   * Returns the row as it was, or null when there was nothing to revoke.
   */
  async revoke(
    id: string,
    userId: string | null,
    reason: string,
  ): Promise<AdminApiToken | null> {
    if (!isCanonicalUuid(id)) return null;
    if (userId !== null && !isCanonicalUuid(userId)) return null;
    const [row] = returnedRows<{
      id: string;
      user_id: string;
      kind: AdminApiTokenKind;
      name: string | null;
      client_name: string | null;
      prefix: string;
    }>(
      await this.tokens.query(
        `UPDATE admin_api_tokens
            SET revoked_at = now(), revoked_reason = $3
          WHERE id = $1::uuid
            AND revoked_at IS NULL
            AND ($2::uuid IS NULL OR user_id = $2::uuid)
        RETURNING id, user_id, kind, name, client_name, prefix`,
        [id, userId, reason],
      ),
    );
    if (!row) return null;
    await this.secrets.query(
      `DELETE FROM admin_api_token_secrets WHERE token_id = $1::uuid`,
      [row.id],
    );
    return Object.assign(new AdminApiToken(), {
      id: row.id,
      userId: row.user_id,
      kind: row.kind,
      name: row.name,
      clientName: row.client_name,
      prefix: row.prefix,
    });
  }

  /** Every token and grant of an account, at once: deactivation, sign-out-everywhere. */
  async revokeAllForUser(userId: string, reason: string): Promise<number> {
    if (!isCanonicalUuid(userId)) return 0;
    const rows = returnedRows<{ id: string }>(
      await this.tokens.query(
        `UPDATE admin_api_tokens
            SET revoked_at = now(), revoked_reason = $2
          WHERE user_id = $1::uuid AND revoked_at IS NULL
        RETURNING id`,
        [userId, reason],
      ),
    );
    if (rows.length > 0) {
      await this.secrets.query(
        `DELETE FROM admin_api_token_secrets WHERE token_id = ANY($1::uuid[])`,
        [rows.map((row) => row.id)],
      );
    }
    return rows.length;
  }

  /**
   * RFC 7009: revoke the grant an access or refresh token belongs to, when the
   * client presenting it is the one it was issued to. A PAT is not revoked
   * here; it has no client, and its owner manages it on the account page.
   */
  async revokeByValue(token: string, clientId: string): Promise<boolean> {
    const [row] = (await this.secrets.query(
      `SELECT t.id
         FROM admin_api_token_secrets s
         JOIN admin_api_tokens t ON t.id = s.token_id
        WHERE s.token_hash = $1
          AND s.kind IN ('access', 'refresh')
          AND t.kind = 'oauth'
          AND t.client_id = $2`,
      [hashToken(token), clientId],
    )) as Array<{ id: string }>;
    if (!row) return false;
    return (await this.revoke(row.id, null, "client")) !== null;
  }

  private activeQuery(userId: string) {
    return this.tokens
      .createQueryBuilder("t")
      .where("t.user_id = CAST(:userId AS uuid)", { userId })
      .andWhere("t.revoked_at IS NULL")
      .andWhere("(t.expires_at IS NULL OR t.expires_at > now())");
  }

  private refreshExpiry(): Date {
    return new Date(Date.now() + REFRESH_TOKEN_TTL_SECONDS * 1000);
  }

  private accessSecret(token: string, tokenId: string) {
    return {
      tokenHash: hashToken(token),
      tokenId,
      kind: "access" as const,
      expiresAt: new Date(Date.now() + ACCESS_TOKEN_TTL_SECONDS * 1000),
      consumedAt: null,
    };
  }

  private refreshSecret(token: string, tokenId: string, expiresAt: Date) {
    return {
      tokenHash: hashToken(token),
      tokenId,
      kind: "refresh" as const,
      expiresAt,
      consumedAt: null,
    };
  }

  /** At most one write a minute per token, and none for a burst of tool calls. */
  private touch(tokenId: string, lastUsedAt: Date | null): void {
    if (
      lastUsedAt &&
      Date.now() - new Date(lastUsedAt).getTime() < LAST_USED_RESOLUTION_MS
    ) {
      return;
    }
    this.tokens
      .query(
        `UPDATE admin_api_tokens
            SET last_used_at = now()
          WHERE id = $1::uuid
            AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute')`,
        [tokenId],
      )
      .catch((error: unknown) =>
        this.logger.warn(
          `Could not record token use: ${error instanceof Error ? error.message : String(error)}`,
        ),
      );
  }

  private async pruneGrants(userId: string): Promise<void> {
    const surplus = (await this.tokens.query(
      `SELECT id FROM admin_api_tokens
        WHERE user_id = $1::uuid AND kind = 'oauth' AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
        ORDER BY created_at DESC
        OFFSET $2`,
      [userId, MAX_ACTIVE_GRANTS_PER_USER],
    )) as Array<{ id: string }>;
    for (const { id } of surplus) {
      await this.revoke(id, null, "superseded");
    }
  }

  /**
   * Expired secrets are dead weight; a consumed refresh token is kept only
   * until it would have expired, which is as long as replaying it matters.
   */
  private purgeExpiredSoon(): void {
    const now = Date.now();
    if (now - this.lastPurgeAt < PURGE_INTERVAL_MS) return;
    this.lastPurgeAt = now;
    this.secrets
      .query(
        `DELETE FROM admin_api_token_secrets
          WHERE expires_at < now() - interval '1 day'`,
      )
      .catch((error: unknown) =>
        this.logger.warn(
          `Could not purge expired token secrets: ${error instanceof Error ? error.message : String(error)}`,
        ),
      );
  }
}
