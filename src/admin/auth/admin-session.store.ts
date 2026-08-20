import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { createHash, randomBytes } from "crypto";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import type { AdminRole } from "./entities/admin-user.entity";

/**
 * Server-side admin sessions, in Redis.
 *
 * Opaque random tokens rather than JWTs, and that is the whole design decision:
 * a JWT cannot be revoked. An admin session grants the right to merge parks and
 * retire attractions, so "log this device out now" and "deactivate this account
 * now" have to mean now, not "in fifteen minutes when the access token
 * expires". Statelessness buys nothing here either — every admin request
 * already touches Postgres.
 *
 * What Redis stores is the SHA-256 of the token, never the token: an attacker
 * who reads a Redis dump then holds hashes they cannot present. Same reasoning
 * as a password table, for the same kind of secret.
 *
 * Two clocks bound a session. Idle expiry slides on every request (a working
 * curation session stays alive), absolute expiry does not (a forgotten tab
 * eventually dies regardless).
 */

/** Sliding: a session unused for this long is gone. */
const IDLE_TTL_SECONDS = 12 * 60 * 60;

/** Hard ceiling from creation, whatever the activity. */
const ABSOLUTE_TTL_SECONDS = 7 * 24 * 60 * 60;

/** Refresh the sliding TTL at most this often, so a burst of requests does not
 *  turn into a burst of writes for no behavioural difference. */
const TOUCH_INTERVAL_SECONDS = 60;

const SESSION_PREFIX = "admin:session:";
const USER_SESSIONS_PREFIX = "admin:sessions:";

export interface AdminSession {
  userId: string;
  email: string;
  displayName: string;
  role: AdminRole;
  /** Epoch ms. */
  createdAt: number;
  /** Epoch ms; the moment the absolute ceiling is reached. */
  expiresAt: number;
  /** Epoch ms of the last request that touched it. */
  lastSeenAt: number;
  ip: string | null;
  userAgent: string | null;
  /** Set while the account still owes us a password change: the guard lets
   *  such a session reach the change-password endpoint and nothing else. */
  mustChangePassword: boolean;
}

export interface IssuedSession {
  token: string;
  session: AdminSession;
}

function hashToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

@Injectable()
export class AdminSessionStore {
  private readonly logger = new Logger(AdminSessionStore.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async create(
    input: Omit<AdminSession, "createdAt" | "expiresAt" | "lastSeenAt">,
  ): Promise<IssuedSession> {
    const now = Date.now();
    const session: AdminSession = {
      ...input,
      createdAt: now,
      expiresAt: now + ABSOLUTE_TTL_SECONDS * 1000,
      lastSeenAt: now,
    };

    // 32 bytes of CSPRNG, base64url — 256 bits of entropy, and no characters
    // that need escaping in a header or a Set-Cookie value.
    const token = randomBytes(32).toString("base64url");
    const key = SESSION_PREFIX + hashToken(token);

    await this.redis.set(key, JSON.stringify(session), "EX", IDLE_TTL_SECONDS);
    // Index by user so "log out everywhere" and the active-sessions list do not
    // have to SCAN the keyspace. Given the same absolute TTL so the index
    // cannot outlive every session it points at.
    const indexKey = USER_SESSIONS_PREFIX + session.userId;
    await this.redis.sadd(indexKey, hashToken(token));
    await this.redis.expire(indexKey, ABSOLUTE_TTL_SECONDS);

    return { token, session };
  }

  /**
   * Resolve a token, sliding its idle expiry.
   *
   * Returns null for absent, expired-by-absolute-ceiling, or malformed — the
   * caller must not be able to tell those apart, and neither should it care.
   */
  async resolve(token: string): Promise<AdminSession | null> {
    if (!token) return null;
    const tokenHash = hashToken(token);
    const key = SESSION_PREFIX + tokenHash;

    const raw = await this.redis.get(key);
    if (!raw) return null;

    let session: AdminSession;
    try {
      session = JSON.parse(raw) as AdminSession;
    } catch {
      await this.redis.del(key);
      return null;
    }

    const now = Date.now();
    if (session.expiresAt <= now) {
      await this.destroyByHash(session.userId, tokenHash);
      return null;
    }

    if (now - session.lastSeenAt > TOUCH_INTERVAL_SECONDS * 1000) {
      session.lastSeenAt = now;
      // Never let the sliding window push past the absolute ceiling.
      const remaining = Math.ceil((session.expiresAt - now) / 1000);
      await this.redis.set(
        key,
        JSON.stringify(session),
        "EX",
        Math.min(IDLE_TTL_SECONDS, remaining),
      );
    }

    return session;
  }

  /** Rewrite every live session of one user — used when a role changes or the
   *  password-change debt is cleared, so the change takes effect immediately
   *  rather than on the next login. */
  async patchUserSessions(
    userId: string,
    patch: Partial<
      Pick<AdminSession, "role" | "mustChangePassword" | "displayName">
    >,
  ): Promise<number> {
    const hashes = await this.redis.smembers(USER_SESSIONS_PREFIX + userId);
    let patched = 0;
    for (const tokenHash of hashes) {
      const key = SESSION_PREFIX + tokenHash;
      const raw = await this.redis.get(key);
      if (!raw) {
        await this.redis.srem(USER_SESSIONS_PREFIX + userId, tokenHash);
        continue;
      }
      try {
        const session = JSON.parse(raw) as AdminSession;
        const ttl = await this.redis.ttl(key);
        Object.assign(session, patch);
        await this.redis.set(
          key,
          JSON.stringify(session),
          "EX",
          ttl > 0 ? ttl : IDLE_TTL_SECONDS,
        );
        patched++;
      } catch {
        await this.redis.del(key);
      }
    }
    return patched;
  }

  async destroy(token: string): Promise<void> {
    if (!token) return;
    const tokenHash = hashToken(token);
    const raw = await this.redis.get(SESSION_PREFIX + tokenHash);
    let userId: string | null = null;
    if (raw) {
      try {
        userId = (JSON.parse(raw) as AdminSession).userId;
      } catch {
        userId = null;
      }
    }
    await this.redis.del(SESSION_PREFIX + tokenHash);
    if (userId) await this.redis.srem(USER_SESSIONS_PREFIX + userId, tokenHash);
  }

  private async destroyByHash(
    userId: string,
    tokenHash: string,
  ): Promise<void> {
    await this.redis.del(SESSION_PREFIX + tokenHash);
    await this.redis.srem(USER_SESSIONS_PREFIX + userId, tokenHash);
  }

  /** Drop every session of one user. Called on deactivation, on a password
   *  change, and by "sign out everywhere". */
  async destroyAllForUser(userId: string): Promise<number> {
    const indexKey = USER_SESSIONS_PREFIX + userId;
    const hashes = await this.redis.smembers(indexKey);
    if (hashes.length > 0) {
      for (let i = 0; i < hashes.length; i += 250) {
        await this.redis.del(
          ...hashes.slice(i, i + 250).map((h) => SESSION_PREFIX + h),
        );
      }
    }
    await this.redis.del(indexKey);
    return hashes.length;
  }

  /**
   * The live sessions of one user, for the "where am I signed in" list.
   * Prunes index entries whose session has already expired.
   */
  async listForUser(
    userId: string,
    currentToken?: string,
  ): Promise<Array<AdminSession & { id: string; current: boolean }>> {
    const indexKey = USER_SESSIONS_PREFIX + userId;
    const hashes = await this.redis.smembers(indexKey);
    const currentHash = currentToken ? hashToken(currentToken) : null;
    const sessions: Array<AdminSession & { id: string; current: boolean }> = [];

    for (const tokenHash of hashes) {
      const raw = await this.redis.get(SESSION_PREFIX + tokenHash);
      if (!raw) {
        await this.redis.srem(indexKey, tokenHash);
        continue;
      }
      try {
        const session = JSON.parse(raw) as AdminSession;
        sessions.push({
          ...session,
          // The first 12 hex characters of the token hash: enough to name a
          // session in a revoke button, useless for presenting one.
          id: tokenHash.slice(0, 12),
          current: tokenHash === currentHash,
        });
      } catch {
        await this.redis.del(SESSION_PREFIX + tokenHash);
        await this.redis.srem(indexKey, tokenHash);
      }
    }

    return sessions.sort((a, b) => b.lastSeenAt - a.lastSeenAt);
  }

  /** Revoke one session of a user by the short id `listForUser` reports. */
  async destroyByShortId(userId: string, shortId: string): Promise<boolean> {
    const indexKey = USER_SESSIONS_PREFIX + userId;
    const hashes = await this.redis.smembers(indexKey);
    const match = hashes.find((h) => h.startsWith(shortId));
    if (!match) return false;
    await this.destroyByHash(userId, match);
    return true;
  }
}
