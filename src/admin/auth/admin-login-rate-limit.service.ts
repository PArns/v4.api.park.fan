import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { createHash } from "crypto";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * A rate limiter the login endpoint owns, rather than the global one.
 *
 * The global `CfThrottlerGuard` cannot do this job: it skips any request
 * carrying a valid `THROTTLE_BYPASS_KEYS` value, and our own frontend sends
 * exactly that on every server-side call. Since the admin UI reaches this API
 * only through that proxy, a `@Throttle()` on the login handler would be
 * skipped for every real login attempt and enforced only against callers who
 * are not the admin UI — the precise inverse of what is wanted.
 *
 * So the counting happens here, in Redis, unconditionally. Two buckets, and
 * they answer different questions:
 *
 *  - by **address**: one place guessing at many accounts (a spray). The
 *    account lockout in AdminAuthService never sees this, because each
 *    individual account only fails once or twice.
 *  - by **account**: many places guessing at one account. This overlaps with
 *    the lockout on purpose — the lockout is durable and visible to an owner
 *    in the UI, this one is cheap and sheds load before a password hash is
 *    ever computed (~100 ms of scrypt per attempt is itself a DoS surface).
 *
 * Counters are only incremented on FAILURE. A person signing in ten times a
 * day from an office is not an attack, and a limiter that cannot tell the
 * difference gets switched off by the person it annoys.
 */

const IP_PREFIX = "admin:login:ip:";
const ACCOUNT_PREFIX = "admin:login:acct:";
const ACTION_PREFIX = "admin:action:";

/**
 * The window and ceiling for a guessable secret that is not a password.
 *
 * Tighter than the login's, because the secrets these protect are small: a TOTP
 * code is six digits and three of them are valid at any moment, so an
 * unthrottled attacker needs ~333k requests — minutes, not years. The login's
 * secret is a passphrase and its limiter is sized against a spray, not against
 * exhausting the space.
 */
const ACTION_WINDOW_SECONDS = 15 * 60;
const ACTION_MAX_FAILURES = 10;

const IP_WINDOW_SECONDS = 15 * 60;
const IP_MAX_FAILURES = 25;

const ACCOUNT_WINDOW_SECONDS = 15 * 60;
const ACCOUNT_MAX_FAILURES = 10;

export interface RateLimitVerdict {
  allowed: boolean;
  /** Seconds until the caller may try again; 0 when allowed. */
  retryAfterSeconds: number;
  reason: "ip" | "account" | null;
}

@Injectable()
export class AdminLoginRateLimitService {
  private readonly logger = new Logger(AdminLoginRateLimitService.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /**
   * Check before doing any work. Never throws: Redis being down must not lock
   * everybody out of the admin — the per-account lockout in Postgres is still
   * standing, and it is the durable half of the defence.
   */
  async check(ip: string | null, email: string): Promise<RateLimitVerdict> {
    try {
      const checks: Array<[string, number, "ip" | "account"]> = [
        [ACCOUNT_PREFIX + hash(email), ACCOUNT_MAX_FAILURES, "account"],
      ];
      if (ip) checks.unshift([IP_PREFIX + hash(ip), IP_MAX_FAILURES, "ip"]);

      for (const [key, max, reason] of checks) {
        const raw = await this.redis.get(key);
        const count = raw ? Number.parseInt(raw, 10) : 0;
        if (count >= max) {
          const ttl = await this.redis.ttl(key);
          return {
            allowed: false,
            retryAfterSeconds: ttl > 0 ? ttl : ACCOUNT_WINDOW_SECONDS,
            reason,
          };
        }
      }
      return { allowed: true, retryAfterSeconds: 0, reason: null };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(`Login rate-limit check unavailable: ${message}`);
      return { allowed: true, retryAfterSeconds: 0, reason: null };
    }
  }

  /** Count one failed attempt against both buckets. */
  async recordFailure(ip: string | null, email: string): Promise<void> {
    try {
      await this.bump(ACCOUNT_PREFIX + hash(email), ACCOUNT_WINDOW_SECONDS);
      if (ip) await this.bump(IP_PREFIX + hash(ip), IP_WINDOW_SECONDS);
    } catch {
      // Best effort — see check().
    }
  }

  /** Clear the account bucket after a success. The address bucket is left
   *  alone: one correct password does not vouch for the other 24 attempts
   *  from the same place. */
  async recordSuccess(email: string): Promise<void> {
    try {
      await this.redis.del(ACCOUNT_PREFIX + hash(email));
    } catch {
      // Best effort.
    }
  }

  /**
   * The same counter, for a sensitive action by an already-signed-in account.
   *
   * The global throttler cannot do this either, for the same reason it cannot
   * do the login: it skips any request carrying a valid bypass key, and the
   * admin UI sends one on every server-side call. So `@Throttle()` on
   * `totp/disable` would be skipped for every request the admin actually
   * makes.
   *
   * What that leaves unprotected is not theoretical. `totp/disable` takes a
   * six-digit code, three of which are valid at any moment; an attacker
   * holding a stolen session and the account password needs about 333k
   * unthrottled requests to remove the second factor, which is minutes.
   */
  async checkAction(
    action: string,
    subject: string,
  ): Promise<RateLimitVerdict> {
    try {
      const key = ACTION_PREFIX + action + ":" + hash(subject);
      const raw = await this.redis.get(key);
      const count = raw ? Number.parseInt(raw, 10) : 0;
      if (count >= ACTION_MAX_FAILURES) {
        const ttl = await this.redis.ttl(key);
        return {
          allowed: false,
          retryAfterSeconds: ttl > 0 ? ttl : ACTION_WINDOW_SECONDS,
          reason: "account",
        };
      }
      return { allowed: true, retryAfterSeconds: 0, reason: null };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(`Action rate-limit check unavailable: ${message}`);
      return { allowed: true, retryAfterSeconds: 0, reason: null };
    }
  }

  async recordActionFailure(action: string, subject: string): Promise<void> {
    try {
      await this.bump(
        ACTION_PREFIX + action + ":" + hash(subject),
        ACTION_WINDOW_SECONDS,
      );
    } catch {
      // Best effort — see check().
    }
  }

  async recordActionSuccess(action: string, subject: string): Promise<void> {
    try {
      await this.redis.del(ACTION_PREFIX + action + ":" + hash(subject));
    } catch {
      // Best effort.
    }
  }

  private async bump(key: string, windowSeconds: number): Promise<void> {
    const count = await this.redis.incr(key);
    // Only the first failure in a window sets the expiry, so the window is
    // fixed from the first attempt rather than sliding forward with each one —
    // a sliding window would let a slow attacker hold the door shut forever.
    if (count === 1) await this.redis.expire(key, windowSeconds);
  }
}

/**
 * Hash the bucket key.
 *
 * An address and an email address are both personal data, and this is a cache
 * that a support engineer may well `KEYS admin:login:*` one day. The counter
 * works exactly as well against a digest.
 */
function hash(value: string): string {
  return createHash("sha256")
    .update(value.toLowerCase())
    .digest("hex")
    .slice(0, 32);
}
