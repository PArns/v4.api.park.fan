import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { createHash } from "crypto";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { incrementWithWindow } from "../../common/redis/rate-limit.util";

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
 * Only failures are counted in the end. A person signing in ten times a day
 * from an office is not an attack, and a limiter that cannot tell the
 * difference gets switched off by the person it annoys. Every attempt is
 * counted up front, though, and the ones that turn out not to be failures are
 * handed back: counting only after the ~100 ms password check let a hundred
 * parallel guesses all read the same counter and all pass. */

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
   * Check before doing any work, and count this attempt while doing so. Never
   * throws: Redis being down must not lock everybody out of the admin — the
   * per-account lockout in Postgres is still standing, and it is the durable
   * half of the defence.
   *
   * A refused attempt is handed back, so being refused never extends a bucket.
   * An attempt that is not a failure is handed back by `recordSuccess` or
   * `release`; one that fails simply keeps its count.
   */
  async check(ip: string | null, email: string): Promise<RateLimitVerdict> {
    const buckets = this.loginBuckets(ip, email);
    const counted: Array<[string, number]> = [];
    try {
      for (const [key, max, windowSeconds, reason] of buckets) {
        const count = await incrementWithWindow(this.redis, key, windowSeconds);
        counted.push([key, windowSeconds]);
        if (count > max) {
          const ttl = await this.redis.ttl(key);
          await this.refund(counted);
          return {
            allowed: false,
            retryAfterSeconds: ttl > 0 ? ttl : windowSeconds,
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

  /** Hand back an attempt `check` counted that was not a failure (a correct
   *  password that still needs its second factor, or a locked account). */
  async release(ip: string | null, email: string): Promise<void> {
    try {
      await this.refund(
        this.loginBuckets(ip, email).map(([key, , w]) => [key, w]),
      );
    } catch {
      // Best effort — see check().
    }
  }

  /** Clear the account bucket after a success, and hand this attempt back to
   *  the address bucket. The rest of the address bucket is left alone: one
   *  correct password does not vouch for the other 24 attempts from the same
   *  place. */
  async recordSuccess(ip: string | null, email: string): Promise<void> {
    try {
      await this.redis.del(ACCOUNT_PREFIX + hash(email));
      if (ip) await this.refund([[IP_PREFIX + hash(ip), IP_WINDOW_SECONDS]]);
    } catch {
      // Best effort.
    }
  }

  private loginBuckets(
    ip: string | null,
    email: string,
  ): Array<[string, number, number, "ip" | "account"]> {
    const buckets: Array<[string, number, number, "ip" | "account"]> = [
      [
        ACCOUNT_PREFIX + hash(email),
        ACCOUNT_MAX_FAILURES,
        ACCOUNT_WINDOW_SECONDS,
        "account",
      ],
    ];
    if (ip) {
      buckets.unshift([
        IP_PREFIX + hash(ip),
        IP_MAX_FAILURES,
        IP_WINDOW_SECONDS,
        "ip",
      ]);
    }
    return buckets;
  }

  private async refund(buckets: Array<[string, number]>): Promise<void> {
    for (const [key, windowSeconds] of buckets) {
      await this.redis.decr(key);
      // A key that expired in between comes back from DECR with no TTL.
      await this.redis.expire(key, windowSeconds, "NX");
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
    // `incrementWithWindow` sets the expiry with `NX` on every call rather
    // than only the first, so the window is still fixed from the first
    // attempt rather than sliding forward with each one — a sliding window
    // would let a slow attacker hold the door shut forever — but a crash
    // between the increment and the expiry no longer leaves the key
    // permanently uncounted-down (see the util's own docstring).
    await incrementWithWindow(this.redis, key, windowSeconds);
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
