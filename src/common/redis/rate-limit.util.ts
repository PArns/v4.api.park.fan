import { Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { createHash } from "crypto";

/**
 * The counting shape every IP/id-bucketed write limiter in this codebase
 * shares: `TripWriteRateLimitService`, `PushFollowWriteRateLimitService`, and
 * (via {@link incrementWithWindow} alone, since it also has to answer
 * separate `check`/`record` calls) `AdminLoginRateLimitService`. Extracted
 * here after the same incr-then-conditional-expire body was found copied
 * verbatim into two of those three files.
 */

export interface RateLimitVerdict {
  allowed: boolean;
  /** Seconds until the caller may try again; 0 when allowed. */
  retryAfterSeconds: number;
}

/**
 * Increment a counter and make sure it will eventually expire — without the
 * window a plain `if (count === 1) await redis.expire(...)` leaves open.
 *
 * That guard only ever attempts the `EXPIRE` on the exact call that sees
 * `count === 1`. A crash, a dropped connection, or the two commands landing
 * on different Redis Cluster nodes between the `INCR` and that one `EXPIRE`
 * leaves the key permanently un-timed — every later call sees `count > 1`
 * and never tries again, so the bucket is stuck (over its limit, if the
 * crash happened after enough attempts) until something else deletes the
 * key. `EXPIRE key seconds NX` ("only if the key has no TTL yet") run on
 * EVERY call instead makes the same attempt self-healing: whichever call
 * first finds the key without a TTL sets one, however many increments came
 * before it.
 */
export async function incrementWithWindow(
  redis: Redis,
  key: string,
  windowSeconds: number,
): Promise<number> {
  const count = await redis.incr(key);
  await redis.expire(key, windowSeconds, "NX");
  return count;
}

/**
 * Count one event against a bucket and say whether it may proceed.
 *
 * Never throws — every caller treats a Redis outage as "allow": the limiter
 * is a defence in depth in front of storage or a secret that has its own
 * durable guard, and refusing every write because a cache is unavailable
 * trades a bounded abuse risk for a certain outage.
 */
export async function checkRateLimit(params: {
  redis: Redis;
  logger: Logger;
  /** Named in the warning log when Redis is unavailable, e.g. "Trip write limiter". */
  label: string;
  key: string;
  max: number;
  windowSeconds: number;
}): Promise<RateLimitVerdict> {
  const { redis, logger, label, key, max, windowSeconds } = params;
  try {
    const count = await incrementWithWindow(redis, key, windowSeconds);
    if (count <= max) return { allowed: true, retryAfterSeconds: 0 };

    const ttl = await redis.ttl(key);
    return {
      allowed: false,
      retryAfterSeconds: ttl > 0 ? ttl : windowSeconds,
    };
  } catch (error) {
    logger.warn(
      `${label} unavailable, allowing: ${(error as Error).message}`,
    );
    return { allowed: true, retryAfterSeconds: 0 };
  }
}

/**
 * Hash a bucket key before it becomes part of a Redis key.
 *
 * A Redis key list is dumped in a support ticket or a screenshot far more
 * casually than a database table is, and e.g. `trip:create:ip:1.2.3.4` next
 * to a timestamp is a visitor's location. The limiter only ever needs
 * equality, never the original value back.
 *
 * Not used by `AdminLoginRateLimitService`, which lowercases an email before
 * hashing it (an address is case-insensitive; an IP is not) — a genuine
 * difference in what "the same bucket" means, not a copy of this one.
 */
export function hashRateLimitKey(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 32);
}
