import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { createHash } from "crypto";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * A limiter the trip endpoint owns, rather than the global one.
 *
 * `CfThrottlerGuard` cannot do this job, for the same reason it cannot do the
 * admin login's: it skips any request carrying a valid `THROTTLE_BYPASS_KEYS`
 * value, and our own frontend sends exactly that on every server-side call.
 * Since the planner reaches this API only through that proxy, a `@Throttle()`
 * here would be skipped for every real write and enforced only against callers
 * who are NOT the planner — the precise inverse of what is wanted. So the
 * counting happens here, unconditionally.
 *
 * It counts **every** write, not just failures, which is the difference from
 * `AdminLoginRateLimitService`. That one guards a secret, so only a wrong guess
 * is evidence of an attack; this one guards storage, and a successful write is
 * exactly the thing being abused.
 *
 * Two buckets. Creating a trip is the expensive one — it makes a new row — so
 * it is held far tighter than updating one, where the id is already a secret
 * the caller had to know and the row count does not move.
 *
 * Redis here rather than Postgres, deliberately, and it is the opposite of the
 * choice made for anything durable: this instance runs `allkeys-lru`, so a
 * counter may be evicted at any moment. For a limiter that means a window
 * occasionally resets early, which is a bounded and acceptable failure. For a
 * subscription or a trip it would mean silent data loss, which is why those are
 * in Postgres.
 */

const CREATE_PREFIX = "trip:create:ip:";
const UPDATE_PREFIX = "trip:update:ip:";

/** A person plans a handful of trips. A script makes thousands. */
const CREATE_WINDOW_SECONDS = 60 * 60;
const CREATE_MAX = 20;

/**
 * An update is one autosave. The planner debounces, but a visitor dragging
 * blocks around for an afternoon legitimately makes a lot of them, and a
 * limiter that cannot tell that from abuse gets in the way of the feature it is
 * protecting.
 */
const UPDATE_WINDOW_SECONDS = 60 * 60;
const UPDATE_MAX = 600;

export interface TripWriteVerdict {
  allowed: boolean;
  /** Seconds until the caller may try again; 0 when allowed. */
  retryAfterSeconds: number;
}

@Injectable()
export class TripWriteRateLimitService {
  private readonly logger = new Logger(TripWriteRateLimitService.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /**
   * Count this write and say whether it may proceed.
   *
   * Never throws. Redis being down must not take the planner's sync with it —
   * the endpoint is still bounded by the payload cap and by Cloudflare in front
   * of it, and refusing every write because a cache is unavailable trades a
   * bounded abuse risk for a certain outage.
   *
   * An absent address does not count. The alternative is one shared bucket for
   * everybody whose IP could not be read, which throttles the innocent majority
   * the first time anything upstream stops forwarding a header.
   */
  async check(
    ip: string | null,
    kind: "create" | "update",
  ): Promise<TripWriteVerdict> {
    if (!ip) return { allowed: true, retryAfterSeconds: 0 };

    const [prefix, max, window] =
      kind === "create"
        ? ([CREATE_PREFIX, CREATE_MAX, CREATE_WINDOW_SECONDS] as const)
        : ([UPDATE_PREFIX, UPDATE_MAX, UPDATE_WINDOW_SECONDS] as const);

    const key = prefix + hash(ip);
    try {
      const count = await this.redis.incr(key);
      // Only on the first increment, or a caller hammering the endpoint would
      // keep pushing its own window forward and never fall out of it.
      if (count === 1) await this.redis.expire(key, window);
      if (count <= max) return { allowed: true, retryAfterSeconds: 0 };

      const ttl = await this.redis.ttl(key);
      return { allowed: false, retryAfterSeconds: ttl > 0 ? ttl : window };
    } catch (error) {
      this.logger.warn(
        `Trip write limiter unavailable, allowing: ${(error as Error).message}`,
      );
      return { allowed: true, retryAfterSeconds: 0 };
    }
  }
}

/**
 * The address is hashed before it becomes a key.
 *
 * A Redis key list is dumped in a support ticket or a screenshot far more
 * casually than a database table is, and `trip:create:ip:1.2.3.4` next to a
 * timestamp is a visitor's location. The limiter only ever needs equality.
 */
function hash(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 32);
}
