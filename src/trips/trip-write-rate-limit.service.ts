import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  checkRateLimit,
  hashRateLimitKey,
  type RateLimitVerdict,
} from "../common/redis/rate-limit.util";

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

export type TripWriteVerdict = RateLimitVerdict;

@Injectable()
export class TripWriteRateLimitService {
  private readonly logger = new Logger(TripWriteRateLimitService.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /**
   * Count this write and say whether it may proceed.
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

    const [prefix, max, windowSeconds] =
      kind === "create"
        ? ([CREATE_PREFIX, CREATE_MAX, CREATE_WINDOW_SECONDS] as const)
        : ([UPDATE_PREFIX, UPDATE_MAX, UPDATE_WINDOW_SECONDS] as const);

    return checkRateLimit({
      redis: this.redis,
      logger: this.logger,
      label: "Trip write limiter",
      key: prefix + hashRateLimitKey(ip),
      max,
      windowSeconds,
    });
  }
}
