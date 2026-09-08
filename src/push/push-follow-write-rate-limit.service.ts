import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { createHash } from "crypto";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * A limiter `ride-alerts` and `show-follows` own, rather than the global one —
 * same reasoning as `TripWriteRateLimitService`: `CfThrottlerGuard` skips any
 * request carrying a valid `THROTTLE_BYPASS_KEYS` value, and our own frontend
 * sends exactly that on every server-side call, so a `@Throttle()` here would
 * be enforced only against callers who are NOT the frontend — the inverse of
 * what is wanted.
 *
 * Two independent buckets, shared by both resources under one service so
 * neither duplicates the Redis-counter plumbing: abuse of one must not throttle
 * the other, since they are unrelated features that happen to share a write
 * shape (an upsert keyed by endpoint + a foreign id).
 */

const PREFIX: Record<"ride-alert" | "show-follow", string> = {
  "ride-alert": "push-follow:ride-alert:ip:",
  "show-follow": "push-follow:show-follow:ip:",
};

/** A person configures a handful of alerts. A script makes thousands. */
const WINDOW_SECONDS = 60 * 60;
const MAX = 60;

export interface PushFollowWriteVerdict {
  allowed: boolean;
  /** Seconds until the caller may try again; 0 when allowed. */
  retryAfterSeconds: number;
}

@Injectable()
export class PushFollowWriteRateLimitService {
  private readonly logger = new Logger(PushFollowWriteRateLimitService.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /**
   * Never throws. Redis being down must not take the feature down with it —
   * refusing every write because a cache is unavailable trades a bounded abuse
   * risk for a certain outage, the same trade `TripWriteRateLimitService` makes.
   */
  async check(
    ip: string | null,
    kind: "ride-alert" | "show-follow",
  ): Promise<PushFollowWriteVerdict> {
    if (!ip) return { allowed: true, retryAfterSeconds: 0 };

    const key = PREFIX[kind] + hash(ip);
    try {
      const count = await this.redis.incr(key);
      if (count === 1) await this.redis.expire(key, WINDOW_SECONDS);
      if (count <= MAX) return { allowed: true, retryAfterSeconds: 0 };

      const ttl = await this.redis.ttl(key);
      return {
        allowed: false,
        retryAfterSeconds: ttl > 0 ? ttl : WINDOW_SECONDS,
      };
    } catch (error) {
      this.logger.warn(
        `Push-follow write limiter unavailable, allowing: ${(error as Error).message}`,
      );
      return { allowed: true, retryAfterSeconds: 0 };
    }
  }
}

/** Same reasoning as `TripWriteRateLimitService`'s `hash` — never log a raw address. */
function hash(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 32);
}
