import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  checkRateLimit,
  hashRateLimitKey,
  type RateLimitVerdict,
} from "../common/redis/rate-limit.util";

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

// `subscription` is `POST /v1/push/subscriptions`, the row the other two hang off.
const PREFIX = {
  "ride-alert": "push-follow:ride-alert:ip:",
  "show-follow": "push-follow:show-follow:ip:",
  subscription: "push-follow:subscription:ip:",
  // `POST /v1/push/devices/test`: one bucket per address, one per install.
  "push-test": "push-test:ip:",
  "push-test-install": "push-test:install:",
} as const;

export type PushWriteKind = keyof typeof PREFIX;

/** A person configures a handful of alerts. A script makes thousands. */
const WINDOW_SECONDS = 60 * 60;
const MAX = 60;
/** A tester runs every kind twice an hour; each one is a real push to Apple. */
const MAX_BY_KIND: Partial<Record<PushWriteKind, number>> = {
  "push-test-install": 20,
};

export type PushFollowWriteVerdict = RateLimitVerdict;

@Injectable()
export class PushFollowWriteRateLimitService {
  private readonly logger = new Logger(PushFollowWriteRateLimitService.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /** `subject` is the address, or the install id for `push-test-install`. */
  async check(
    subject: string | null,
    kind: PushWriteKind,
  ): Promise<PushFollowWriteVerdict> {
    if (!subject) return { allowed: true, retryAfterSeconds: 0 };

    return checkRateLimit({
      redis: this.redis,
      logger: this.logger,
      label: "Push-follow write limiter",
      key: PREFIX[kind] + hashRateLimitKey(subject),
      max: MAX_BY_KIND[kind] ?? MAX,
      windowSeconds: WINDOW_SECONDS,
    });
  }
}
