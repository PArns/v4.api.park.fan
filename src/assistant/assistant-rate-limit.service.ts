import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  checkRateLimit,
  hashRateLimitKey,
  type RateLimitVerdict,
} from "../common/redis/rate-limit.util";
import { addressBucket } from "../common/utils/request.util";

/**
 * Parky's own limits per address, counted here for the reason the trip
 * limiter gives: the global throttler is a loose circuit breaker, and every
 * chat costs money at Anthropic. A chat is held in two windows, so a burst
 * and a whole day of questions are both bounded.
 */
const BUCKETS = {
  chatBurst: { max: 20, windowSeconds: 10 * 60 },
  chatDay: { max: 150, windowSeconds: 24 * 60 * 60 },
  feedback: { max: 5, windowSeconds: 60 * 60 },
  rating: { max: 60, windowSeconds: 60 * 60 },
  session: { max: 10, windowSeconds: 60 * 60 },
} as const;

type Bucket = keyof typeof BUCKETS;

@Injectable()
export class AssistantRateLimitService {
  private readonly logger = new Logger(AssistantRateLimitService.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /**
   * Counts the event in every bucket named and refuses when one is full. `who` is a session id
   * (`{ session }`) for chats, ratings and feedback, or an address (`{ ip }`) for issuing sessions,
   * where an IPv6 address counts as its /64.
   */
  async check(
    who: { session: string } | { ip: string | null },
    ...buckets: Bucket[]
  ): Promise<RateLimitVerdict> {
    const id =
      "session" in who
        ? `s:${who.session}`
        : who.ip
          ? `ip:${addressBucket(who.ip) ?? who.ip}`
          : null;
    // No address, no shared bucket: see TripWriteRateLimitService.check.
    if (!id) return { allowed: true, retryAfterSeconds: 0 };
    let verdict: RateLimitVerdict = { allowed: true, retryAfterSeconds: 0 };
    for (const bucket of buckets) {
      const { max, windowSeconds } = BUCKETS[bucket];
      const result = await checkRateLimit({
        redis: this.redis,
        logger: this.logger,
        label: "Assistant limiter",
        key: `assistant:${bucket}:${hashRateLimitKey(id)}`,
        max,
        windowSeconds,
      });
      if (
        !result.allowed &&
        result.retryAfterSeconds >= verdict.retryAfterSeconds
      ) {
        verdict = result;
      }
    }
    return verdict;
  }
}
