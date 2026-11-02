import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  checkRateLimit,
  hashRateLimitKey,
  incrementWithWindow,
  type RateLimitVerdict,
} from "../common/redis/rate-limit.util";
import { addressBucket } from "../common/utils/request.util";

/**
 * Parky's own limits per address, counted here for the reason the trip
 * limiter gives: the global throttler is a loose circuit breaker, and every
 * chat costs money at Anthropic. A chat is held in three windows, so rapid
 * fire, a long burst and a whole day of questions are all bounded. Three in
 * thirty seconds leaves room for a quick "yes" to an offer Parky made.
 */
const BUCKETS = {
  chatShort: { max: 3, windowSeconds: 30 },
  chatBurst: { max: 20, windowSeconds: 10 * 60 },
  chatDay: { max: 150, windowSeconds: 24 * 60 * 60 },
  feedback: { max: 5, windowSeconds: 60 * 60 },
  rating: { max: 60, windowSeconds: 60 * 60 },
  session: { max: 10, windowSeconds: 60 * 60 },
} as const;

type Bucket = keyof typeof BUCKETS;

/** Junk messages in a row that earn a session its pause, and how long they are remembered. */
const JUNK_STRIKES = { max: 3, windowSeconds: 10 * 60 } as const;

/** The pause a session gets for {@link JUNK_STRIKES} junk messages in a row. */
export const JUNK_COOLDOWN_SECONDS = 10 * 60;

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

  /** Seconds left of a session's pause for junk, 0 when it has none. */
  async cooldown(session: string): Promise<number> {
    try {
      const left = await this.redis.ttl(this.key("cooldown", session));
      return left > 0 ? left : 0;
    } catch (error) {
      this.logger.warn(`Assistant cooldown unreadable: ${String(error)}`);
      return 0;
    }
  }

  /**
   * Counts a junk message. The one that makes {@link JUNK_STRIKES} in a row
   * starts the pause and returns its length; before that it returns 0.
   */
  async strike(session: string): Promise<number> {
    try {
      const strikes = this.key("junk", session);
      const count = await incrementWithWindow(
        this.redis,
        strikes,
        JUNK_STRIKES.windowSeconds,
      );
      if (count < JUNK_STRIKES.max) return 0;
      await this.redis.del(strikes);
      await this.redis.set(
        this.key("cooldown", session),
        "1",
        "EX",
        JUNK_COOLDOWN_SECONDS,
      );
      return JUNK_COOLDOWN_SECONDS;
    } catch (error) {
      this.logger.warn(`Assistant strike not counted: ${String(error)}`);
      return 0;
    }
  }

  /** A real question ends the run of junk: the strikes only count in a row. */
  async clearStrikes(session: string): Promise<void> {
    try {
      await this.redis.del(this.key("junk", session));
    } catch (error) {
      this.logger.warn(`Assistant strikes not cleared: ${String(error)}`);
    }
  }

  private key(kind: "junk" | "cooldown", session: string): string {
    return `assistant:${kind}:${hashRateLimitKey(`s:${session}`)}`;
  }
}
