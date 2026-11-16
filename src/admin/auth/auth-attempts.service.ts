import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/** What somebody tried: signing in, or making a visitor or developer account. */
export type AuthAttemptKind = "login" | "register-user" | "register-developer";

/** How a sign-in ended; a success is split by the account's kind. */
export type LoginOutcome =
  | "success-staff"
  | "success-developer"
  | "success-user"
  | "wrong-credentials"
  | "totp-required"
  | "locked"
  | "rate-limited"
  | "turnstile-failed";

/** How a registration ended. */
export type RegisterOutcome =
  "created" | "closed" | "exists" | "turnstile-failed" | "failed";

/** One day's counts, keyed `<kind>:<outcome>`. */
export interface AuthAttemptDay {
  date: string;
  counts: Record<string, number>;
}

const PREFIX = "auth:attempts:";
const KEEP_DAYS = 90;

/**
 * Counts sign-ins and registrations per UTC day and outcome, so an owner sees how many try,
 * including at a registration that is closed. Only numbers: no address, no account, no network,
 * because a stranger's request writes them. A lost count is not worth failing a sign-in for, so
 * a Redis error is logged and swallowed.
 */
@Injectable()
export class AuthAttemptsService {
  private readonly logger = new Logger(AuthAttemptsService.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async count(kind: "login", outcome: LoginOutcome): Promise<void>;
  async count(
    kind: "register-user" | "register-developer",
    outcome: RegisterOutcome,
  ): Promise<void>;
  async count(kind: AuthAttemptKind, outcome: string): Promise<void> {
    const key = PREFIX + utcDate(new Date());
    try {
      await this.redis
        .multi()
        .hincrby(key, `${kind}:${outcome}`, 1)
        .expire(key, KEEP_DAYS * 86_400)
        .exec();
    } catch (error) {
      this.logger.warn(
        `Could not count ${kind}:${outcome}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  /** The last `days` UTC days, oldest first, each with its counts (empty when nothing happened). */
  async lastDays(days: number): Promise<AuthAttemptDay[]> {
    const span = Math.min(Math.max(Math.trunc(days), 1), KEEP_DAYS);
    const today = Date.now();
    const dates = Array.from({ length: span }, (_, index) =>
      utcDate(new Date(today - (span - 1 - index) * 86_400_000)),
    );
    const pipeline = this.redis.pipeline();
    for (const date of dates) pipeline.hgetall(PREFIX + date);
    const results = (await pipeline.exec()) ?? [];
    return dates.map((date, index) => {
      const raw = (results[index]?.[1] ?? {}) as Record<string, string>;
      return {
        date,
        counts: Object.fromEntries(
          Object.entries(raw).map(([field, value]) => [field, Number(value)]),
        ),
      };
    });
  }
}

function utcDate(moment: Date): string {
  return moment.toISOString().slice(0, 10);
}
