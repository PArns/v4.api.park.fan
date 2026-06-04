import { ThrottlerModuleOptions } from "@nestjs/throttler";

/**
 * Origin rate-limit configuration.
 *
 * This is a CIRCUIT BREAKER against pathological traffic (a buggy app
 * build stuck in a retry loop, a scraper), NOT a fairness limiter. It is
 * deliberately generous because:
 *
 *  - Most read traffic is absorbed by Cloudflare and never reaches the
 *    origin, so per-IP origin counts are already low.
 *  - Mobile clients sit behind carrier-grade NAT, so a single public IP
 *    (our throttle key) can represent hundreds of distinct users. A tight
 *    limit would punish legitimate users sharing a carrier gateway.
 *
 * Tunable via env; set THROTTLE_LIMIT=0 to disable entirely.
 *
 * NOTE: every value is read lazily (functions, not module-level consts).
 * `@nestjs/config` only assigns a .env file into `process.env` when
 * `ConfigModule.forRoot()` runs, which happens AFTER this file's imports
 * are evaluated. Reading at import time would miss .env-provided values
 * and silently fall back to defaults (e.g. dropping the frontend bypass).
 */
const getThrottleLimit = (): number =>
  parseInt(process.env.THROTTLE_LIMIT ?? "300", 10);

const getThrottleTtlSeconds = (): number =>
  parseInt(process.env.THROTTLE_TTL ?? "60", 10);

export const isThrottlingEnabled = (): boolean => getThrottleLimit() > 0;

export const getThrottlerOptions = (): ThrottlerModuleOptions => [
  {
    // @nestjs/throttler v6 expects the window in milliseconds.
    ttl: getThrottleTtlSeconds() * 1000,
    limit: getThrottleLimit(),
  },
];

/**
 * Bypass allow-list. A request carrying the bypass header with a value
 * matching one of the configured keys skips the rate limiter entirely.
 * This is how our own frontend opts out of the circuit breaker. The keys
 * are shared secrets — rotate them via env, never commit real values.
 * Bypass is disabled when no keys are configured.
 */
export const getThrottleBypassHeader = (): string =>
  (process.env.THROTTLE_BYPASS_HEADER ?? "x-auth-key").toLowerCase();

export const getThrottleBypassKeys = (): string[] =>
  (process.env.THROTTLE_BYPASS_KEYS ?? "")
    .split(",")
    .map((key) => key.trim())
    .filter((key) => key.length > 0);
