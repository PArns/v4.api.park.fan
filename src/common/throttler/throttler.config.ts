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
 */
export const THROTTLE_TTL_SECONDS = parseInt(
  process.env.THROTTLE_TTL ?? "60",
  10,
);

export const THROTTLE_LIMIT = parseInt(process.env.THROTTLE_LIMIT ?? "300", 10);

export const isThrottlingEnabled = (): boolean => THROTTLE_LIMIT > 0;

export const throttlerOptions: ThrottlerModuleOptions = [
  {
    // @nestjs/throttler v6 expects the window in milliseconds.
    ttl: THROTTLE_TTL_SECONDS * 1000,
    limit: THROTTLE_LIMIT,
  },
];
