import { createHmac, randomBytes } from "crypto";
import type { Logger } from "@nestjs/common";
import { addIsoDays } from "./date.util";

/**
 * Helpers for the site statistic (`src/site-analytics/`), which counts readers
 * without storing who they are: a day is a UTC day, and every Redis key that
 * stands for a reader is an HMAC under a key derived per day from a secret
 * that lives in the environment, never in Redis.
 */

/**
 * Today's UTC date, `YYYY-MM-DD`. A reader's day is UTC, not a park's: readers
 * sit in every zone. The one documented exception to the park-timezone rule.
 */
export function utcDay(now: Date): string {
  return now.toISOString().slice(0, 10);
}

/**
 * Seconds until the UTC day of `now` ends, plus `graceSeconds`, so a key that
 * caps one day never expires before that day does.
 */
export function secondsUntilUtcDayEnds(now: Date, graceSeconds = 0): number {
  const endOfDay = Date.parse(`${addIsoDays(utcDay(now), 1)}T00:00:00Z`);
  return Math.ceil((endOfDay - now.getTime()) / 1000) + graceSeconds;
}

/**
 * The first of `names` that is set in the environment. Without one, a random
 * secret for this process and a warning: addresses stay out of Redis all the
 * same, only a restart or a second instance no longer recognises a reader.
 */
export function readerHashSecret(
  names: readonly string[],
  logger: Pick<Logger, "warn">,
  what: string,
): string {
  for (const name of names) {
    const value = process.env[name]?.trim();
    if (value) return value;
  }
  logger.warn(
    `${names.join(" / ")} not set; ${what} uses a per-process secret.`,
  );
  return randomBytes(32).toString("hex");
}

/**
 * HMAC-SHA256 under `secret` of the parts joined with `|`, as 32 hex
 * characters. Without the secret a key cannot be traced back to the address
 * and User-Agent in it; with it, only by testing a given pair.
 */
export function readerHash(secret: string, ...parts: string[]): string {
  return createHmac("sha256", secret)
    .update(parts.join("|"))
    .digest("hex")
    .slice(0, 32);
}

/**
 * The key one UTC day's reader hashes are made under: HMAC(secret, day). The
 * values are pseudonyms for that day only: the same reader's keys of two days
 * have nothing in common, so nothing in Redis links one day to the next.
 * Whoever holds the secret can still test a given address against a key of a
 * day that has not expired yet.
 */
export function dailyHashKey(secret: string, day: string): string {
  return createHmac("sha256", secret).update(`day|${day}`).digest("hex");
}
