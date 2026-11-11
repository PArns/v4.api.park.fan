import type { Redis } from "ioredis";
import { safeJsonParse } from "../utils/json.util";
import { SingleFlight } from "../utils/single-flight.util";

/** One per process: concurrent misses on the same key share one build. */
const flights = new SingleFlight();

/**
 * Answers from Redis when `key` holds a value, otherwise computes it and keeps
 * it for `ttlSeconds`. `keep` decides whether a computed value may be stored;
 * an answer that says "try again shortly" must not be.
 *
 * Concurrent misses on the same key in this process run `compute` once and
 * share its result, so an expiring entry under load costs one rebuild, not
 * one per waiting request. A Redis failure in either direction, or an entry
 * that does not parse, falls back to computing: the cache saves work, it
 * never decides whether a request is answered.
 */
export async function cachedJson<T>(
  redis: Redis,
  key: string,
  ttlSeconds: number,
  compute: () => Promise<T>,
  keep: (value: T) => boolean = () => true,
): Promise<T> {
  // Wrapped so a client that throws instead of rejecting is a miss too.
  const hit = safeJsonParse<T>(
    await Promise.resolve()
      .then(() => redis.get(key))
      .catch(() => null),
  );
  if (hit !== null) return hit;
  return flights.run(key, async () => {
    const value = await compute();
    if (value !== null && value !== undefined && keep(value)) {
      await Promise.resolve()
        .then(() =>
          redis.set(
            key,
            JSON.stringify(value),
            "EX",
            Math.max(1, Math.floor(ttlSeconds)),
          ),
        )
        .catch(() => undefined);
    }
    return value;
  });
}
