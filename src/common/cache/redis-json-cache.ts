import type { Redis } from "ioredis";

/**
 * Answers from Redis when `key` holds a value, otherwise computes it and keeps
 * it for `ttlSeconds`. `keep` decides whether a computed value may be stored;
 * an answer that says "try again shortly" must not be.
 *
 * A Redis failure in either direction falls back to computing: the cache saves
 * work, it never decides whether a request is answered.
 */
export async function cachedJson<T>(
  redis: Redis,
  key: string,
  ttlSeconds: number,
  compute: () => Promise<T>,
  keep: (value: T) => boolean = () => true,
): Promise<T> {
  const hit = await redis.get(key).catch(() => null);
  if (hit) return JSON.parse(hit) as T;
  const value = await compute();
  if (value !== null && value !== undefined && keep(value)) {
    await redis
      .set(key, JSON.stringify(value), "EX", ttlSeconds)
      .catch(() => undefined);
  }
  return value;
}
