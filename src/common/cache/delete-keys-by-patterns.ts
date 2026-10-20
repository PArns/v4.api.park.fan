import type { Redis } from "ioredis";

/** Keys asked for per SCAN step; a hint to Redis, not a limit. */
const SCAN_COUNT = 1000;

/**
 * Deletes every key matching one of `patterns`, walking the keyspace with
 * SCAN instead of KEYS so Redis is not blocked for the length of a full
 * keyspace walk. Each SCAN batch is deleted in one pipeline as it arrives.
 *
 * SCAN may return a key more than once while the keyspace is rehashing, so
 * keys are de-duplicated and the returned count is the number of distinct
 * keys sent to DEL.
 */
export async function deleteKeysByPatterns(
  redis: Pick<Redis, "scan" | "pipeline">,
  patterns: readonly string[],
): Promise<number> {
  const seen = new Set<string>();

  for (const pattern of patterns) {
    let cursor = "0";
    do {
      const [next, batch] = await redis.scan(
        cursor,
        "MATCH",
        pattern,
        "COUNT",
        SCAN_COUNT,
      );
      cursor = next;

      const fresh = batch.filter((key) => !seen.has(key));
      if (fresh.length > 0) {
        const pipeline = redis.pipeline();
        for (const key of fresh) {
          seen.add(key);
          pipeline.del(key);
        }
        await pipeline.exec();
      }
    } while (cursor !== "0");
  }

  return seen.size;
}
