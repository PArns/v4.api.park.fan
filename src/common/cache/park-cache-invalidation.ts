import { Redis } from "ioredis";
import { CacheKeys } from "./cache-keys";

/**
 * Delete every park-scoped cache entry for one park: the exact keys first,
 * then the glob patterns (the schedule keys have the parkId as their THIRD
 * segment — schedule:today:<parkId>:<date> — so a naive
 * `schedule:${parkId}:*` pattern matches nothing; see CacheKeys).
 *
 * Shared by park merge and park repair so the two invalidation paths can't
 * drift apart again. Callers own error handling/logging.
 */
export async function invalidateParkCaches(
  redis: Redis,
  parkId: string,
): Promise<void> {
  await redis.del(
    CacheKeys.parkIntegrated(parkId),
    CacheKeys.parkOpDateRange(parkId),
  );

  const patterns = [
    CacheKeys.scheduleParkPattern(parkId),
    CacheKeys.calendarMonthPattern(parkId),
  ];
  for (const pattern of patterns) {
    const keys = await redis.keys(pattern);
    if (keys.length > 0) await redis.del(...keys);
  }
}
