import { Redis } from "ioredis";
import { CacheKeys } from "./cache-keys";

/**
 * Delete every park-scoped cache entry for one park: the exact keys first,
 * then the glob patterns (the schedule keys have the parkId as their THIRD
 * segment — schedule:today:<parkId>:<date> — so a naive
 * `schedule:${parkId}:*` pattern matches nothing; see CacheKeys).
 *
 * Covers the integrated/schedule/calendar caches AND the analytics-derived
 * caches (statistics, P50/P90 + typical-day-peak baselines, headliners,
 * crowd levels, historical stats, derived hours, ML predictions) plus the
 * global discovery geo-structure skeleton — all of which a park merge/repair
 * invalidates. Before this they lingered on TTL (up to 48h) and served
 * pre-merge data.
 *
 * Pass the winner park's current attraction IDs after a merge so the
 * migrated attractions' integrated/baseline caches (which embed park context)
 * are evicted too; they are otherwise unreachable by a park-scoped glob.
 *
 * Shared by park merge and park repair so the two invalidation paths can't
 * drift apart again. Callers own error handling/logging.
 */
export async function invalidateParkCaches(
  redis: Redis,
  parkId: string,
  attractionIds: string[] = [],
): Promise<void> {
  const exactKeys = [
    CacheKeys.parkIntegrated(parkId),
    CacheKeys.parkOpDateRange(parkId),
    CacheKeys.parkStatistics(parkId),
    CacheKeys.parkP50(parkId),
    CacheKeys.parkP90(parkId),
    CacheKeys.parkTypicalDayPeak(parkId),
    CacheKeys.parkTypicalPeakHour(parkId),
    CacheKeys.parkIsSeasonal(parkId),
    CacheKeys.analyticsHeadliners(parkId),
    // The geo skeleton lists every park; a merge/repair changes it.
    CacheKeys.discoveryGeoStructure(),
  ];

  // Migrated attractions keep their IDs but their integrated payload embeds
  // the old park context; their baselines are cheap to recompute.
  for (const id of attractionIds) {
    exactKeys.push(
      CacheKeys.attractionIntegrated(id),
      CacheKeys.attractionP50(id),
      CacheKeys.attractionP90(id),
      CacheKeys.attractionRopeDrop(id),
    );
  }

  // redis.del accepts many keys, but cap the batch so a park with hundreds of
  // attractions doesn't build one oversized command.
  for (let i = 0; i < exactKeys.length; i += 250) {
    await redis.del(...exactKeys.slice(i, i + 250));
  }

  const patterns = [
    CacheKeys.scheduleParkPattern(parkId),
    CacheKeys.calendarMonthPattern(parkId),
    CacheKeys.parkCrowdLevelPattern(parkId),
    CacheKeys.parkHistoricalStatsPattern(parkId),
    CacheKeys.parkDerivedHoursPattern(parkId),
    CacheKeys.mlParkPattern(parkId),
    ...attractionIds.map((id) => CacheKeys.attractionHistoryPattern(id)),
  ];
  for (const pattern of patterns) {
    const keys = await redis.keys(pattern);
    if (keys.length > 0) await redis.del(...keys);
  }
}
