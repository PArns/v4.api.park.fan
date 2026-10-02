import type { QueueType } from "../external-apis/themeparks/themeparks.types";

/**
 * Redis keys QueueDataService caches under. They share the `parkfan:` root
 * with Bull (see queues/queue-registrations.ts), so the admin cache flush
 * names these two families explicitly instead of `parkfan:*`.
 */
export const QUEUE_LATEST_CACHE_PREFIX = "parkfan:queue:latest:";
export const ATTRACTION_TZ_CACHE_PREFIX = "parkfan:attraction:tz:";

export function latestQueueCacheKey(
  attractionId: string,
  queueType: QueueType,
): string {
  return `${QUEUE_LATEST_CACHE_PREFIX}${attractionId}:${queueType}`;
}

export function attractionTimezoneCacheKey(attractionId: string): string {
  return `${ATTRACTION_TZ_CACHE_PREFIX}${attractionId}`;
}
