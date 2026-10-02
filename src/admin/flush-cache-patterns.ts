import {
  ATTRACTION_TZ_CACHE_PREFIX,
  QUEUE_LATEST_CACHE_PREFIX,
} from "../queue-data/queue-data-cache-keys";

/**
 * Key patterns (SCAN MATCH globs) that `POST /v1/admin/flush-cache` and
 * `POST /v1/admin/cache/reset` delete. Every entry must match a
 * prefix that is actually written somewhere (see CacheKeys + inline keys).
 *
 * Nothing here may match a Bull key. Bull stores every queue under
 * `<BULL_PREFIX>:<queue>:*` (default prefix `parkfan`) in the same Redis, so a
 * bare `parkfan:*` deletes every job hash, `wait`, `delayed` and `repeat` set
 * and no cron runs again until the API restarts. The spec checks each pattern
 * against every registered queue.
 *
 * Deliberately NOT flushed (state, not cache): popularity:* (ranking),
 * downtime:* (open downtime tracking), prediction:deviation:* (deviation
 * tracking), ratelimit:* (circuit breakers), ml:accuracy:* and
 * ml:last-accuracy-check (job markers).
 */
export const PARK_CACHE_FLUSH_PATTERNS: readonly string[] = [
  "schedule:*",
  "park:*", // integrated, occupancy, statistics, baselines, …
  "attraction:*", // integrated, history, baselines, ropedrop, last-seen
  "calendar:*", // month caches + refresh-check markers
  "analytics:*",
  "holiday:*",
  "weather:*", // forecasts + sync:done markers (flush ⇒ next sync re-runs)
  "search:*",
  "discovery:*",
  "favorites:*",
  `${QUEUE_LATEST_CACHE_PREFIX}*`, // latest queue snapshot per attraction
  `${ATTRACTION_TZ_CACHE_PREFIX}*`, // attraction → park timezone
  "accuracy:*", // prediction accuracy badges
  "ml:park:*", // serving predictions (daily/hourly/yearly)
  "ml:tft-daily:*",
  "ml:active-attractions:*",
  "ml:dashboard:*", // ML dashboard snapshot (5min cache)
  "location:*", // /nearby shared park-coordinate index
];
