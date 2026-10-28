/**
 * Limits and Redis keys of the site statistic. See
 * docs/frontend/site-analytics.md.
 */

/**
 * The dimensions a page view is counted under, each a row per value, language
 * and UTC day in `site_traffic_days`. `entry` and `referrer` are written only
 * when a page view starts a visit.
 */
export const TRAFFIC_DIMS = [
  "path",
  "page_type",
  "entry",
  "referrer",
  "viewport",
  "browser",
  "os",
  "country",
  "locale",
] as const;
export type TrafficDim = (typeof TRAFFIC_DIMS)[number];

/** The dimensions that exist only on a visit's first page view. */
export const VISIT_START_DIMS: readonly TrafficDim[] = ["entry", "referrer"];

/**
 * How many hits one address (an IPv6 /64) may count per UTC day. Generous,
 * because a carrier gateway is many readers; a reader browsing all day sends a
 * few hundred.
 */
export const MAX_HITS_PER_ADDRESS_PER_DAY = 3000;

/** A visit ends after this long without a hit. */
export const VISIT_IDLE_SECONDS = 30 * 60;

/** "Active now": a hit within this many minutes, the current one included. */
export const REALTIME_ACTIVE_MINUTES = 5;

/** The page views per minute the overview shows, the current one included. */
export const REALTIME_SERIES_MINUTES = 30;

/**
 * How long a day's visitor HyperLogLog outlives the day: long enough for the
 * nightly snapshot to miss two runs and still find it.
 */
export const VISITOR_KEY_GRACE_SECONDS = 3 * 24 * 3600;

/** Rows older than this are pruned: a year and a month for the comparison. */
export const RETENTION_DAYS = 400;

/** The longest window the admin may ask for. */
export const MAX_ADMIN_WINDOW_DAYS = 365;

/** Visitors over all languages, beside one row per language. */
export const ALL_LOCALES_SCOPE = "all";

/** The country of an address GeoIP cannot place. */
export const UNKNOWN_COUNTRY = "zz";

/** The most entities `/events/:name` lists. */
export const MAX_EVENT_ENTITIES = 50;

/** The most values of an open (slug) property `/events/:name` lists. */
export const MAX_OPEN_PROP_VALUES = 100;

/**
 * Redis keys. Under `site-analytics:` and not `analytics:`, which the admin's
 * cache reset flushes (`PARK_CACHE_FLUSH_PATTERNS`).
 */
export const SITE_ANALYTICS_KEYS = {
  /** One address bucket's hits today: `INCRBY`. */
  cap: (hash: string) => `site-analytics:cap:${hash}`,
  /** One visit: a hash of what it has seen, sliding {@link VISIT_IDLE_SECONDS}. */
  visit: (hash: string) => `site-analytics:visit:${hash}`,
  /** One UTC day's visitors, all languages or one: a HyperLogLog. */
  visitors: (day: string, scope: string) => `site-analytics:uv:${day}:${scope}`,
  /** One minute's active visitors: a HyperLogLog. */
  activeMinute: (minute: string, scope: string) =>
    `site-analytics:rt:uv:${minute}:${scope}`,
  /** One minute's page views: a counter. */
  pageviewsMinute: (minute: string, scope: string) =>
    `site-analytics:rt:pv:${minute}:${scope}`,
} as const;

/** `YYYYMMDDHHmm` of an instant, UTC: the realtime keys' minute. */
export function minuteKey(at: Date): string {
  return at.toISOString().slice(0, 16).replace(/[-:T]/g, "");
}
