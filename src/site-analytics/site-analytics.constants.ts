/**
 * Limits, dimensions and Redis keys of the site statistic. See
 * docs/frontend/site-analytics.md.
 */
import {
  PAGE_VIEW_LOCALES,
  type PageViewLocale,
} from "../page-views/page-view.constants";

/** The site's six languages, the same list the page-view rankings use. */
export const SITE_LOCALES = PAGE_VIEW_LOCALES;
export type SiteLocale = PageViewLocale;

/** Below 768, 768–1023 and from 1024 CSS pixels of `window.innerWidth`. */
export const SITE_VIEWPORTS = ["mobile", "tablet", "desktop"] as const;

/**
 * The dimensions every stored row carries, page view and event alike, and the
 * ones a report can filter on (`filter=<dim>:<value>`). `entry`, `referrer`,
 * `channel` and `campaign` belong to the visit and are copied onto each of its
 * rows from the visit's Redis key, so a page breakdown can be filtered by
 * where its visits came from.
 */
export const FILTER_DIMS = [
  "path",
  "page_type",
  "entry",
  "referrer",
  "channel",
  "campaign",
  "country",
  "viewport",
  "browser",
  "os",
  "locale",
] as const;
export type FilterDim = (typeof FILTER_DIMS)[number];

/** The dimensions `breakdown` groups by: the filter dimensions. */
export const BREAKDOWN_DIMS = FILTER_DIMS;
export type BreakdownDim = FilterDim;

/**
 * The dimensions that describe how a visit began rather than a page view:
 * their breakdown counts visits started (a view is a visit), and carries a
 * bounce rate.
 */
export const VISIT_DIMS: readonly BreakdownDim[] = [
  "entry",
  "referrer",
  "channel",
  "campaign",
];

/** Where a visit came from, derived from its referrer (`referrer.util.ts`). */
export const CHANNELS = [
  "search",
  "ai",
  "social",
  "referral",
  "campaign",
  "direct",
] as const;
export type Channel = (typeof CHANNELS)[number];

/** `pageType` of the not-found page, which the SEO report lists. */
export const NOT_FOUND_PAGE_TYPE = "not_found";

/** The page type of a hit whose type is past {@link MAX_PAGE_TYPES_PER_DAY}. */
export const OVERFLOW_PAGE_TYPE = "other";

/**
 * The event the API knows by name: a counted read of a post, park or ride,
 * which feeds the public "most read" rankings (`/v1/page-views/top`). It keeps
 * the page-view counter's own guards; see `site-analytics.hits.ts`.
 */
export const PAGE_READ_EVENT = "page_read";

/** The props an over-limit event keeps instead of its own. */
export const OVERFLOW_PROPS: Readonly<Record<string, string>> = {
  _overflow: "1",
};

/**
 * How many hits one address (an IPv6 /64) may count per UTC day. Generous,
 * because a carrier gateway is many readers; one reader browsing all day sends
 * a few hundred.
 */
export const MAX_HITS_PER_ADDRESS_PER_DAY = 3000;

/**
 * How many distinct event names the site may introduce per UTC day. The site
 * sends about forty; this is what keeps a script from creating a name per
 * request. A new name past it is dropped.
 */
export const MAX_EVENT_NAMES_PER_DAY = 300;

/**
 * How many distinct property combinations (with the entity) one event may
 * reach per UTC day. Past it the hit still counts, under
 * {@link OVERFLOW_PROPS} and without its entity, so the total stays right and
 * the table stops growing. `page_read` is exempt: its keys are checked one by
 * one, and every park and ride is a combination of its own.
 */
export const MAX_PROP_COMBINATIONS_PER_EVENT_PER_DAY = 2000;

/** How many distinct page types per UTC day; past it a hit is `other`. */
export const MAX_PAGE_TYPES_PER_DAY = 100;

/** The most properties one event keeps, in key order. */
export const MAX_EVENT_PROPS = 8;

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

/** The length of one flush window, and so of one stored bucket. */
export const FLUSH_WINDOW_SECONDS = 5 * 60;

/**
 * Chunks wholly older than this are dropped by the nightly job: a year and a
 * month, so a year compares with the one before.
 */
export const RETENTION_DAYS = 400;

/** The longest window the admin may ask for. */
export const MAX_ADMIN_WINDOW_DAYS = 365;

/** Visitors over all languages, beside one row per language. */
export const ALL_LOCALES_SCOPE = "all";

/** The country of an address GeoIP cannot place. */
export const UNKNOWN_COUNTRY = "zz";

/** The most values per property, entities and combinations `/events/:name` lists. */
export const MAX_EVENT_DETAIL_ITEMS = 50;

/** The most rows and columns a crosstab keeps; the rest is `other`. */
export const MAX_CROSSTAB_SIDE = 30;

/** The fewest visits (window and previous together) a page needs to rank as a trend. */
export const MIN_TREND_VISITS = 10;

/** How many rising and falling pages the trends report lists. */
export const TREND_ITEMS = 25;

/**
 * Redis keys. Under `site-analytics:` and not `analytics:`, which the admin's
 * cache reset flushes (`PARK_CACHE_FLUSH_PATTERNS`).
 */
export const SITE_ANALYTICS_KEYS = {
  /** One address bucket's hits today: `INCRBY`. */
  cap: (hash: string) => `site-analytics:cap:${hash}`,
  /** One visit: its first page view's dimensions and its page-view count. */
  visit: (hash: string) => `site-analytics:visit:${hash}`,
  /** One reader's counted reads of one page today (`page_read`). */
  read: (hash: string) => `site-analytics:read:${hash}`,
  /** One address bucket's counted reads today (`page_read`). */
  readAddress: (hash: string) => `site-analytics:read-addr:${hash}`,
  /** The event names seen today. */
  names: (day: string) => `site-analytics:names:${day}`,
  /** The page types seen today. */
  pageTypes: (day: string) => `site-analytics:page-types:${day}`,
  /** One event's property combinations seen today. */
  combinations: (day: string, name: string) =>
    `site-analytics:combos:${day}:${name}`,
  /** One UTC day's visitors, all languages or one: a HyperLogLog. */
  visitors: (day: string, scope: string) => `site-analytics:uv:${day}:${scope}`,
  /** One UTC hour's visitors, for the hourly view of today. */
  hourVisitors: (hour: string, scope: string) =>
    `site-analytics:uvh:${hour}:${scope}`,
  /** One minute's active visitors: a HyperLogLog. */
  activeMinute: (minute: string, scope: string) =>
    `site-analytics:rt:uv:${minute}:${scope}`,
  /** One minute's page views: a counter. */
  pageviewsMinute: (minute: string, scope: string) =>
    `site-analytics:rt:pv:${minute}:${scope}`,
  /**
   * One flush window's counts: a hash of row → count. No TTL on purpose:
   * this Redis runs `volatile-lru`, which only evicts keys that have one.
   */
  buffer: (window: string) => `site-analytics:buf:${window}`,
  /** A window the flusher has claimed and not yet written. */
  flushing: (window: string) => `site-analytics:buf:${window}:flushing`,
  /** Every window with a buffer, written or claimed. */
  windows: "site-analytics:buf:windows",
  /** Held by the one instance that is flushing. */
  flushLock: "site-analytics:flush-lock",
} as const;

/** `YYYYMMDDHHmm` of an instant, UTC: the realtime keys' minute. */
export function minuteKey(at: Date): string {
  return at.toISOString().slice(0, 16).replace(/[-:T]/g, "");
}

/** `YYYYMMDDHH` of an instant, UTC. */
export function hourKey(at: Date): string {
  return minuteKey(at).slice(0, 10);
}

/** The start of the flush window an instant falls in, as an ISO string. */
export function flushWindowOf(at: Date): string {
  const ms = FLUSH_WINDOW_SECONDS * 1000;
  return new Date(Math.floor(at.getTime() / ms) * ms).toISOString();
}
