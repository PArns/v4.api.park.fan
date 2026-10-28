/**
 * Limits, dimensions and Redis keys of the site statistic. See
 * docs/frontend/site-analytics.md.
 */
import type { SITE_ANALYTICS_HYPERTABLES } from "../database/hypertables";
import {
  PAGE_VIEW_LOCALES,
  type PageViewLocale,
} from "../page-views/page-view.constants";

/** The site's six languages, the same list the page-view rankings use. */
export const SITE_LOCALES = PAGE_VIEW_LOCALES;
export type SiteLocale = PageViewLocale;

/**
 * The device classes, from the request's User-Agent on the server
 * (`deviceClassOf`); the browser sends nothing about its screen.
 */
export const SITE_DEVICES = ["mobile", "tablet", "desktop"] as const;
export type SiteDevice = (typeof SITE_DEVICES)[number];

/**
 * The dimensions of an event's row, which every other row carries too: the
 * page, and the device class, browser, OS and country of its reader.
 */
export const EVENT_DIMS = [
  "path",
  "page_type",
  "country",
  "device",
  "browser",
  "os",
  "locale",
] as const;

/**
 * The dimensions of a page view: the event's and the browser's language,
 * which only `site_pageviews` stores.
 */
export const PAGE_DIMS = [...EVENT_DIMS, "browser_language"] as const;

/**
 * The dimensions that describe how a visit began rather than a page view.
 * They are stored only on the rows that record a visit (its start with its
 * `engaged` and its seconds, and its exit), never on a page view's or an
 * event's row, so a page in the middle of a visit cannot be joined to where
 * its reader came from. A breakdown by one, or under a filter on one, counts
 * visits; a report that counts page views or events refuses a filter on one.
 */
export const VISIT_DIMS = ["entry", "referrer", "channel"] as const;
export type VisitDim = (typeof VISIT_DIMS)[number];

/**
 * The dimensions a report can filter on (`filter=<dim>:<value>`): the page's
 * and the visit's.
 */
export const FILTER_DIMS = [...PAGE_DIMS, ...VISIT_DIMS] as const;
export type FilterDim = (typeof FILTER_DIMS)[number];

/**
 * The dimensions `breakdown` groups by: the filter dimensions, and `exit`,
 * the last page of the visits that ended (from their exit rows).
 */
export const BREAKDOWN_DIMS = [...FILTER_DIMS, "exit"] as const;
export type BreakdownDim = (typeof BREAKDOWN_DIMS)[number];

/** Whether a dimension is the visit's ({@link VISIT_DIMS}, and `exit`). */
export function isVisitDim(dim: BreakdownDim): dim is VisitDim | "exit" {
  return dim === "exit" || (VISIT_DIMS as readonly string[]).includes(dim);
}

/**
 * The dimensions whose visitors are counted on their own, one HyperLogLog per
 * day, language and value: a reader's country, device, browser and OS never
 * change within a day (the reader is the address and the User-Agent), and a
 * visit's channel is the one it started with. Closed lists, bar the country's
 * two letters, so the keys stay few. A report gives visitors under one value
 * of one of these (`visitorBasisOf`).
 */
export const VISITOR_DIMS = [
  "country",
  "device",
  "browser",
  "os",
  "channel",
] as const satisfies readonly FilterDim[];
export type VisitorDim = (typeof VISITOR_DIMS)[number];

/** A report's series step. */
export const INTERVALS = ["hour", "day", "week", "month"] as const;
export type SiteInterval = (typeof INTERVALS)[number];

/** The longest window that may be drawn by the hour. */
export const MAX_HOURLY_DAYS = 2;

/** The longest `from`–`to` window. */
export const MAX_RANGE_DAYS = 366;

/** The browser language of a request whose `Accept-Language` names none. */
export const UNKNOWN_BROWSER_LANGUAGE = "zz";

/**
 * The most foreground seconds one `engagement` hit may report: half an hour,
 * the idle time after which the visit would have ended anyway.
 */
export const MAX_ENGAGEMENT_SECONDS = 1800;

/** Where a visit came from, derived from its referrer (`referrer.util.ts`). */
export const CHANNELS = [
  "search",
  "ai",
  "social",
  "referral",
  "direct",
] as const;
export type Channel = (typeof CHANNELS)[number];

/** `pageType` of the not-found page, which the SEO report lists. */
export const NOT_FOUND_PAGE_TYPE = "not_found";

/** The page type a hit is stored under past the day's page-type guard. */
export const OVERFLOW_PAGE_TYPE = "other";

/**
 * The path or referrer a hit is stored under when its own is past the day's
 * guard: the hit still counts, the value is not kept.
 */
export const OVERFLOW_VALUE = "(other)";

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
 * Events whose entity would say where the reader is: the parks near them and
 * the in-park compass. Their `entity` is dropped on the way in, whatever the
 * client sends; a trailing `*` matches a prefix. The props stay.
 */
export const LOCATION_DERIVED_EVENTS: readonly string[] = [
  "nearby_parks_loaded",
  "compass_*",
];

/** Whether an event's entity would tell where its reader is. */
export function isLocationDerivedEvent(name: string): boolean {
  return LOCATION_DERIVED_EVENTS.some((entry) =>
    entry.endsWith("*") ? name.startsWith(entry.slice(0, -1)) : name === entry,
  );
}

/**
 * The IPv6 prefix an address cap counts by. Providers delegate a /56 (or a
 * /48) to one customer, who can take a new /64 per request; capped per /64,
 * one customer would have 256 budgets, per /56 it has one. A reader (the
 * visit, the visitor, three reads of one page) stays a /64 and a User-Agent.
 */
export const ADDRESS_CAP_IPV6_PREFIX = 56;

/**
 * How many hits one address may count per UTC day. Generous, because a
 * carrier gateway is many readers; one reader browsing all day sends a few
 * hundred.
 */
export const MAX_HITS_PER_ADDRESS_PER_DAY = 3000;

/**
 * How many hits all readers together may count per five-minute window: a
 * few hundred times today's busiest window. Past it hits are dropped and
 * counted as dropped, so a flood costs Redis and Postgres a bounded amount.
 */
export const MAX_HITS_PER_WINDOW = 5000;

/**
 * The day's guards: distinct values the statistic accepts per UTC day, and
 * how many of them one address may be the first to send. Each is a Redis set
 * a value joins on first sight; a value already in yesterday's set joins
 * without using the address's budget, and up to {@link KNOWN_MEMBER_HEADROOM}
 * times the limit, so whoever fills a set just after midnight cannot lock out
 * the values the site really sends. Past a guard a hit is dropped (a name) or
 * kept under {@link OVERFLOW_VALUE}, {@link OVERFLOW_PAGE_TYPE} or
 * {@link OVERFLOW_PROPS}.
 *
 * - `names`: event names; the site sends about forty. `page_read` is exempt.
 * - `pageTypes`: kinds of page.
 * - `combinations`: one event's property combinations with its entity;
 *   `page_read` is exempt, its keys are checked one by one.
 * - `paths`: the site has some 40,000 pages in six languages.
 * - `referrers`: referring sites outside the known search engines, AI
 *   assistants and social networks.
 * - `pageKeys`: the keys of read posts and news items, which are only
 *   pattern-checked.
 */
export const SITE_GUARDS = {
  names: { limit: 300, perAddress: 20 },
  pageTypes: { limit: 100, perAddress: 10 },
  combinations: { limit: 2000, perAddress: 100 },
  paths: { limit: 20000, perAddress: 200 },
  referrers: { limit: 1000, perAddress: 10 },
  pageKeys: { limit: 5000, perAddress: 200 },
} as const;
export type SiteGuard = keyof typeof SITE_GUARDS;

/** How far past its limit a set takes values it knew yesterday. */
export const KNOWN_MEMBER_HEADROOM = 2;

/**
 * Why a hit was dropped, or which value of it was stored under an overflow
 * value: the fields of the day's drop counts (`SITE_ANALYTICS_KEYS.drops`),
 * which the system-health report shows.
 */
export const DROP_REASONS = {
  /** Past {@link MAX_HITS_PER_ADDRESS_PER_DAY}. */
  address: "dropped:address",
  /** Past {@link MAX_HITS_PER_WINDOW}. */
  window: "dropped:window",
  /** An event name past the names guard. */
  name: "dropped:name",
  /** A `page_read` past one of its caps. */
  read: "dropped:read",
  /** A `page_read` of a park or ride that does not exist. */
  entity: "dropped:entity",
  /** Redis was away and this instance's fallback buffer was full. */
  fallback: "dropped:fallback",
  /** A visit whose key expired before a flush run could count its exit. */
  exit: "dropped:exit",
  pageType: "overflow:page_type",
  path: "overflow:path",
  referrer: "overflow:referrer",
  combination: "overflow:combination",
} as const;
export type DropReason = (typeof DROP_REASONS)[keyof typeof DROP_REASONS];

/** The most properties one event keeps, in key order. */
export const MAX_EVENT_PROPS = 8;

/** A visit ends after this long without a hit. */
export const VISIT_IDLE_SECONDS = 30 * 60;

/**
 * How long a visit's key outlives its last hit: the idle time and two flush
 * runs, so the run that closes the visit and counts its exit still finds it.
 */
export const VISIT_KEY_TTL_SECONDS = 40 * 60;

/**
 * How long a visit's place among the open ones outlasts its last hit when no
 * flush run took it: the collector drops older places, so with the flusher
 * stopped no pseudonym waits there longer.
 */
export const OPEN_VISIT_MAX_AGE_SECONDS = 2 * 3600;

/** How many idle visits one flush run closes at most; the rest wait. */
export const MAX_VISITS_CLOSED_PER_RUN = 20000;

/**
 * The live view's lists: pages, countries and devices over the last few
 * minutes, channels, referrers and events over the last half hour, the
 * longest ones cut.
 */
export const LIVE_SHORT_MINUTES = 5;
export const LIVE_LONG_MINUTES = 30;
export const LIVE_TOP_PAGES = 10;
export const LIVE_TOP_REFERRERS = 10;
export const LIVE_TOP_EVENTS = 15;

/**
 * A live key, and a minute's visitors and page views, outlive the longest
 * list that reads them by a few minutes.
 */
export const LIVE_KEY_TTL_SECONDS = (LIVE_LONG_MINUTES + 5) * 60;

/** What a live hash counts, per minute and language. */
export const LIVE_LISTS = [
  "pages",
  "countries",
  "channels",
  "referrers",
  "devices",
  "events",
] as const;
export type LiveList = (typeof LIVE_LISTS)[number];

/** "Active now": a hit within this many minutes, the current one included. */
export const REALTIME_ACTIVE_MINUTES = 5;

/** The page views per minute the overview shows, the current one included. */
export const REALTIME_SERIES_MINUTES = 30;

/**
 * How long a day's visitor HyperLogLog, and each of its hours' and
 * dimension values', outlives the day: the hour in which the snapshot runs four times, so no key outlives its
 * day by more and none is older than 25 hours.
 */
export const VISITOR_KEY_GRACE_SECONDS = 3600;

/** The length of one flush window: how often counts move to Postgres. */
export const FLUSH_WINDOW_SECONDS = 5 * 60;

/**
 * What a stored row's `bucket` is: the UTC hour. A window's rows carry their
 * hour, so one hour holds up to twelve rows per combination, summed when read.
 */
export const BUCKET_SECONDS = 3600;

/**
 * Reports over more days than this have no `previous`: the window before
 * would lie mostly past {@link DAILY_RETENTION_DAYS}.
 */
export const MAX_COMPARABLE_DAYS = 180;

/**
 * Counts one instance keeps in memory while Redis is away, written to
 * Postgres every {@link FALLBACK_DRAIN_MS}; past it, hits are dropped and
 * counted.
 */
export const FALLBACK_MAX_FIELDS = 5000;
export const FALLBACK_DRAIN_MS = 60 * 1000;

/**
 * How often the flusher retries a window that fails before it sets the
 * window aside (an hour of runs), and how long a set-aside window is kept for
 * an admin to look at.
 */
export const MAX_FLUSH_ATTEMPTS = 12;
export const PARKED_WINDOW_TTL_SECONDS = 7 * 24 * 3600;

/**
 * The days the hourly rows (`site_pageviews`, `site_events`) are kept at the
 * least. They carry every dimension at once, so they are the finest and the
 * largest data the statistic holds; the daily roll-ups answer anything older
 * than this.
 */
export const HOURLY_RETENTION_DAYS = 90;

/**
 * The days the daily roll-ups (`site_pageviews_daily`, `site_events_daily`)
 * and the visitor counts (`site_visitor_days`, `site_visitor_hours`,
 * `site_visitor_dim_days`) are kept at the least: a year and a month, so a year compares with the one before.
 */
export const DAILY_RETENTION_DAYS = 400;

/**
 * The span of one chunk of the statistic's week-chunked hypertables. A chunk
 * is dropped whole once all of it is older than its retention, so a row
 * lives up to this many days longer than the retention says.
 */
export const SITE_CHUNK_DAYS = 7;

/**
 * The days a roll-up marker (`site_rollup_days`) is kept: past the daily rows
 * it vouches for, which live up to a chunk longer than
 * {@link DAILY_RETENTION_DAYS}, so a report never reads a day from the hourly
 * rows while its daily rows are still there, and the job never rolls a day up
 * twice.
 */
export const ROLLUP_MARKER_RETENTION_DAYS =
  DAILY_RETENTION_DAYS + 2 * SITE_CHUNK_DAYS;

/**
 * How long each of the statistic's hypertables is kept, whole chunks at a
 * time. The flush claims only have to outlive a window's retry, and follow
 * the hourly rows they stand for.
 */
export const HYPERTABLE_RETENTION_DAYS: Readonly<
  Record<(typeof SITE_ANALYTICS_HYPERTABLES)[number], number>
> = {
  site_pageviews: HOURLY_RETENTION_DAYS,
  site_events: HOURLY_RETENTION_DAYS,
  site_analytics_flushes: HOURLY_RETENTION_DAYS,
  site_pageviews_daily: DAILY_RETENTION_DAYS,
  site_events_daily: DAILY_RETENTION_DAYS,
};

/** The longest window the admin may ask for. */
export const MAX_ADMIN_WINDOW_DAYS = 365;

/**
 * How long one statement of an admin report may run before Postgres cancels
 * it and the report answers 503 (`SITE_ANALYTICS_STATEMENT_TIMEOUT_MS`). The
 * slowest report of a year of ten times today's traffic takes about a second
 * on the daily level and three on the hourly rows.
 */
export const REPORT_STATEMENT_TIMEOUT_MS = 15_000;

/**
 * How many admin reports one process runs at once, how many more may wait for
 * a place, and how long one waits before it answers 503
 * (`SITE_ANALYTICS_REPORT_CONCURRENCY`, `_QUEUE`, `_QUEUE_WAIT_MS`). The
 * Traffic page asks for about sixteen at once; they queue behind these four
 * rather than fill the pool and every CPU of the database.
 */
export const REPORT_CONCURRENCY = 4;
export const REPORT_QUEUE_LIMIT = 32;
export const REPORT_QUEUE_WAIT_MS = 30_000;

/**
 * The memory a report statement may use for each sort and hash
 * (`SITE_ANALYTICS_REPORT_WORK_MEM_MB`), so that a grouping by path does not
 * spill to disk on a server whose default is 4 MB. The most it can hold is
 * this times the hash tables of its parallel workers, for the few reports
 * that run at once.
 */
export const REPORT_WORK_MEM_MB = 32;

/** How long a process keeps the list of rolled-up days it read. */
export const ROLLED_DAYS_CACHE_MS = 60_000;

/**
 * A day is rolled up only this long after it ended: the flusher has written
 * its last windows (a window that fails is retried for an hour) and ended
 * its last visits (30 to 35 minutes after the last hit), whose exits belong
 * to the day.
 */
export const ROLLUP_SETTLE_SECONDS = 2 * 3600;

/** One run of the rollup job stops starting new days after this long. */
export const ROLLUP_RUN_BUDGET_MS = 25 * 60 * 1000;

/** Visitors over all languages, beside one row per language. */
export const ALL_LOCALES_SCOPE = "all";

/** The country of an address GeoIP cannot place. */
export const UNKNOWN_COUNTRY = "zz";

/** The most values per property, entities and combinations `/events/:name` lists. */
export const MAX_EVENT_DETAIL_ITEMS = 50;

/** The most rows and columns a crosstab keeps; the rest is {@link CROSSTAB_REST}. */
export const MAX_CROSSTAB_SIDE = 30;

/**
 * The crosstab's label for the values past {@link MAX_CROSSTAB_SIDE}. No real
 * value can take this form: a page type and a property value start with a
 * letter or a digit, a path with a slash, and `other` is a page type.
 */
export const CROSSTAB_REST = "__rest__";

/** The fewest visits (window and previous together) a page needs to rank as a trend. */
export const MIN_TREND_VISITS = 10;

/** How many rising and falling pages the trends report lists. */
export const TREND_ITEMS = 25;

/**
 * Redis keys. Under `site-analytics:` and not `analytics:`, which the admin's
 * cache reset flushes (`PARK_CACHE_FLUSH_PATTERNS`). A key that stands for a
 * reader or an address is an HMAC under that day's key
 * (`dailyHashKey`), so it is pseudonymous for one UTC day at most.
 */
export const SITE_ANALYTICS_KEYS = {
  /** One address's hits today: `INCRBY`. */
  cap: (hash: string) => `site-analytics:cap:${hash}`,
  /** All hits of one five-minute window: `INCRBY`. */
  windowHits: (window: string) => `site-analytics:window-hits:${window}`,
  /**
   * One visit, a hash: how it began (`c`), its page views (`n`), its first
   * and last hit (`s`, `l`, UTC seconds) and the exit row of its last page
   * view (`x`). Lives {@link VISIT_KEY_TTL_SECONDS} past the last hit.
   */
  visit: (hash: string) => `site-analytics:visit:${hash}`,
  /** One reader's counted reads of one page today (`page_read`). */
  read: (hash: string) => `site-analytics:read:${hash}`,
  /** One address's counted reads of one page today (`page_read`). */
  readPage: (hash: string) => `site-analytics:read-page:${hash}`,
  /** One address's counted reads today (`page_read`). */
  readAddress: (hash: string) => `site-analytics:read-addr:${hash}`,
  /** The values one guard admitted on one UTC day. */
  guard: (guard: SiteGuard, day: string, scope = "") =>
    `site-analytics:guard:${guard}:${day}${scope ? `:${scope}` : ""}`,
  /** How many new values one address brought to one guard today. */
  newMembers: (hash: string) => `site-analytics:new:${hash}`,
  /** One UTC day's visitors, all languages or one: a HyperLogLog. */
  visitors: (day: string, scope: string) => `site-analytics:uv:${day}:${scope}`,
  /**
   * One UTC day's visitors with one value of a {@link VISITOR_DIMS} dimension,
   * all languages or one: a HyperLogLog. The value is the last part, so it
   * needs no escaping.
   */
  visitorDim: (day: string, scope: string, dim: string, value: string) =>
    `site-analytics:uvd:${day}:${scope}:${dim}:${value}`,
  /**
   * The members (`scope|dim|value`, split at the first two bars) of the
   * dimension keys one day wrote, so the snapshot finds them without a scan.
   */
  visitorDimIndex: (day: string) => `site-analytics:uvd-index:${day}`,
  /** One UTC hour's visitors: a HyperLogLog, until the snapshot writes it. */
  hourVisitors: (hour: string, scope: string) =>
    `site-analytics:uvh:${hour}:${scope}`,
  /** One minute's active visitors: a HyperLogLog. */
  activeMinute: (minute: string, scope: string) =>
    `site-analytics:rt:uv:${minute}:${scope}`,
  /** One minute's page views: a counter. */
  pageviewsMinute: (minute: string, scope: string) =>
    `site-analytics:rt:pv:${minute}:${scope}`,
  /** One minute's events: a counter. */
  eventsMinute: (minute: string, scope: string) =>
    `site-analytics:rt:ev:${minute}:${scope}`,
  /** One minute's counts of one live list: a hash of value → count. */
  live: (list: LiveList, minute: string, scope: string) =>
    `site-analytics:live:${list}:${minute}:${scope}`,
  /** The open visits, by their key's hash, scored by their last hit. */
  openVisits: "site-analytics:visits:open",
  /** Hits dropped or stored under an overflow value, per reason, per UTC day. */
  drops: (day: string) => `site-analytics:drops:${day}`,
  /**
   * One flush window's counts: a hash of row → count. No TTL on purpose:
   * this Redis runs `volatile-lru`, which only evicts keys that have one.
   */
  buffer: (window: string) => `site-analytics:buf:${window}`,
  /** A window the flusher has claimed and not yet written. */
  flushing: (window: string) => `site-analytics:buf:${window}:flushing`,
  /** A window the flusher gave up on, kept for a week. */
  parked: (window: string) => `site-analytics:buf:${window}:parked`,
  /** Every window with a buffer, written or claimed. */
  windows: "site-analytics:buf:windows",
  /** Failed flush attempts per window. */
  failures: "site-analytics:buf:failures",
  /** The windows set aside. */
  parkedWindows: "site-analytics:buf:parked",
  /** Held by the one instance that is flushing. */
  flushLock: "site-analytics:flush-lock",
  /** Held by the one instance that is rolling days up. */
  rollupLock: "site-analytics:rollup-lock",
} as const;

/** The field of a claimed window that names its claim. */
export const CLAIM_FIELD = "~claim";

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

/** The stored bucket of a flush window (or any instant): its UTC hour. */
export function bucketOf(at: Date | string): string {
  const ms = BUCKET_SECONDS * 1000;
  return new Date(Math.floor(new Date(at).getTime() / ms) * ms).toISOString();
}
