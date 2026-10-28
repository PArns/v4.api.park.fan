/**
 * Limits and names of the Google Search Console import. See
 * docs/frontend/search-console.md.
 */

/** The read-only scope; the service account never writes to Search Console. */
export const GSC_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly";

/** Where a service account's signed assertion is exchanged for an access token. */
export const GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";

/** The Search Console API, v3: `sites/{siteUrl}/searchAnalytics/query`. */
export const GSC_API_BASE = "https://www.googleapis.com/webmasters/v3";

/** The most rows one `searchAnalytics.query` answers; the next page starts at `startRow`. */
export const GSC_ROW_LIMIT = 25_000;

/** Per request, so a hung call cannot hold the import for ever. */
export const GSC_REQUEST_TIMEOUT_MS = 60_000;

/** Tries per request on a 429 or a 5xx, with a doubling pause between them. */
export const GSC_MAX_ATTEMPTS = 4;

/**
 * The layout a batch was written in. 2 stores complete page, country and
 * device rows beside the query rows; a batch below it holds totals that miss
 * the anonymised queries, so its day counts as never fetched.
 */
export const BATCH_FORMAT = 2;

/** Days re-imported on every run when `GSC_IMPORT_DAYS` is unset: Search Console finalises late. */
export const DEFAULT_IMPORT_DAYS = 3;
export const MAX_IMPORT_DAYS = 30;

/**
 * How far back the first run reaches. Search Console keeps 16 months; a few
 * days short of it, since a start date past its limit is a 400.
 */
export const BACKFILL_DAYS = 480;

/**
 * The nightly `drop-old-chunks` drops chunks wholly older than this: 16 months
 * and a little, since a chunk holds 30 days and goes only when all of it is old.
 */
export const RETENTION_DAYS = 500;

/** How long a run's own record is kept. */
export const RUN_RETENTION_DAYS = 90;

/** Day rows in a row of `unnest` arrays per `INSERT`. */
export const INSERT_CHUNK_ROWS = 5_000;

/** Consecutive days that fail before a run gives up: the API, not a day, is the problem. */
export const MAX_CONSECUTIVE_FAILURES = 3;

/** `drop_chunks` gives up on a lock after this, rather than queueing the API behind it. */
export const DROP_LOCK_TIMEOUT = "5s";

/** The Redis lock of a running import, in ms; the job ends long before it. */
export const IMPORT_LOCK_TTL_MS = 3 * 60 * 60 * 1000;
export const IMPORT_LOCK_KEY = "search-console:import:lock";

/** A second click on "import now" within this many seconds queues nothing new. */
export const IMPORT_REQUEST_COOLDOWN_SECONDS = 60;
export const IMPORT_REQUEST_KEY = "search-console:import:requested";

/** Rows a report lists at most. */
export const MAX_REPORT_LIMIT = 200;
export const DEFAULT_REPORT_LIMIT = 50;

/** The series steps Search Console's daily data supports. */
export const GSC_INTERVALS = ["day", "week", "month"] as const;
export type GscInterval = (typeof GSC_INTERVALS)[number];

/** Devices Search Console reports, lower case. */
export const GSC_DEVICES = ["desktop", "mobile", "tablet"] as const;

/** The longest a stored page path or query is; longer ones are skipped or cut. */
export const MAX_PATH_LENGTH = 820;
export const MAX_QUERY_LENGTH = 500;
