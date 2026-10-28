/**
 * The daily level of the site statistic: which groupings of the hourly rows
 * are kept per UTC day (cuboids), how the nightly job writes one, and how a
 * report reads the days it may take from the daily tables and the hours it
 * must take from the hourly ones, with the same answer either way. See
 * docs/frontend/site-analytics.md.
 *
 * A cuboid is a set of dimensions. Its rows are the hourly rows summed per
 * UTC day and per value of those dimensions (and the language, which every
 * cuboid keeps); a column the cuboid does not name holds `''`. A report needs
 * some columns, so it reads the smallest cuboid that has them all, or the
 * hourly rows when none has. Full-dimension daily rows would not shrink the
 * data (a page view's row is nearly unique at this traffic: 25,300 rows a
 * day instead of 29,600), so the cuboids are the few dimensions a panel or a
 * filter asks about at a time.
 */
import {
  type BucketRange,
  type SiteAnalyticsWindow,
  type SqlParams,
  bucketRanges,
  columnOf,
  dimConditionsOf,
  whereOf,
} from "./site-analytics.query";

/** Which hourly table a rollup sums. */
export type RollupSource = "pageviews" | "events";

/** One grouping kept per day. */
export interface Cuboid {
  /**
   * Stored on every row and marker, so it never changes meaning: a new
   * grouping takes the next number, an old one is never renumbered.
   */
  readonly id: number;
  /** The columns besides `locale`, in the order of the table. */
  readonly dims: readonly string[];
}

/** The measures of a page-view row, each summed. */
export const PAGEVIEW_MEASURES = [
  "views",
  "seconds",
  "visits",
  "engaged",
  "visit_seconds",
  "exits",
  "visit_span_seconds",
] as const;

/** Every dimension of a page-view row but `locale`, in column order. */
export const PAGEVIEW_COLUMNS = [
  "path",
  "page_type",
  "viewport",
  "browser",
  "os",
  "country",
  "browser_language",
  "entry",
  "referrer",
  "channel",
  "campaign",
] as const;

/**
 * The columns with about a hundred thousand values a year: a grouping by one
 * is one hash table over millions of rows however the rows are read, and
 * summing it in pieces first only adds work.
 */
export const WIDE_COLUMNS: readonly string[] = ["path", "entry"];

/** Every dimension of an event row but `locale`, in column order. */
export const EVENT_COLUMNS = [
  "name",
  "path",
  "page_type",
  "viewport",
  "browser",
  "os",
  "country",
  "props_key",
  "entity",
] as const;

/**
 * The groupings of the page-view rows, each of at most a few thousand rows a
 * day against the hourly table's thirty thousand (measured on the synthetic
 * year at ten times today's traffic, docs/frontend/site-analytics.md):
 * the day alone, each dimension a panel groups by, the pairs of the narrow
 * ones (a filter on one with a panel on another), and the entry page with
 * where it came from for the SEO reports. The path and the entry page are not
 * paired with others: a pair of them is as large as the hourly rows. The
 * numbers are fixed; add at the end.
 */
export const PAGEVIEW_CUBOIDS: readonly Cuboid[] = [
  { id: 1, dims: [] },
  { id: 2, dims: ["path", "page_type"] },
  { id: 3, dims: ["page_type"] },
  { id: 4, dims: ["viewport"] },
  { id: 5, dims: ["browser"] },
  { id: 6, dims: ["os"] },
  { id: 7, dims: ["country"] },
  { id: 8, dims: ["browser_language"] },
  { id: 9, dims: ["entry"] },
  { id: 10, dims: ["referrer"] },
  { id: 11, dims: ["channel"] },
  { id: 12, dims: ["campaign"] },
  { id: 13, dims: ["page_type", "entry", "referrer", "channel"] },
  { id: 14, dims: ["page_type", "viewport"] },
  { id: 15, dims: ["page_type", "browser"] },
  { id: 16, dims: ["page_type", "os"] },
  { id: 17, dims: ["page_type", "country"] },
  { id: 18, dims: ["page_type", "browser_language"] },
  { id: 19, dims: ["page_type", "channel"] },
  { id: 20, dims: ["page_type", "campaign"] },
  { id: 21, dims: ["page_type", "referrer"] },
  { id: 22, dims: ["viewport", "browser"] },
  { id: 23, dims: ["viewport", "os"] },
  { id: 24, dims: ["viewport", "country"] },
  { id: 25, dims: ["viewport", "browser_language"] },
  { id: 26, dims: ["viewport", "channel"] },
  { id: 27, dims: ["viewport", "campaign"] },
  { id: 28, dims: ["viewport", "referrer"] },
  { id: 29, dims: ["browser", "os"] },
  { id: 30, dims: ["browser", "country"] },
  { id: 31, dims: ["browser", "browser_language"] },
  { id: 32, dims: ["browser", "channel"] },
  { id: 33, dims: ["browser", "campaign"] },
  { id: 34, dims: ["browser", "referrer"] },
  { id: 35, dims: ["os", "country"] },
  { id: 36, dims: ["os", "browser_language"] },
  { id: 37, dims: ["os", "channel"] },
  { id: 38, dims: ["os", "campaign"] },
  { id: 39, dims: ["os", "referrer"] },
  { id: 40, dims: ["country", "browser_language"] },
  { id: 41, dims: ["country", "channel"] },
  { id: 42, dims: ["country", "campaign"] },
  { id: 43, dims: ["country", "referrer"] },
  { id: 44, dims: ["browser_language", "channel"] },
  { id: 45, dims: ["browser_language", "campaign"] },
  { id: 46, dims: ["browser_language", "referrer"] },
  { id: 47, dims: ["channel", "campaign"] },
  { id: 48, dims: ["channel", "referrer"] },
  { id: 49, dims: ["campaign", "referrer"] },
];

/**
 * The groupings of the event rows: the name alone (the events list), the name
 * with the property combination (one property's values, a crosstab or the
 * search report's properties), with the page type and viewport as well (the
 * event's detail, the search report's openings), and with the entity. Events
 * filtered by the path, country, browser or system come from the hourly rows.
 */
export const EVENT_CUBOIDS: readonly Cuboid[] = [
  { id: 1, dims: ["name"] },
  { id: 2, dims: ["name", "props_key"] },
  { id: 3, dims: ["name", "page_type", "viewport", "props_key"] },
  { id: 4, dims: ["name", "entity"] },
];

/** What differs between the two rolled-up tables. */
interface RollupTable {
  source: RollupSource;
  /** The hourly table, summed from. */
  hourly: string;
  /** The daily table, written to. */
  daily: string;
  /** Dimension columns, but `locale`. */
  columns: readonly string[];
  /** The summed columns. */
  measures: readonly string[];
  cuboids: readonly Cuboid[];
}

export const ROLLUP_TABLES: Readonly<Record<RollupSource, RollupTable>> = {
  pageviews: {
    source: "pageviews",
    hourly: "site_pageviews",
    daily: "site_pageviews_daily",
    columns: PAGEVIEW_COLUMNS,
    measures: PAGEVIEW_MEASURES,
    cuboids: PAGEVIEW_CUBOIDS,
  },
  events: {
    source: "events",
    hourly: "site_events",
    daily: "site_events_daily",
    columns: EVENT_COLUMNS,
    measures: ["count"],
    cuboids: EVENT_CUBOIDS,
  },
};

/** Columns every row has whatever its cuboid. */
const ALWAYS = ["bucket", "locale"] as const;

/**
 * The columns a cuboid's rows carry for reading: the bucket, the language, its
 * dimensions (the event's properties go with their key) and the measures.
 */
export function cuboidColumns(
  source: RollupSource,
  cuboid: Cuboid,
): readonly string[] {
  const table = ROLLUP_TABLES[source];
  return [
    ...ALWAYS,
    ...cuboid.dims.flatMap((dim) =>
      dim === "props_key" ? ["props_key", "props"] : [dim],
    ),
    ...table.measures,
  ];
}

/**
 * The smallest cuboid that holds every column a statement reads (`locale` and
 * the bucket are always there), or null when none does: the statement then
 * reads the hourly rows. Fewer dimensions mean fewer rows; of equals the
 * lowest number.
 */
export function chooseCuboid(
  source: RollupSource,
  needed: Iterable<string>,
): Cuboid | null {
  const wanted = [...new Set(needed)].filter((dim) => dim !== "locale");
  let best: Cuboid | null = null;
  for (const cuboid of ROLLUP_TABLES[source].cuboids) {
    if (!wanted.every((dim) => cuboid.dims.includes(dim))) continue;
    if (!best || cuboid.dims.length < best.dims.length) best = cuboid;
  }
  return best;
}

const DAY_MS = 86_400_000;

/** The UTC day of an instant, `YYYY-MM-DD`. */
function dayOfMs(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

/**
 * Splits bucket ranges into the stretches of whole days that are rolled up and
 * the rest, which is read hourly: today, a day the job has not reached, and a
 * day a range cuts (the window before ends at an hour). Neighbours of one
 * kind are joined, so a year is usually one daily and one hourly range.
 */
export function splitRanges(
  ranges: readonly BucketRange[],
  rolled: ReadonlySet<string>,
): { daily: BucketRange[]; hourly: BucketRange[] } {
  const daily: BucketRange[] = [];
  const hourly: BucketRange[] = [];
  for (const range of ranges) {
    const end = Date.parse(range.end);
    let cursor = Date.parse(range.start);
    let last: { into: BucketRange[]; piece: BucketRange } | null = null;
    while (cursor < end) {
      const next = Math.min((Math.floor(cursor / DAY_MS) + 1) * DAY_MS, end);
      const whole = cursor % DAY_MS === 0 && next - cursor === DAY_MS;
      const into = whole && rolled.has(dayOfMs(cursor)) ? daily : hourly;
      const stop = new Date(next).toISOString();
      if (last && last.into === into) {
        last.piece.end = stop;
      } else {
        const piece = { start: new Date(cursor).toISOString(), end: stop };
        into.push(piece);
        last = { into, piece };
      }
      cursor = next;
    }
  }
  return { daily, hourly };
}

/** `bucket >= $1 AND bucket < $2`, and `(…) OR (…)` for more than one range. */
function rangesCondition(params: SqlParams, ranges: BucketRange[]): string {
  const parts = ranges.map(
    (range) =>
      `bucket >= ${params.add(range.start)}::timestamptz AND bucket < ${params.add(range.end)}::timestamptz`,
  );
  return parts.length === 1
    ? parts[0]
    : parts.map((p) => `(${p})`).join(" OR ");
}

/**
 * What a statement reads in place of the hourly table: the days of its ranges
 * that are rolled up from the cuboid's daily rows, the remaining hours from
 * the hourly rows, both with the window's language and filters, and
 * both limited to the columns the cuboid has so that a statement that reads
 * another fails loudly instead of reading `''`. With `collapse`, the columns
 * the statement reads, each branch is summed over them first (the days of a
 * window together, when the statement asks for no day), in parallel inside the
 * branch, so the statement above groups one row per value of those columns
 * instead of one per day and value. That pays where it cuts the rows a lot, not
 * for {@link WIDE_COLUMNS}. Null when no whole day of the ranges is rolled up:
 * the caller reads the hourly table as before.
 */
export function rolledSource(
  source: RollupSource,
  cuboid: Cuboid,
  params: SqlParams,
  window: Pick<SiteAnalyticsWindow, "locale" | "filters">,
  ranges: readonly BucketRange[],
  rolled: ReadonlySet<string>,
  alias: string,
  collapse: readonly string[] | null = null,
): string | null {
  const split = splitRanges(ranges, rolled);
  if (split.daily.length === 0) return null;
  const table = ROLLUP_TABLES[source];
  const dims = dimConditionsOf(params, window, "");
  const kept = collapse
    ? [
        ...new Set(
          collapse.flatMap((column) =>
            column === "props_key" ? ["props_key", "props"] : [column],
          ),
        ),
      ]
    : [];
  const select = collapse
    ? [...kept, ...table.measures.map((m) => `SUM(${m}) AS ${m}`)].join(", ")
    : cuboidColumns(source, cuboid).join(", ");
  const group =
    collapse && kept.length > 0 ? ` GROUP BY ${kept.join(", ")}` : "";
  const branch = (from: string, where: string[]) =>
    `SELECT ${select} FROM ${from} WHERE ${where.join(" AND ")}${group}`;
  const parts = [
    branch(table.daily, [
      `cuboid = ${params.add(cuboid.id)}::smallint`,
      `(${rangesCondition(params, split.daily)})`,
      ...dims,
    ]),
  ];
  if (split.hourly.length > 0) {
    parts.push(
      branch(table.hourly, [
        `(${rangesCondition(params, split.hourly)})`,
        ...dims,
      ]),
    );
  }
  return `(${parts.join(" UNION ALL ")}) AS ${alias}`;
}

/** The days of one cuboid that the nightly job has rolled up. */
export type RolledDays = (
  source: RollupSource,
  cuboid: number,
) => Promise<ReadonlySet<string>>;

/**
 * What a statement puts after `FROM` and `WHERE` to read the page-view or
 * event rows of a window: the hourly table as before, or, when the statement
 * reads only columns of some cuboid and whole days of the window are rolled
 * up, those days from the cuboid's daily rows and the rest from the hourly
 * ones, with the window's language and filters applied to both. The
 * statement lists in `needed` every dimension column it reads; those of the
 * window's filters are added. An hourly series reads the hourly rows.
 * `collapse` says the statement reads no bucket, only those columns: the days
 * are summed together before the statement groups them.
 */
export async function rowsOf(
  rolledDays: RolledDays,
  source: RollupSource,
  params: SqlParams,
  window: Pick<
    SiteAnalyticsWindow,
    "locale" | "filters" | "start" | "end" | "previous" | "hourly"
  >,
  scope: "current" | "both",
  needed: readonly string[],
  options: { alias?: string; collapse?: boolean } = {},
): Promise<{ from: string; where: string }> {
  const alias = options.alias ?? "";
  const table = ROLLUP_TABLES[source];
  const hourly = () => ({
    from: alias ? `${table.hourly} ${alias}` : table.hourly,
    where: whereOf(params, window, scope, alias),
  });
  if (window.hourly) return hourly();
  const cuboid = chooseCuboid(source, [
    ...needed,
    ...window.filters.map((filter) => columnOf(filter.dim)),
  ]);
  if (!cuboid) return hourly();
  const from = rolledSource(
    source,
    cuboid,
    params,
    window,
    bucketRanges(window, scope),
    await rolledDays(source, cuboid.id),
    alias || table.hourly,
    options.collapse ? needed : null,
  );
  return from ? { from, where: "TRUE" } : hourly();
}

/** The SQL literal that fills a column outside a cuboid. */
function emptyOf(column: string): string {
  if (column === "props") return `'{}'::jsonb`;
  if (column === "entity") return "NULL::uuid";
  return "''";
}

/**
 * The statement that writes one cuboid of one day and its marker, once: the
 * rows are the day's hourly rows grouped by the cuboid's dimensions, and the
 * marker (with the rows' count) goes in with them only if the cuboid has none
 * for that day yet. Parameters: `$1` the day's midnight, `$2` the day as a
 * date, `$3` the cuboid. Callers serialise on an advisory lock so two
 * runners cannot both pass the check.
 */
export function rollupSql(source: RollupSource, cuboid: Cuboid): string {
  const table = ROLLUP_TABLES[source];
  const member = (column: string) =>
    column === "props"
      ? cuboid.dims.includes("props_key")
      : cuboid.dims.includes(column);
  const dimensionColumns = [
    ...table.columns.flatMap((column) =>
      column === "props_key" ? ["props_key", "props"] : [column],
    ),
  ];
  const selected = dimensionColumns.map((column) =>
    member(column) ? column : emptyOf(column),
  );
  const groups = [
    "locale",
    ...dimensionColumns.filter((column) => member(column)),
  ];
  const sums = table.measures.map((measure) => `SUM(${measure})::bigint`);
  return `WITH open AS (
       SELECT 1 WHERE NOT EXISTS (
         SELECT 1 FROM site_rollup_days
          WHERE source = '${source}' AND cuboid = $3::smallint AND day = $2::date)
     ), written AS (
       INSERT INTO ${table.daily}
         (bucket, cuboid, locale, ${dimensionColumns.join(", ")}, ${table.measures.join(", ")})
       SELECT $1::timestamptz, $3::smallint, locale, ${selected.join(", ")}, ${sums.join(", ")}
         FROM ${table.hourly}, open
        WHERE bucket >= $1::timestamptz
          AND bucket < $1::timestamptz + interval '1 day'
        GROUP BY ${groups.join(", ")}
       RETURNING 1
     )
     INSERT INTO site_rollup_days (source, cuboid, day, rows)
     SELECT '${source}', $3::smallint, $2::date, count(*)::int
       FROM written
     ON CONFLICT (source, cuboid, day) DO NOTHING
     RETURNING rows`;
}
