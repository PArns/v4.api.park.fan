import { BadRequestException } from "@nestjs/common";
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import {
  BUCKET_SECONDS,
  FILTER_DIMS,
  MAX_COMPARABLE_DAYS,
  bucketOf,
  isVisitDim,
  type FilterDim,
  type SiteLocale,
} from "./site-analytics.constants";

/** `filter=<dim>:<value>`, as the admin sends it. */
export const FILTER_PATTERN = new RegExp(
  `^(${FILTER_DIMS.join("|")}):.{1,820}$`,
  "s",
);

/**
 * A crosstab side: one of an event's page dimensions, the day, or one
 * property (`prop:<key>`). Events carry no visit dimension.
 */
export const CROSSTAB_AXIS_PATTERN =
  /^(page_type|viewport|locale|day|path|country|browser|os|prop:[a-z_][a-z0-9_]{0,31})$/;

export interface SiteFilter {
  dim: FilterDim;
  value: string;
}

/** What every report takes: a window of UTC days, a language, filters. */
export interface SiteAnalyticsQuery {
  days: number;
  locale?: SiteLocale | null;
  /** `<dim>:<value>`, all of which a row must match. */
  filter?: string[] | null;
}

/** A stretch of UTC days, and the instants that bound its rows: `[start, end)`. */
export interface SiteAnalyticsRange {
  from: string;
  to: string;
  start: string;
  end: string;
}

/**
 * A report's window: `days` UTC days ending today, and the same before it.
 * Today is under way, so the window before ends at the same hour of its last
 * day: a change compares like with like, not today so far with a whole day.
 * Over {@link MAX_COMPARABLE_DAYS} there is no window before, since most of it
 * would lie past the retention.
 */
export interface SiteAnalyticsWindow extends SiteAnalyticsRange {
  days: number;
  locale: SiteLocale | null;
  filters: SiteFilter[];
  previous: SiteAnalyticsRange | null;
  /** Hourly points for a window of one day, daily ones otherwise. */
  hourly: boolean;
}

/** The filters of a query, parsed; a malformed one is left out. */
export function parseFilters(raw: SiteAnalyticsQuery["filter"]): SiteFilter[] {
  return (raw ?? [])
    .filter((entry) => FILTER_PATTERN.test(entry))
    .map((entry) => {
      const at = entry.indexOf(":");
      return {
        dim: entry.slice(0, at) as FilterDim,
        value: entry.slice(at + 1),
      };
    });
}

/** The window of `days` UTC days ending today, and the one before it. */
export function siteAnalyticsWindow(
  query: SiteAnalyticsQuery,
  now: Date,
): SiteAnalyticsWindow {
  const to = utcDay(now);
  const from = addIsoDays(to, -(query.days - 1));
  const shift = query.days * 86_400_000;
  const startMs = Date.parse(`${from}T00:00:00Z`);
  // The previous window takes the current hour whole, as the current one
  // does: rows are hours, and the current hour's row holds what came so far.
  const hourEndMs = Date.parse(bucketOf(now)) + BUCKET_SECONDS * 1000;
  const previousFrom = addIsoDays(from, -query.days);
  return {
    from,
    to,
    start: new Date(startMs).toISOString(),
    end: `${addIsoDays(to, 1)}T00:00:00.000Z`,
    days: query.days,
    locale: query.locale ?? null,
    filters: parseFilters(query.filter),
    previous:
      query.days > MAX_COMPARABLE_DAYS
        ? null
        : {
            from: previousFrom,
            to: addIsoDays(from, -1),
            start: new Date(startMs - shift).toISOString(),
            end: new Date(Math.min(hourEndMs - shift, startMs)).toISOString(),
          },
    hourly: query.days === 1,
  };
}

/** The window's filters on a visit dimension. */
export function visitFilters(window: SiteAnalyticsWindow): SiteFilter[] {
  return window.filters.filter((filter) => isVisitDim(filter.dim));
}

/**
 * Refuses a filter on a visit dimension for a report that counts page views
 * or events: those rows do not carry where the visit came from.
 */
export function refuseVisitFilters(
  window: SiteAnalyticsWindow,
  report: string,
): void {
  const visit = visitFilters(window);
  if (visit.length === 0) return;
  throw new BadRequestException(
    `${report} counts page views or events, which carry no visit dimension; ` +
      `${visit.map((f) => f.dim).join(", ")} can only filter visits`,
  );
}

/** Every day from `from` to `to`, both included. */
export function daysBetween(from: string, to: string): string[] {
  const days: string[] = [];
  for (let day = from; day <= to; day = addIsoDays(day, 1)) days.push(day);
  return days;
}

/** Every hour of `day` up to the hour of `now`, as `2026-10-28T07:00:00Z`. */
export function hoursOf(day: string, now: Date): string[] {
  const hours: string[] = [];
  const last =
    utcDay(now) === day ? now.getUTCHours() : day < utcDay(now) ? 23 : -1;
  for (let h = 0; h <= last; h++) {
    hours.push(`${day}T${String(h).padStart(2, "0")}:00:00Z`);
  }
  return hours;
}

/** A rate rounded to four places; 0 when there is nothing to divide by. */
export function rate(numerator: number, divisor: number): number {
  return divisor > 0 ? Math.round((numerator / divisor) * 10_000) / 10_000 : 0;
}

/** `1 − engaged / visits`, rounded; 0 without a visit. */
export function bounceRate(visits: number, engaged: number): number {
  return visits > 0 ? rate(visits - Math.min(engaged, visits), visits) : 0;
}

/**
 * Positional parameters for one statement. Every value a report puts into
 * SQL goes through here; the column names come from {@link FILTER_DIMS},
 * never from the request.
 */
export class SqlParams {
  readonly values: unknown[] = [];

  add(value: unknown): string {
    this.values.push(value);
    return `$${this.values.length}`;
  }
}

/** The column of a dimension: the same name on both tables. */
export function columnOf(dim: FilterDim): string {
  if (!FILTER_DIMS.includes(dim)) throw new Error(`Unknown dimension ${dim}`);
  return dim;
}

/**
 * The `WHERE` of a report over `site_pageviews` or `site_events`: the window's
 * rows, or with `both` the window's and the one before's, the language and
 * every filter. The range is two timestamps, so chunk exclusion and the
 * compressed batches' min/max both apply; the hours between the window before
 * and the window are left out by a second condition.
 */
export function whereOf(
  params: SqlParams,
  window: Pick<
    SiteAnalyticsWindow,
    "locale" | "filters" | "start" | "end" | "previous"
  >,
  scope: "current" | "both",
  alias = "",
): string {
  const col = (name: string) => (alias ? `${alias}.${name}` : name);
  const previous = scope === "both" ? window.previous : null;
  const conditions = [
    `${col("bucket")} >= ${params.add(previous?.start ?? window.start)}::timestamptz`,
    `${col("bucket")} < ${params.add(window.end)}::timestamptz`,
  ];
  if (previous && previous.end < window.start) {
    conditions.push(
      `NOT (${col("bucket")} >= ${params.add(previous.end)}::timestamptz ` +
        `AND ${col("bucket")} < ${params.add(window.start)}::timestamptz)`,
    );
  }
  if (window.locale) {
    conditions.push(`${col("locale")} = ${params.add(window.locale)}`);
  }
  for (const filter of window.filters) {
    conditions.push(
      `${col(columnOf(filter.dim))} = ${params.add(filter.value)}`,
    );
  }
  return conditions.join(" AND ");
}

/**
 * The SQL of a bucket's UTC day or hour, to group by. `time_bucket` groups the
 * compressed batches without formatting every row, so the text is made in
 * {@link periodText} after the grouping.
 */
export function periodOf(hourly: boolean, alias = ""): string {
  const bucket = alias ? `${alias}.bucket` : "bucket";
  return `time_bucket('${hourly ? "1 hour" : "1 day"}'::interval, ${bucket})`;
}

/**
 * A period as the reports print it: `2026-10-28`, or `2026-10-28T07:00:00Z`
 * for an hour. Null stays null (a grouping set without the period).
 */
export function periodText(value: unknown, hourly: boolean): string | null {
  if (value === null || value === undefined) return null;
  const iso = (
    value instanceof Date ? value : new Date(String(value))
  ).toISOString();
  return hourly ? `${iso.slice(0, 13)}:00:00Z` : iso.slice(0, 10);
}

/** Most counted first, ties in the given order. */
export function byCountDesc<T>(items: T[], count: (item: T) => number): T[] {
  return items
    .map((item, index) => ({ item, index }))
    .sort((a, b) => count(b.item) - count(a.item) || a.index - b.index)
    .map(({ item }) => item);
}

export type Count = string | number | null;
export const num = (value: Count | undefined): number =>
  Number(value ?? 0) || 0;
