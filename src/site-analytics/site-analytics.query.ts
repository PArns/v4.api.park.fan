import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import {
  FILTER_DIMS,
  type FilterDim,
  type SiteLocale,
} from "./site-analytics.constants";

/** `filter=<dim>:<value>`, as the admin sends it. */
export const FILTER_PATTERN = new RegExp(
  `^(${FILTER_DIMS.join("|")}):.{1,820}$`,
  "s",
);

/** A crosstab side: a dimension, the day, or one property (`prop:<key>`). */
export const CROSSTAB_AXIS_PATTERN =
  /^(page_type|viewport|locale|day|path|entry|referrer|channel|campaign|country|browser|os|prop:[a-z_][a-z0-9_]{0,31})$/;

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

/** A report's window: `days` UTC days ending today, and the same before it. */
export interface SiteAnalyticsWindow {
  from: string;
  to: string;
  days: number;
  locale: SiteLocale | null;
  filters: SiteFilter[];
  previousFrom: string;
  previousTo: string;
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
  return {
    from,
    to,
    days: query.days,
    locale: query.locale ?? null,
    filters: parseFilters(query.filter),
    previousFrom: addIsoDays(from, -query.days),
    previousTo: addIsoDays(from, -1),
    hourly: query.days === 1,
  };
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
 * The `WHERE` of a report over `site_pageviews` or `site_events`: the bucket
 * range from `from` to the end of `to`, the language and every filter. The
 * range is two timestamps, so chunk exclusion and the compressed batches'
 * min/max both apply.
 */
export function whereOf(
  params: SqlParams,
  window: Pick<SiteAnalyticsWindow, "locale" | "filters">,
  from: string,
  to: string,
  alias = "",
): string {
  const col = (name: string) => (alias ? `${alias}.${name}` : name);
  const conditions = [
    `${col("bucket")} >= ${params.add(`${from}T00:00:00Z`)}::timestamptz`,
    `${col("bucket")} < ${params.add(`${addIsoDays(to, 1)}T00:00:00Z`)}::timestamptz`,
  ];
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
