import { BadRequestException } from "@nestjs/common";
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import {
  BUCKET_SECONDS,
  FILTER_DIMS,
  INTERVALS,
  MAX_ADMIN_WINDOW_DAYS,
  MAX_COMPARABLE_DAYS,
  MAX_HOURLY_DAYS,
  MAX_RANGE_DAYS,
  bucketOf,
  isVisitDim,
  type FilterDim,
  type SiteInterval,
  type SiteLocale,
} from "./site-analytics.constants";
import { HourlyReach, hourlyHorizon } from "./site-analytics.retention";

/**
 * `filter=<dim>:<value>`, as the admin sends it. A NUL byte is refused: the
 * database cannot store it, and the statement would fail as a 500.
 */
export const FILTER_PATTERN = new RegExp(
  `^(${FILTER_DIMS.join("|")}):[^\\u0000]{1,820}$`,
);

/**
 * A crosstab side: one of an event's page dimensions, the day, or one
 * property (`prop:<key>`). Events carry no visit dimension.
 */
export const CROSSTAB_AXIS_PATTERN =
  /^(page_type|device|locale|day|path|country|browser|os|prop:[a-z_][a-z0-9_]{0,31})$/;

export interface SiteFilter {
  dim: FilterDim;
  value: string;
}

/** `YYYY-MM-DD`. */
export const DAY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/**
 * What every report takes: a window of UTC days, either the last `days` or
 * `from`–`to` (both included), a series step, a language, filters.
 */
export interface SiteAnalyticsQuery {
  days?: number | null;
  from?: string | null;
  to?: string | null;
  interval?: SiteInterval | null;
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
 * would lie past the daily rows' retention.
 */
export interface SiteAnalyticsWindow extends SiteAnalyticsRange {
  days: number;
  locale: SiteLocale | null;
  filters: SiteFilter[];
  previous: SiteAnalyticsRange | null;
  /** The series step. */
  interval: SiteInterval;
  /** Whether the SQL groups by the hour; otherwise by the day. */
  hourly: boolean;
  /** The first UTC day the hourly rows hold today; see {@link hourlyHorizon}. */
  horizon: string;
  /**
   * What the report's statements read from hourly rows beyond the horizon.
   * Each statement adds to it as it plans its read, and `echo` reports it, so
   * no report names `hourlyFrom` by hand.
   */
  reach: HourlyReach;
}

/**
 * The series step a window gets when none is asked for: the hour for one day,
 * the day up to 92 days, the week up to 183, the month beyond.
 */
export function defaultInterval(days: number): SiteInterval {
  if (days === 1) return "hour";
  if (days <= 92) return "day";
  if (days <= 183) return "week";
  return "month";
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

/**
 * The window a query asks for, and the one before it. `days` (1–365) ends
 * today; `from`–`to` is any stretch of up to {@link MAX_RANGE_DAYS} days that
 * ends today at the latest. The window before has the same length; when the
 * window ends today it ends at the same hour of its last day. A query the
 * rules refuse is a 400.
 */
export function siteAnalyticsWindow(
  query: SiteAnalyticsQuery,
  now: Date,
): SiteAnalyticsWindow {
  const today = utcDay(now);
  let from: string;
  let to: string;
  if (query.from || query.to) {
    if (!query.from || !query.to) {
      throw new BadRequestException("from and to go together");
    }
    if (query.days !== null && query.days !== undefined) {
      throw new BadRequestException("days or from and to, not both");
    }
    if (!isDay(query.from) || !isDay(query.to)) {
      throw new BadRequestException("from and to are YYYY-MM-DD");
    }
    ({ from, to } = query as { from: string; to: string });
    if (from > to) throw new BadRequestException("from is after to");
    if (to > today) throw new BadRequestException("to is after today");
    // Arithmetic first: building the list for year 0000 blocks the event loop.
    if (dayCount(from, to) > MAX_RANGE_DAYS) {
      throw new BadRequestException(
        `from–to covers more than ${MAX_RANGE_DAYS} days`,
      );
    }
  } else {
    const days = query.days ?? 30;
    if (!Number.isInteger(days) || days < 1 || days > MAX_ADMIN_WINDOW_DAYS) {
      throw new BadRequestException(`days is 1 to ${MAX_ADMIN_WINDOW_DAYS}`);
    }
    to = today;
    from = addIsoDays(to, -(days - 1));
  }
  const days = daysBetween(from, to).length;
  const interval = query.interval ?? defaultInterval(days);
  if (!INTERVALS.includes(interval)) {
    throw new BadRequestException(`interval is ${INTERVALS.join(", ")}`);
  }
  if (interval === "hour" && days > MAX_HOURLY_DAYS) {
    throw new BadRequestException(
      `interval=hour takes at most ${MAX_HOURLY_DAYS} days`,
    );
  }
  const horizon = hourlyHorizon(now);
  const shift = days * 86_400_000;
  const startMs = Date.parse(`${from}T00:00:00Z`);
  // A window that ends today compares with a window before that ends at the
  // same hour, the current hour taken whole on both sides: rows are hours,
  // and the current hour's row holds what came so far.
  const hourEndMs = Date.parse(bucketOf(now)) + BUCKET_SECONDS * 1000;
  const previousEndMs =
    to === today ? Math.min(hourEndMs - shift, startMs) : startMs;
  return {
    from,
    to,
    start: new Date(startMs).toISOString(),
    end: `${addIsoDays(to, 1)}T00:00:00.000Z`,
    days,
    locale: query.locale ?? null,
    filters: parseFilters(query.filter),
    previous:
      days > MAX_COMPARABLE_DAYS
        ? null
        : {
            from: addIsoDays(from, -days),
            to: addIsoDays(from, -1),
            start: new Date(startMs - shift).toISOString(),
            end: new Date(previousEndMs).toISOString(),
          },
    interval,
    hourly: interval === "hour",
    horizon,
    reach: new HourlyReach(horizon),
  };
}

function isDay(value: string): boolean {
  return (
    DAY_PATTERN.test(value) &&
    new Date(`${value}T00:00:00Z`).toISOString().slice(0, 10) === value
  );
}

/** One point of a report's series. */
export interface SeriesPeriod {
  /** The period's start: `YYYY-MM-DD`, or `…T07:00:00Z` for an hour. */
  day: string;
  /** The period reaches past the window, or is still under way. */
  partial: boolean;
}

/**
 * The period of a day or an hour at the window's interval: the hour or the
 * day itself, the Monday of its ISO week, the first of its month.
 */
export function periodStartOf(point: string, interval: SiteInterval): string {
  if (interval === "hour" || interval === "day") return point;
  const day = point.slice(0, 10);
  if (interval === "month") return `${day.slice(0, 8)}01`;
  const weekday = (new Date(`${day}T00:00:00Z`).getUTCDay() + 6) % 7;
  return addIsoDays(day, -weekday);
}

/**
 * Every point of a window's series, in order: hours up to the current one,
 * or days, ISO weeks (Monday, UTC) or months that touch the window. A point
 * that reaches past the window, or is still under way, is `partial`.
 */
export function seriesPeriods(
  window: Pick<SiteAnalyticsWindow, "from" | "to" | "interval">,
  now: Date,
): SeriesPeriod[] {
  const today = utcDay(now);
  if (window.interval === "hour") {
    const periods: SeriesPeriod[] = [];
    for (const day of daysBetween(window.from, window.to)) {
      for (const hour of hoursOf(day, now)) {
        periods.push({
          day: hour,
          partial:
            hour ===
            `${today}T${String(now.getUTCHours()).padStart(2, "0")}:00:00Z`,
        });
      }
    }
    return periods;
  }
  // The last day the window has whole: today is under way.
  const lastWhole = window.to < today ? window.to : addIsoDays(today, -1);
  const starts: string[] = [];
  for (const day of daysBetween(window.from, window.to)) {
    const start = periodStartOf(day, window.interval);
    if (starts[starts.length - 1] !== start) starts.push(start);
  }
  return starts.map((start, i) => {
    const next = starts[i + 1] ?? nextPeriodStart(start, window.interval);
    const last = addIsoDays(next, -1);
    return {
      day: start,
      partial: start < window.from || last > lastWhole,
    };
  });
}

function nextPeriodStart(start: string, interval: SiteInterval): string {
  if (interval === "day") return addIsoDays(start, 1);
  if (interval === "week") return addIsoDays(start, 7);
  const [year, month] = start.split("-").map(Number);
  return month === 12
    ? `${year + 1}-01-01`
    : `${year}-${String(month + 1).padStart(2, "0")}-01`;
}

/** The window's filters on a visit dimension. */
export function visitFilters(window: SiteAnalyticsWindow): SiteFilter[] {
  return window.filters.filter((filter) => isVisitDim(filter.dim));
}

/**
 * Refuses a filter on a dimension the report's rows do not carry: a visit
 * dimension for a report over page views or events, the browser language for
 * one over events.
 */
export function refuseFiltersBeyond(
  window: SiteAnalyticsWindow,
  report: string,
  dims: readonly string[],
): void {
  const beyond = window.filters.filter((filter) => !dims.includes(filter.dim));
  if (beyond.length === 0) return;
  const names = [...new Set(beyond.map((f) => f.dim))].join(", ");
  throw new BadRequestException(
    `${report} counts rows that do not carry ${names}` +
      (beyond.some((f) => isVisitDim(f.dim))
        ? "; the visit dimensions can only filter visits"
        : ""),
  );
}

/** How many days lie from `from` to `to`, both included. */
function dayCount(from: string, to: string): number {
  return Math.round((Date.parse(to) - Date.parse(from)) / 86_400_000) + 1;
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

/** Seconds per page view or visit, whole; null with nothing to divide by. */
export function averageSeconds(
  seconds: number,
  divisor: number,
): number | null {
  return divisor > 0 ? Math.round(seconds / divisor) : null;
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

/** Instants that bound rows: `[start, end)`. */
export interface BucketRange {
  start: string;
  end: string;
}

/**
 * The stretches of buckets a report reads: the window's, and with `both` the
 * window before's as well, which is one stretch with the window when it ends
 * where the window starts and two when it ends at an earlier hour.
 */
export function bucketRanges(
  window: Pick<SiteAnalyticsWindow, "start" | "end" | "previous">,
  scope: "current" | "both",
): BucketRange[] {
  const previous = scope === "both" ? window.previous : null;
  if (!previous) return [{ start: window.start, end: window.end }];
  if (previous.end < window.start) {
    return [
      { start: previous.start, end: previous.end },
      { start: window.start, end: window.end },
    ];
  }
  return [{ start: previous.start, end: window.end }];
}

/**
 * The window's language and filters as conditions on the dimension columns,
 * values as parameters; the same on every table a report reads.
 */
export function dimConditionsOf(
  params: SqlParams,
  window: Pick<SiteAnalyticsWindow, "locale" | "filters">,
  alias = "",
): string[] {
  const col = (name: string) => (alias ? `${alias}.${name}` : name);
  const conditions: string[] = [];
  if (window.locale) {
    conditions.push(`${col("locale")} = ${params.add(window.locale)}`);
  }
  for (const filter of window.filters) {
    conditions.push(
      `${col(columnOf(filter.dim))} = ${params.add(filter.value)}`,
    );
  }
  return conditions;
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
  const ranges = bucketRanges(window, scope);
  const first = ranges[0];
  const last = ranges[ranges.length - 1];
  const conditions = [
    `${col("bucket")} >= ${params.add(first.start)}::timestamptz`,
    `${col("bucket")} < ${params.add(last.end)}::timestamptz`,
  ];
  if (ranges.length > 1) {
    conditions.push(
      `NOT (${col("bucket")} >= ${params.add(first.end)}::timestamptz ` +
        `AND ${col("bucket")} < ${params.add(last.start)}::timestamptz)`,
    );
  }
  return [...conditions, ...dimConditionsOf(params, window, alias)].join(
    " AND ",
  );
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

/**
 * Most counted first. Ties go by `tie` (a plain string comparison) when it is
 * given, else in the given order: an order that came out of a `GROUP BY` is
 * whatever plan produced it, so a list built from one gives `tie`, and the
 * answer is the same whichever table the rows were read from.
 */
export function byCountDesc<T>(
  items: T[],
  count: (item: T) => number,
  tie?: (item: T) => string,
): T[] {
  return items
    .map((item, index) => ({ item, index }))
    .sort(
      (a, b) =>
        count(b.item) - count(a.item) ||
        (tie ? compareText(tie(a.item), tie(b.item)) : 0) ||
        a.index - b.index,
    )
    .map(({ item }) => item);
}

/** Code-unit order, the same in every runtime and for every database collation. */
export function compareText(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

export type Count = string | number | null;
export const num = (value: Count | undefined): number =>
  Number(value ?? 0) || 0;
