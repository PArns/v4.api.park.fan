import { EVENT_CUBOIDS, rowsOf } from "./site-analytics.rollup";
import {
  SqlParams,
  num,
  periodOf,
  periodText,
  type Count,
  type SiteAnalyticsWindow,
} from "./site-analytics.query";
import type { SiteReportRunner } from "./site-analytics-report-runner";

/** One event's count for one combination of its properties, optionally per period and page type. */
export interface EventPropRow {
  name: string;
  props: Record<string, string>;
  count: number;
  /** The row's hour (an hourly window) or UTC day, as the series' points name them. */
  period?: string;
  /** The UTC day of `period`. */
  day?: string;
  pageType?: string;
}

/**
 * The properties of some events summed over a report's window, one row per
 * name and combination, and per period (the hour in an hourly window, else
 * the UTC day) or page type when asked: one
 * statement over `rowsOf`, so whole rolled-up days come from the daily rows
 * (the name-and-properties cuboid, with the page type the one that has it).
 * What the event reports over one feature's events read (the trip planner,
 * Parky).
 */
export async function readEventProps(
  runner: SiteReportRunner,
  window: SiteAnalyticsWindow,
  names: readonly string[],
  options: { period?: boolean; pageType?: boolean } = {},
): Promise<EventPropRow[]> {
  const params = new SqlParams();
  const namesParam = params.add(names);
  const needed = [
    "name",
    "props_key",
    ...(options.pageType ? ["page_type"] : []),
  ];
  const source = await rowsOf(
    (s, cuboid) => runner.rolledDays(s, cuboid),
    "events",
    params,
    window,
    "current",
    needed,
    { alias: "e", collapse: !options.period },
  );
  const extra = [
    ...(options.period ? [`${periodOf(window.hourly, "e")} AS period`] : []),
    ...(options.pageType ? ["e.page_type"] : []),
  ];
  const groups = [
    "name",
    "props",
    ...(options.period ? ["period"] : []),
    ...(options.pageType ? ["page_type"] : []),
  ].join(", ");
  const rows: Array<{
    name: string;
    props: Record<string, string> | null;
    period?: unknown;
    page_type?: string;
    count: Count;
  }> = await runner.query(
    `SELECT ${groups}, SUM(count) AS count
       FROM (SELECT e.name, e.props, ${[...extra, "e.count"].join(", ")}
               FROM ${source.from}
              WHERE e.name = ANY(${namesParam}::text[]) AND ${source.where}) s
      GROUP BY ${groups}`,
    params.values,
  );
  return rows.map((row) => {
    const period =
      row.period === undefined ? null : periodText(row.period, window.hourly);
    return {
      name: row.name,
      props: row.props ?? {},
      count: num(row.count),
      ...(period !== null && { period, day: period.slice(0, 10) }),
      ...(row.page_type !== undefined && { pageType: row.page_type }),
    };
  });
}

/** The daily roll-up of the event names alone. */
const NAME_CUBOID = EVENT_CUBOIDS.find(
  (c) => c.dims.length === 1 && c.dims[0] === "name",
)?.id;

/**
 * The first UTC day any of `names` was counted, in the hourly rows or the
 * daily roll-up of names alone; null when none was.
 */
export async function firstDayOf(
  runner: SiteReportRunner,
  names: readonly string[],
): Promise<string | null> {
  const [row]: Array<{ first: string | null }> = await runner.query(
    `SELECT to_char(min(day) AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS first
       FROM (SELECT min(bucket) AS day FROM site_events WHERE name = ANY($1::text[])
             UNION ALL
             SELECT min(bucket) FROM site_events_daily
              WHERE cuboid = $2 AND name = ANY($1::text[])) s`,
    [names, NAME_CUBOID],
  );
  return row?.first ?? null;
}
