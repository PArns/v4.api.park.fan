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
import { byCountDesc } from "./site-analytics.query";

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

/**
 * A breakdown over event rows: the events it counts, the properties that
 * name a row, and the values the rows must have (`*`: any but empty) or must
 * not have. The event reports declare theirs as data, so a new event is one
 * line.
 */
export interface EventListSpec {
  events: readonly string[];
  props: readonly string[];
  where?: Readonly<Record<string, string | readonly string[]>>;
  not?: Readonly<Record<string, string>>;
}

/** Whether a row is one of a spec's events with the property values it asks for. */
export function matchesSpec(
  spec: EventListSpec,
  row: Pick<EventPropRow, "name" | "props">,
): boolean {
  if (!spec.events.includes(row.name)) return false;
  for (const [prop, wanted] of Object.entries(spec.where ?? {})) {
    const value = row.props[prop] ?? "";
    const ok =
      wanted === "*"
        ? value !== ""
        : ([] as string[]).concat(wanted).includes(value);
    if (!ok) return false;
  }
  for (const [prop, unwanted] of Object.entries(spec.not ?? {})) {
    if ((row.props[prop] ?? "") === unwanted) return false;
  }
  return true;
}

/** The label of an empty property value in a breakdown. */
export const NO_VALUE = "(none)";

/** The key a breakdown counts a row under: its values of `props`, empty ones `(none)`. */
export function tallyKey(
  row: Pick<EventPropRow, "props">,
  props: readonly string[],
): string {
  return JSON.stringify(props.map((p) => row.props[p] || NO_VALUE));
}

/** A breakdown's counts by {@link tallyKey} as rows named by their properties, most first. */
export function tallyRows(
  counts: ReadonlyMap<string, number> | undefined,
  props: readonly string[],
): Array<Record<string, string> & { count: number }> {
  return byCountDesc(
    [...(counts ?? new Map<string, number>())].map(([k, count]) => {
      const values = JSON.parse(k) as string[];
      const row = {
        ...Object.fromEntries(props.map((p, i) => [p, values[i]])),
        count,
      } as Record<string, string> & { count: number };
      return { k, row };
    }),
    (entry) => entry.row.count,
    (entry) => entry.k,
  ).map((entry) => entry.row);
}

/** Every breakdown of `specs` over the rows. */
export function tallyLists<K extends string>(
  rows: readonly Pick<EventPropRow, "name" | "props" | "count">[],
  specs: Readonly<Record<K, EventListSpec>>,
): Record<K, Array<Record<string, string> & { count: number }>> {
  const counts = new Map<K, Map<string, number>>();
  for (const row of rows) {
    for (const [key, spec] of Object.entries(specs) as Array<
      [K, EventListSpec]
    >) {
      if (!matchesSpec(spec, row)) continue;
      const k = tallyKey(row, spec.props);
      const tally = counts.get(key) ?? new Map<string, number>();
      tally.set(k, (tally.get(k) ?? 0) + row.count);
      counts.set(key, tally);
    }
  }
  return Object.fromEntries(
    (Object.entries(specs) as Array<[K, EventListSpec]>).map(([key, spec]) => [
      key,
      tallyRows(counts.get(key), spec.props),
    ]),
  ) as Record<K, Array<Record<string, string> & { count: number }>>;
}

/** Every total of `specs` over the rows: the count of the rows each matches. */
export function tallyTotals<K extends string>(
  rows: readonly Pick<EventPropRow, "name" | "props" | "count">[],
  specs: Readonly<Record<K, EventListSpec>>,
): Record<K, number> {
  const totals = Object.fromEntries(
    Object.keys(specs).map((key) => [key, 0]),
  ) as Record<K, number>;
  for (const row of rows) {
    for (const [key, spec] of Object.entries(specs) as Array<
      [K, EventListSpec]
    >) {
      if (matchesSpec(spec, row)) totals[key] += row.count;
    }
  }
  return totals;
}
