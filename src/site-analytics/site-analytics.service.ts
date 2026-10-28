import {
  BadRequestException,
  Inject,
  Injectable,
  Logger,
} from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import {
  ALL_LOCALES_SCOPE,
  CROSSTAB_REST,
  EVENT_DIMS,
  MAX_CROSSTAB_SIDE,
  MAX_EVENT_DETAIL_ITEMS,
  PAGE_DIMS,
  REALTIME_ACTIVE_MINUTES,
  REALTIME_SERIES_MINUTES,
  SITE_ANALYTICS_KEYS as KEYS,
  SITE_DEVICES,
  VISITOR_KEY_GRACE_SECONDS,
  VISIT_DIMS,
  isVisitDim,
  hourKey,
  minuteKey,
  type BreakdownDim,
  type FilterDim,
  type SiteLocale,
  type VisitorDim,
} from "./site-analytics.constants";
import { propsOfKey } from "./site-analytics.hits";
import { WIDE_COLUMNS, rowsOf } from "./site-analytics.rollup";
import {
  CROSSTAB_AXIS_PATTERN,
  SqlParams,
  averageSeconds,
  bounceRate,
  byCountDesc,
  columnOf,
  compareText,
  daysBetween,
  hoursOf,
  num,
  periodOf,
  periodStartOf,
  periodText,
  rate,
  refuseFiltersBeyond,
  seriesPeriods,
  siteAnalyticsWindow,
  visitFilters,
  type Count,
  type SeriesPeriod,
  type SiteAnalyticsQuery,
  type SiteAnalyticsWindow,
} from "./site-analytics.query";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import { SiteReportRunner } from "./site-analytics-report-runner";
import { SiteEntityNames } from "./site-entity-names.service";
import { visitorBasisOf, type VisitorFilter } from "./visitor-dims.util";
import type {
  SiteBreakdownDto,
  SiteCrosstabDto,
  SiteEventDetailDto,
  SiteEventsDto,
  SiteOverviewDto,
  SiteSearchDto,
  SiteTotalsDto,
  SiteTrafficCrosstabDto,
} from "./dto/site-analytics.dto";

/** The search events and what the search report calls each. */
export const SEARCH_EVENTS = {
  opened: "search_opened",
  typed: "search_typed",
  resultClicks: "search_result_clicked",
  browseClicks: "search_browse_clicked",
  noResults: "search_no_results",
  viewAll: "search_view_all",
} as const;

/** The position buckets the frontend sends, in their order. */
const SEARCH_POSITIONS = ["1", "2", "3", "4-5", "6-10", "11+"];

/** The label of an empty value in a crosstab or a property list. */
const NONE = "(none)";

/**
 * The read side of the site statistic: the admin's reports. Public so the
 * admin's MCP server can offer the same answers as tools; every method
 * returns exactly the shape its endpoint serves. Every report takes a window
 * of UTC days (the last `days`, or `from`–`to`), a series interval, a
 * language and `filter=<dim>:<value>` pairs, and is a GROUP BY over
 * `site_pageviews` or `site_events`, or over their daily roll-ups for the whole
 * days that have one (`rowsOf`), with the same answer; the series fold the
 * hours or days the SQL groups into weeks and months. Each report runs through
 * the {@link SiteReportRunner}: a limit on reports at once and a timeout on
 * each statement, 503 past either. docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteAnalyticsService {
  private readonly logger = new Logger(SiteAnalyticsService.name);

  constructor(
    private readonly runner: SiteReportRunner,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly blocklist: SiteAnalyticsBlocklist,
    private readonly entityNames: SiteEntityNames,
  ) {}

  /** The days of a cuboid the nightly job has rolled up, for {@link rowsOf}. */
  private readonly rolled = (
    source: Parameters<SiteReportRunner["rolledDays"]>[0],
    cuboid: number,
  ) => this.runner.rolledDays(source, cuboid);

  /**
   * Page views, visits, visitors, events, views per visit, bounce rate and
   * the average visit and time on page over the window and the one before,
   * a point per period of the window's interval with all of them, and the
   * realtime minute. Visitors are unique per UTC day and are counted per
   * language and per value of country, device, browser, OS and channel, so
   * under one filter on one of those they follow it, and under any other
   * filter they are null rather than a guess ({@link visitorBasisOf}); an
   * hourly series has none under a filter. Under a filter on a
   * visit dimension only the visit figures can follow it: page views,
   * events, views per visit and time on page are null.
   */
  async overview(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteOverviewDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      const visitorBasis = visitorBasisOf(window.filters);
      const visitsOnly = visitFilters(window).length > 0;
      const period = periodOf(window.hourly);
      const traffic = new SqlParams();
      const events = new SqlParams();
      const pageviewRows = await rowsOf(
        this.rolled,
        "pageviews",
        traffic,
        window,
        "both",
        [],
      );
      const eventSource = visitsOnly
        ? null
        : await rowsOf(this.rolled, "events", events, window, "both", []);
      const [trafficRows, eventRows, visitors, realtime] = await Promise.all([
        this.runner.query(
          `SELECT ${period} AS period, SUM(views) AS views,
                SUM(seconds) AS seconds, SUM(visits) AS visits,
                SUM(engaged) AS engaged, SUM(visit_seconds) AS visit_seconds
           FROM ${pageviewRows.from}
          WHERE ${pageviewRows.where}
          GROUP BY 1`,
          traffic.values,
        ) as Promise<Array<{ period: string } & Record<TrafficMeasure, Count>>>,
        eventSource
          ? (this.runner.query(
              `SELECT ${period} AS period, SUM(count) AS events
               FROM ${eventSource.from}
              WHERE ${eventSource.where}
              GROUP BY 1`,
              events.values,
            ) as Promise<Array<{ period: string; events: Count }>>)
          : Promise.resolve([] as Array<{ period: string; events: Count }>),
        !visitorBasis
          ? Promise.resolve(null)
          : visitorBasis.filter
            ? this.dimVisitors(window, visitorBasis.filter, now)
            : this.visitors(window, now),
        this.realtime(window.locale, now),
      ]);

      for (const row of [...trafficRows, ...eventRows]) {
        row.period = periodText(row.period, window.hourly) as string;
      }
      const sums = (from: string, to: string, at?: (p: string) => boolean) => {
        const sum: Record<TrafficMeasure | "events", number> = {
          views: 0,
          seconds: 0,
          visits: 0,
          engaged: 0,
          visit_seconds: 0,
          events: 0,
        };
        const inside = (point: string) =>
          point.slice(0, 10) >= from &&
          point.slice(0, 10) <= to &&
          (!at || at(point));
        for (const row of trafficRows) {
          if (!inside(row.period)) continue;
          for (const key of TRAFFIC_MEASURES) sum[key] += num(row[key]);
        }
        for (const row of eventRows)
          if (inside(row.period)) sum.events += num(row.events);
        return sum;
      };
      const figures = (
        sum: ReturnType<typeof sums>,
        visitorCount: number | null,
      ): SiteTotalsDto => ({
        pageviews: visitsOnly ? null : sum.views,
        visits: sum.visits,
        visitors: visitorCount,
        events: visitsOnly ? null : sum.events,
        viewsPerVisit: visitsOnly
          ? null
          : sum.visits > 0
            ? Math.round((sum.views / sum.visits) * 100) / 100
            : 0,
        bounceRate: bounceRate(sum.visits, sum.engaged),
        avgVisitSeconds: averageSeconds(sum.visit_seconds, sum.visits),
        avgTimeOnPageSeconds: visitsOnly
          ? null
          : averageSeconds(sum.seconds, sum.views),
      });
      const periods = seriesPeriods(window, now);

      return {
        ...echo(window),
        totals: figures(
          sums(window.from, window.to),
          visitors ? sumVisitors(visitors.days, window.from, window.to) : null,
        ),
        previous: window.previous
          ? figures(
              sums(window.previous.from, window.previous.to),
              visitors
                ? sumVisitors(
                    visitors.days,
                    window.previous.from,
                    window.previous.to,
                  )
                : null,
            )
          : null,
        series: periods.map((point) => {
          const inPeriod = (p: string) =>
            periodStartOf(p, window.interval) === point.day;
          return {
            ...pointOf(point),
            ...figures(
              sums(window.from, window.to, inPeriod),
              visitors ? periodVisitors(visitors, window, point) : null,
            ),
          };
        }),
        realtime,
      };
    });
  }

  /**
   * The values of one dimension, most counted first, with the total over all
   * values. A page dimension counts page views with their average time on
   * the page, `path` also the visits that ended there (`exits`, `exitRate`).
   * A visit dimension (`entry`, `referrer`, `channel`), or any
   * dimension under a filter on one, counts visits started (`measure:
   * "visits"`, a view being a visit) from the rows that record visits, with
   * a bounce rate and the average visit: `dim=path` under `channel:search`
   * lists where search visits entered, `dim=country` where they came from.
   * `exit` counts the visits that ended on each page (`measure: "exits"`). A
   * direct visit has no referrer and none is listed. A breakdown by country,
   * device, browser, OS or channel, unfiltered or under a filter on that
   * dimension, also lists each value's `visitors`.
   */
  async breakdown(
    query: SiteAnalyticsQuery & { dim: BreakdownDim; limit: number },
    now = new Date(),
  ): Promise<SiteBreakdownDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      const exits = query.dim === "exit";
      const column = query.dim === "exit" ? "path" : columnOf(query.dim);
      const visitScope =
        !exits && (isVisitDim(query.dim) || visitFilters(window).length > 0);
      const measure = exits ? "exits" : visitScope ? "visits" : "views";
      const counted = exits ? "exits" : visitScope ? "visits" : "views";
      const params = new SqlParams();
      const source = await rowsOf(
        this.rolled,
        "pageviews",
        params,
        window,
        "current",
        [column],
        // Summing a hundred thousand paths first, and again over the
        // branches, costs more CPU than the one hash table it saves.
        { collapse: !WIDE_COLUMNS.includes(column) },
      );
      // Only the sums the figures of this mode read.
      const measures = exits
        ? ["exits"]
        : visitScope
          ? ["visits", "engaged", "visit_seconds"]
          : [
              "views",
              "seconds",
              "visits",
              ...(column === "path" ? ["exits"] : []),
            ];
      const nonEmpty = ["entry", "referrer"].includes(query.dim)
        ? `AND ${column} <> ''`
        : "";
      const rows: Array<
        Partial<Record<(typeof measures)[number], Count>> & {
          value: string;
          total: Count;
          total_visits: Count;
        }
      > = await this.runner.query(
        `WITH per_value AS (
         SELECT ${column} AS value,
                ${measures.map((m) => `SUM(${m}) AS ${m}`).join(", ")}
           FROM ${source.from}
          WHERE ${source.where} ${nonEmpty}
          GROUP BY 1
          HAVING SUM(${counted}) > 0
       )
       SELECT value, ${measures.join(", ")},
              SUM(${counted}) OVER () AS total,
              ${measures.includes("visits") ? "SUM(visits) OVER ()" : "0"} AS total_visits
         FROM per_value
        ORDER BY ${counted} DESC, value ASC
        LIMIT ${params.add(query.limit)}`,
        params.values,
      );
      const total = num(rows[0]?.total);
      const visitors = await this.valueVisitors(
        window,
        query.dim,
        rows.map((row) => row.value),
        now,
      );
      return {
        ...echo(window),
        dim: query.dim,
        measure,
        total: {
          views: total,
          visits: exits ? total : num(rows[0]?.total_visits),
        },
        items: rows.map((row) => {
          const visits = num(row.visits);
          if (exits) {
            const ended = num(row.exits);
            return {
              value: row.value,
              views: ended,
              visits: ended,
              share: rate(ended, total),
            };
          }
          if (visitScope) {
            return {
              value: row.value,
              views: visits,
              visits,
              share: rate(visits, total),
              bounceRate: bounceRate(visits, num(row.engaged)),
              avgVisitSeconds: averageSeconds(num(row.visit_seconds), visits),
              ...(visitors && { visitors: visitors.get(row.value) ?? 0 }),
            };
          }
          const views = num(row.views);
          return {
            value: row.value,
            views,
            visits,
            share: rate(views, total),
            avgTimeOnPageSeconds: averageSeconds(num(row.seconds), views),
            ...(visitors && { visitors: visitors.get(row.value) ?? 0 }),
            ...(query.dim === "path" && {
              exits: num(row.exits),
              exitRate: Math.min(1, rate(num(row.exits), views)),
            }),
          };
        }),
      };
    });
  }

  /**
   * Every event name seen in the window or the one before, and every blocked
   * name, with its count, the previous window's (null over
   * `MAX_COMPARABLE_DAYS`) and a series; most counted first.
   */
  async events(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteEventsDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      refuseFiltersBeyond(window, "The events report", EVENT_DIMS);
      const params = new SqlParams();
      const source = await rowsOf(
        this.rolled,
        "events",
        params,
        window,
        "both",
        ["name"],
      );
      const [rows, blocked] = await Promise.all([
        this.runner.query(
          `SELECT name, ${periodOf(window.hourly)} AS period, SUM(count) AS count
           FROM ${source.from}
          WHERE ${source.where}
          GROUP BY 1, 2`,
          params.values,
        ) as Promise<Array<{ name: string; period: string; count: Count }>>,
        this.blocklist.names(),
      ]);
      const perName = new Map<string, Map<string, number>>();
      for (const name of blocked) perName.set(name, new Map());
      for (const row of rows) {
        const periods = perName.get(row.name) ?? new Map<string, number>();
        periods.set(
          periodText(row.period, window.hourly) as string,
          num(row.count),
        );
        perName.set(row.name, periods);
      }
      const points = seriesPeriods(window, now);
      const items = [...perName.entries()].map(([name, periods]) => {
        let count = 0;
        let previous = 0;
        const current = new Map<string, number>();
        for (const [period, value] of periods) {
          if (period.slice(0, 10) >= window.from) {
            count += value;
            addTo(current, periodStartOf(period, window.interval), value);
          } else previous += value;
        }
        return {
          name,
          count,
          previous: window.previous ? previous : null,
          blocked: blocked.has(name),
          series: points.map((point) => ({
            ...pointOf(point),
            count: current.get(point.day) ?? 0,
          })),
        };
      });
      items.sort(
        (a, b) =>
          b.count - a.count ||
          (b.previous ?? 0) - (a.previous ?? 0) ||
          (a.name < b.name ? -1 : 1),
      );
      return { ...echo(window), items };
    });
  }

  /**
   * One event: its total, every property key seen with its top values, the
   * most frequent property combinations, the kinds of page and devices it
   * came from, the entities it was about (a merged-away one under its
   * survivor) and its series. Three statements in parallel.
   */
  async event(
    name: string,
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteEventDetailDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      refuseFiltersBeyond(window, "The event report", EVENT_DIMS);
      const scan = new SqlParams();
      const nameParam = scan.add(name);
      const props = new SqlParams();
      const propsName = props.add(name);
      const entities = new SqlParams();
      const entityName = entities.add(name);
      const rows = (
        params: SqlParams,
        needed: string[],
        options: { alias?: string; collapse?: boolean } = {},
      ) =>
        rowsOf(
          this.rolled,
          "events",
          params,
          window,
          "current",
          needed,
          options,
        );
      // The grouping sets draw the series, so they read the days; the
      // property values and the entities do not.
      const [scanRows, propRowsSource, entitySource] = await Promise.all([
        rows(scan, ["name", "page_type", "device", "props_key"]),
        rows(props, ["name", "props_key"], { alias: "e", collapse: true }),
        rows(entities, ["name", "entity"], { alias: "e", collapse: true }),
      ]);
      const [grouped, propRows, entityRows, blocked] = await Promise.all([
        this.runner.query(
          // Every grouped column is NOT NULL, so a NULL says which grouping
          // set produced the row, and the empty set gives the total.
          `SELECT page_type, device, props_key, period, SUM(count) AS count
           FROM (SELECT page_type, device, props_key, count,
                        ${periodOf(window.hourly)} AS period
                   FROM ${scanRows.from}
                  WHERE name = ${nameParam}
                    AND ${scanRows.where}) s
          GROUP BY GROUPING SETS ((page_type), (device), (props_key),
                                  (period), ())`,
          scan.values,
        ) as Promise<
          Array<{
            page_type: string | null;
            device: string | null;
            props_key: string | null;
            period: string | null;
            count: Count;
          }>
        >,
        this.runner.query(
          `SELECT kv.key, kv.value, SUM(e.count) AS count
           FROM ${propRowsSource.from}
          CROSS JOIN LATERAL jsonb_each_text(e.props) AS kv
          WHERE e.name = ${propsName} AND ${propRowsSource.where}
          GROUP BY 1, 2`,
          props.values,
        ) as Promise<Array<{ key: string; value: string; count: Count }>>,
        this.runner.query(
          `SELECT COALESCE(m.winner_id, e.entity)::text AS entity,
                SUM(e.count) AS count
           FROM ${entitySource.from}
           LEFT JOIN site_entity_merges m ON m.loser_id = e.entity
          WHERE e.name = ${entityName} AND e.entity IS NOT NULL
            AND ${entitySource.where}
          GROUP BY 1
          ORDER BY 2 DESC, 1 ASC
          LIMIT ${entities.add(MAX_EVENT_DETAIL_ITEMS)}`,
          entities.values,
        ) as Promise<Array<{ entity: string; count: Count }>>,
        this.blocklist.names(),
      ]);

      for (const row of grouped)
        row.period = periodText(row.period, window.hourly);
      const set = (key: "page_type" | "device" | "props_key" | "period") =>
        grouped.filter(
          (row) =>
            row[key] !== null &&
            (["page_type", "device", "props_key", "period"] as const).every(
              (other) => other === key || row[other] === null,
            ),
        );
      const valueCounts = (
        key: "page_type" | "device",
        known: readonly string[] = [],
      ) => {
        const found = new Map(
          set(key).map((row) => [row[key] as string, num(row.count)]),
        );
        // The known values first, in their order, then the others by name:
        // ties keep that order, whichever order the rows came in.
        const values = [
          ...known,
          ...[...found.keys()]
            .filter((v) => !known.includes(v))
            .sort(compareText),
        ];
        return byCountDesc(
          values.map((value) => ({ value, count: found.get(value) ?? 0 })),
          (item) => item.count,
        );
      };
      const total = grouped.find((row) =>
        (["page_type", "device", "props_key", "period"] as const).every(
          (key) => row[key] === null,
        ),
      );
      const periods = new Map<string, number>();
      for (const row of set("period")) {
        addTo(
          periods,
          periodStartOf(row.period as string, window.interval),
          num(row.count),
        );
      }
      const named = await this.entityNames.resolve(
        entityRows.map((row) => row.entity),
      );

      return {
        ...echo(window),
        name,
        blocked: blocked.has(name),
        total: num(total?.count),
        props: propsSummary(propRows),
        combinations: byCountDesc(
          set("props_key"),
          (row) => num(row.count),
          (row) => row.props_key as string,
        )
          .slice(0, MAX_EVENT_DETAIL_ITEMS)
          .map((row) => ({
            props: propsOfKey(row.props_key as string),
            count: num(row.count),
          })),
        byPageType: valueCounts("page_type"),
        byDevice: valueCounts("device", SITE_DEVICES),
        entities: entityRows.map((row) => ({
          id: row.entity,
          kind: named.get(row.entity)?.kind ?? null,
          name: named.get(row.entity)?.name ?? null,
          url: named.get(row.entity)?.url ?? null,
          count: num(row.count),
        })),
        series: seriesPeriods(window, now).map((point) => ({
          ...pointOf(point),
          count: periods.get(point.day) ?? 0,
        })),
      };
    });
  }

  /**
   * One event counted by two sides at once: any dimension, the day or one
   * property. "Which search, from which page" is `row=prop:surface` and
   * `col=page_type` over `search_opened`. Each side keeps its
   * {@link MAX_CROSSTAB_SIDE} largest values and folds the rest into
   * {@link CROSSTAB_REST}; the day keeps every day, in order.
   */
  async crosstab(
    name: string,
    query: SiteAnalyticsQuery & { row: string; col: string },
    now = new Date(),
  ): Promise<SiteCrosstabDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      refuseFiltersBeyond(window, "The crosstab", EVENT_DIMS);
      const params = new SqlParams();
      const nameParam = params.add(name);
      const axis = (side: string) => {
        if (side === "day") return periodOf(window.hourly);
        if (side.startsWith("prop:"))
          return `COALESCE(props->>${params.add(side.slice(5))}, '')`;
        if (!CROSSTAB_AXIS_PATTERN.test(side))
          throw new Error(`Unknown crosstab side ${side}`);
        return side;
      };
      const rowExpr = axis(query.row);
      const colExpr = axis(query.col);
      // The day is a bucket, not a column: only a crosstab without one can
      // sum the days of a window together first.
      const sides = [query.row, query.col].filter((side) => side !== "day");
      const source = await rowsOf(
        this.rolled,
        "events",
        params,
        window,
        "current",
        [
          "name",
          ...sides.map((side) =>
            side.startsWith("prop:") ? "props_key" : side,
          ),
        ],
        { collapse: sides.length === 2 },
      );
      const cells: CrosstabCell[] = await this.runner.query(
        `SELECT ${rowExpr} AS r, ${colExpr} AS c, SUM(count) AS count
         FROM ${source.from}
        WHERE name = ${nameParam} AND ${source.where}
        GROUP BY 1, 2`,
        params.values,
      );
      return {
        ...echo(window),
        name,
        row: query.row,
        col: query.col,
        ...foldCrosstab(cells, query.row, query.col, window),
      };
    });
  }

  /**
   * Traffic by two dimensions at once, as the event crosstab draws an event:
   * `metric=pageviews` over the page dimensions (and the day) of the page
   * views' rows, `metric=visits` over the visit dimensions and the first
   * page's dimensions of the visits' start rows. With `exit` on a side the
   * visits are those that ended, counted on their exit rows, whose page
   * dimensions are the last page's. "OS by device" is `row=os&col=device`,
   * "channel by country" `metric=visits&row=channel&col=country`.
   */
  async trafficCrosstab(
    query: SiteAnalyticsQuery & {
      row: string;
      col: string;
      metric: "pageviews" | "visits";
    },
    now = new Date(),
  ): Promise<SiteTrafficCrosstabDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      const visits = query.metric === "visits";
      const sides = visits
        ? [...PAGE_DIMS, ...VISIT_DIMS, "exit", "day"]
        : [...PAGE_DIMS, "day"];
      for (const side of [query.row, query.col]) {
        if (!sides.includes(side)) {
          throw new BadRequestException(
            `metric=${query.metric} takes ${sides.join(", ")} as a side`,
          );
        }
      }
      if (!visits) {
        refuseFiltersBeyond(window, "metric=pageviews", PAGE_DIMS);
      }
      const ended = query.row === "exit" || query.col === "exit";
      const counted = !visits ? "views" : ended ? "exits" : "visits";
      const params = new SqlParams();
      const axis = (side: string) =>
        side === "day"
          ? periodOf(window.hourly)
          : side === "exit"
            ? "path"
            : columnOf(side as FilterDim);
      const source = await rowsOf(
        this.rolled,
        "pageviews",
        params,
        window,
        "current",
        [query.row, query.col].filter((side) => side !== "day").map(axis),
        { collapse: ![query.row, query.col].includes("day") },
      );
      const cells: CrosstabCell[] = await this.runner.query(
        `SELECT ${axis(query.row)} AS r, ${axis(query.col)} AS c,
              SUM(${counted}) AS count
         FROM ${source.from}
        WHERE ${source.where} AND ${counted} > 0
        GROUP BY 1, 2`,
        params.values,
      );
      return {
        ...echo(window),
        metric: query.metric,
        row: query.row,
        col: query.col,
        ...foldCrosstab(cells, query.row, query.col, window),
      };
    });
  }

  /**
   * Which search is used, where and how well, over the `search_*` events:
   * per surface seen the openings, typed queries, clicks and dead ends with
   * their rates; openings by kind of page and device; what was clicked and
   * at which position; the openings per day. Three statements, each over only
   * the events its figures need.
   */
  async search(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteSearchDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      refuseFiltersBeyond(window, "The search report", EVENT_DIMS);
      // The openings are drawn by page type, device and day; the other
      // scopes need only their properties, summed over the window.
      const scope = async (names: readonly string[], drawn: boolean) => {
        const params = new SqlParams();
        const namesParam = params.add(names);
        const source = await rowsOf(
          this.rolled,
          "events",
          params,
          window,
          "current",
          ["name", "props_key", ...(drawn ? ["page_type", "device"] : [])],
          { collapse: !drawn },
        );
        const sql = `SELECT name, COALESCE(props->>'surface', '') AS surface,
                          COALESCE(props->>'result', '') AS result,
                          COALESCE(props->>'item', '') AS item,
                          COALESCE(props->>'position', '') AS position,
                          ${drawn ? `page_type, device, ${periodOf(window.hourly)} AS period,` : ""}
                          count
                     FROM ${source.from}
                    WHERE name = ANY(${namesParam}::text[])
                      AND ${source.where}`;
        return { sql, params };
      };
      type Row = {
        name: string;
        surface: string | null;
        result: string | null;
        item: string | null;
        position: string | null;
        page_type: string | null;
        device: string | null;
        period: string | null;
        count: Count;
      };
      const [all, opened, clicks] = await Promise.all([
        scope(Object.values(SEARCH_EVENTS), false),
        scope([SEARCH_EVENTS.opened], true),
        scope([SEARCH_EVENTS.resultClicks, SEARCH_EVENTS.browseClicks], false),
      ]);
      const [surfaceRows, openedRows, clickRows]: Row[][] = await Promise.all([
        this.runner.query(
          `SELECT name, surface, SUM(count) AS count FROM (${all.sql}) s
          GROUP BY 1, 2`,
          all.params.values,
        ),
        this.runner.query(
          `SELECT name, surface, page_type, device, period, SUM(count) AS count
           FROM (${opened.sql}) s
          GROUP BY GROUPING SETS ((name, surface, page_type),
                                  (name, surface, device),
                                  (name, surface, period))`,
          opened.params.values,
        ),
        this.runner.query(
          `SELECT name, surface, result, item, position, SUM(count) AS count
           FROM (${clicks.sql}) s
          GROUP BY GROUPING SETS ((name, surface, result), (name, surface, item),
                                  (name, position))`,
          clicks.params.values,
        ),
      ]);

      const perSurface = new Map<string, number>();
      for (const row of surfaceRows) {
        perSurface.set(`${row.name}|${row.surface}`, num(row.count));
      }
      const of = (event: string, surface: string) =>
        perSurface.get(`${event}|${surface}`) ?? 0;
      const surfaceNames = byCountDesc(
        [...new Set(surfaceRows.map((row) => row.surface ?? ""))].filter(
          Boolean,
        ),
        (surface) => of(SEARCH_EVENTS.opened, surface),
        (surface) => surface,
      );
      const surfaces = surfaceNames.map((surface) => {
        const openedCount = of(SEARCH_EVENTS.opened, surface);
        const typed = of(SEARCH_EVENTS.typed, surface);
        const resultClicks = of(SEARCH_EVENTS.resultClicks, surface);
        const browseClicks = of(SEARCH_EVENTS.browseClicks, surface);
        const noResults = of(SEARCH_EVENTS.noResults, surface);
        return {
          surface,
          opened: openedCount,
          typed,
          resultClicks,
          browseClicks,
          noResults,
          viewAll: of(SEARCH_EVENTS.viewAll, surface),
          // The search page counts an opening per visit to it, and a click on
          // it may come from a list opened elsewhere: a rate past 1 means "every
          // opening led to a click", so it stops there.
          clickRate: Math.min(
            1,
            rate(resultClicks + browseClicks, openedCount),
          ),
          noResultRate: rate(noResults, typed),
        };
      });

      const devices = new Map(
        openedRows
          .filter((row) => row.device !== null)
          .map((row) => [`${row.surface}|${row.device}`, num(row.count)]),
      );
      const series = new Map<string, number>();
      for (const row of openedRows) {
        if (row.period === null) continue;
        const start = periodStartOf(
          periodText(row.period, window.hourly) as string,
          window.interval,
        );
        addTo(series, `${start}|${row.surface}`, num(row.count));
      }
      const positions = new Map(
        clickRows
          .filter(
            (row) =>
              row.position !== null &&
              row.surface === null &&
              row.name === SEARCH_EVENTS.resultClicks,
          )
          .map((row) => [row.position as string, num(row.count)]),
      );
      const positionNames = [
        ...SEARCH_POSITIONS,
        ...[...positions.keys()]
          .filter((p) => p !== "" && !SEARCH_POSITIONS.includes(p))
          .sort(compareText),
      ];
      const desc = <T>(
        items: T[],
        count: (item: T) => number,
        tie: (item: T) => string,
      ) => byCountDesc(items, count, tie);

      return {
        ...echo(window),
        surfaces,
        bySurfaceAndPageType: desc(
          openedRows
            .filter((row) => row.page_type !== null && row.surface)
            .map((row) => ({
              surface: row.surface as string,
              pageType: row.page_type as string,
              opened: num(row.count),
            })),
          (item) => item.opened,
          (item) => `${item.surface}|${item.pageType}`,
        ),
        bySurfaceAndDevice: surfaceNames.flatMap((surface) =>
          SITE_DEVICES.map((device) => ({
            surface,
            device,
            opened: devices.get(`${surface}|${device}`) ?? 0,
          })),
        ),
        resultTypes: desc(
          clickRows
            .filter(
              (row) =>
                row.name === SEARCH_EVENTS.resultClicks &&
                row.result !== null &&
                row.surface,
            )
            .map((row) => ({
              surface: row.surface as string,
              result: row.result || NONE,
              clicks: num(row.count),
            })),
          (item) => item.clicks,
          (item) => `${item.surface}|${item.result}`,
        ),
        positions: positionNames.map((position) => ({
          position,
          clicks: positions.get(position) ?? 0,
        })),
        browseItems: desc(
          clickRows
            .filter(
              (row) =>
                row.name === SEARCH_EVENTS.browseClicks &&
                row.item !== null &&
                row.surface,
            )
            .map((row) => ({
              surface: row.surface as string,
              item: row.item || NONE,
              clicks: num(row.count),
            })),
          (item) => item.clicks,
          (item) => `${item.surface}|${item.item}`,
        ),
        series: seriesPeriods(window, now).flatMap((point) =>
          surfaceNames.map((surface) => ({
            ...pointOf(point),
            surface,
            opened: series.get(`${point.day}|${surface}`) ?? 0,
          })),
        ),
      };
    });
  }

  /**
   * Unique visitors per day over both windows (the nightly snapshot's rows,
   * and for the days it has not written yet the HyperLogLog still in Redis),
   * and for an hourly series per hour of its days (the snapshot's rows, and
   * for the hours it has not written yet the hour's HyperLogLog). An hour no
   * row and no key covers stays unknown. With Redis down the live ones read
   * zero.
   */
  private async visitors(
    window: SiteAnalyticsWindow,
    now: Date,
  ): Promise<Visitors> {
    const scope = window.locale ?? ALL_LOCALES_SCOPE;
    const today = utcDay(now);
    const first = window.previous?.from ?? window.from;
    const hourPoints = window.hourly
      ? daysBetween(window.from, window.to).flatMap((day) => hoursOf(day, now))
      : [];
    const [rows, hourRows]: [
      Array<{ day: string; visitors: Count }>,
      Array<{ hour: string; visitors: Count }>,
    ] = await Promise.all([
      this.runner.query(
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, visitors
           FROM site_visitor_days
          WHERE scope = $1 AND day BETWEEN $2::date AND $3::date`,
        [scope, first, window.to],
      ),
      hourPoints.length === 0
        ? Promise.resolve([])
        : this.runner.query(
            `SELECT to_char(hour AT TIME ZONE 'UTC',
                            'YYYY-MM-DD"T"HH24:00:00"Z"') AS hour, visitors
               FROM site_visitor_hours
              WHERE scope = $1 AND hour >= $2::timestamptz
                AND hour < $3::timestamptz`,
            [scope, window.start, window.end],
          ),
    ]);
    const days = new Map(rows.map((row) => [row.day, num(row.visitors)]));
    const hours = new Map(hourRows.map((row) => [row.hour, num(row.visitors)]));

    const graceDays = Math.ceil(VISITOR_KEY_GRACE_SECONDS / 86_400);
    const liveFrom = addIsoDays(today, -graceDays);
    const live = daysBetween(liveFrom, today).filter(
      (day) => day >= first && day <= window.to && !days.has(day),
    );
    const liveHours = hourPoints.filter(
      (hour) => !hours.has(hour) && hour.slice(0, 10) >= liveFrom,
    );
    // A past hour nobody visited has no key: it reads zero only when its
    // day's key says the day was counted.
    const liveHourDays = [
      ...new Set(liveHours.map((hour) => hour.slice(0, 10))),
    ].filter((day) => day < today);
    try {
      const pipeline = this.redis.pipeline();
      for (const day of live) pipeline.pfcount(KEYS.visitors(day, scope));
      for (const day of liveHourDays)
        pipeline.exists(KEYS.visitors(day, scope));
      for (const hour of liveHours) {
        pipeline.pfcount(KEYS.hourVisitors(hourKey(new Date(hour)), scope));
      }
      const results = (await pipeline.exec()) ?? [];
      live.forEach((day, i) => {
        const count = num(results[i]?.[1] as Count);
        if (count > 0) days.set(day, count);
      });
      const counted = new Set(
        liveHourDays.filter(
          (_, i) => num(results[live.length + i]?.[1] as Count) === 1,
        ),
      );
      liveHours.forEach((hour, i) => {
        const count = num(
          results[live.length + liveHourDays.length + i]?.[1] as Count,
        );
        const day = hour.slice(0, 10);
        if (count > 0 || day === today || counted.has(day)) {
          hours.set(hour, count);
        }
      });
    } catch (error) {
      this.logger.warn(
        `Live visitor counts unavailable: ${(error as Error).message}`,
      );
    }
    return { days, hours, today };
  }

  /**
   * {@link visitors} under one value of country, device, browser, OS or
   * channel: the days only, from `site_visitor_dim_days` and, for the days it
   * has not written yet, the HyperLogLog still in Redis. The hours are not
   * counted by dimension, so an hourly series has none.
   */
  private async dimVisitors(
    window: SiteAnalyticsWindow,
    filter: VisitorFilter,
    now: Date,
  ): Promise<Visitors> {
    const byValue = await this.dimDays(
      window.locale ?? ALL_LOCALES_SCOPE,
      filter.dim,
      [filter.value],
      window.previous?.from ?? window.from,
      window.to,
      now,
    );
    return {
      days: byValue.get(filter.value) ?? new Map(),
      hours: null,
      today: utcDay(now),
    };
  }

  /**
   * The window's visitors per value of a breakdown's dimension (the days
   * summed, as the totals count them), for the listed values; null where the
   * filters leave no count ({@link visitorBasisOf}).
   */
  private async valueVisitors(
    window: SiteAnalyticsWindow,
    dim: BreakdownDim,
    values: string[],
    now: Date,
  ): Promise<Map<string, number> | null> {
    const basis = visitorBasisOf(window.filters, dim);
    if (!basis?.by) return null;
    const days = await this.dimDays(
      window.locale ?? ALL_LOCALES_SCOPE,
      basis.by,
      values,
      window.from,
      window.to,
      now,
    );
    return new Map(
      values.map((value) => [
        value,
        sumVisitors(days.get(value) ?? new Map(), window.from, window.to),
      ]),
    );
  }

  /**
   * Unique visitors per day with each of `values` of one dimension: the
   * snapshot's rows, and for the days it has not written yet (today, and
   * yesterday until it has run) the HyperLogLog in Redis. With Redis down the
   * live days read zero.
   */
  private async dimDays(
    scope: string,
    dim: VisitorDim,
    values: string[],
    from: string,
    to: string,
    now: Date,
  ): Promise<Map<string, Map<string, number>>> {
    const perValue = new Map(
      values.map((value) => [value, new Map<string, number>()]),
    );
    if (values.length === 0) return perValue;
    const rows: Array<{ day: string; value: string; visitors: Count }> =
      await this.runner.query(
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, value, visitors
           FROM site_visitor_dim_days
          WHERE scope = $1 AND dim = $2 AND value = ANY($3::text[])
            AND day BETWEEN $4::date AND $5::date`,
        [scope, dim, values, from, to],
      );
    for (const row of rows) {
      perValue.get(row.value)?.set(row.day, num(row.visitors));
    }
    const today = utcDay(now);
    const liveFrom = addIsoDays(
      today,
      -Math.ceil(VISITOR_KEY_GRACE_SECONDS / 86_400),
    );
    const live = daysBetween(liveFrom, today)
      .filter((day) => day >= from && day <= to)
      .flatMap((day) =>
        values
          .filter((value) => !perValue.get(value)?.has(day))
          .map((value) => ({ day, value })),
      );
    if (live.length === 0) return perValue;
    try {
      const pipeline = this.redis.pipeline();
      for (const { day, value } of live) {
        pipeline.pfcount(KEYS.visitorDim(day, scope, dim, value));
      }
      const results = (await pipeline.exec()) ?? [];
      live.forEach(({ day, value }, i) => {
        const count = num(results[i]?.[1] as Count);
        if (count > 0) perValue.get(value)?.set(day, count);
      });
    } catch (error) {
      this.logger.warn(
        `Live visitor counts unavailable: ${(error as Error).message}`,
      );
    }
    return perValue;
  }

  /**
   * Distinct visitors over the last {@link REALTIME_ACTIVE_MINUTES} minutes
   * (one PFCOUNT over the minute keys counts their union) and page views per
   * minute over the last {@link REALTIME_SERIES_MINUTES}, for all languages or
   * one; filters do not apply. Zero with Redis down.
   */
  private async realtime(
    locale: SiteLocale | null,
    now: Date,
  ): Promise<SiteOverviewDto["realtime"]> {
    const scope = locale ?? ALL_LOCALES_SCOPE;
    const start = Math.floor(now.getTime() / 60_000) * 60_000;
    const minutes = Array.from(
      { length: REALTIME_SERIES_MINUTES },
      (_, i) => new Date(start - (REALTIME_SERIES_MINUTES - 1 - i) * 60_000),
    );
    const empty = {
      activeVisitors: 0,
      perMinute: minutes.map((minute) => ({
        minute: isoMinute(minute),
        pageviews: 0,
      })),
    };
    try {
      const active = minutes
        .slice(-REALTIME_ACTIVE_MINUTES)
        .map((minute) => KEYS.activeMinute(minuteKey(minute), scope));
      const [activeVisitors, counts] = await Promise.all([
        this.redis.pfcount(...active),
        this.redis.mget(
          ...minutes.map((minute) =>
            KEYS.pageviewsMinute(minuteKey(minute), scope),
          ),
        ),
      ]);
      return {
        activeVisitors: num(activeVisitors),
        perMinute: minutes.map((minute, i) => ({
          minute: isoMinute(minute),
          pageviews: num(counts[i]),
        })),
      };
    } catch (error) {
      this.logger.warn(
        `Realtime figures unavailable: ${(error as Error).message}`,
      );
      return empty;
    }
  }
}

/**
 * The fields every report carries: the window it echoes, and `hourlyFrom`, the
 * day from which the figures that needed hourly rows are complete. Read after
 * the statements have run, since they are what tell the window's `reach`.
 */
export function echo(window: SiteAnalyticsWindow) {
  return {
    from: window.from,
    to: window.to,
    days: window.days,
    interval: window.interval,
    locale: window.locale,
    filters: window.filters.map((f) => `${f.dim}:${f.value}`),
    hourlyFrom: window.reach.hourlyFrom,
  };
}

/** A series point's own fields: its start, and `partial` only when it is. */
function pointOf(point: SeriesPeriod): { day: string; partial?: true } {
  return point.partial ? { day: point.day, partial: true } : { day: point.day };
}

function addTo(map: Map<string, number>, key: string, count: number): void {
  map.set(key, (map.get(key) ?? 0) + count);
}

/** What the traffic figures sum over `site_pageviews`. */
const TRAFFIC_MEASURES = [
  "views",
  "seconds",
  "visits",
  "engaged",
  "visit_seconds",
] as const;
type TrafficMeasure = (typeof TRAFFIC_MEASURES)[number];

/**
 * Visitors per UTC day, and per hour when the series is hourly; `hours` is
 * null where hours are not counted (under a filter).
 */
interface Visitors {
  days: Map<string, number>;
  hours: Map<string, number> | null;
  today: string;
}

function sumVisitors(days: Map<string, number>, from: string, to: string) {
  let sum = 0;
  for (const [day, count] of days) if (day >= from && day <= to) sum += count;
  return sum;
}

/**
 * A series point's visitors: an hour's from the snapshot's row or its
 * HyperLogLog, zero for an hour of today nobody visited, null for an earlier
 * hour that was never counted (before the snapshot kept hours), and for a
 * day, week or month the sum of its days' visitors inside the window, as the
 * totals count them.
 */
function periodVisitors(
  visitors: Visitors,
  window: SiteAnalyticsWindow,
  point: SeriesPeriod,
): number | null {
  if (window.hourly) {
    if (!visitors.hours) return null;
    return (
      visitors.hours.get(point.day) ??
      (point.day.startsWith(visitors.today) ? 0 : null)
    );
  }
  if (window.interval === "day") return visitors.days.get(point.day) ?? 0;
  let sum = 0;
  for (const [day, count] of visitors.days) {
    if (
      day >= window.from &&
      day <= window.to &&
      periodStartOf(day, window.interval) === point.day
    ) {
      sum += count;
    }
  }
  return sum;
}

/** One cell of a crosstab as the SQL returns it. */
interface CrosstabCell {
  r: unknown;
  c: unknown;
  count: Count;
}

/**
 * A crosstab's cells folded to its sides: each side keeps its
 * {@link MAX_CROSSTAB_SIDE} largest values and folds the rest into
 * {@link CROSSTAB_REST}; the day keeps every period of the window's interval,
 * in order.
 */
function foldCrosstab(
  cells: CrosstabCell[],
  rowSide: string,
  colSide: string,
  window: SiteAnalyticsWindow,
): Pick<SiteCrosstabDto, "rows" | "cols" | "cells" | "total"> {
  const label = (value: unknown, side: string) =>
    side === "day"
      ? periodStartOf(
          periodText(value, window.hourly) as string,
          window.interval,
        )
      : value === "" || value === null
        ? NONE
        : String(value);
  const sideOf = (pick: (cell: CrosstabCell) => unknown, side: string) => {
    const totals = new Map<string, number>();
    for (const cell of cells)
      addTo(totals, label(pick(cell), side), num(cell.count));
    if (side === "day") {
      const kept = [...totals.keys()].sort();
      return { kept, fold: (value: string) => value };
    }
    const ranked = byCountDesc(
      [...totals.keys()],
      (key) => totals.get(key) ?? 0,
      (key) => key,
    );
    const kept = ranked.slice(0, MAX_CROSSTAB_SIDE);
    const keep = new Set(kept);
    if (ranked.length > kept.length) kept.push(CROSSTAB_REST);
    return {
      kept,
      fold: (value: string) => (keep.has(value) ? value : CROSSTAB_REST),
    };
  };
  const rows = sideOf((cell) => cell.r, rowSide);
  const cols = sideOf((cell) => cell.c, colSide);
  const folded = new Map<string, { row: string; col: string; count: number }>();
  let total = 0;
  for (const cell of cells) {
    const row = rows.fold(label(cell.r, rowSide));
    const col = cols.fold(label(cell.c, colSide));
    const key = `${row}\u0000${col}`;
    const entry = folded.get(key) ?? { row, col, count: 0 };
    entry.count += num(cell.count);
    total += num(cell.count);
    folded.set(key, entry);
  }
  // In the order of the sides, not of the rows the database happened to give.
  const at = (kept: string[]) => new Map(kept.map((key, i) => [key, i]));
  const rowAt = at(rows.kept);
  const colAt = at(cols.kept);
  return {
    rows: rows.kept,
    cols: cols.kept,
    cells: [...folded.values()].sort(
      (a, b) =>
        (rowAt.get(a.row) ?? 0) - (rowAt.get(b.row) ?? 0) ||
        (colAt.get(a.col) ?? 0) - (colAt.get(b.col) ?? 0),
    ),
    total,
  };
}

/**
 * Every property key with its {@link MAX_EVENT_DETAIL_ITEMS} most frequent
 * values and the count of the rest, keys with the most events first.
 */
export function propsSummary(
  rows: Array<{ key: string; value: string; count: Count }>,
): SiteEventDetailDto["props"] {
  const perKey = new Map<string, Array<{ value: string; count: number }>>();
  for (const row of rows) {
    const values = perKey.get(row.key) ?? [];
    values.push({ value: row.value || NONE, count: num(row.count) });
    perKey.set(row.key, values);
  }
  const summaries = [...perKey.entries()]
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([prop, values]) => {
      const sorted = byCountDesc(
        values.sort((a, b) => (a.value < b.value ? -1 : 1)),
        (v) => v.count,
      );
      const kept = sorted.slice(0, MAX_EVENT_DETAIL_ITEMS);
      return {
        prop,
        values: kept,
        otherCount: sorted
          .slice(MAX_EVENT_DETAIL_ITEMS)
          .reduce((sum, v) => sum + v.count, 0),
        total: sorted.reduce((sum, v) => sum + v.count, 0),
      };
    });
  return byCountDesc(summaries, (s) => s.total).map(
    ({ total: _total, ...summary }) => summary,
  );
}

/** `2026-10-28T07:12:00Z`. */
function isoMinute(at: Date): string {
  return `${at.toISOString().slice(0, 16)}:00Z`;
}
