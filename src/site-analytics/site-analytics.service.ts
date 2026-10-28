import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { addIsoDays } from "../common/utils/date.util";
import {
  ALL_LOCALES_SCOPE,
  MAX_CROSSTAB_SIDE,
  MAX_EVENT_DETAIL_ITEMS,
  REALTIME_ACTIVE_MINUTES,
  REALTIME_SERIES_MINUTES,
  SITE_ANALYTICS_KEYS as KEYS,
  SITE_VIEWPORTS,
  VISITOR_KEY_GRACE_SECONDS,
  VISIT_DIMS,
  hourKey,
  minuteKey,
  type BreakdownDim,
  type SiteLocale,
} from "./site-analytics.constants";
import { propsOfKey } from "./site-analytics.hits";
import {
  CROSSTAB_AXIS_PATTERN,
  SqlParams,
  bounceRate,
  byCountDesc,
  columnOf,
  daysBetween,
  hoursOf,
  num,
  periodOf,
  periodText,
  rate,
  siteAnalyticsWindow,
  whereOf,
  type Count,
  type SiteAnalyticsQuery,
  type SiteAnalyticsWindow,
} from "./site-analytics.query";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import { SiteEntityNames } from "./site-entity-names.service";
import type {
  SiteBreakdownDto,
  SiteCrosstabDto,
  SiteEventDetailDto,
  SiteEventsDto,
  SiteOverviewDto,
  SiteSearchDto,
  SiteTotalsDto,
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
 * of UTC days, a language and `filter=<dim>:<value>` pairs, and is a GROUP BY
 * over `site_pageviews` or `site_events`. docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteAnalyticsService {
  private readonly logger = new Logger(SiteAnalyticsService.name);

  constructor(
    @InjectDataSource() private readonly db: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly blocklist: SiteAnalyticsBlocklist,
    private readonly entityNames: SiteEntityNames,
  ) {}

  /**
   * Page views, visits, visitors, events and the bounce rate over the window
   * and the one before, per day (per hour for a one-day window), and the
   * realtime minute. Visitors are unique per UTC day and language only, so
   * with a filter they are null rather than a guess.
   */
  async overview(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteOverviewDto> {
    const window = siteAnalyticsWindow(query, now);
    const period = periodOf(window.hourly);
    const traffic = new SqlParams();
    const events = new SqlParams();
    const [trafficRows, eventRows, visitors, realtime] = await Promise.all([
      this.db.query(
        `SELECT ${period} AS period, SUM(views) AS pageviews,
                SUM(visits) AS visits, SUM(engaged) AS engaged
           FROM site_pageviews
          WHERE ${whereOf(traffic, window, window.previousFrom, window.to)}
          GROUP BY 1`,
        traffic.values,
      ) as Promise<
        Array<{
          period: string;
          pageviews: Count;
          visits: Count;
          engaged: Count;
        }>
      >,
      this.db.query(
        `SELECT ${period} AS period, SUM(count) AS events
           FROM site_events
          WHERE ${whereOf(events, window, window.previousFrom, window.to)}
          GROUP BY 1`,
        events.values,
      ) as Promise<Array<{ period: string; events: Count }>>,
      window.filters.length > 0
        ? Promise.resolve(null)
        : this.visitors(window, now),
      this.realtime(window.locale, now),
    ]);

    for (const row of [...trafficRows, ...eventRows]) {
      row.period = periodText(row.period, window.hourly) as string;
    }
    const byPeriod = new Map(trafficRows.map((row) => [row.period, row]));
    const eventsByPeriod = new Map(
      eventRows.map((row) => [row.period, num(row.events)]),
    );
    const totals = (from: string, to: string): SiteTotalsDto => {
      const sum = { pageviews: 0, visits: 0, engaged: 0, events: 0 };
      for (const row of trafficRows) {
        const day = row.period.slice(0, 10);
        if (day < from || day > to) continue;
        sum.pageviews += num(row.pageviews);
        sum.visits += num(row.visits);
        sum.engaged += num(row.engaged);
      }
      for (const row of eventRows) {
        const day = row.period.slice(0, 10);
        if (day >= from && day <= to) sum.events += num(row.events);
      }
      return {
        pageviews: sum.pageviews,
        visits: sum.visits,
        visitors: visitors ? sumVisitors(visitors.days, from, to) : null,
        events: sum.events,
        viewsPerVisit:
          sum.visits > 0
            ? Math.round((sum.pageviews / sum.visits) * 100) / 100
            : 0,
        bounceRate: bounceRate(sum.visits, sum.engaged),
      };
    };
    const points = window.hourly
      ? hoursOf(window.to, now)
      : daysBetween(window.from, window.to);

    return {
      ...echo(window),
      totals: totals(window.from, window.to),
      previous: totals(window.previousFrom, window.previousTo),
      series: points.map((point) => ({
        day: point,
        pageviews: num(byPeriod.get(point)?.pageviews),
        visits: num(byPeriod.get(point)?.visits),
        visitors: visitors
          ? ((window.hourly ? visitors.hours : visitors.days).get(point) ?? 0)
          : null,
        events: eventsByPeriod.get(point) ?? 0,
      })),
      realtime,
    };
  }

  /**
   * The values of one dimension, most viewed first, with the total over all
   * values. A visit dimension (`entry`, `referrer`, `channel`, `campaign`)
   * counts visits started, a view being a visit, and carries a bounce rate;
   * a direct visit has no referrer and none is listed.
   */
  async breakdown(
    query: SiteAnalyticsQuery & { dim: BreakdownDim; limit: number },
    now = new Date(),
  ): Promise<SiteBreakdownDto> {
    const window = siteAnalyticsWindow(query, now);
    const column = columnOf(query.dim);
    const visitDim = VISIT_DIMS.includes(query.dim);
    const params = new SqlParams();
    const where = whereOf(params, window, window.from, window.to);
    const nonEmpty = ["entry", "referrer", "campaign"].includes(query.dim)
      ? `AND ${column} <> ''`
      : "";
    const rows: Array<{
      value: string;
      views: Count;
      visits: Count;
      engaged: Count;
      total_views: Count;
      total_visits: Count;
    }> = await this.db.query(
      `WITH per_value AS (
         SELECT ${column} AS value, SUM(views) AS views,
                SUM(visits) AS visits, SUM(engaged) AS engaged
           FROM site_pageviews
          WHERE ${where} ${nonEmpty}
          GROUP BY 1
          ${visitDim ? "HAVING SUM(visits) > 0" : ""}
       )
       SELECT value, views, visits, engaged,
              SUM(views) OVER () AS total_views,
              SUM(visits) OVER () AS total_visits
         FROM per_value
        ORDER BY ${visitDim ? "visits" : "views"} DESC, value ASC
        LIMIT ${params.add(query.limit)}`,
      params.values,
    );
    const totalVisits = num(rows[0]?.total_visits);
    const totalViews = visitDim ? totalVisits : num(rows[0]?.total_views);
    return {
      ...echo(window),
      dim: query.dim,
      total: { views: totalViews, visits: totalVisits },
      items: rows.map((row) => {
        const visits = num(row.visits);
        const views = visitDim ? visits : num(row.views);
        return {
          value: row.value,
          views,
          visits,
          share: rate(views, totalViews),
          ...(visitDim && { bounceRate: bounceRate(visits, num(row.engaged)) }),
        };
      }),
    };
  }

  /**
   * Every event name seen in the window or the one before, and every blocked
   * name, with its count, the previous window's and a series; most counted
   * first.
   */
  async events(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteEventsDto> {
    const window = siteAnalyticsWindow(query, now);
    const params = new SqlParams();
    const [rows, blocked] = await Promise.all([
      this.db.query(
        `SELECT name, ${periodOf(window.hourly)} AS period, SUM(count) AS count
           FROM site_events
          WHERE ${whereOf(params, window, window.previousFrom, window.to)}
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
    const points = seriesPoints(window, now);
    const items = [...perName.entries()].map(([name, periods]) => {
      let count = 0;
      let previous = 0;
      for (const [period, value] of periods) {
        if (period.slice(0, 10) >= window.from) count += value;
        else previous += value;
      }
      return {
        name,
        count,
        previous,
        blocked: blocked.has(name),
        series: points.map((point) => ({
          day: point,
          count: periods.get(point) ?? 0,
        })),
      };
    });
    items.sort(
      (a, b) =>
        b.count - a.count ||
        b.previous - a.previous ||
        (a.name < b.name ? -1 : 1),
    );
    return { ...echo(window), items };
  }

  /**
   * One event: its total, every property key seen with its top values, the
   * most frequent property combinations, the kinds of page and viewports it
   * came from, the entities it was about (a merged-away one under its
   * survivor) and its series. Three statements in parallel.
   */
  async event(
    name: string,
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteEventDetailDto> {
    const window = siteAnalyticsWindow(query, now);
    const scan = new SqlParams();
    const nameParam = scan.add(name);
    const props = new SqlParams();
    const propsName = props.add(name);
    const entities = new SqlParams();
    const entityName = entities.add(name);
    const [grouped, propRows, entityRows, blocked] = await Promise.all([
      this.db.query(
        // Every grouped column is NOT NULL, so a NULL says which grouping
        // set produced the row, and the empty set gives the total.
        `SELECT page_type, viewport, props_key, period, SUM(count) AS count
           FROM (SELECT page_type, viewport, props_key, count,
                        ${periodOf(window.hourly)} AS period
                   FROM site_events
                  WHERE name = ${nameParam}
                    AND ${whereOf(scan, window, window.from, window.to)}) s
          GROUP BY GROUPING SETS ((page_type), (viewport), (props_key),
                                  (period), ())`,
        scan.values,
      ) as Promise<
        Array<{
          page_type: string | null;
          viewport: string | null;
          props_key: string | null;
          period: string | null;
          count: Count;
        }>
      >,
      this.db.query(
        `SELECT kv.key, kv.value, SUM(e.count) AS count
           FROM site_events e
          CROSS JOIN LATERAL jsonb_each_text(e.props) AS kv
          WHERE e.name = ${propsName} AND ${whereOf(props, window, window.from, window.to, "e")}
          GROUP BY 1, 2`,
        props.values,
      ) as Promise<Array<{ key: string; value: string; count: Count }>>,
      this.db.query(
        `SELECT COALESCE(m.winner_id, e.entity)::text AS entity,
                SUM(e.count) AS count
           FROM site_events e
           LEFT JOIN site_entity_merges m ON m.loser_id = e.entity
          WHERE e.name = ${entityName} AND e.entity IS NOT NULL
            AND ${whereOf(entities, window, window.from, window.to, "e")}
          GROUP BY 1
          ORDER BY 2 DESC, 1 ASC
          LIMIT ${entities.add(MAX_EVENT_DETAIL_ITEMS)}`,
        entities.values,
      ) as Promise<Array<{ entity: string; count: Count }>>,
      this.blocklist.names(),
    ]);

    for (const row of grouped)
      row.period = periodText(row.period, window.hourly);
    const set = (key: "page_type" | "viewport" | "props_key" | "period") =>
      grouped.filter(
        (row) =>
          row[key] !== null &&
          (["page_type", "viewport", "props_key", "period"] as const).every(
            (other) => other === key || row[other] === null,
          ),
      );
    const valueCounts = (
      key: "page_type" | "viewport",
      known: readonly string[] = [],
    ) => {
      const found = new Map(
        set(key).map((row) => [row[key] as string, num(row.count)]),
      );
      const values = [
        ...known,
        ...[...found.keys()].filter((v) => !known.includes(v)),
      ];
      return byCountDesc(
        values.map((value) => ({ value, count: found.get(value) ?? 0 })),
        (item) => item.count,
      );
    };
    const total = grouped.find((row) =>
      (["page_type", "viewport", "props_key", "period"] as const).every(
        (key) => row[key] === null,
      ),
    );
    const periods = new Map(
      set("period").map((row) => [row.period as string, num(row.count)]),
    );
    const named = await this.entityNames.resolve(
      entityRows.map((row) => row.entity),
    );

    return {
      ...echo(window),
      name,
      blocked: blocked.has(name),
      total: num(total?.count),
      props: propsSummary(propRows),
      combinations: byCountDesc(set("props_key"), (row) => num(row.count))
        .slice(0, MAX_EVENT_DETAIL_ITEMS)
        .map((row) => ({
          props: propsOfKey(row.props_key as string),
          count: num(row.count),
        })),
      byPageType: valueCounts("page_type"),
      byViewport: valueCounts("viewport", SITE_VIEWPORTS),
      entities: entityRows.map((row) => ({
        id: row.entity,
        kind: named.get(row.entity)?.kind ?? null,
        name: named.get(row.entity)?.name ?? null,
        url: named.get(row.entity)?.url ?? null,
        count: num(row.count),
      })),
      series: seriesPoints(window, now).map((point) => ({
        day: point,
        count: periods.get(point) ?? 0,
      })),
    };
  }

  /**
   * One event counted by two sides at once: any dimension, the day or one
   * property. "Which search, from which page" is `row=prop:surface` and
   * `col=page_type` over `search_opened`. Each side keeps its
   * {@link MAX_CROSSTAB_SIDE} largest values and folds the rest into
   * `other`; the day keeps every day, in order.
   */
  async crosstab(
    name: string,
    query: SiteAnalyticsQuery & { row: string; col: string },
    now = new Date(),
  ): Promise<SiteCrosstabDto> {
    const window = siteAnalyticsWindow(query, now);
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
    const cells: Array<{ r: unknown; c: unknown; count: Count }> =
      await this.db.query(
        `SELECT ${rowExpr} AS r, ${colExpr} AS c, SUM(count) AS count
           FROM site_events
          WHERE name = ${nameParam} AND ${whereOf(params, window, window.from, window.to)}
          GROUP BY 1, 2`,
        params.values,
      );
    const label = (value: unknown) =>
      value instanceof Date
        ? (periodText(value, window.hourly) as string)
        : value === "" || value === null
          ? NONE
          : String(value);
    const sideOf = (
      pick: (cell: { r: unknown; c: unknown }) => unknown,
      side: string,
    ) => {
      const totals = new Map<string, number>();
      for (const cell of cells) {
        const key = label(pick(cell));
        totals.set(key, (totals.get(key) ?? 0) + num(cell.count));
      }
      if (side === "day") {
        const kept = [...totals.keys()].sort();
        return { kept, fold: (value: string) => value };
      }
      const ranked = byCountDesc(
        [...totals.keys()],
        (key) => totals.get(key) ?? 0,
      );
      const kept = ranked.slice(0, MAX_CROSSTAB_SIDE);
      const keep = new Set(kept);
      if (ranked.length > kept.length) kept.push("other");
      return {
        kept,
        fold: (value: string) => (keep.has(value) ? value : "other"),
      };
    };
    const rows = sideOf((cell) => cell.r, query.row);
    const cols = sideOf((cell) => cell.c, query.col);
    const folded = new Map<
      string,
      { row: string; col: string; count: number }
    >();
    let total = 0;
    for (const cell of cells) {
      const row = rows.fold(label(cell.r));
      const col = cols.fold(label(cell.c));
      const key = `${row}\u0000${col}`;
      const entry = folded.get(key) ?? { row, col, count: 0 };
      entry.count += num(cell.count);
      total += num(cell.count);
      folded.set(key, entry);
    }
    return {
      ...echo(window),
      name,
      row: query.row,
      col: query.col,
      rows: rows.kept,
      cols: cols.kept,
      cells: [...folded.values()],
      total,
    };
  }

  /**
   * Which search is used, where and how well, over the `search_*` events:
   * per surface seen the openings, typed queries, clicks and dead ends with
   * their rates; openings by kind of page and viewport; what was clicked and
   * at which position; the openings per day. Three statements, each over only
   * the events its figures need.
   */
  async search(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteSearchDto> {
    const window = siteAnalyticsWindow(query, now);
    const scope = (names: readonly string[]) => {
      const params = new SqlParams();
      const sql = `SELECT name, COALESCE(props->>'surface', '') AS surface,
                          COALESCE(props->>'result', '') AS result,
                          COALESCE(props->>'item', '') AS item,
                          COALESCE(props->>'position', '') AS position,
                          page_type, viewport,
                          ${periodOf(window.hourly)} AS period, count
                     FROM site_events
                    WHERE name = ANY(${params.add(names)}::text[])
                      AND ${whereOf(params, window, window.from, window.to)}`;
      return { sql, params };
    };
    type Row = {
      name: string;
      surface: string | null;
      result: string | null;
      item: string | null;
      position: string | null;
      page_type: string | null;
      viewport: string | null;
      period: string | null;
      count: Count;
    };
    const all = scope(Object.values(SEARCH_EVENTS));
    const opened = scope([SEARCH_EVENTS.opened]);
    const clicks = scope([
      SEARCH_EVENTS.resultClicks,
      SEARCH_EVENTS.browseClicks,
    ]);
    const [surfaceRows, openedRows, clickRows]: Row[][] = await Promise.all([
      this.db.query(
        `SELECT name, surface, SUM(count) AS count FROM (${all.sql}) s
          GROUP BY 1, 2`,
        all.params.values,
      ),
      this.db.query(
        `SELECT name, surface, page_type, viewport, period, SUM(count) AS count
           FROM (${opened.sql}) s
          GROUP BY GROUPING SETS ((name, surface, page_type),
                                  (name, surface, viewport),
                                  (name, surface, period))`,
        opened.params.values,
      ),
      this.db.query(
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
      [...new Set(surfaceRows.map((row) => row.surface ?? ""))].filter(Boolean),
      (surface) => of(SEARCH_EVENTS.opened, surface),
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
        clickRate: rate(resultClicks + browseClicks, openedCount),
        noResultRate: rate(noResults, typed),
      };
    });

    const viewports = new Map(
      openedRows
        .filter((row) => row.viewport !== null)
        .map((row) => [`${row.surface}|${row.viewport}`, num(row.count)]),
    );
    const series = new Map(
      openedRows
        .filter((row) => row.period !== null)
        .map((row) => [
          `${periodText(row.period, window.hourly)}|${row.surface}`,
          num(row.count),
        ]),
    );
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
      ...[...positions.keys()].filter(
        (p) => p !== "" && !SEARCH_POSITIONS.includes(p),
      ),
    ];
    const desc = <T>(items: T[], count: (item: T) => number) =>
      byCountDesc(items, count);

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
      ),
      bySurfaceAndViewport: surfaceNames.flatMap((surface) =>
        SITE_VIEWPORTS.map((viewport) => ({
          surface,
          viewport,
          opened: viewports.get(`${surface}|${viewport}`) ?? 0,
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
      ),
      series: seriesPoints(window, now).flatMap((point) =>
        surfaceNames.map((surface) => ({
          day: point,
          surface,
          opened: series.get(`${point}|${surface}`) ?? 0,
        })),
      ),
    };
  }

  /**
   * Unique visitors per day over both windows (the nightly snapshot's rows,
   * and for the days it has not written yet the HyperLogLog still in Redis),
   * and per hour of today for the one-day view. With Redis down those read
   * zero.
   */
  private async visitors(
    window: SiteAnalyticsWindow,
    now: Date,
  ): Promise<{ days: Map<string, number>; hours: Map<string, number> }> {
    const scope = window.locale ?? ALL_LOCALES_SCOPE;
    const rows: Array<{ day: string; visitors: Count }> = await this.db.query(
      `SELECT to_char(day, 'YYYY-MM-DD') AS day, visitors
         FROM site_visitor_days
        WHERE scope = $1 AND day BETWEEN $2::date AND $3::date`,
      [scope, window.previousFrom, window.to],
    );
    const days = new Map(rows.map((row) => [row.day, num(row.visitors)]));
    const hours = new Map<string, number>();

    const graceDays = Math.ceil(VISITOR_KEY_GRACE_SECONDS / 86_400);
    const live = daysBetween(
      addIsoDays(window.to, -graceDays),
      window.to,
    ).filter((day) => day >= window.previousFrom && !days.has(day));
    const hourPoints = window.hourly ? hoursOf(window.to, now) : [];
    try {
      const pipeline = this.redis.pipeline();
      for (const day of live) pipeline.pfcount(KEYS.visitors(day, scope));
      for (const hour of hourPoints) {
        pipeline.pfcount(KEYS.hourVisitors(hourKey(new Date(hour)), scope));
      }
      const results = (await pipeline.exec()) ?? [];
      live.forEach((day, i) => {
        const count = num(results[i]?.[1] as Count);
        if (count > 0) days.set(day, count);
      });
      hourPoints.forEach((hour, i) => {
        hours.set(hour, num(results[live.length + i]?.[1] as Count));
      });
    } catch (error) {
      this.logger.warn(
        `Live visitor counts unavailable: ${(error as Error).message}`,
      );
    }
    return { days, hours };
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

/** The fields every report echoes. */
export function echo(window: SiteAnalyticsWindow) {
  return {
    from: window.from,
    to: window.to,
    days: window.days,
    locale: window.locale,
    filters: window.filters.map((f) => `${f.dim}:${f.value}`),
  };
}

/** Every point of a report's series: days, or today's hours. */
function seriesPoints(window: SiteAnalyticsWindow, now: Date): string[] {
  return window.hourly
    ? hoursOf(window.to, now)
    : daysBetween(window.from, window.to);
}

function sumVisitors(days: Map<string, number>, from: string, to: string) {
  let sum = 0;
  for (const [day, count] of days) if (day >= from && day <= to) sum += count;
  return sum;
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
