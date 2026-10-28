import { Inject, Injectable, Logger, NotFoundException } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, Repository } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import {
  frontendAttractionPath,
  frontendParkPath,
  frontendShowsPath,
} from "../common/utils/frontend-url.util";
import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { SiteTrafficDay } from "./entities/site-traffic-day.entity";
import {
  ALL_LOCALES_SCOPE,
  MAX_EVENT_ENTITIES,
  MAX_OPEN_PROP_VALUES,
  REALTIME_ACTIVE_MINUTES,
  REALTIME_SERIES_MINUTES,
  SITE_ANALYTICS_KEYS as KEYS,
  VISITOR_KEY_GRACE_SECONDS,
  minuteKey,
  type TrafficDim,
} from "./site-analytics.constants";
import {
  SEARCH_EVENTS,
  SEARCH_POSITIONS,
  SEARCH_SURFACES,
  SITE_EVENT_NAMES,
  SITE_PAGE_TYPES,
  SITE_VIEWPORTS,
  isSiteEventName,
  propValues,
  siteEventSpec,
  type SiteEntityKind,
  type SiteLocale,
} from "./site-analytics.schema";
import type {
  SiteBreakdownDto,
  SiteEventDetailDto,
  SiteEventEntityDto,
  SiteEventsDto,
  SiteOverviewDto,
  SiteSearchDto,
  SiteTotalsDto,
  SiteValueCountDto,
} from "./dto/site-analytics.dto";

/** A report's window: `days` UTC days ending today, and the same before it. */
export interface SiteAnalyticsWindow {
  from: string;
  to: string;
  days: number;
  locale: SiteLocale | null;
  previousFrom: string;
  previousTo: string;
}

/** What every report takes. */
export interface SiteAnalyticsQuery {
  days: number;
  locale?: SiteLocale | null;
}

type Count = string | number | null;
const num = (value: Count | undefined): number => Number(value ?? 0) || 0;

/** A rate rounded to four places; 0 when there is nothing to divide by. */
export function rate(numerator: number, divisor: number): number {
  return divisor > 0 ? Math.round((numerator / divisor) * 10_000) / 10_000 : 0;
}

/** Every day from `from` to `to`, both included. */
export function daysBetween(from: string, to: string): string[] {
  const days: string[] = [];
  for (let day = from; day <= to; day = addIsoDays(day, 1)) days.push(day);
  return days;
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
    previousFrom: addIsoDays(from, -query.days),
    previousTo: addIsoDays(from, -1),
  };
}

/** Most counted first, ties in the given order. */
function byCountDesc<T extends { count: number }>(items: T[]): T[] {
  return items
    .map((item, index) => ({ item, index }))
    .sort((a, b) => b.item.count - a.item.count || a.index - b.index)
    .map(({ item }) => item);
}

/**
 * The read side of the site statistic: the five admin reports. Public so the
 * admin's MCP server can offer the same answers as tools; every method returns
 * exactly the shape its endpoint serves. See docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteAnalyticsService {
  private readonly logger = new Logger(SiteAnalyticsService.name);

  constructor(
    @InjectRepository(SiteTrafficDay)
    private readonly traffic: Repository<SiteTrafficDay>,
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
    @InjectRepository(Show) private readonly shows: Repository<Show>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Page views, visits, visitors and events over the window and the one
   * before, per day, and the realtime minute. Without a language, visits are
   * the visits started; with one, the visits that viewed a page in it, so a
   * visit that switched language counts in both.
   */
  async overview(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteOverviewDto> {
    const window = siteAnalyticsWindow(query, now);
    const [traffic, events, visitors, realtime] = await Promise.all([
      this.traffic.query(
        `SELECT to_char(day, 'YYYY-MM-DD') AS day,
                SUM(views) FILTER (WHERE dim = 'locale') AS pageviews,
                CASE WHEN $3::text IS NULL
                     THEN SUM(views) FILTER (WHERE dim = 'entry')
                     ELSE SUM(visits) FILTER (WHERE dim = 'locale')
                END AS visits
           FROM site_traffic_days
          WHERE dim IN ('locale', 'entry')
            AND day BETWEEN $1::date AND $2::date
            AND ($3::text IS NULL OR locale = $3)
          GROUP BY day`,
        [window.previousFrom, window.to, window.locale],
      ) as Promise<Array<{ day: string; pageviews: Count; visits: Count }>>,
      this.eventsPerDay(window),
      this.visitorsPerDay(window),
      this.realtime(window.locale, now),
    ]);

    const byDay = new Map(traffic.map((row) => [row.day, row]));
    const eventsByDay = new Map<string, number>();
    for (const row of events) {
      eventsByDay.set(row.day, (eventsByDay.get(row.day) ?? 0) + row.count);
    }
    const dayFigures = (day: string) => ({
      day,
      pageviews: num(byDay.get(day)?.pageviews),
      visits: num(byDay.get(day)?.visits),
      visitors: visitors.get(day) ?? 0,
      events: eventsByDay.get(day) ?? 0,
    });
    const totals = (from: string, to: string): SiteTotalsDto => {
      const sum = { pageviews: 0, visits: 0, visitors: 0, events: 0 };
      for (const day of daysBetween(from, to)) {
        const figures = dayFigures(day);
        sum.pageviews += figures.pageviews;
        sum.visits += figures.visits;
        sum.visitors += figures.visitors;
        sum.events += figures.events;
      }
      return {
        ...sum,
        viewsPerVisit:
          sum.visits > 0
            ? Math.round((sum.pageviews / sum.visits) * 100) / 100
            : 0,
      };
    };

    return {
      ...echo(window),
      totals: totals(window.from, window.to),
      previous: totals(window.previousFrom, window.previousTo),
      series: daysBetween(window.from, window.to).map((day) => {
        const { events: _events, ...figures } = dayFigures(day);
        return figures;
      }),
      realtime,
    };
  }

  /**
   * The values of one dimension, most viewed first, with the total over all
   * values. For `entry` and `referrer` a view is a visit started.
   */
  async breakdown(
    query: SiteAnalyticsQuery & { dim: TrafficDim; limit: number },
    now = new Date(),
  ): Promise<SiteBreakdownDto> {
    const window = siteAnalyticsWindow(query, now);
    const rows: Array<{
      value: string;
      views: Count;
      visits: Count;
      total_views: Count;
      total_visits: Count;
    }> = await this.traffic.query(
      `WITH per_value AS (
         SELECT value, SUM(views) AS views, SUM(visits) AS visits
           FROM site_traffic_days
          WHERE dim = $1 AND day BETWEEN $2::date AND $3::date
            AND ($4::text IS NULL OR locale = $4)
          GROUP BY value
       )
       SELECT value, views, visits,
              SUM(views) OVER () AS total_views,
              SUM(visits) OVER () AS total_visits
         FROM per_value
        ORDER BY views DESC, value ASC
        LIMIT $5`,
      [query.dim, window.from, window.to, window.locale, query.limit],
    );
    const total = {
      views: num(rows[0]?.total_views),
      visits: num(rows[0]?.total_visits),
    };
    return {
      ...echo(window),
      dim: query.dim,
      total,
      items: rows.map((row) => ({
        value: row.value,
        views: num(row.views),
        visits: num(row.visits),
        share: rate(num(row.views), total.views),
      })),
    };
  }

  /** Every event of the schema with its count, the previous window's and a series. */
  async events(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteEventsDto> {
    const window = siteAnalyticsWindow(query, now);
    const rows = await this.eventsPerDay(window);
    const perName = new Map<string, Map<string, number>>();
    for (const row of rows) {
      const days = perName.get(row.name) ?? new Map<string, number>();
      days.set(row.day, row.count);
      perName.set(row.name, days);
    }
    const windowDays = daysBetween(window.from, window.to);
    const items = SITE_EVENT_NAMES.map((name) => {
      const days = perName.get(name) ?? new Map<string, number>();
      let count = 0;
      let previous = 0;
      for (const [day, value] of days) {
        if (day >= window.from) count += value;
        else previous += value;
      }
      return {
        name,
        count,
        previous,
        series: windowDays.map((day) => ({ day, count: days.get(day) ?? 0 })),
      };
    });
    return { ...echo(window), items: byCountDesc(items) };
  }

  /**
   * One event: its total, each property's values, the kinds of page and the
   * viewports it came from, the entities it was about and its series. One
   * scan with grouping sets for all but the entities. 404 for a name the
   * schema does not have.
   */
  async event(
    name: string,
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteEventDetailDto> {
    if (!isSiteEventName(name)) {
      throw new NotFoundException(`No event "${name}" in the schema`);
    }
    const window = siteAnalyticsWindow(query, now);
    const spec = siteEventSpec(name);
    const [grouped, entityRows]: [
      Array<{
        p1: string | null;
        p2: string | null;
        p3: string | null;
        page_type: string | null;
        viewport: string | null;
        day: string | null;
        count: Count;
      }>,
      Array<{ entity: string; kind: SiteEntityKind; count: Count }>,
    ] = await Promise.all([
      this.traffic.query(
        // Every column is NOT NULL, so a NULL in a row says which grouping
        // set produced it, and the empty set gives the total.
        `SELECT p1, p2, p3, page_type, viewport,
                to_char(day, 'YYYY-MM-DD') AS day, SUM(count) AS count
           FROM site_event_days
          WHERE name = $1 AND day BETWEEN $2::date AND $3::date
            AND ($4::text IS NULL OR locale = $4)
          GROUP BY GROUPING SETS ((p1), (p2), (p3), (page_type), (viewport), (day), ())`,
        [name, window.from, window.to, window.locale],
      ),
      spec.entity
        ? this.traffic.query(
            `SELECT entity::text AS entity, MIN(kind) AS kind, SUM(count) AS count
               FROM site_event_entity_days
              WHERE name = $1 AND day BETWEEN $2::date AND $3::date
                AND ($4::text IS NULL OR locale = $4)
              GROUP BY entity
              ORDER BY SUM(count) DESC, entity ASC
              LIMIT $5`,
            [name, window.from, window.to, window.locale, MAX_EVENT_ENTITIES],
          )
        : Promise.resolve([]),
    ]);

    const column = (key: "p1" | "p2" | "p3" | "page_type" | "viewport") =>
      grouped.filter(
        (row): row is typeof row & Record<typeof key, string> =>
          row[key] !== null,
      );
    const counted = (
      key: "p1" | "p2" | "p3" | "page_type" | "viewport",
      known: readonly string[] | null,
      max = Infinity,
    ): SiteValueCountDto[] => {
      const found = new Map(
        column(key).map((row) => [row[key] as string, num(row.count)]),
      );
      const values = known
        ? [...known, ...[...found.keys()].filter((v) => !known.includes(v))]
        : [...found.keys()];
      return byCountDesc(
        values.map((value) => ({ value, count: found.get(value) ?? 0 })),
      ).slice(0, max);
    };
    const days = new Map(
      grouped
        .filter((row) => row.day !== null)
        .map((row) => [row.day as string, num(row.count)]),
    );
    const total = grouped.find(
      (row) =>
        row.p1 === null &&
        row.p2 === null &&
        row.p3 === null &&
        row.page_type === null &&
        row.viewport === null &&
        row.day === null,
    );

    return {
      ...echo(window),
      name,
      total: num(total?.count),
      props: spec.props.map((prop, index) => ({
        prop: prop.name,
        values: counted(
          (["p1", "p2", "p3"] as const)[index],
          propValues(prop),
          propValues(prop) ? Infinity : MAX_OPEN_PROP_VALUES,
        ),
      })),
      byPageType: counted("page_type", SITE_PAGE_TYPES),
      byViewport: counted("viewport", SITE_VIEWPORTS),
      entities: await this.namedEntities(entityRows),
      series: daysBetween(window.from, window.to).map((day) => ({
        day,
        count: days.get(day) ?? 0,
      })),
    };
  }

  /**
   * Which search is used, where and how well: per surface the openings,
   * typed queries, clicks and dead ends with their rates; openings by kind
   * of page and viewport; what was clicked and at which position; and the
   * openings per day. One scan of the six search events with grouping sets.
   */
  async search(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteSearchDto> {
    const window = siteAnalyticsWindow(query, now);
    const rows: Array<{
      name: string;
      p1: string | null;
      p2: string | null;
      p3: string | null;
      page_type: string | null;
      viewport: string | null;
      day: string | null;
      count: Count;
    }> = await this.traffic.query(
      `SELECT name, p1, p2, p3, page_type, viewport,
              to_char(day, 'YYYY-MM-DD') AS day, SUM(count) AS count
         FROM site_event_days
        WHERE name = ANY($1::text[]) AND day BETWEEN $2::date AND $3::date
          AND ($4::text IS NULL OR locale = $4)
        GROUP BY GROUPING SETS (
          (name, p1), (name, p1, page_type), (name, p1, viewport),
          (name, p1, p2), (name, p3), (name, p1, day)
        )`,
      [Object.values(SEARCH_EVENTS), window.from, window.to, window.locale],
    );

    const only = (shape: {
      p1?: boolean;
      p2?: boolean;
      p3?: boolean;
      page_type?: boolean;
      viewport?: boolean;
      day?: boolean;
    }) =>
      rows.filter(
        (row) =>
          (row.p1 !== null) === !!shape.p1 &&
          (row.p2 !== null) === !!shape.p2 &&
          (row.p3 !== null) === !!shape.p3 &&
          (row.page_type !== null) === !!shape.page_type &&
          (row.viewport !== null) === !!shape.viewport &&
          (row.day !== null) === !!shape.day,
      );

    const perSurface = new Map<string, number>();
    for (const row of only({ p1: true })) {
      perSurface.set(`${row.name}|${row.p1}`, num(row.count));
    }
    const of = (event: string, surface: string) =>
      perSurface.get(`${event}|${surface}`) ?? 0;
    const surfaces = SEARCH_SURFACES.map((surface) => {
      const opened = of(SEARCH_EVENTS.opened, surface);
      const typed = of(SEARCH_EVENTS.typed, surface);
      const resultClicks = of(SEARCH_EVENTS.resultClicks, surface);
      const browseClicks = of(SEARCH_EVENTS.browseClicks, surface);
      const noResults = of(SEARCH_EVENTS.noResults, surface);
      return {
        surface,
        opened,
        typed,
        resultClicks,
        browseClicks,
        noResults,
        viewAll: of(SEARCH_EVENTS.viewAll, surface),
        clickRate: rate(resultClicks + browseClicks, opened),
        noResultRate: rate(noResults, typed),
      };
    });

    const opened = (row: { name: string }) => row.name === SEARCH_EVENTS.opened;
    const viewports = new Map(
      only({ p1: true, viewport: true })
        .filter(opened)
        .map((row) => [`${row.p1}|${row.viewport}`, num(row.count)]),
    );
    const positions = new Map(
      only({ p3: true })
        .filter((row) => row.name === SEARCH_EVENTS.resultClicks)
        .map((row) => [row.p3 as string, num(row.count)]),
    );
    const series = new Map(
      only({ p1: true, day: true })
        .filter(opened)
        .map((row) => [`${row.day}|${row.p1}`, num(row.count)]),
    );
    const desc = <T>(items: T[], count: (item: T) => number) =>
      items.sort((a, b) => count(b) - count(a));

    return {
      ...echo(window),
      surfaces,
      bySurfaceAndPageType: desc(
        only({ p1: true, page_type: true })
          .filter(opened)
          .map((row) => ({
            surface: row.p1 as string,
            pageType: row.page_type as string,
            opened: num(row.count),
          })),
        (item) => item.opened,
      ),
      bySurfaceAndViewport: SEARCH_SURFACES.flatMap((surface) =>
        SITE_VIEWPORTS.map((viewport) => ({
          surface,
          viewport,
          opened: viewports.get(`${surface}|${viewport}`) ?? 0,
        })),
      ),
      resultTypes: desc(
        only({ p1: true, p2: true })
          .filter((row) => row.name === SEARCH_EVENTS.resultClicks)
          .map((row) => ({
            surface: row.p1 as string,
            result: row.p2 as string,
            clicks: num(row.count),
          })),
        (item) => item.clicks,
      ),
      positions: SEARCH_POSITIONS.map((position) => ({
        position,
        clicks: positions.get(position) ?? 0,
      })),
      browseItems: desc(
        only({ p1: true, p2: true })
          .filter((row) => row.name === SEARCH_EVENTS.browseClicks)
          .map((row) => ({
            surface: row.p1 as string,
            item: row.p2 as string,
            clicks: num(row.count),
          })),
        (item) => item.clicks,
      ),
      series: daysBetween(window.from, window.to).flatMap((day) =>
        SEARCH_SURFACES.map((surface) => ({
          day,
          surface,
          opened: series.get(`${day}|${surface}`) ?? 0,
        })),
      ),
    };
  }

  /** Events per name and day over the window and the one before it. */
  private async eventsPerDay(
    window: SiteAnalyticsWindow,
  ): Promise<Array<{ name: string; day: string; count: number }>> {
    const rows: Array<{ name: string; day: string; count: Count }> =
      await this.traffic.query(
        `SELECT name, to_char(day, 'YYYY-MM-DD') AS day, SUM(count) AS count
           FROM site_event_days
          WHERE day BETWEEN $1::date AND $2::date
            AND ($3::text IS NULL OR locale = $3)
          GROUP BY name, day`,
        [window.previousFrom, window.to, window.locale],
      );
    return rows.map((row) => ({ ...row, count: num(row.count) }));
  }

  /**
   * Unique visitors per day over both windows: the nightly snapshot's rows,
   * and for the days it has not written yet (today, and yesterday until the
   * snapshot ran) the HyperLogLog that is still in Redis. With Redis down
   * those days read zero.
   */
  private async visitorsPerDay(
    window: SiteAnalyticsWindow,
  ): Promise<Map<string, number>> {
    const scope = window.locale ?? ALL_LOCALES_SCOPE;
    const rows: Array<{ day: string; visitors: Count }> =
      await this.traffic.query(
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, visitors
           FROM site_visitor_days
          WHERE scope = $1 AND day BETWEEN $2::date AND $3::date`,
        [scope, window.previousFrom, window.to],
      );
    const visitors = new Map(rows.map((row) => [row.day, num(row.visitors)]));

    const graceDays = Math.ceil(VISITOR_KEY_GRACE_SECONDS / 86_400);
    const live = daysBetween(
      addIsoDays(window.to, -graceDays),
      window.to,
    ).filter((day) => day >= window.previousFrom && !visitors.has(day));
    if (live.length === 0) return visitors;
    try {
      const pipeline = this.redis.pipeline();
      for (const day of live) pipeline.pfcount(KEYS.visitors(day, scope));
      const results = (await pipeline.exec()) ?? [];
      live.forEach((day, i) => {
        const count = num(results[i]?.[1] as Count);
        if (count > 0) visitors.set(day, count);
      });
    } catch (error) {
      this.logger.warn(
        `Live visitor counts unavailable: ${(error as Error).message}`,
      );
    }
    return visitors;
  }

  /**
   * Distinct visitors over the last {@link REALTIME_ACTIVE_MINUTES} minutes
   * (one PFCOUNT over the minute keys counts their union) and page views per
   * minute over the last {@link REALTIME_SERIES_MINUTES}. Zero with Redis
   * down.
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

  /**
   * Names and frontend paths for the entities of an event, one query per
   * kind. An id that no longer resolves keeps its kind and count, with no
   * name and no path.
   */
  private async namedEntities(
    rows: Array<{ entity: string; kind: SiteEntityKind; count: Count }>,
  ): Promise<SiteEventEntityDto[]> {
    const idsOf = (kind: SiteEntityKind) =>
      rows
        .filter((row) => row.kind === kind && isCanonicalUuid(row.entity))
        .map((row) => row.entity);
    const parkIds = idsOf("park");
    const attractionIds = idsOf("attraction");
    const showIds = idsOf("show");
    const [parks, attractions, shows] = await Promise.all([
      parkIds.length ? this.parks.findBy({ id: In(parkIds) }) : [],
      attractionIds.length
        ? this.attractions.find({
            where: { id: In(attractionIds) },
            relations: { park: true },
          })
        : [],
      showIds.length
        ? this.shows.find({
            where: { id: In(showIds) },
            relations: { park: true },
          })
        : [],
    ]);
    const named = new Map<string, { name: string; url: string | null }>();
    for (const park of parks) {
      named.set(park.id, { name: park.name, url: frontendParkPath(park) });
    }
    for (const attraction of attractions) {
      named.set(attraction.id, {
        name: attraction.name,
        url: attraction.park
          ? frontendAttractionPath(attraction.park, attraction)
          : null,
      });
    }
    for (const show of shows) {
      named.set(show.id, {
        name: show.name,
        url: show.park ? frontendShowsPath(show.park) : null,
      });
    }
    return rows.map((row) => ({
      id: row.entity,
      kind: row.kind,
      name: named.get(row.entity)?.name ?? null,
      url: named.get(row.entity)?.url ?? null,
      count: num(row.count),
    }));
  }
}

/** The fields every report echoes. */
function echo(window: SiteAnalyticsWindow) {
  return {
    from: window.from,
    to: window.to,
    days: window.days,
    locale: window.locale,
  };
}

/** `2026-10-28T07:12:00Z`. */
function isoMinute(at: Date): string {
  return `${at.toISOString().slice(0, 16)}:00Z`;
}
