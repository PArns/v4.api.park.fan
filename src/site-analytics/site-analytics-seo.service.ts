import { BadRequestException, Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { IsNull, Repository } from "typeorm";
import {
  frontendAttractionPath,
  frontendParkPath,
} from "../common/utils/frontend-url.util";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import {
  MAX_COMPARABLE_DAYS,
  MIN_TREND_VISITS,
  NOT_FOUND_PAGE_TYPE,
  PAGE_DIMS,
  TREND_ITEMS,
  type Channel,
} from "./site-analytics.constants";
import { rowsOf } from "./site-analytics.rollup";
import {
  SqlParams,
  averageSeconds,
  bounceRate,
  num,
  refuseFiltersBeyond,
  siteAnalyticsWindow,
  type Count,
  type SiteAnalyticsQuery,
  type SiteAnalyticsWindow,
} from "./site-analytics.query";
import { SiteReportRunner } from "./site-analytics-report-runner";
import { echo } from "./site-analytics.service";
import type {
  SiteLandingPagesDto,
  SiteNotFoundDto,
  SiteTrendsDto,
  SiteUncoveredDto,
} from "./dto/site-analytics.dto";

/** How many referrers each landing page or missing page lists. */
const SOURCES_PER_ITEM = 5;

interface EntryRow {
  path: string;
  page_type: string | null;
  visits: Count;
  previous: Count;
  engaged: Count;
  visit_seconds: Count;
}

/**
 * The SEO view of the site statistic: where search engines and AI assistants
 * send people, what grows and shrinks, which pages are missing and which
 * parks and rides no search brought anybody to. Public so the admin's MCP
 * server can offer the same answers as tools. Same window, language and
 * filters as every report; all of it reads the rows that record visits,
 * except the not-found page views. docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteAnalyticsSeoService {
  constructor(
    private readonly runner: SiteReportRunner,
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
  ) {}

  /** The days of a cuboid the nightly job has rolled up, for {@link rowsOf}. */
  private readonly rolled = (
    source: Parameters<SiteReportRunner["rolledDays"]>[0],
    cuboid: number,
  ) => this.runner.rolledDays(source, cuboid);

  /**
   * The pages visits from one channel (`search` unless asked) entered on, with
   * the window before (null over `MAX_COMPARABLE_DAYS`), the bounce rate, the
   * average visit and the sites they came from.
   */
  async landingPages(
    query: SiteAnalyticsQuery & { channel?: Channel; limit: number },
    now = new Date(),
  ): Promise<SiteLandingPagesDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      const channel = query.channel ?? "search";
      const rows = await this.entries(window, channel, query.limit);
      const sources = await this.split(
        window,
        rows.map((row) => row.path),
        "channel",
        `channel = $CHANNEL`,
        channel,
      );
      return {
        ...echo(window),
        channel,
        items: rows.map((row) => {
          const visits = num(row.visits);
          const previous = window.previous ? num(row.previous) : null;
          return {
            path: row.path,
            pageType: row.page_type ?? "",
            visits,
            previous,
            change: previous === null ? null : visits - previous,
            bounceRate: bounceRate(visits, num(row.engaged)),
            avgVisitSeconds: averageSeconds(num(row.visit_seconds), visits),
            sources: sources.get(row.path) ?? [],
          };
        }),
      };
    });
  }

  /**
   * The landing pages of one channel that gained and lost the most visits
   * against the window before, {@link TREND_ITEMS} each, among pages with at
   * least {@link MIN_TREND_VISITS} visits in the two windows together so that
   * noise does not rank. Needs a window before: at most
   * {@link MAX_COMPARABLE_DAYS} days.
   */
  async trends(
    query: SiteAnalyticsQuery & { channel?: Channel },
    now = new Date(),
  ): Promise<SiteTrendsDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      if (!window.previous) {
        throw new BadRequestException(
          `Trends compare with the window before, so at most ${MAX_COMPARABLE_DAYS} days`,
        );
      }
      const channel = query.channel ?? "search";
      const rows = (await this.entries(window, channel, null))
        .map((row) => {
          const visits = num(row.visits);
          const previous = num(row.previous);
          return {
            path: row.path,
            pageType: row.page_type ?? "",
            visits,
            previous,
            change: visits - previous,
            avgVisitSeconds: averageSeconds(num(row.visit_seconds), visits),
          };
        })
        .filter((row) => row.visits + row.previous >= MIN_TREND_VISITS);
      const byChange = (a: { change: number; path: string }, b: typeof a) =>
        b.change - a.change || (a.path < b.path ? -1 : 1);
      return {
        ...echo(window),
        channel,
        rising: rows
          .filter((row) => row.change > 0)
          .sort(byChange)
          .slice(0, TREND_ITEMS),
        falling: rows
          .filter((row) => row.change < 0)
          .sort((a, b) => -byChange(a, b))
          .slice(0, TREND_ITEMS),
      };
    });
  }

  /**
   * Paths that showed the not-found page, most viewed first, with the sites
   * of the visits that entered on them: a dead URL still indexed or linked
   * from outside. A broken link inside the site brings views but no visit
   * from outside, so no referrer.
   */
  async notFound(
    query: SiteAnalyticsQuery & { limit: number },
    now = new Date(),
  ): Promise<SiteNotFoundDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      refuseFiltersBeyond(window, "The not-found report", PAGE_DIMS);
      const params = new SqlParams();
      const source = await rowsOf(
        this.rolled,
        "pageviews",
        params,
        window,
        "current",
        ["path", "page_type"],
        { collapse: true },
      );
      const rows: Array<{ path: string; views: Count }> =
        await this.runner.query(
          `SELECT path, SUM(views) AS views
         FROM ${source.from}
        WHERE ${source.where}
          AND page_type = ${params.add(NOT_FOUND_PAGE_TYPE)}
        GROUP BY path
        HAVING SUM(views) > 0
        ORDER BY 2 DESC, 1 ASC
        LIMIT ${params.add(query.limit)}`,
          params.values,
        );
      const referrers = await this.split(
        window,
        rows.map((row) => row.path),
        "page_type",
        `page_type = $CHANNEL AND referrer <> ''`,
        NOT_FOUND_PAGE_TYPE,
      );
      return {
        ...echo(window),
        items: rows.map((row) => ({
          path: row.path,
          views: num(row.views),
          referrers: referrers.get(row.path) ?? [],
        })),
      };
    });
  }

  /**
   * Parks or rides whose page no search visit entered on in the window, by
   * name. A page is matched by its frontend path in any language, so a ride
   * renamed since is listed although its old path was found.
   */
  async uncovered(
    query: SiteAnalyticsQuery & { kind: "park" | "attraction"; limit: number },
    now = new Date(),
  ): Promise<SiteUncoveredDto> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      const params = new SqlParams();
      const source = await rowsOf(
        this.rolled,
        "pageviews",
        params,
        window,
        "current",
        ["entry", "channel"],
        { collapse: true },
      );
      const covered: Array<{ path: string }> = await this.runner.query(
        `SELECT DISTINCT substring(entry from 4) AS path
         FROM ${source.from}
        WHERE ${source.where}
          AND channel = 'search' AND visits > 0 AND entry <> ''`,
        params.values,
      );
      const found = new Set(covered.map((row) => row.path));

      const pages =
        query.kind === "park"
          ? (await this.parks.find({ where: { retiredAt: IsNull() } })).map(
              (park) => ({
                id: park.id,
                name: park.name,
                park: null as string | null,
                url: frontendParkPath(park),
              }),
            )
          : (
              await this.attractions.find({
                where: { retiredAt: IsNull() },
                relations: { park: true },
              })
            )
              .filter((ride) => ride.park && !ride.park.retiredAt)
              .map((ride) => ({
                id: ride.id,
                name: ride.name,
                park: ride.park.name,
                url: frontendAttractionPath(ride.park, ride),
              }));
      const uncovered = pages
        .filter((page) => page.url && !found.has(page.url))
        .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
      return {
        ...echo(window),
        kind: query.kind,
        total: pages.length,
        uncovered: uncovered.length,
        items: uncovered.slice(0, query.limit).map((page) => ({
          id: page.id,
          kind: query.kind,
          name: page.name,
          ...(page.park && { park: page.park }),
          url: page.url as string,
        })),
      };
    });
  }

  /**
   * Visits that entered on each page from one channel, in the window and the
   * one before. On the rows that record visits the path is the entry; an exit
   * row carries the entry too, with the last page's type, so the page type
   * is read from the start rows only.
   */
  private async entries(
    window: SiteAnalyticsWindow,
    channel: string,
    limit: number | null,
  ): Promise<EntryRow[]> {
    const params = new SqlParams();
    const source = await rowsOf(
      this.rolled,
      "pageviews",
      params,
      window,
      "both",
      ["entry", "channel", "page_type"],
    );
    const start = params.add(window.start);
    return this.runner.query(
      `SELECT entry AS path,
              SUM(visits) FILTER (WHERE bucket >= ${start}::timestamptz) AS visits,
              SUM(visits) FILTER (WHERE bucket < ${start}::timestamptz) AS previous,
              SUM(engaged) FILTER (WHERE bucket >= ${start}::timestamptz) AS engaged,
              SUM(visit_seconds) FILTER (WHERE bucket >= ${start}::timestamptz) AS visit_seconds,
              MAX(page_type) FILTER (WHERE visits > 0) AS page_type
         FROM ${source.from}
        WHERE ${source.where} AND channel = ${params.add(channel)} AND entry <> ''
        GROUP BY entry
        ${limit === null ? "" : `HAVING SUM(visits) FILTER (WHERE bucket >= ${start}::timestamptz) > 0`}
        ORDER BY 2 DESC NULLS LAST, 1 ASC
        ${limit === null ? "" : `LIMIT ${params.add(limit)}`}`,
      params.values,
    );
  }

  /**
   * The referrers of the visits that entered on a list of pages in the
   * window, the largest {@link SOURCES_PER_ITEM} per page. `condition` names
   * its one parameter `$CHANNEL` and reads `column` of the rows.
   */
  private async split(
    window: SiteAnalyticsWindow,
    pages: string[],
    column: "channel" | "page_type",
    condition: string,
    value: string,
  ): Promise<Map<string, Array<{ value: string; visits: number }>>> {
    const split = new Map<string, Array<{ value: string; visits: number }>>();
    if (pages.length === 0) return split;
    const params = new SqlParams();
    const source = await rowsOf(
      this.rolled,
      "pageviews",
      params,
      window,
      "current",
      ["entry", "referrer", column],
      { collapse: true },
    );
    const rows: Array<{ page: string; referrer: string; count: Count }> =
      await this.runner.query(
        `SELECT entry AS page, referrer, SUM(visits) AS count
           FROM ${source.from}
          WHERE ${source.where} AND visits > 0
            AND ${condition.replace("$CHANNEL", params.add(value))}
            AND entry = ANY(${params.add(pages)}::text[])
          GROUP BY 1, 2
          ORDER BY 3 DESC, 2 ASC`,
        params.values,
      );
    for (const row of rows) {
      const list = split.get(row.page) ?? [];
      if (list.length < SOURCES_PER_ITEM) {
        list.push({
          value: row.referrer || "(direct)",
          visits: num(row.count),
        });
      }
      split.set(row.page, list);
    }
    return split;
  }
}
