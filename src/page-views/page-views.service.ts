import { Injectable } from "@nestjs/common";
import { InjectDataSource, InjectRepository } from "@nestjs/typeorm";
import { DataSource, In, Repository } from "typeorm";
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import { buildAttractionUrl, buildParkUrl } from "../common/utils/url.util";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { SiteAnalyticsCollectService } from "../site-analytics/site-analytics-collect.service";
import { PAGE_READ_EVENT } from "../site-analytics/site-analytics.constants";
import {
  propsKeyOf,
  type ValidEventHit,
} from "../site-analytics/site-analytics.hits";
import { periodText } from "../site-analytics/site-analytics.query";
import {
  PAGE_VIEW_KINDS,
  isPageKeyFor,
  type PageViewKind,
  type PageViewLocale,
} from "./page-view.constants";
import type {
  AdminPageViewEntryDto,
  AdminPageViewsResponseDto,
  TopPageViewDto,
} from "./dto/page-view.dto";

export interface PageView {
  kind: PageViewKind;
  key: string;
  locale: PageViewLocale;
  ip: string | null;
  userAgent: string | null;
}

/**
 * The page a `page_read` row names: a post's `translationKey`, or a park's or
 * ride's id mapped through `site_entity_merges` to its survivor, so a merge
 * keeps its reads. Over `site_events e LEFT JOIN site_entity_merges m`.
 */
const PAGE_KEY_SQL = `CASE WHEN e.props->>'kind' IN ('park', 'attraction')
       THEN COALESCE(m.winner_id, e.entity)::text
       ELSE e.props->>'key' END`;

/**
 * The reads of a window, with their page key and, for a park or ride, its
 * surviving id. `$1` is the window's start.
 */
const READS_SQL = `SELECT e.props->>'kind' AS kind, ${PAGE_KEY_SQL} AS key,
            CASE WHEN e.props->>'kind' IN ('park', 'attraction')
                 THEN COALESCE(m.winner_id, e.entity) END AS entity,
            e.locale, e.count, e.bucket
       FROM site_events e
       LEFT JOIN site_entity_merges m ON m.loser_id = e.entity
      WHERE e.name = '${PAGE_READ_EVENT}' AND e.bucket >= $1::timestamptz`;

/**
 * The "most read" rankings and the admin's popularity overview, served from
 * the site statistic: a counted read is the event `page_read`
 * (`src/site-analytics/`), which keeps this counter's guards (three reads per
 * reader and page, ten per address and page and 200 per address a UTC day,
 * the key rules per kind, and parks and rides that exist).
 * `page_view_days` is no longer written; its rows were copied into
 * `site_events` once. See docs/frontend/page-views.md.
 */
@Injectable()
export class PageViewsService {
  constructor(
    @InjectDataSource() private readonly db: DataSource,
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
    private readonly collector: SiteAnalyticsCollectService,
  ) {}

  /**
   * Records one view sent to the deprecated `POST /v1/page-views` as a
   * `page_read`. The old beacon knows no path, so its row has none (`''`),
   * and its page type is the kind. Returns whether it counted.
   */
  async record(view: PageView, now: Date = new Date()): Promise<boolean> {
    const entityKind = view.kind === "park" || view.kind === "attraction";
    const props: Record<string, string> = entityKind
      ? { kind: view.kind }
      : { key: view.key, kind: view.kind };
    const hit = {
      t: "event",
      locale: view.locale,
      path: "",
      pageType: view.kind === "article" ? "guide" : view.kind,
      name: PAGE_READ_EVENT,
      props,
      propsKey: propsKeyOf(props),
      entity: entityKind ? view.key : null,
    } as unknown as ValidEventHit;
    const counted = await this.collector.record(
      [hit],
      view.ip,
      view.userAgent,
      now,
    );
    return counted > 0;
  }

  /** The first UTC day of a window of `days` days ending today. */
  windowStart(days: number, now: Date = new Date()): string {
    return addIsoDays(utcDay(now), -(days - 1));
  }

  /**
   * The most viewed pages of one kind in one language, most viewed first. A
   * park or ride ranks only while it exists: the collector already refuses a
   * read of an unknown id, and this keeps a deleted one off the public list.
   */
  async top(
    kind: PageViewKind,
    locale: PageViewLocale,
    days: number,
    limit: number,
    now: Date = new Date(),
  ): Promise<TopPageViewDto[]> {
    const rows: Array<{ key: string; views: string | number }> =
      await this.db.query(
        `SELECT key, views
           FROM (SELECT key, entity, SUM(count) AS views
                   FROM (${READS_SQL} AND e.props->>'kind' = $2 AND e.locale = $3) r
                  WHERE key IS NOT NULL
                  GROUP BY key, entity) g
          WHERE g.entity IS NULL
             OR ($2 = 'park'
                 AND EXISTS (SELECT 1 FROM parks p WHERE p.id = g.entity))
             OR ($2 = 'attraction'
                 AND EXISTS (SELECT 1 FROM attractions a WHERE a.id = g.entity))
          ORDER BY views DESC, key ASC
          LIMIT $4`,
        [`${this.windowStart(days, now)}T00:00:00Z`, kind, locale, limit],
      );
    return rows.map((row) => ({ key: row.key, views: Number(row.views) }));
  }

  /**
   * Everything the admin shows: per kind the total and the split by language,
   * the most viewed pages with their own split, and the views per day. With a
   * `locale` the pages are ranked by that language's views, so a page read in
   * Dutch is found even though most views are German. The ranking runs in
   * SQL; only the top pages' rows come back.
   */
  async adminOverview(
    days: number,
    limit: number,
    locale?: PageViewLocale,
    now: Date = new Date(),
  ): Promise<AdminPageViewsResponseDto> {
    const from = this.windowStart(days, now);
    const start = `${from}T00:00:00Z`;
    const [totals, topRows, byDay]: [
      Array<{
        kind: PageViewKind;
        locale: PageViewLocale;
        views: string | number;
      }>,
      Array<{
        kind: PageViewKind;
        key: string;
        locale: PageViewLocale;
        views: string | number;
        rank: string | number;
      }>,
      Array<{ day: Date | string; kind: PageViewKind; views: string | number }>,
    ] = await Promise.all([
      this.db.query(
        `SELECT kind, locale, SUM(count) AS views
           FROM (${READS_SQL}) r
          GROUP BY kind, locale`,
        [start],
      ),
      this.db.query(
        // One pass: the views per page and language, each page's score beside
        // them (all languages, or the one asked for), ranked per kind. Joining
        // a second scan back for the split took 2.6 times as long.
        `WITH per_locale AS (
           SELECT kind, key, locale, SUM(count) AS views
           FROM (${READS_SQL}) r
           WHERE key IS NOT NULL
           GROUP BY kind, key, locale
         ), scored AS (
           SELECT kind, key, locale, views,
                  SUM(views) FILTER (WHERE $2::text IS NULL OR locale = $2)
                    OVER (PARTITION BY kind, key) AS score
           FROM per_locale
         ), ranked AS (
           SELECT kind, key, locale, views,
                  DENSE_RANK() OVER (PARTITION BY kind ORDER BY score DESC, key ASC) AS rank
           FROM scored
           WHERE score > 0
         )
         SELECT kind, key, locale, views, rank
         FROM ranked
         WHERE rank <= $3
         ORDER BY kind, rank`,
        [start, locale ?? null, limit],
      ),
      this.db.query(
        // As text, and the UTC day: the session's zone must not move a read.
        `SELECT time_bucket('1 day'::interval, bucket) AS day,
                kind, SUM(count) AS views
           FROM (${READS_SQL}) r
          GROUP BY 1, 2`,
        [start],
      ),
    ]);

    const kinds = await Promise.all(
      PAGE_VIEW_KINDS.map(async (kind) => {
        const byLocale: Partial<Record<PageViewLocale, number>> = {};
        let total = 0;
        for (const row of totals) {
          if (row.kind !== kind) continue;
          byLocale[row.locale] = Number(row.views);
          total += Number(row.views);
        }
        const entries = new Map<string, AdminPageViewEntryDto>();
        for (const row of topRows) {
          if (row.kind !== kind) continue;
          const entry = entries.get(row.key) ?? {
            key: row.key,
            total: 0,
            byLocale: {},
          };
          entry.total += Number(row.views);
          entry.byLocale[row.locale] = Number(row.views);
          entries.set(row.key, entry);
        }
        // `topRows` comes ordered by rank, and a Map keeps insertion order.
        return {
          kind,
          total,
          byLocale,
          top: await this.named(kind, [...entries.values()]),
        };
      }),
    );

    const daily = new Map<string, Partial<Record<PageViewKind, number>>>();
    for (let day = from; day <= utcDay(now); day = addIsoDays(day, 1)) {
      daily.set(day, {});
    }
    for (const row of byDay) {
      const entry = daily.get(periodText(row.day, false) as string);
      if (entry) entry[row.kind] = Number(row.views);
    }

    return {
      days,
      from,
      ...(locale && { locale }),
      kinds,
      daily: [...daily.entries()].map(([day, byKind]) => ({ day, byKind })),
    };
  }

  /**
   * Names and links for parks and attractions, read in one query per kind.
   * Posts are named by the frontend, which owns the manifest.
   */
  private async named(
    kind: PageViewKind,
    entries: AdminPageViewEntryDto[],
  ): Promise<AdminPageViewEntryDto[]> {
    // Rows written before keys were checked per kind may hold anything, and a
    // non-UUID inside `IN (…)` on a uuid column fails the whole overview.
    const ids = entries
      .map((entry) => entry.key)
      .filter((key) => isPageKeyFor(kind, key));
    if (ids.length === 0) return entries;

    if (kind === "park") {
      const parks = await this.parks.findBy({ id: In(ids) });
      const byId = new Map(parks.map((park) => [park.id, park]));
      return entries.map((entry) => {
        const park = byId.get(entry.key);
        if (!park) return entry;
        return {
          ...entry,
          name: park.name,
          context: [park.city, park.country].filter(Boolean).join(", "),
          ...withUrl(buildParkUrl(park)),
        };
      });
    }

    if (kind === "attraction") {
      const attractions = await this.attractions.find({
        where: { id: In(ids) },
        relations: { park: true },
      });
      const byId = new Map(
        attractions.map((attraction) => [attraction.id, attraction]),
      );
      return entries.map((entry) => {
        const attraction = byId.get(entry.key);
        if (!attraction) return entry;
        return {
          ...entry,
          name: attraction.name,
          ...(attraction.park && { context: attraction.park.name }),
          ...withUrl(
            attraction.park
              ? buildAttractionUrl(attraction.park, attraction)
              : null,
          ),
        };
      });
    }

    return entries;
  }
}

function withUrl(url: string | null): { url?: string } {
  return url ? { url } : {};
}
