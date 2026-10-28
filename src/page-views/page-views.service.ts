import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, Repository } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { addIsoDays } from "../common/utils/date.util";
import { addressBucket } from "../common/utils/request.util";
import {
  readerHash,
  readerHashSecret,
  secondsUntilUtcDayEnds,
  utcDay,
} from "../common/utils/reader-hash.util";
import { ensureTableStorageOptions } from "../database/table-storage-options";
import { buildAttractionUrl, buildParkUrl } from "../common/utils/url.util";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { PageViewDay } from "./entities/page-view-day.entity";
import { isBotUserAgent } from "./bot-user-agent.util";
import {
  MAX_VIEWS_PER_ADDRESS_PER_DAY,
  MAX_VIEWS_PER_READER_PER_DAY,
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

/** One reader's views of one page today: address bucket, User-Agent, page. */
const SEEN_PREFIX = "page-views:seen:";

/** One address bucket's views today, across all pages. */
const ADDRESS_PREFIX = "page-views:addr:";

/** A minute past midnight UTC, so a key never expires before the day it caps. */
const SEEN_GRACE_SECONDS = 60;

/**
 * Storage settings `synchronize` cannot express, as `pg_class.reloptions`
 * spells them. Every counted view after a page's first that day updates the
 * same row. At the default fill no page has room for the new version, so 92 %
 * of those updates were not HOT and left an entry in both indexes for VACUUM;
 * at 80 % 57 % are HOT and pruned on the page. And the default 20 % would let
 * autovacuum wait months on a table that only grows, where 1 % runs it every
 * few days over the few pages that changed. Measured in
 * docs/frontend/page-views.md.
 */
export const PAGE_VIEW_STORAGE_OPTIONS = [
  "fillfactor=80",
  "autovacuum_vacuum_scale_factor=0.01",
] as const;

export interface PageView {
  kind: PageViewKind;
  key: string;
  locale: PageViewLocale;
  ip: string | null;
  userAgent: string | null;
}

/**
 * The page-view counter behind the "most read" lists and the admin's
 * popularity overview. See docs/frontend/page-views.md.
 *
 * Counts live in Postgres, not Redis: this Redis runs `volatile-lru`, and a
 * ranking that loses a day of counts under memory pressure ranks wrong without
 * saying so.
 *
 * No address is stored. The two caps are Redis counters whose keys are HMACs
 * under `PAGE_VIEWS_HASH_SECRET`, which lives in the environment and never in
 * Redis, and which expire when their UTC day ends: without the secret a key
 * cannot be traced to an address, and with it only by testing a given one.
 */
@Injectable()
export class PageViewsService implements OnModuleInit {
  private readonly logger = new Logger(PageViewsService.name);
  private readonly hashSecret: string;

  constructor(
    @InjectRepository(PageViewDay)
    private readonly views: Repository<PageViewDay>,
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    this.hashSecret = readerHashSecret(
      ["PAGE_VIEWS_HASH_SECRET"],
      this.logger,
      "the page-view cap",
    );
  }

  /** Writes {@link PAGE_VIEW_STORAGE_OPTIONS} when the table lacks them. */
  async onModuleInit(): Promise<void> {
    await ensureTableStorageOptions(
      this.views,
      "page_view_days",
      PAGE_VIEW_STORAGE_OPTIONS,
      this.logger,
    );
  }

  /**
   * Count one view, unless it comes from a bot, from a reader who has counted
   * this page {@link MAX_VIEWS_PER_READER_PER_DAY} times today, or from an
   * address that has counted {@link MAX_VIEWS_PER_ADDRESS_PER_DAY} views
   * today. Returns whether it was counted.
   */
  async record(view: PageView, now: Date = new Date()): Promise<boolean> {
    if (isBotUserAgent(view.userAgent)) return false;
    const day = utcDay(now);
    if (!(await this.withinCaps(view, day, now))) return false;

    await this.views.query(
      `INSERT INTO page_view_days (kind, key, locale, day, views)
       VALUES ($1, $2, $3, $4, 1)
       ON CONFLICT (kind, key, locale, day)
       DO UPDATE SET views = page_view_days.views + 1`,
      [view.kind, view.key, view.locale, day],
    );
    return true;
  }

  /**
   * The reader is the address bucket (an IPv6 /64) and the User-Agent, not
   * the address alone: behind a carrier gateway one address is many readers.
   * The page is kind and key, not the language, so one reader reading a post
   * in two languages is still one reader. Without an address, or with Redis
   * down, the view counts: the caps are a guard against one machine, and
   * refusing every reader because a cache is away would empty the rankings.
   */
  private async withinCaps(
    view: PageView,
    day: string,
    now: Date,
  ): Promise<boolean> {
    const bucket = addressBucket(view.ip);
    if (!bucket) return true;
    const reader =
      SEEN_PREFIX +
      this.hash(day, bucket, view.userAgent ?? "", view.kind, view.key);
    const address = ADDRESS_PREFIX + this.hash(day, bucket);
    const ttl = secondsUntilUtcDayEnds(now, SEEN_GRACE_SECONDS);
    try {
      // One round trip; `EXPIRE … NX` on every call heals a key whose first
      // EXPIRE was lost, as in `incrementWithWindow`.
      const results = await this.redis
        .pipeline()
        .incr(reader)
        .expire(reader, ttl, "NX")
        .incr(address)
        .expire(address, ttl, "NX")
        .exec();
      const readerCount = Number(results?.[0]?.[1] ?? 0);
      const addressCount = Number(results?.[2]?.[1] ?? 0);
      return (
        readerCount <= MAX_VIEWS_PER_READER_PER_DAY &&
        addressCount <= MAX_VIEWS_PER_ADDRESS_PER_DAY
      );
    } catch (error) {
      this.logger.warn(
        `Page-view cap unavailable, counting: ${(error as Error).message}`,
      );
      return true;
    }
  }

  private hash(...parts: string[]): string {
    return readerHash(this.hashSecret, ...parts);
  }

  /** The first UTC day of a window of `days` days ending today. */
  windowStart(days: number, now: Date = new Date()): string {
    return addIsoDays(utcDay(now), -(days - 1));
  }

  /** The most viewed pages of one kind in one language, most viewed first. */
  async top(
    kind: PageViewKind,
    locale: PageViewLocale,
    days: number,
    limit: number,
    now: Date = new Date(),
  ): Promise<TopPageViewDto[]> {
    const rows: Array<{ key: string; views: string | number }> =
      await this.views.query(
        `SELECT key, SUM(views) AS views
         FROM page_view_days
         WHERE kind = $1 AND locale = $2 AND day >= $3
         GROUP BY key
         ORDER BY SUM(views) DESC, key ASC
         LIMIT $4`,
        [kind, locale, this.windowStart(days, now), limit],
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
      Array<{ day: string; kind: PageViewKind; views: string | number }>,
    ] = await Promise.all([
      this.views.query(
        `SELECT kind, locale, SUM(views) AS views
         FROM page_view_days
         WHERE day >= $1
         GROUP BY kind, locale`,
        [from],
      ),
      this.views.query(
        // One pass: the views per page and language, each page's score beside
        // them (all languages, or the one asked for), ranked per kind. Joining
        // a second scan back for the split took 2.6 times as long.
        `WITH per_locale AS (
           SELECT kind, key, locale, SUM(views) AS views
           FROM page_view_days
           WHERE day >= $1
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
        [from, locale ?? null, limit],
      ),
      this.views.query(
        // As text: the driver turns a `date` into a Date at local midnight,
        // which a process outside UTC would read back as the day before.
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, kind, SUM(views) AS views
         FROM page_view_days
         WHERE day >= $1
         GROUP BY day, kind`,
        [from],
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
      const entry = daily.get(row.day);
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
