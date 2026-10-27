import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, Repository } from "typeorm";
import { Redis } from "ioredis";
import { createHmac, randomBytes } from "crypto";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { incrementWithWindow } from "../common/redis/rate-limit.util";
import { addIsoDays } from "../common/utils/date.util";
import { buildAttractionUrl, buildParkUrl } from "../common/utils/url.util";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { PageViewDay } from "./entities/page-view-day.entity";
import { isBotUserAgent } from "./bot-user-agent.util";
import {
  MAX_VIEWS_PER_ADDRESS_PER_DAY,
  PAGE_VIEW_KINDS,
  type PageViewKind,
  type PageViewLocale,
} from "./page-view.constants";
import type {
  AdminPageViewEntryDto,
  AdminPageViewsResponseDto,
  TopPageViewDto,
} from "./dto/page-view.dto";

const SEEN_PREFIX = "page-views:seen:";

/** A minute past midnight UTC, so a key never expires before the day it caps. */
const SEEN_GRACE_SECONDS = 60;

export interface PageView {
  kind: PageViewKind;
  key: string;
  locale: PageViewLocale;
  ip: string | null;
  userAgent: string | null;
}

/** Today's UTC date, `YYYY-MM-DD`. A view's day is UTC, see `PageViewDay`. */
export function utcDay(now: Date): string {
  return now.toISOString().slice(0, 10);
}

/**
 * The page-view counter behind the "most read" lists and the admin's
 * popularity overview. See docs/frontend/page-views.md.
 *
 * Counts live in Postgres, not Redis: this Redis runs `volatile-lru`, and a
 * ranking that loses a day of counts under memory pressure ranks wrong without
 * saying so.
 *
 * No address is stored. The cap against counting one reader twice is a Redis
 * counter whose key is an HMAC of day, address and page under
 * `PAGE_VIEWS_HASH_SECRET`, which lives in the environment and never in
 * Redis: a key cannot be turned back into an address without it, and it
 * expires when its UTC day ends.
 */
@Injectable()
export class PageViewsService {
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
    const configured = process.env.PAGE_VIEWS_HASH_SECRET?.trim();
    if (!configured) {
      // Still no address in Redis; only the cap stops being shared between
      // instances and across a restart.
      this.logger.warn(
        "PAGE_VIEWS_HASH_SECRET is not set; the page-view cap uses a per-process secret.",
      );
    }
    this.hashSecret = configured || randomBytes(32).toString("hex");
  }

  /**
   * Count one view, unless it comes from a bot or from an address that has
   * already counted this page {@link MAX_VIEWS_PER_ADDRESS_PER_DAY} times
   * today. Returns whether it was counted.
   */
  async record(view: PageView, now: Date = new Date()): Promise<boolean> {
    if (isBotUserAgent(view.userAgent)) return false;
    const day = utcDay(now);
    if (!(await this.withinAddressCap(view, day, now))) return false;

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
   * Without an address, or with Redis down, the view counts: the cap is a
   * guard against one machine, and refusing every reader because a cache is
   * away would empty the rankings.
   */
  private async withinAddressCap(
    view: PageView,
    day: string,
    now: Date,
  ): Promise<boolean> {
    if (!view.ip) return true;
    const key =
      SEEN_PREFIX +
      createHmac("sha256", this.hashSecret)
        .update(`${day}|${view.ip}|${view.kind}|${view.key}|${view.locale}`)
        .digest("hex")
        .slice(0, 32);
    const endOfDay = Date.parse(`${addIsoDays(day, 1)}T00:00:00Z`);
    const ttl =
      Math.ceil((endOfDay - now.getTime()) / 1000) + SEEN_GRACE_SECONDS;
    try {
      const count = await incrementWithWindow(this.redis, key, ttl);
      return count <= MAX_VIEWS_PER_ADDRESS_PER_DAY;
    } catch (error) {
      this.logger.warn(
        `Page-view cap unavailable, counting: ${(error as Error).message}`,
      );
      return true;
    }
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
   * the most viewed pages with their own split, and the views per day.
   */
  async adminOverview(
    days: number,
    limit: number,
    now: Date = new Date(),
  ): Promise<AdminPageViewsResponseDto> {
    const from = this.windowStart(days, now);
    const [byKey, byDay]: [
      Array<{
        kind: PageViewKind;
        key: string;
        locale: PageViewLocale;
        views: string | number;
      }>,
      Array<{ day: string; kind: PageViewKind; views: string | number }>,
    ] = await Promise.all([
      this.views.query(
        `SELECT kind, key, locale, SUM(views) AS views
         FROM page_view_days
         WHERE day >= $1
         GROUP BY kind, key, locale`,
        [from],
      ),
      this.views.query(
        // As text: the driver turns a `date` into a Date at local midnight,
        // which a process outside UTC would read back as the day before.
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, kind, SUM(views) AS views
         FROM page_view_days
         WHERE day >= $1
         GROUP BY day, kind
         ORDER BY day ASC`,
        [from],
      ),
    ]);

    const kinds = await Promise.all(
      PAGE_VIEW_KINDS.map(async (kind) => {
        const entries = new Map<string, AdminPageViewEntryDto>();
        const byLocale: Partial<Record<PageViewLocale, number>> = {};
        let total = 0;
        for (const row of byKey) {
          if (row.kind !== kind) continue;
          const views = Number(row.views);
          const entry = entries.get(row.key) ?? {
            key: row.key,
            total: 0,
            byLocale: {},
          };
          entry.total += views;
          entry.byLocale[row.locale] =
            (entry.byLocale[row.locale] ?? 0) + views;
          entries.set(row.key, entry);
          byLocale[row.locale] = (byLocale[row.locale] ?? 0) + views;
          total += views;
        }
        const top = [...entries.values()]
          .sort((a, b) => b.total - a.total || a.key.localeCompare(b.key))
          .slice(0, limit);
        return { kind, total, byLocale, top: await this.named(kind, top) };
      }),
    );

    const daily = new Map<string, Partial<Record<PageViewKind, number>>>();
    for (const row of byDay) {
      const entry = daily.get(row.day) ?? {};
      entry[row.kind] = (entry[row.kind] ?? 0) + Number(row.views);
      daily.set(row.day, entry);
    }

    return {
      days,
      from,
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
    const ids = entries.map((entry) => entry.key);
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
