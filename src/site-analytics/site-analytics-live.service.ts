import {
  Inject,
  Injectable,
  Logger,
  ServiceUnavailableException,
} from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  ALL_LOCALES_SCOPE,
  LIVE_LISTS,
  LIVE_LONG_MINUTES,
  LIVE_SHORT_MINUTES,
  LIVE_TOP_EVENTS,
  LIVE_TOP_PAGES,
  LIVE_TOP_REFERRERS,
  SITE_ANALYTICS_KEYS as KEYS,
  minuteKey,
  type LiveList,
  type SiteLocale,
} from "./site-analytics.constants";
import { num, type Count } from "./site-analytics.query";
import type { SiteLiveDto } from "./dto/site-analytics.dto";

/** The lists counted over the last few minutes; the rest over half an hour. */
const SHORT_LISTS: ReadonlySet<LiveList> = new Set([
  "pages",
  "countries",
  "devices",
]);

/**
 * The admin's live view: who is on the site now, from the per-minute Redis
 * keys the collector writes, never from Postgres: one pipeline per call, so
 * an admin page can poll it every few seconds. No figure in it is about one
 * reader.
 */
@Injectable()
export class SiteAnalyticsLiveService {
  private readonly logger = new Logger(SiteAnalyticsLiveService.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /**
   * The last {@link LIVE_LONG_MINUTES} whole minutes and the current one, for
   * all languages or one. 503 when Redis does not answer: zeros would read as
   * an empty site.
   */
  async live(
    locale: SiteLocale | null = null,
    now = new Date(),
  ): Promise<SiteLiveDto> {
    const scope = locale ?? ALL_LOCALES_SCOPE;
    const current = Math.floor(now.getTime() / 60_000) * 60_000;
    const minutes = Array.from(
      { length: LIVE_LONG_MINUTES + 1 },
      (_, i) => new Date(current - (LIVE_LONG_MINUTES - i) * 60_000),
    );
    const keys = minutes.map(minuteKey);
    const short = keys.slice(-LIVE_SHORT_MINUTES);

    const pipeline = this.redis.pipeline();
    pipeline.pfcount(...short.map((key) => KEYS.activeMinute(key, scope)));
    pipeline.mget(...keys.map((key) => KEYS.pageviewsMinute(key, scope)));
    pipeline.mget(...keys.map((key) => KEYS.eventsMinute(key, scope)));
    for (const key of keys) pipeline.pfcount(KEYS.activeMinute(key, scope));
    const reads: Array<{ list: LiveList; at: number }> = [];
    let at = 3 + keys.length;
    for (const list of LIVE_LISTS) {
      for (const key of SHORT_LISTS.has(list) ? short : keys) {
        pipeline.hgetall(KEYS.live(list, key, scope));
        reads.push({ list, at: at++ });
      }
    }

    let results: Array<[Error | null, unknown]>;
    try {
      results = (await pipeline.exec()) ?? [];
    } catch (error) {
      this.logger.warn(`Live view unavailable: ${(error as Error).message}`);
      throw new ServiceUnavailableException("Live counts are unavailable");
    }
    if (results.length === 0 || results.some(([error]) => error)) {
      throw new ServiceUnavailableException("Live counts are unavailable");
    }
    const reply = (i: number) => results[i]?.[1];

    const totals = new Map<LiveList, Map<string, number>>(
      LIVE_LISTS.map((list) => [list, new Map()]),
    );
    for (const { list, at: index } of reads) {
      const fields = (reply(index) ?? {}) as Record<string, string>;
      const sums = totals.get(list) as Map<string, number>;
      for (const [field, count] of Object.entries(fields)) {
        sums.set(field, (sums.get(field) ?? 0) + num(count));
      }
    }
    const ranked = (list: LiveList, limit = Infinity) =>
      [...(totals.get(list) as Map<string, number>)]
        .filter(([, count]) => count > 0)
        .sort(([a, x], [b, y]) => y - x || (a < b ? -1 : a > b ? 1 : 0))
        .slice(0, limit);

    const pageviews = reply(1) as Count[];
    const events = reply(2) as Count[];
    return {
      at: `${now.toISOString().slice(0, 19)}Z`,
      activeVisitors: num(reply(0) as Count),
      perMinute: minutes.map((minute, i) => ({
        minute: `${minute.toISOString().slice(0, 16)}:00Z`,
        pageviews: num(pageviews[i]),
        events: num(events[i]),
        visitors: num(reply(3 + i) as Count),
      })),
      pages: ranked("pages", LIVE_TOP_PAGES).map(([field, views]) => {
        const [path, pageType] = parsePage(field);
        return { path, pageType, views };
      }),
      countries: ranked("countries").map(([country, views]) => ({
        country: country.toUpperCase(),
        views,
      })),
      channels: ranked("channels").map(([channel, visits]) => ({
        channel,
        visits,
      })),
      referrers: ranked("referrers", LIVE_TOP_REFERRERS).map(
        ([referrer, visits]) => ({ referrer, visits }),
      ),
      devices: ranked("devices").map(([device, views]) => ({
        device,
        views,
      })),
      events: ranked("events", LIVE_TOP_EVENTS).map(([name, count]) => ({
        name,
        count,
      })),
    };
  }
}

/** A live page's field: `[path, pageType]` as JSON. */
function parsePage(field: string): [string, string] {
  try {
    const parsed = JSON.parse(field) as unknown;
    if (
      Array.isArray(parsed) &&
      typeof parsed[0] === "string" &&
      typeof parsed[1] === "string"
    ) {
      return [parsed[0], parsed[1]];
    }
  } catch {
    // A field the collector did not write; shown as it is.
  }
  return [field, ""];
}
