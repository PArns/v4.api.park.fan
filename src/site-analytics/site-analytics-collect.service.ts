import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { addressBucket } from "../common/utils/request.util";
import {
  readerHash,
  readerHashSecret,
  secondsUntilUtcDayEnds,
  utcDay,
} from "../common/utils/reader-hash.util";
import { GeoipService } from "../geoip/geoip.service";
import { isBotUserAgent } from "../page-views/bot-user-agent.util";
import {
  MAX_VIEWS_PER_ADDRESS_PER_DAY,
  MAX_VIEWS_PER_READER_PER_DAY,
} from "../page-views/page-view.constants";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import {
  SiteAnalyticsStorageService,
  eventField,
  pageviewField,
  type PageviewDims,
} from "./site-analytics-storage.service";
import {
  ALL_LOCALES_SCOPE,
  MAX_EVENT_NAMES_PER_DAY,
  MAX_HITS_PER_ADDRESS_PER_DAY,
  MAX_PAGE_TYPES_PER_DAY,
  MAX_PROP_COMBINATIONS_PER_EVENT_PER_DAY,
  OVERFLOW_PAGE_TYPE,
  OVERFLOW_PROPS,
  PAGE_READ_EVENT,
  REALTIME_ACTIVE_MINUTES,
  REALTIME_SERIES_MINUTES,
  SITE_ANALYTICS_KEYS as KEYS,
  UNKNOWN_COUNTRY,
  VISITOR_KEY_GRACE_SECONDS,
  VISIT_IDLE_SECONDS,
  flushWindowOf,
  hourKey,
  minuteKey,
  type Channel,
} from "./site-analytics.constants";
import {
  propsKeyOf,
  validateHit,
  type ValidEventHit,
  type ValidHit,
  type ValidPageviewHit,
} from "./site-analytics.hits";
import { classifyReferrer } from "./referrer.util";
import { parseUserAgent } from "./user-agent.util";

/** A minute past midnight UTC, so a daily key never expires before its day. */
const DAY_GRACE_SECONDS = 60;

/** The realtime keys live a little longer than the window that reads them. */
const ACTIVE_MINUTE_TTL_SECONDS = (REALTIME_ACTIVE_MINUTES + 5) * 60;
const PAGEVIEWS_MINUTE_TTL_SECONDS = (REALTIME_SERIES_MINUTES + 5) * 60;

export interface CollectRequest {
  /** The body's hits, unchecked: each one is validated here. */
  hits: unknown[];
  ip: string | null;
  userAgent: string | null;
}

/** What the server derives about the browser, never what it is. */
interface Reader {
  browser: string;
  os: string;
  country: string;
}

/**
 * A visit's first page view, as its Redis key keeps it: where the visit
 * entered and came from. No address, no User-Agent, no time.
 */
export interface VisitStart {
  locale: string;
  path: string;
  pageType: string;
  viewport: string;
  referrer: string;
  channel: Channel;
  campaign: string;
}

/** A hit that passed the guards, with what Redis said about its visit. */
export type ObservedHit =
  | {
      hit: ValidPageviewHit;
      /** The visit this page view belongs to; null where it is not known. */
      visit: VisitStart | null;
      /** This page view opened the visit. */
      start: boolean;
      /** The visit whose second page view this is. */
      engaged: VisitStart | null;
    }
  | { hit: ValidEventHit; visit: VisitStart | null };

/**
 * The write side of the site statistic: `POST /v1/analytics/collect`.
 *
 * Postgres is not touched on the way in. A request costs three Redis round
 * trips: the address's daily cap; the guards, the visit, the visitors and the
 * realtime minute in one pipeline; and the counts, added with `HINCRBY` into
 * the current five-minute window's hash, which the flusher writes to the
 * insert-only hypertables (`SiteAnalyticsStorageService.flush`).
 *
 * Nothing about a reader is stored. The visit, visitor and guard keys are
 * HMACs of address bucket and User-Agent under `ANALYTICS_HASH_SECRET`, which
 * lives in the environment and never in Redis, and every one of them expires.
 * docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteAnalyticsCollectService {
  private readonly logger = new Logger(SiteAnalyticsCollectService.name);
  private readonly hashSecret: string;

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly geoip: GeoipService,
    private readonly storage: SiteAnalyticsStorageService,
    private readonly blocklist: SiteAnalyticsBlocklist,
  ) {
    this.hashSecret = readerHashSecret(
      ["ANALYTICS_HASH_SECRET", "PAGE_VIEWS_HASH_SECRET"],
      this.logger,
      "the site statistic",
    );
  }

  /**
   * Counts the hits of one request that pass the wire format and the guards.
   * Returns how many counted; the beacon gets its 204 either way.
   */
  async collect(request: CollectRequest, now = new Date()): Promise<number> {
    if (isBotUserAgent(request.userAgent)) return 0;
    const valid = request.hits
      .map(validateHit)
      .filter((hit): hit is ValidHit => hit !== null);
    return this.record(valid, request.ip, request.userAgent, now);
  }

  /**
   * Counts hits that are already checked: from `collect`, and from the
   * deprecated `POST /v1/page-views`, which builds its `page_read` itself.
   * Blocked names go first, then the address's daily budget, then the
   * per-day guards.
   */
  async record(
    valid: ValidHit[],
    ip: string | null,
    userAgent: string | null,
    now = new Date(),
  ): Promise<number> {
    if (isBotUserAgent(userAgent)) return 0;
    const blocked = await this.blocklist.names();
    let hits = valid.filter(
      (hit) => hit.t !== "event" || !blocked.has(hit.name),
    );
    if (hits.length === 0) return 0;

    const day = utcDay(now);
    const bucket = addressBucket(ip);
    const allowed = await this.withinCap(bucket, day, hits.length, now);
    hits = hits.slice(0, allowed);
    if (hits.length === 0) return 0;

    const { browser, os } = parseUserAgent(userAgent);
    const country = (ip && this.geoip.lookupCountry(ip)) || UNKNOWN_COUNTRY;
    const reader = { browser, os, country };
    const observed = await this.observe(hits, bucket, userAgent ?? "", now);
    if (observed.hits.length === 0) return 0;

    const increments = bufferIncrements(observed.hits, reader);
    await this.buffer(flushWindowOf(now), increments, observed);
    return observed.hits.length;
  }

  /**
   * How many of `count` hits the address may still count today. Without an
   * address, or with Redis down, all of them: the cap guards against one
   * machine, and refusing every reader because a cache is away would empty
   * the statistic.
   */
  private async withinCap(
    bucket: string | null,
    day: string,
    count: number,
    now: Date,
  ): Promise<number> {
    if (!bucket) return count;
    const key = KEYS.cap(this.hash("cap", day, bucket));
    try {
      const results = await this.redis
        .pipeline()
        .incrby(key, count)
        .expire(key, secondsUntilUtcDayEnds(now, DAY_GRACE_SECONDS), "NX")
        .exec();
      const total = Number(results?.[0]?.[1] ?? 0);
      if (!Number.isFinite(total) || total <= 0) return count;
      const before = total - count;
      return Math.max(
        0,
        Math.min(count, MAX_HITS_PER_ADDRESS_PER_DAY - before),
      );
    } catch (error) {
      this.logger.warn(
        `Site analytics cap unavailable, counting: ${(error as Error).message}`,
      );
      return count;
    }
  }

  /**
   * One pipeline for the guards and the visit: the day's event names, page
   * types and each event's property combinations (a Redis set each, `SADD`
   * then `SCARD`, so a new member past the limit is caught and taken out
   * again), `page_read`'s per-reader and per-address counters, the visit key
   * (its first page view and its page-view count), the day's and the hour's
   * visitors and the realtime minute.
   *
   * With Redis down every hit passes the guards and no visit is known: page
   * views and events count, visits and visitors do not.
   */
  private async observe(
    hits: ValidHit[],
    bucket: string | null,
    userAgent: string,
    now: Date,
  ): Promise<{
    hits: ObservedHit[];
    newVisit: { key: string; start: VisitStart } | null;
    release: Array<[string, string]>;
  }> {
    const day = utcDay(now);
    const dayTtl = secondsUntilUtcDayEnds(now, DAY_GRACE_SECONDS);
    const pipeline = this.redis.pipeline();
    let index = 0;
    const next = () => index++;

    /** One `SADD`/`SCARD` pair per distinct member of a daily set. */
    const sets = new Map<
      string,
      { key: string; member: string; limit: number; add: number }
    >();
    const guard = (key: string, member: string, limit: number) => {
      const id = `${key}\u0000${member}`;
      if (sets.has(id)) return id;
      pipeline.sadd(key, member);
      const add = next();
      pipeline.scard(key);
      next();
      pipeline.expire(key, dayTtl, "NX");
      next();
      sets.set(id, { key, member, limit, add });
      return id;
    };
    const pageTypeGuard = hits.map((hit) =>
      guard(KEYS.pageTypes(day), hit.pageType, MAX_PAGE_TYPES_PER_DAY),
    );
    const eventGuards = hits.map((hit) => {
      if (hit.t !== "event") return null;
      const name = guard(KEYS.names(day), hit.name, MAX_EVENT_NAMES_PER_DAY);
      const combination =
        hit.name === PAGE_READ_EVENT
          ? null
          : guard(
              KEYS.combinations(day, hit.name),
              this.hash("combo", hit.propsKey, hit.entity ?? "").slice(0, 16),
              MAX_PROP_COMBINATIONS_PER_EVENT_PER_DAY,
            );
      return { name, combination };
    });

    // page_read keeps the page-view counter's caps: three reads per reader
    // and page, and a daily budget per address.
    const reads = hits.map((hit) => {
      if (!bucket || hit.t !== "event" || hit.name !== PAGE_READ_EVENT)
        return null;
      const page = hit.entity ?? hit.props.key ?? "";
      const key = KEYS.read(
        this.hash("read", day, bucket, userAgent, hit.props.kind, page),
      );
      pipeline.incr(key);
      const at = next();
      pipeline.expire(key, dayTtl, "NX");
      next();
      return at;
    });
    const readCount = reads.filter((at) => at !== null).length;
    let readAddress: number | null = null;
    if (bucket && readCount > 0) {
      const key = KEYS.readAddress(this.hash("read-addr", day, bucket));
      pipeline.incrby(key, readCount);
      readAddress = next();
      pipeline.expire(key, dayTtl, "NX");
      next();
    }

    const pageviews = hits.filter(
      (hit): hit is ValidPageviewHit => hit.t === "pageview",
    );
    let visitKey: string | null = null;
    let startReply: number | null = null;
    const countReplies = new Map<ValidHit, number>();
    if (bucket) {
      visitKey = KEYS.visit(this.hash("visit", bucket, userAgent));
      const visitor = this.hash("visitor", day, bucket, userAgent);
      pipeline.hget(visitKey, "c");
      startReply = next();
      for (const hit of pageviews) {
        pipeline.hincrby(visitKey, "n", 1);
        countReplies.set(hit, next());
      }
      // Every hit keeps the visit alive; only a page view can open one.
      pipeline.expire(visitKey, VISIT_IDLE_SECONDS);
      next();

      const locales = new Set(pageviews.map((hit) => hit.locale));
      const scopes = [ALL_LOCALES_SCOPE, ...locales];
      if (pageviews.length > 0) {
        const dayKeyTtl = secondsUntilUtcDayEnds(
          now,
          VISITOR_KEY_GRACE_SECONDS,
        );
        const hourKeyTtl = secondsUntilUtcDayEnds(now, 3600);
        for (const scope of scopes) {
          const dayKey = KEYS.visitors(day, scope);
          const hourKeyName = KEYS.hourVisitors(hourKey(now), scope);
          pipeline.pfadd(dayKey, visitor).expire(dayKey, dayKeyTtl, "NX");
          pipeline
            .pfadd(hourKeyName, visitor)
            .expire(hourKeyName, hourKeyTtl, "NX");
          index += 4;
        }
      }
      const minute = minuteKey(now);
      const active = new Set(hits.map((hit) => hit.locale));
      for (const scope of [ALL_LOCALES_SCOPE, ...active]) {
        const key = KEYS.activeMinute(minute, scope);
        pipeline.pfadd(key, visitor).expire(key, ACTIVE_MINUTE_TTL_SECONDS);
        index += 2;
      }
      const perLocale = new Map<string, number>();
      for (const hit of pageviews) {
        perLocale.set(hit.locale, (perLocale.get(hit.locale) ?? 0) + 1);
      }
      if (pageviews.length > 0)
        perLocale.set(ALL_LOCALES_SCOPE, pageviews.length);
      for (const [scope, count] of perLocale) {
        const key = KEYS.pageviewsMinute(minute, scope);
        pipeline.incrby(key, count).expire(key, PAGEVIEWS_MINUTE_TTL_SECONDS);
        index += 2;
      }
    }

    let results: Array<[Error | null, unknown]> | null = null;
    try {
      results = await pipeline.exec();
    } catch (error) {
      this.logger.warn(
        `Site analytics guards unavailable, counting: ${(error as Error).message}`,
      );
    }
    const reply = (at: number | null): unknown =>
      at === null || !results || results[at]?.[0] ? null : results[at]?.[1];

    const release: Array<[string, string]> = [];
    const passed = new Map<string, boolean>();
    for (const [id, set] of sets) {
      const added = Number(reply(set.add)) === 1;
      const size = Number(reply(set.add + 1));
      const over = added && size > set.limit;
      if (over) release.push([set.key, set.member]);
      passed.set(id, !over);
    }

    let readBudget = Infinity;
    if (readAddress !== null && reply(readAddress) !== null) {
      const total = Number(reply(readAddress));
      readBudget = MAX_VIEWS_PER_ADDRESS_PER_DAY - (total - readCount);
    }

    let current = parseVisitStart(reply(startReply));
    let newVisit: { key: string; start: VisitStart } | null = null;
    const observed: ObservedHit[] = [];
    hits.forEach((raw, i) => {
      const pageType = passed.get(pageTypeGuard[i])
        ? raw.pageType
        : OVERFLOW_PAGE_TYPE;
      if (raw.t === "pageview") {
        const hit = { ...raw, pageType };
        const count = Number(reply(countReplies.get(raw) ?? null) ?? 0);
        if (count === 1 && visitKey) {
          const { referrer, channel } = classifyReferrer(
            hit.referrer,
            hit.campaign,
          );
          current = {
            locale: hit.locale,
            path: hit.path,
            pageType: hit.pageType,
            viewport: hit.viewport,
            referrer,
            channel,
            campaign: hit.campaign ?? "",
          };
          newVisit = { key: visitKey, start: current };
          observed.push({ hit, visit: current, start: true, engaged: null });
          return;
        }
        observed.push({
          hit,
          visit: current,
          start: false,
          engaged: count === 2 ? current : null,
        });
        return;
      }

      const guards = eventGuards[i];
      if (guards && passed.get(guards.name) === false) return;
      const at = reads[i];
      if (at !== undefined && at !== null) {
        const readerCount = Number(reply(at) ?? 0);
        if (readerCount > MAX_VIEWS_PER_READER_PER_DAY) return;
        if (readBudget <= 0) return;
        readBudget -= 1;
      }
      let hit: ValidEventHit = { ...raw, pageType };
      if (guards?.combination && passed.get(guards.combination) === false) {
        hit = {
          ...hit,
          props: { ...OVERFLOW_PROPS },
          propsKey: propsKeyOf(OVERFLOW_PROPS),
          entity: null,
        };
      }
      observed.push({ hit, visit: current });
    });
    return { hits: observed, newVisit, release };
  }

  /**
   * Adds the counts to the window's hash, remembers a new visit's first page
   * view and takes over-limit members out of their sets, in one round trip.
   * When Redis cannot take them, the counts are written to Postgres directly,
   * so a Redis outage loses no hit.
   */
  private async buffer(
    window: string,
    increments: Map<string, number>,
    observed: {
      newVisit: { key: string; start: VisitStart } | null;
      release: Array<[string, string]>;
    },
  ): Promise<void> {
    const key = KEYS.buffer(window);
    try {
      const pipeline = this.redis.pipeline();
      for (const [field, count] of increments) {
        pipeline.hincrby(key, field, count);
      }
      pipeline.sadd(KEYS.windows, window);
      if (observed.newVisit) {
        pipeline.hset(
          observed.newVisit.key,
          "c",
          JSON.stringify(observed.newVisit.start),
        );
      }
      for (const [set, member] of observed.release) pipeline.srem(set, member);
      const results = await pipeline.exec();
      if (!results || results.some(([error]) => error)) {
        throw new Error("a buffer command failed");
      }
    } catch (error) {
      this.logger.warn(
        `Site analytics buffer unavailable, writing directly: ${(error as Error).message}`,
      );
      try {
        await this.storage.writeDirect(window, increments);
      } catch (writeError) {
        this.logger.error(
          `Site analytics hits lost: ${(writeError as Error).message}`,
        );
      }
    }
  }

  private hash(...parts: string[]): string {
    return readerHash(this.hashSecret, "site-analytics", ...parts);
  }
}

function parseVisitStart(raw: unknown): VisitStart | null {
  if (typeof raw !== "string") return null;
  try {
    const start = JSON.parse(raw) as VisitStart;
    return typeof start?.path === "string" ? start : null;
  } catch {
    return null;
  }
}

/**
 * The buffer fields one request adds, summed. Pure, so the counting rules are
 * testable without Redis: a page view counts a view under its own dimensions
 * and its visit's, a visit where it opened one, and an `engaged` under its
 * visit's first page view where it is the second; an event counts once.
 */
export function bufferIncrements(
  observed: ObservedHit[],
  reader: Reader,
): Map<string, number> {
  const increments = new Map<string, number>();
  const add = (field: string) =>
    increments.set(field, (increments.get(field) ?? 0) + 1);
  const visitDims = (visit: VisitStart | null) =>
    [
      visit?.path ?? "",
      visit?.referrer ?? "",
      visit?.channel ?? "",
      visit?.campaign ?? "",
    ] as const;

  for (const entry of observed) {
    if (entry.hit.t === "event") {
      const hit = entry.hit;
      add(
        eventField([
          hit.name,
          hit.locale,
          hit.path,
          hit.pageType,
          hit.viewport,
          reader.browser,
          reader.os,
          reader.country,
          ...visitDims(entry.visit),
          hit.propsKey,
          hit.entity ?? "",
        ]),
      );
      continue;
    }
    const { hit } = entry;
    const dims: PageviewDims = [
      hit.locale,
      hit.path,
      hit.pageType,
      hit.viewport,
      reader.browser,
      reader.os,
      reader.country,
      ...visitDims(entry.visit),
    ];
    add(pageviewField("v", dims));
    if ("start" in entry && entry.start) add(pageviewField("s", dims));
    if ("engaged" in entry && entry.engaged) {
      const start = entry.engaged;
      add(
        pageviewField("e", [
          start.locale,
          start.path,
          start.pageType,
          start.viewport,
          reader.browser,
          reader.os,
          reader.country,
          ...visitDims(start),
        ]),
      );
    }
  }
  return increments;
}
