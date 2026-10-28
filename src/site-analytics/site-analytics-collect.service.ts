import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { addIsoDays } from "../common/utils/date.util";
import { addressBucket } from "../common/utils/request.util";
import {
  dailyHashKey,
  readerHash,
  readerHashSecret,
  secondsUntilUtcDayEnds,
  utcDay,
} from "../common/utils/reader-hash.util";
import { GeoipService } from "../geoip/geoip.service";
import { isBotUserAgent } from "../page-views/bot-user-agent.util";
import {
  MAX_VIEWS_PER_ADDRESS_AND_PAGE_PER_DAY,
  MAX_VIEWS_PER_ADDRESS_PER_DAY,
  MAX_VIEWS_PER_READER_PER_DAY,
} from "../page-views/page-view.constants";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import { defineAdmitCommand } from "./site-analytics-admit";
import { FallbackBuffer } from "./site-analytics-fallback-buffer";
import {
  SiteAnalyticsStorageService,
  eventField,
  pageviewField,
  type PageviewDims,
} from "./site-analytics-storage.service";
import { SiteEntityNames } from "./site-entity-names.service";
import {
  ADDRESS_CAP_IPV6_PREFIX,
  ALL_LOCALES_SCOPE,
  DROP_REASONS,
  FALLBACK_DRAIN_MS,
  FALLBACK_MAX_FIELDS,
  KNOWN_MEMBER_HEADROOM,
  MAX_HITS_PER_ADDRESS_PER_DAY,
  MAX_HITS_PER_WINDOW,
  OVERFLOW_PAGE_TYPE,
  OVERFLOW_PROPS,
  OVERFLOW_VALUE,
  PAGE_READ_EVENT,
  REALTIME_ACTIVE_MINUTES,
  REALTIME_SERIES_MINUTES,
  SITE_ANALYTICS_KEYS as KEYS,
  SITE_GUARDS,
  UNKNOWN_COUNTRY,
  VISITOR_KEY_GRACE_SECONDS,
  VISIT_IDLE_SECONDS,
  flushWindowOf,
  hourKey,
  minuteKey,
  type Channel,
  type DropReason,
  type SiteGuard,
} from "./site-analytics.constants";
import {
  propsKeyOf,
  validateHit,
  type ValidEventHit,
  type ValidHit,
  type ValidPageviewHit,
} from "./site-analytics.hits";
import { classifyReferrer } from "./referrer.util";
import { MAX_USER_AGENT_LENGTH, parseUserAgent } from "./user-agent.util";

/** A minute past midnight UTC, so a daily key never expires before its day. */
const DAY_GRACE_SECONDS = 60;

/** A guard set lives through the next day, which admits its values as known. */
const GUARD_SET_GRACE_SECONDS = 24 * 3600 + DAY_GRACE_SECONDS;

/** A window's hit counter outlives the window by the flusher's lag. */
const WINDOW_HITS_TTL_SECONDS = 15 * 60;

/** The realtime keys live a little longer than the window that reads them. */
const ACTIVE_MINUTE_TTL_SECONDS = (REALTIME_ACTIVE_MINUTES + 5) * 60;
const PAGEVIEWS_MINUTE_TTL_SECONDS = (REALTIME_SERIES_MINUTES + 5) * 60;

/**
 * The address of a request without one: every such request shares one
 * budget, rather than none having a cap. Production always has an address.
 */
const NO_ADDRESS = "none";

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
      /** This page view opened a visit, which started like this. */
      start: VisitStart | null;
      /** The visit whose second page view this is. */
      engaged: VisitStart | null;
    }
  | { hit: ValidEventHit };

/** Counts of dropped and overflowed hits by reason. */
type Drops = Map<DropReason, number>;

const countDrop = (drops: Drops, reason: DropReason, by = 1) => {
  if (by > 0) drops.set(reason, (drops.get(reason) ?? 0) + by);
};

/**
 * The write side of the site statistic: `POST /v1/analytics/collect`.
 *
 * Postgres is not touched on the way in. A request costs three Redis round
 * trips: the address's and the window's caps; the guards, the visit, the
 * visitors and the realtime minute in one pipeline; and the counts, added in
 * one `MULTI` with `HINCRBY` into the current five-minute window's hash, which
 * the flusher writes to the insert-only hypertables
 * (`SiteAnalyticsStorageService.flush`). With Redis away the counts wait in a
 * bounded buffer in this process and go to Postgres once a minute.
 *
 * Nothing about a reader is stored. The visit, visitor, cap and guard keys are
 * HMACs of address and User-Agent under the day's key, derived from
 * `ANALYTICS_HASH_SECRET`, which lives in the environment and never in Redis;
 * every one of them expires within about a day. docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteAnalyticsCollectService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(SiteAnalyticsCollectService.name);
  private readonly hashSecret: string;
  private readonly dayKeys = new Map<string, string>();
  private readonly fallback = new FallbackBuffer(FALLBACK_MAX_FIELDS);
  /** Drops counted while Redis was away, added with the next buffer write. */
  private readonly pendingDrops: Drops = new Map();
  private drainTimer: NodeJS.Timeout | null = null;

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly geoip: GeoipService,
    private readonly storage: SiteAnalyticsStorageService,
    private readonly blocklist: SiteAnalyticsBlocklist,
    private readonly entities: SiteEntityNames,
  ) {
    this.hashSecret = readerHashSecret(
      ["ANALYTICS_HASH_SECRET", "PAGE_VIEWS_HASH_SECRET"],
      this.logger,
      "the site statistic",
    );
    defineAdmitCommand(this.redis);
  }

  onModuleInit(): void {
    this.drainTimer = setInterval(
      () => void this.drainFallback(),
      FALLBACK_DRAIN_MS,
    );
    this.drainTimer.unref();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.drainTimer) clearInterval(this.drainTimer);
    await this.drainFallback();
  }

  /**
   * Counts the hits of one request that pass the wire format and the guards.
   * Returns how many counted; the beacon gets its 204 either way.
   */
  async collect(request: CollectRequest, now = new Date()): Promise<number> {
    if (isBotUserAgent(request.userAgent?.slice(0, MAX_USER_AGENT_LENGTH)))
      return 0;
    const valid = request.hits
      .map(validateHit)
      .filter((hit): hit is ValidHit => hit !== null);
    return this.record(valid, request.ip, request.userAgent, now);
  }

  /**
   * Counts hits that are already checked: from `collect`, and from the
   * deprecated `POST /v1/page-views`, which builds its `page_read` itself.
   * Blocked names and reads of parks or rides that do not exist go first,
   * then the address's and the window's caps, then the per-day guards.
   */
  async record(
    valid: ValidHit[],
    ip: string | null,
    rawUserAgent: string | null,
    now = new Date(),
  ): Promise<number> {
    const userAgent = rawUserAgent?.slice(0, MAX_USER_AGENT_LENGTH) ?? null;
    if (isBotUserAgent(userAgent)) return 0;
    const drops: Drops = new Map();
    const blocked = await this.blocklist.names();
    let hits = await this.existing(
      valid.filter((hit) => hit.t !== "event" || !blocked.has(hit.name)),
      drops,
    );
    if (hits.length === 0) return this.noteDrops(drops, now);

    const day = utcDay(now);
    const window = flushWindowOf(now);
    const capBucket = addressBucket(ip, ADDRESS_CAP_IPV6_PREFIX) ?? NO_ADDRESS;
    const { browser, os } = parseUserAgent(userAgent);
    const country = (ip && this.geoip.lookupCountry(ip)) || UNKNOWN_COUNTRY;
    const reader = { browser, os, country };

    let observed: {
      hits: ObservedHit[];
      newVisit: { key: string; start: VisitStart } | null;
    };
    try {
      const allowed = await this.withinCaps(
        capBucket,
        day,
        window,
        hits.length,
        drops,
        now,
      );
      hits = hits.slice(0, allowed);
      if (hits.length === 0) return this.noteDrops(drops, now);
      observed = await this.observe(
        hits,
        capBucket,
        addressBucket(ip),
        userAgent ?? "",
        drops,
        now,
      );
    } catch (error) {
      this.logger.warn(
        `Site analytics guards unavailable, buffering in memory: ${(error as Error).message}`,
      );
      return this.recordWithoutRedis(hits, reader, window, drops);
    }
    if (observed.hits.length === 0) return this.noteDrops(drops, now);

    const increments = bufferIncrements(observed.hits, reader);
    await this.buffer(window, day, increments, observed.newVisit, drops, now);
    return observed.hits.length;
  }

  /**
   * Writes what the fallback buffer holds to Postgres, a transaction per
   * window. A window that fails goes back into the buffer if it still fits;
   * otherwise its hits are counted as dropped.
   */
  async drainFallback(): Promise<void> {
    for (const [window, increments] of this.fallback.take()) {
      try {
        await this.storage.writeDirect(window, increments);
      } catch (error) {
        if (!this.fallback.add(window, increments)) {
          const hits = [...increments.values()].reduce((a, b) => a + b, 0);
          countDrop(this.pendingDrops, DROP_REASONS.fallback, hits);
          this.logger.error(
            `Site analytics hits lost while Redis and Postgres were away: ${(error as Error).message}`,
          );
        }
      }
    }
  }

  /**
   * Drops `page_read` hits about a park or ride that does not exist: a made-up
   * id would be a row in the rankings' table. Without the id list (the
   * database away since start-up) every read passes.
   */
  private async existing(hits: ValidHit[], drops: Drops): Promise<ValidHit[]> {
    const reads = hits.filter(
      (hit) => hit.t === "event" && hit.name === PAGE_READ_EVENT && hit.entity,
    );
    if (reads.length === 0) return hits;
    const known = await this.entities.knownIds();
    if (!known) return hits;
    return hits.filter((hit) => {
      if (hit.t !== "event" || hit.name !== PAGE_READ_EVENT || !hit.entity)
        return true;
      const ids = hit.props.kind === "park" ? known.parks : known.attractions;
      if (ids.has(hit.entity)) return true;
      countDrop(drops, DROP_REASONS.entity);
      return false;
    });
  }

  /**
   * How many of `count` hits may count: the address's daily budget and the
   * window's, whichever is smaller. Throws when Redis is away.
   */
  private async withinCaps(
    capBucket: string,
    day: string,
    window: string,
    count: number,
    drops: Drops,
    now: Date,
  ): Promise<number> {
    const address = KEYS.cap(this.hash(day, "cap", capBucket));
    const windowKey = KEYS.windowHits(window);
    const results = await this.redis
      .pipeline()
      .incrby(address, count)
      .expire(address, secondsUntilUtcDayEnds(now, DAY_GRACE_SECONDS), "NX")
      .incrby(windowKey, count)
      .expire(windowKey, WINDOW_HITS_TTL_SECONDS, "NX")
      .exec();
    if (!results || results.some(([error]) => error)) {
      throw new Error("a cap command failed");
    }
    const remaining = (total: unknown, limit: number) =>
      Math.max(0, Math.min(count, limit - (Number(total) - count)));
    const byAddress = remaining(results[0][1], MAX_HITS_PER_ADDRESS_PER_DAY);
    const byWindow = remaining(results[2][1], MAX_HITS_PER_WINDOW);
    countDrop(drops, DROP_REASONS.address, count - byAddress);
    countDrop(
      drops,
      DROP_REASONS.window,
      byAddress - Math.min(byAddress, byWindow),
    );
    return Math.min(byAddress, byWindow);
  }

  /**
   * One pipeline for the guards and the visit: the day's guards (each value
   * through {@link ADMIT_SCRIPT}), `page_read`'s per-reader, per-page and
   * per-address counters, the visit key (its first page view and its
   * page-view count), the day's and the hour's visitors and the realtime
   * minute. Throws when Redis is away.
   */
  private async observe(
    hits: ValidHit[],
    capBucket: string,
    readerBucket: string | null,
    userAgent: string,
    drops: Drops,
    now: Date,
  ): Promise<{
    hits: ObservedHit[];
    newVisit: { key: string; start: VisitStart } | null;
  }> {
    const day = utcDay(now);
    const yesterday = addIsoDays(day, -1);
    const dayTtl = secondsUntilUtcDayEnds(now, DAY_GRACE_SECONDS);
    const setTtl = secondsUntilUtcDayEnds(now, GUARD_SET_GRACE_SECONDS);
    const pipeline = this.redis.pipeline();
    let index = 0;
    const next = () => index++;

    /** One admit call per distinct value of a guard. */
    const admits = new Map<string, number>();
    const guard = (name: SiteGuard, value: string, scope = "") => {
      const id = `${name}\u0000${scope}\u0000${value}`;
      if (admits.has(id)) return id;
      const { limit, perAddress } = SITE_GUARDS[name];
      pipeline.siteAnalyticsAdmit(
        KEYS.guard(name, day, scope),
        KEYS.guard(name, yesterday, scope),
        KEYS.newMembers(this.hash(day, "new", name, capBucket)),
        value,
        limit,
        limit * KNOWN_MEMBER_HEADROOM,
        perAddress,
        setTtl,
        dayTtl,
      );
      admits.set(id, next());
      return id;
    };

    const visitSources = new Map<
      ValidPageviewHit,
      { referrer: string; channel: Channel }
    >();
    const guards = hits.map((hit) => {
      const pageType = guard("pageTypes", hit.pageType);
      const path = guard("paths", hit.path);
      if (hit.t === "pageview") {
        const source = classifyReferrer(hit.referrer, hit.campaign);
        visitSources.set(hit, source);
        // Search engines, AI assistants and social networks are a closed
        // list; every other site, and a utm_source, is open-ended.
        const open =
          source.referrer &&
          (source.channel === "referral" || source.channel === "campaign");
        return {
          pageType,
          path,
          referrer: open ? guard("referrers", source.referrer) : null,
          campaign: hit.campaign ? guard("campaigns", hit.campaign) : null,
          name: null,
          combination: null,
        };
      }
      const read = hit.name === PAGE_READ_EVENT;
      return {
        pageType,
        path,
        referrer: null,
        campaign: null,
        name: read ? null : guard("names", hit.name),
        combination: read
          ? null
          : guard(
              "combinations",
              this.staticHash("combo", hit.propsKey, hit.entity ?? "").slice(
                0,
                16,
              ),
              hit.name,
            ),
      };
    });

    // page_read keeps the page-view counter's caps: three reads per reader
    // and page, ten per address and page, and a daily budget per address.
    const reads = hits.map((hit) => {
      if (hit.t !== "event" || hit.name !== PAGE_READ_EVENT) return null;
      const page = `${hit.props.kind}|${hit.entity ?? hit.props.key ?? ""}`;
      const reader = KEYS.read(
        this.hash(day, "read", readerBucket ?? capBucket, userAgent, page),
      );
      const address = KEYS.readPage(
        this.hash(day, "read-page", capBucket, page),
      );
      pipeline.incr(reader).expire(reader, dayTtl, "NX");
      const readerAt = next();
      next();
      pipeline.incr(address).expire(address, dayTtl, "NX");
      const addressAt = next();
      next();
      return { readerAt, addressAt };
    });
    const readCount = reads.filter(Boolean).length;
    let readAddress: number | null = null;
    if (readCount > 0) {
      const key = KEYS.readAddress(this.hash(day, "read-addr", capBucket));
      pipeline.incrby(key, readCount).expire(key, dayTtl, "NX");
      readAddress = next();
      next();
    }

    const pageviews = hits.filter(
      (hit): hit is ValidPageviewHit => hit.t === "pageview",
    );
    let visitKey: string | null = null;
    let startReply: number | null = null;
    const countReplies = new Map<ValidHit, number>();
    if (readerBucket) {
      visitKey = KEYS.visit(this.hash(day, "visit", readerBucket, userAgent));
      const visitor = this.hash(day, "visitor", readerBucket, userAgent);
      pipeline.hget(visitKey, "c");
      startReply = next();
      for (const hit of pageviews) {
        pipeline.hincrby(visitKey, "n", 1);
        countReplies.set(hit, next());
      }
      // Every hit keeps the visit alive; only a page view can open one.
      pipeline.expire(visitKey, VISIT_IDLE_SECONDS);
      next();

      if (pageviews.length > 0) {
        const dayKeyTtl = secondsUntilUtcDayEnds(
          now,
          VISITOR_KEY_GRACE_SECONDS,
        );
        const scopes = [
          ALL_LOCALES_SCOPE,
          ...new Set(pageviews.map((hit) => hit.locale)),
        ];
        for (const scope of scopes) {
          const dayKey = KEYS.visitors(day, scope);
          const hourKeyName = KEYS.hourVisitors(hourKey(now), scope);
          pipeline.pfadd(dayKey, visitor).expire(dayKey, dayKeyTtl, "NX");
          pipeline
            .pfadd(hourKeyName, visitor)
            .expire(hourKeyName, dayTtl, "NX");
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

    const results = await pipeline.exec();
    if (!results) throw new Error("the guard pipeline did not run");
    const reply = (at: number | null): unknown =>
      at === null || results[at]?.[0] ? null : results[at]?.[1];
    // A guard Redis could not answer lets the value through.
    const passed = (id: string | null) =>
      id === null || reply(admits.get(id) ?? null) !== 0;

    let readBudget = Infinity;
    if (readAddress !== null && reply(readAddress) !== null) {
      readBudget =
        MAX_VIEWS_PER_ADDRESS_PER_DAY -
        (Number(reply(readAddress)) - readCount);
    }

    let current = parseVisitStart(reply(startReply));
    let newVisit: { key: string; start: VisitStart } | null = null;
    const observed: ObservedHit[] = [];
    hits.forEach((raw, i) => {
      const checks = guards[i];
      let pageType = raw.pageType;
      if (!passed(checks.pageType)) {
        pageType = OVERFLOW_PAGE_TYPE;
        countDrop(drops, DROP_REASONS.pageType);
      }
      let path = raw.path;
      if (!passed(checks.path)) {
        path = OVERFLOW_VALUE;
        countDrop(drops, DROP_REASONS.path);
      }

      if (raw.t === "pageview") {
        const hit = { ...raw, pageType, path };
        const count = Number(reply(countReplies.get(raw) ?? null) ?? 0);
        if (count === 1 && visitKey) {
          const source = visitSources.get(raw) ?? {
            referrer: "",
            channel: "direct" as Channel,
          };
          let referrer = source.referrer;
          if (!passed(checks.referrer)) {
            referrer = OVERFLOW_VALUE;
            countDrop(drops, DROP_REASONS.referrer);
          }
          let campaign = hit.campaign ?? "";
          if (campaign && !passed(checks.campaign)) {
            campaign = OVERFLOW_VALUE;
            countDrop(drops, DROP_REASONS.campaign);
          }
          current = {
            locale: hit.locale,
            path: hit.path,
            pageType: hit.pageType,
            viewport: hit.viewport,
            referrer,
            channel: source.channel,
            campaign,
          };
          newVisit = { key: visitKey, start: current };
          observed.push({ hit, start: current, engaged: null });
          return;
        }
        observed.push({
          hit,
          start: null,
          engaged: count === 2 ? current : null,
        });
        return;
      }

      if (!passed(checks.name)) {
        countDrop(drops, DROP_REASONS.name);
        return;
      }
      const read = reads[i];
      if (read) {
        const byReader = Number(reply(read.readerAt) ?? 0);
        const byAddress = Number(reply(read.addressAt) ?? 0);
        if (
          byReader > MAX_VIEWS_PER_READER_PER_DAY ||
          byAddress > MAX_VIEWS_PER_ADDRESS_AND_PAGE_PER_DAY ||
          readBudget <= 0
        ) {
          countDrop(drops, DROP_REASONS.read);
          return;
        }
        readBudget -= 1;
      }
      let hit: ValidEventHit = { ...raw, pageType, path };
      if (!passed(checks.combination)) {
        hit = {
          ...hit,
          props: { ...OVERFLOW_PROPS },
          propsKey: propsKeyOf(OVERFLOW_PROPS),
          entity: null,
        };
        countDrop(drops, DROP_REASONS.combination);
      }
      observed.push({ hit });
    });
    return { hits: observed, newVisit };
  }

  /**
   * Adds the counts to the window's hash, remembers a new visit's first page
   * view and adds the request's drop counts, in one `MULTI`: either all of it
   * lands or none of it does. Counts Redis could not take go to the fallback
   * buffer, only those, so nothing is counted twice.
   */
  private async buffer(
    window: string,
    day: string,
    increments: Map<string, number>,
    newVisit: { key: string; start: VisitStart } | null,
    drops: Drops,
    now: Date,
  ): Promise<void> {
    const key = KEYS.buffer(window);
    const fields = [...increments.keys()];
    for (const [reason, count] of this.pendingDrops)
      countDrop(drops, reason, count);
    this.pendingDrops.clear();
    let failed: string[] = fields;
    try {
      const tx = this.redis.multi();
      for (const field of fields)
        tx.hincrby(key, field, increments.get(field) ?? 0);
      tx.sadd(KEYS.windows, window);
      if (newVisit) tx.hset(newVisit.key, "c", JSON.stringify(newVisit.start));
      this.addDrops(tx, day, drops, now);
      const results = await tx.exec();
      if (results) {
        failed = fields.filter((_, i) => results[i]?.[0]);
        if (
          failed.length > 0 ||
          results.slice(fields.length).some(([e]) => e)
        ) {
          this.logger.warn(
            `Site analytics: ${failed.length} buffer field(s) refused by Redis`,
          );
        }
      }
    } catch (error) {
      this.logger.warn(
        `Site analytics buffer unavailable, buffering in memory: ${(error as Error).message}`,
      );
      for (const [reason, count] of drops)
        countDrop(this.pendingDrops, reason, count);
    }
    if (failed.length > 0) {
      const rest = new Map(
        failed.map((field) => [field, increments.get(field) ?? 0]),
      );
      this.holdInMemory(window, rest);
    }
  }

  /**
   * Redis is away: the hits count without guards, visits or visitors, in this
   * process's bounded buffer, which reaches Postgres within a minute.
   */
  private recordWithoutRedis(
    hits: ValidHit[],
    reader: Reader,
    window: string,
    drops: Drops,
  ): number {
    for (const [reason, count] of drops)
      countDrop(this.pendingDrops, reason, count);
    const observed: ObservedHit[] = hits.map((hit) =>
      hit.t === "pageview" ? { hit, start: null, engaged: null } : { hit },
    );
    return this.holdInMemory(window, bufferIncrements(observed, reader))
      ? hits.length
      : 0;
  }

  private holdInMemory(
    window: string,
    increments: Map<string, number>,
  ): boolean {
    if (this.fallback.add(window, increments)) return true;
    const hits = [...increments.values()].reduce((a, b) => a + b, 0);
    countDrop(this.pendingDrops, DROP_REASONS.fallback, hits);
    return false;
  }

  /** Writes a request's drop counts when nothing else goes to Redis. */
  private async noteDrops(drops: Drops, now: Date): Promise<0> {
    if (drops.size === 0) return 0;
    try {
      const tx = this.redis.multi();
      this.addDrops(tx, utcDay(now), drops, now);
      await tx.exec();
    } catch {
      for (const [reason, count] of drops)
        countDrop(this.pendingDrops, reason, count);
    }
    return 0;
  }

  private addDrops(
    tx: ReturnType<Redis["multi"]>,
    day: string,
    drops: Drops,
    now: Date,
  ): void {
    if (drops.size === 0) return;
    const key = KEYS.drops(day);
    for (const [reason, count] of drops) tx.hincrby(key, reason, count);
    tx.expire(key, secondsUntilUtcDayEnds(now, GUARD_SET_GRACE_SECONDS), "NX");
  }

  /** An HMAC under the day's key: a pseudonym that ends with its UTC day. */
  private hash(day: string, ...parts: string[]): string {
    let key = this.dayKeys.get(day);
    if (!key) {
      key = dailyHashKey(this.hashSecret, day);
      if (this.dayKeys.size > 2) this.dayKeys.clear();
      this.dayKeys.set(day, key);
    }
    return readerHash(key, "site-analytics", ...parts);
  }

  /** An HMAC under the secret, for values that stand for no reader. */
  private staticHash(...parts: string[]): string {
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

/** The dimensions of a visit row: the first page view's, and the visit's. */
function visitDims(start: VisitStart, reader: Reader): PageviewDims {
  return [
    start.locale,
    start.path,
    start.pageType,
    start.viewport,
    reader.browser,
    reader.os,
    reader.country,
    start.path,
    start.referrer,
    start.channel,
    start.campaign,
  ];
}

/**
 * The buffer fields one request adds, summed. Pure, so the counting rules are
 * testable without Redis. A page view counts a view under its own page
 * dimensions, with the visit dimensions empty; the page view that opens a
 * visit also counts the visit, and the second one an `engaged`, both under
 * the visit's first page view and how the visit began. An event counts once,
 * under its page dimensions. So a row never joins a page beyond the first to
 * the visit's referrer.
 */
export function bufferIncrements(
  observed: ObservedHit[],
  reader: Reader,
): Map<string, number> {
  const increments = new Map<string, number>();
  const add = (field: string) =>
    increments.set(field, (increments.get(field) ?? 0) + 1);

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
          hit.propsKey,
          hit.entity ?? "",
        ]),
      );
      continue;
    }
    if (!("start" in entry)) continue;
    const { hit } = entry;
    add(
      pageviewField("v", [
        hit.locale,
        hit.path,
        hit.pageType,
        hit.viewport,
        reader.browser,
        reader.os,
        reader.country,
        "",
        "",
        "",
        "",
      ]),
    );
    if (entry.start) add(pageviewField("s", visitDims(entry.start, reader)));
    if (entry.engaged)
      add(pageviewField("e", visitDims(entry.engaged, reader)));
  }
  return increments;
}
