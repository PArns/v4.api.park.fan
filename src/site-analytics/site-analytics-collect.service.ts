import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
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
import { GeoipService } from "../geoip/geoip.service";
import { isBotUserAgent } from "../page-views/bot-user-agent.util";
import { SiteTrafficDay } from "./entities/site-traffic-day.entity";
import {
  ALL_LOCALES_SCOPE,
  MAX_HITS_PER_ADDRESS_PER_DAY,
  REALTIME_ACTIVE_MINUTES,
  REALTIME_SERIES_MINUTES,
  RETENTION_DAYS,
  SITE_ANALYTICS_KEYS as KEYS,
  UNKNOWN_COUNTRY,
  VISITOR_KEY_GRACE_SECONDS,
  VISIT_IDLE_SECONDS,
  minuteKey,
  type TrafficDim,
} from "./site-analytics.constants";
import {
  validateHit,
  type ValidEventHit,
  type ValidHit,
  type ValidPageviewHit,
} from "./site-analytics.hits";
import { SITE_LOCALES } from "./site-analytics.schema";
import { parseUserAgent } from "./user-agent.util";

/**
 * Storage settings for the three tables a hit writes, as `pg_class.reloptions`
 * spells them: the same as `page_view_days`, for the same reason. Every hit
 * after a value's first that day updates the same row, and at 80 % fill most
 * of those updates stay on their page (HOT); 1 % lets autovacuum visit the few
 * pages that changed instead of waiting for a fifth of the table.
 */
export const SITE_ANALYTICS_STORAGE_OPTIONS = [
  "fillfactor=80",
  "autovacuum_vacuum_scale_factor=0.01",
] as const;

/** The tables a hit writes, which get {@link SITE_ANALYTICS_STORAGE_OPTIONS}. */
export const SITE_ANALYTICS_HIT_TABLES = [
  "site_traffic_days",
  "site_event_days",
  "site_event_entity_days",
] as const;

/** Every table of the statistic, which the prune trims. */
const SITE_ANALYTICS_TABLES = [
  ...SITE_ANALYTICS_HIT_TABLES,
  "site_visitor_days",
] as const;

/** A minute past midnight UTC, so a cap never expires before its day. */
const CAP_GRACE_SECONDS = 60;

/** The realtime keys live a little longer than the window that reads them. */
const ACTIVE_MINUTE_TTL_SECONDS = (REALTIME_ACTIVE_MINUTES + 5) * 60;
const PAGEVIEWS_MINUTE_TTL_SECONDS = (REALTIME_SERIES_MINUTES + 5) * 60;

/** The dimensions every page view is counted under, with its value. */
const PAGEVIEW_DIMS = [
  "path",
  "page_type",
  "viewport",
  "browser",
  "os",
  "country",
  "locale",
] as const satisfies readonly TrafficDim[];
type PageviewDim = (typeof PAGEVIEW_DIMS)[number];

export interface CollectRequest {
  /** The body's hits, unchecked: each one is validated here. */
  hits: unknown[];
  ip: string | null;
  userAgent: string | null;
}

/** What Redis said about one page view's visit. */
interface VisitSighting {
  /** This page view opened the visit. */
  start: boolean;
  /** The dimensions whose value this visit reached for the first time. */
  firstSeen: ReadonlySet<PageviewDim>;
}

const NO_SIGHTING: VisitSighting = { start: false, firstSeen: new Set() };

/** The traffic, event and entity rows one request adds, keyed by their PK. */
export interface CollectRows {
  traffic: Map<
    string,
    {
      dim: TrafficDim;
      locale: string;
      value: string;
      views: number;
      visits: number;
    }
  >;
  events: Map<
    string,
    {
      name: string;
      locale: string;
      pageType: string;
      viewport: string;
      p: [string, string, string];
      count: number;
    }
  >;
  entities: Map<
    string,
    {
      name: string;
      locale: string;
      entity: string;
      kind: string;
      count: number;
    }
  >;
}

/**
 * The write side of the site statistic: `POST /v1/analytics/collect`, the
 * nightly visitor snapshot and the retention prune. See
 * docs/frontend/site-analytics.md.
 *
 * Nothing about a reader is stored. Postgres holds counts per day; Redis holds
 * the daily cap, the visit and the visitor sets under keys that are HMACs of
 * address bucket and User-Agent under `ANALYTICS_HASH_SECRET`, which lives in
 * the environment and never in Redis, and every one of those keys expires.
 */
@Injectable()
export class SiteAnalyticsCollectService implements OnModuleInit {
  private readonly logger = new Logger(SiteAnalyticsCollectService.name);
  private readonly hashSecret: string;

  constructor(
    @InjectRepository(SiteTrafficDay)
    private readonly traffic: Repository<SiteTrafficDay>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly geoip: GeoipService,
  ) {
    this.hashSecret = readerHashSecret(
      ["ANALYTICS_HASH_SECRET", "PAGE_VIEWS_HASH_SECRET"],
      this.logger,
      "the site statistic",
    );
  }

  async onModuleInit(): Promise<void> {
    for (const table of SITE_ANALYTICS_HIT_TABLES) {
      await ensureTableStorageOptions(
        this.traffic,
        table,
        SITE_ANALYTICS_STORAGE_OPTIONS,
        this.logger,
      );
    }
  }

  /**
   * Counts the hits of one request that pass the schema, from a browser that
   * is no bot, up to the address's daily budget. Returns how many counted. A
   * failed write is logged and swallowed: the beacon gets its 204 either way.
   */
  async collect(request: CollectRequest, now = new Date()): Promise<number> {
    if (isBotUserAgent(request.userAgent)) return 0;
    const valid = request.hits
      .map(validateHit)
      .filter((hit): hit is ValidHit => hit !== null);
    if (valid.length === 0) return 0;

    const day = utcDay(now);
    const bucket = addressBucket(request.ip);
    const allowed = await this.withinCap(bucket, day, valid.length, now);
    const hits = valid.slice(0, allowed);
    if (hits.length === 0) return 0;

    const { browser, os } = parseUserAgent(request.userAgent);
    const country =
      (request.ip && this.geoip.lookupCountry(request.ip)) || UNKNOWN_COUNTRY;
    const sightings = await this.sightVisits(
      hits,
      bucket,
      request.userAgent ?? "",
      { browser, os, country },
      now,
    );
    const rows = buildCollectRows(hits, sightings, { browser, os, country });

    try {
      await this.write(day, rows);
    } catch (error) {
      this.logger.warn(
        `Site analytics write failed: ${(error as Error).message}`,
      );
      return 0;
    }
    return hits.length;
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
        .expire(key, secondsUntilUtcDayEnds(now, CAP_GRACE_SECONDS), "NX")
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
   * One pipeline for everything Redis keeps: the visit (does this page view
   * open one, which values has it reached before), the day's visitors, and
   * the realtime minute. The visit key is an HMAC of address bucket and
   * User-Agent and slides on every hit; its fields are HMACs of language,
   * dimension and value, so not even the pages a visit saw are readable.
   *
   * Without an address, or with Redis down, no page view opens a visit and no
   * visitor is added: visits and visitors undercount, page views do not.
   */
  private async sightVisits(
    hits: ValidHit[],
    bucket: string | null,
    userAgent: string,
    reader: { browser: string; os: string; country: string },
    now: Date,
  ): Promise<VisitSighting[]> {
    const pageviews = hits.filter(
      (hit): hit is ValidPageviewHit => hit.t === "pageview",
    );
    const none = () => pageviews.map(() => NO_SIGHTING);
    if (!bucket) return none();

    const day = utcDay(now);
    const minute = minuteKey(now);
    const visit = KEYS.visit(this.hash("visit", bucket, userAgent));
    const visitor = this.hash("visitor", day, bucket, userAgent);
    const pipeline = this.redis.pipeline();
    let index = 0;
    /** Per page view: the index of its `s` reply, then one per dimension. */
    const replies: number[] = [];

    for (const hit of pageviews) {
      replies.push(index);
      pipeline.hsetnx(visit, "s", "1");
      index++;
      for (const dim of PAGEVIEW_DIMS) {
        const value = pageviewValue(hit, dim, reader);
        pipeline.hsetnx(visit, this.hash("seen", hit.locale, dim, value), "1");
        index++;
      }
    }
    // Every hit keeps the visit alive; only a page view can open one.
    pipeline.expire(visit, VISIT_IDLE_SECONDS);

    const pageviewLocales = new Set(pageviews.map((hit) => hit.locale));
    if (pageviews.length > 0) {
      const visitorTtl = secondsUntilUtcDayEnds(now, VISITOR_KEY_GRACE_SECONDS);
      for (const scope of [ALL_LOCALES_SCOPE, ...pageviewLocales]) {
        const key = KEYS.visitors(day, scope);
        pipeline.pfadd(key, visitor).expire(key, visitorTtl, "NX");
      }
    }

    const activeLocales = new Set(hits.map((hit) => hit.locale));
    for (const scope of [ALL_LOCALES_SCOPE, ...activeLocales]) {
      const key = KEYS.activeMinute(minute, scope);
      pipeline.pfadd(key, visitor).expire(key, ACTIVE_MINUTE_TTL_SECONDS);
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
    }

    let results: Array<[Error | null, unknown]> | null;
    try {
      results = await pipeline.exec();
    } catch (error) {
      this.logger.warn(
        `Site analytics visits unavailable: ${(error as Error).message}`,
      );
      return none();
    }
    if (!results) return none();
    const isNew = (i: number) => !results[i]?.[0] && results[i]?.[1] === 1;
    return replies.map((first) => ({
      start: isNew(first),
      firstSeen: new Set(PAGEVIEW_DIMS.filter((_, d) => isNew(first + 1 + d))),
    }));
  }

  /**
   * One statement for the whole request: three upserts as data-modifying
   * CTEs, each fed by arrays, so the statement has the same shape whatever
   * the request held. The rows are pre-aggregated, because one INSERT … ON
   * CONFLICT may not touch a row twice, and sorted, so two requests that
   * share rows lock them in the same order.
   */
  private async write(day: string, rows: CollectRows): Promise<void> {
    const traffic = sortedValues(rows.traffic);
    const events = sortedValues(rows.events);
    const entities = sortedValues(rows.entities);
    await this.traffic.query(
      `WITH traffic AS (
         INSERT INTO site_traffic_days (dim, day, locale, value, views, visits)
         SELECT dim, $1::date, locale, value, views, visits
           FROM unnest($2::text[], $3::text[], $4::text[], $5::int[], $6::int[])
             AS t(dim, locale, value, views, visits)
         ON CONFLICT (dim, day, locale, value) DO UPDATE
           SET views = site_traffic_days.views + EXCLUDED.views,
               visits = site_traffic_days.visits + EXCLUDED.visits
       ), events AS (
         INSERT INTO site_event_days
           (name, day, locale, page_type, viewport, p1, p2, p3, count)
         SELECT name, $1::date, locale, page_type, viewport, p1, p2, p3, count
           FROM unnest($7::text[], $8::text[], $9::text[], $10::text[],
                       $11::text[], $12::text[], $13::text[], $14::int[])
             AS e(name, locale, page_type, viewport, p1, p2, p3, count)
         ON CONFLICT (name, day, locale, page_type, viewport, p1, p2, p3)
           DO UPDATE SET count = site_event_days.count + EXCLUDED.count
       ), entities AS (
         INSERT INTO site_event_entity_days
           (name, day, locale, entity, kind, count)
         SELECT name, $1::date, locale, entity, kind, count
           FROM unnest($15::text[], $16::text[], $17::uuid[], $18::text[],
                       $19::int[])
             AS x(name, locale, entity, kind, count)
         ON CONFLICT (name, day, locale, entity)
           DO UPDATE SET count = site_event_entity_days.count + EXCLUDED.count
       )
       SELECT 1`,
      [
        day,
        traffic.map((r) => r.dim),
        traffic.map((r) => r.locale),
        traffic.map((r) => r.value),
        traffic.map((r) => r.views),
        traffic.map((r) => r.visits),
        events.map((r) => r.name),
        events.map((r) => r.locale),
        events.map((r) => r.pageType),
        events.map((r) => r.viewport),
        events.map((r) => r.p[0]),
        events.map((r) => r.p[1]),
        events.map((r) => r.p[2]),
        events.map((r) => r.count),
        entities.map((r) => r.name),
        entities.map((r) => r.locale),
        entities.map((r) => r.entity),
        entities.map((r) => r.kind),
        entities.map((r) => r.count),
      ],
    );
  }

  /**
   * Writes the visitors of the last two finished UTC days from their
   * HyperLogLogs (all languages, and each language). Two days, so one missed
   * run heals the next night; a key that is gone (expired, or evicted under
   * memory pressure) is skipped rather than written as zero, so it never
   * overwrites a count taken earlier. Returns the rows written.
   */
  async snapshotVisitors(now = new Date()): Promise<number> {
    const today = utcDay(now);
    const scopes = [ALL_LOCALES_SCOPE, ...SITE_LOCALES];
    const keys = [addIsoDays(today, -1), addIsoDays(today, -2)].flatMap((day) =>
      scopes.map((scope) => ({ day, scope, key: KEYS.visitors(day, scope) })),
    );
    const pipeline = this.redis.pipeline();
    for (const { key } of keys) pipeline.exists(key).pfcount(key);
    const results = (await pipeline.exec()) ?? [];

    const rows = keys
      .map((entry, i) => ({
        ...entry,
        exists: Number(results[2 * i]?.[1] ?? 0) === 1,
        visitors: Number(results[2 * i + 1]?.[1] ?? 0),
      }))
      .filter((row) => row.exists && Number.isFinite(row.visitors));
    if (rows.length === 0) return 0;

    await this.traffic.query(
      `INSERT INTO site_visitor_days (day, scope, visitors)
       SELECT * FROM unnest($1::date[], $2::text[], $3::int[])
       ON CONFLICT (day, scope) DO UPDATE SET visitors = EXCLUDED.visitors`,
      [
        rows.map((row) => row.day),
        rows.map((row) => row.scope),
        rows.map((row) => row.visitors),
      ],
    );
    return rows.length;
  }

  /**
   * Deletes every row older than {@link RETENTION_DAYS} from every table of
   * the statistic. Returns the rows deleted per table.
   */
  async prune(now = new Date()): Promise<Record<string, number>> {
    const before = addIsoDays(utcDay(now), -RETENTION_DAYS);
    const deleted: Record<string, number> = {};
    for (const table of SITE_ANALYTICS_TABLES) {
      const result: [unknown[], number] = await this.traffic.query(
        `DELETE FROM ${table} WHERE day < $1::date`,
        [before],
      );
      deleted[table] = Number(result?.[1] ?? 0);
    }
    return deleted;
  }

  private hash(...parts: string[]): string {
    return readerHash(this.hashSecret, "site-analytics", ...parts);
  }
}

function pageviewValue(
  hit: ValidPageviewHit,
  dim: PageviewDim,
  reader: { browser: string; os: string; country: string },
): string {
  switch (dim) {
    case "path":
      return hit.path;
    case "page_type":
      return hit.pageType;
    case "viewport":
      return hit.viewport;
    case "locale":
      return hit.locale;
    default:
      return reader[dim];
  }
}

/**
 * The rows one request adds, summed per primary key. Pure, so the counting
 * rules are testable without Redis or Postgres: each page view counts once
 * under every dimension, and as a visit where Redis saw the value for the
 * first time in this visit; a page view that opens a visit also counts its
 * path as `entry` and its referrer, if any, as `referrer`.
 */
export function buildCollectRows(
  hits: ValidHit[],
  sightings: VisitSighting[],
  reader: { browser: string; os: string; country: string },
): CollectRows {
  const rows: CollectRows = {
    traffic: new Map(),
    events: new Map(),
    entities: new Map(),
  };
  const addTraffic = (
    dim: TrafficDim,
    locale: string,
    value: string,
    visits: number,
  ) => {
    const key = [dim, locale, value].join("\u0000");
    const row = rows.traffic.get(key) ?? {
      dim,
      locale,
      value,
      views: 0,
      visits: 0,
    };
    row.views += 1;
    row.visits += visits;
    rows.traffic.set(key, row);
  };

  let pageview = 0;
  for (const hit of hits) {
    if (hit.t === "pageview") {
      const sighting = sightings[pageview++] ?? NO_SIGHTING;
      for (const dim of PAGEVIEW_DIMS) {
        const seen = sighting.firstSeen.has(dim) ? 1 : 0;
        addTraffic(dim, hit.locale, pageviewValue(hit, dim, reader), seen);
      }
      if (sighting.start) {
        addTraffic("entry", hit.locale, hit.path, 1);
        if (hit.referrer) addTraffic("referrer", hit.locale, hit.referrer, 1);
      }
      continue;
    }
    addEvent(rows, hit);
  }
  return rows;
}

function addEvent(rows: CollectRows, hit: ValidEventHit): void {
  const p: [string, string, string] = [
    hit.props[0] ?? "",
    hit.props[1] ?? "",
    hit.props[2] ?? "",
  ];
  const key = [hit.name, hit.locale, hit.pageType, hit.viewport, ...p].join(
    "\u0000",
  );
  const row = rows.events.get(key) ?? {
    name: hit.name,
    locale: hit.locale,
    pageType: hit.pageType,
    viewport: hit.viewport,
    p,
    count: 0,
  };
  row.count += 1;
  rows.events.set(key, row);

  if (!hit.entity) return;
  const entityKey = [hit.name, hit.locale, hit.entity.id].join("\u0000");
  const entity = rows.entities.get(entityKey) ?? {
    name: hit.name,
    locale: hit.locale,
    entity: hit.entity.id,
    kind: hit.entity.kind,
    count: 0,
  };
  entity.count += 1;
  rows.entities.set(entityKey, entity);
}

/** A map's values in the order of its keys, the lock order of the upsert. */
function sortedValues<T>(map: Map<string, T>): T[] {
  return [...map.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([, value]) => value);
}
