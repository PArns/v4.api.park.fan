import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
} from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource, EntityManager } from "typeorm";
import { Redis } from "ioredis";
import { randomUUID } from "crypto";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import { SITE_ANALYTICS_HYPERTABLES } from "../database/hypertables";
import {
  ALL_LOCALES_SCOPE,
  CLAIM_FIELD,
  DROP_REASONS,
  FLUSH_WINDOW_SECONDS,
  MAX_FLUSH_ATTEMPTS,
  MAX_VISITS_CLOSED_PER_RUN,
  PAGE_READ_EVENT,
  PARKED_WINDOW_TTL_SECONDS,
  RETENTION_DAYS,
  SITE_ANALYTICS_KEYS as KEYS,
  SITE_LOCALES,
  VISIT_IDLE_SECONDS,
  bucketOf,
  flushWindowOf,
} from "./site-analytics.constants";
import { propsOfKey } from "./site-analytics.hits";
import { defineCloseVisitCommand } from "./site-analytics-scripts";

/**
 * A buffered page-view field's dimensions. A page view's row leaves the last
 * four (the visit's) empty; a visit's start row carries the first page view's
 * and the visit's, `entry` being the first page view's path; its exit row the
 * last page view's and the visit's.
 */
export type PageviewDims = [
  locale: string,
  path: string,
  pageType: string,
  viewport: string,
  browser: string,
  os: string,
  country: string,
  browserLanguage: string,
  entry: string,
  referrer: string,
  channel: string,
  campaign: string,
];

/** A buffered event field's dimensions: the page's, never the visit's. */
export type EventDims = [
  name: string,
  locale: string,
  path: string,
  pageType: string,
  viewport: string,
  browser: string,
  os: string,
  country: string,
  propsKey: string,
  entity: string,
];

/**
 * The measure a buffer field counts. On a page view's row: views and their
 * foreground seconds. On a visit's start row: visits started, those that
 * reached a second page view, and the visit's foreground seconds. On its exit
 * row: visits ended, and the seconds from the first hit to the last. Events
 * count on their own rows. The field is the letter followed by the row's
 * dimensions as JSON, so the flusher can rebuild the row from the field alone.
 */
export const BUFFER_MEASURES = {
  views: "v",
  seconds: "d",
  visits: "s",
  engaged: "e",
  visitSeconds: "t",
  exits: "x",
  visitSpanSeconds: "y",
  events: "n",
} as const;
type PageviewMeasure = Exclude<
  (typeof BUFFER_MEASURES)[keyof typeof BUFFER_MEASURES],
  "n"
>;

/** The column each page-view measure sums into. */
const PAGEVIEW_COLUMNS: Record<PageviewMeasure, keyof PageviewCounts> = {
  v: "views",
  d: "seconds",
  s: "visits",
  e: "engaged",
  t: "visitSeconds",
  x: "exits",
  y: "visitSpanSeconds",
};

/** The counts of one `site_pageviews` row. */
export interface PageviewCounts {
  views: number;
  seconds: number;
  visits: number;
  engaged: number;
  visitSeconds: number;
  exits: number;
  visitSpanSeconds: number;
}

export const pageviewField = (
  measure: PageviewMeasure,
  dims: PageviewDims,
): string => measure + JSON.stringify(dims);

export const eventField = (dims: EventDims): string =>
  BUFFER_MEASURES.events + JSON.stringify(dims);

/** How long the flush lock holds when its owner dies mid-flush. */
const FLUSH_LOCK_MS = 4 * 60 * 1000;

/**
 * A window is claimed only once it ended this long ago, so a request that
 * computed its window just before the boundary has written to it.
 */
const FLUSH_GRACE_MS = 60 * 1000;

/** How many fields one `HSCAN` step reads from a window. */
const HSCAN_COUNT = 1000;

/** How many rows one `INSERT … SELECT FROM unnest(…)` takes. */
const INSERT_CHUNK_ROWS = 5000;

/** How long a chunk drop waits for its lock before trying again tomorrow. */
const DROP_LOCK_TIMEOUT = "5s";

/** The name of the one-off copy of the old most-read table. */
export const PAGE_VIEW_DAYS_IMPORT = "page_view_days";

/** What the flusher has not written yet, and what was dropped, for the health report. */
export interface SiteAnalyticsBacklog {
  /** Windows in Redis, the current one included. */
  pendingWindows: number;
  /** Of those, ended more than a minute ago: the next flush's work. */
  readyWindows: number;
  /** Minutes since the oldest pending window began; null without one. */
  oldestPendingMinutes: number | null;
  /** Windows the flusher set aside after repeated failures. */
  parkedWindows: number;
  /** Visits not yet closed: active ones and those idle until the next run. */
  openVisits: number;
  /** The newest written window's write time. */
  lastFlushAt: string | null;
  /** Today's and yesterday's drop and overflow counts by reason. */
  dropsToday: Record<string, number>;
  dropsYesterday: Record<string, number>;
  /** Nothing waits longer than three runs and nothing is parked. */
  healthy: boolean;
}

/**
 * The statistic's Postgres side, all of it insert-only: the flusher that moves
 * finished Redis windows into the hypertables, the direct write the collector
 * uses for what it held in memory while Redis was away, the visitor
 * snapshot, the nightly chunk drop, and the one-off import of
 * `page_view_days`. Nothing here updates or deletes a counted row, which is
 * what keeps the tables free of dead tuples. docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteAnalyticsStorageService implements OnApplicationBootstrap {
  private readonly logger = new Logger(SiteAnalyticsStorageService.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    defineCloseVisitCommand(this.redis);
  }

  /** Starts the one-off import; never blocks or fails the boot. */
  onApplicationBootstrap(): void {
    if (process.env.NODE_ENV === "test") return;
    void this.importPageViewDays().catch((error) =>
      this.logger.warn(
        `page_view_days import failed, retried at the next start: ${(error as Error).message}`,
      ),
    );
  }

  /**
   * Ends the idle visits ({@link closeIdleVisits}), then writes every
   * finished window from Redis to Postgres, oldest first. One instance at a
   * time (a Redis lock). A window is renamed to a claimed key
   * that names its claim, read in `HSCAN` steps, written in one transaction
   * together with its claim's marker in `site_analytics_flushes`, and deleted
   * only after the commit. A crash leaves the claimed key, which the next run
   * writes; a claim whose marker exists is not written again. Counts that
   * reach a window after its claim make a new key, which the next run claims
   * anew. A window that fails is retried by the next runs and set aside after
   * {@link MAX_FLUSH_ATTEMPTS}; the others go on. Returns what was written.
   */
  async flush(now = new Date()): Promise<{
    windows: number;
    pageviewRows: number;
    eventRows: number;
    failed: number;
    visitsClosed: number;
    skipped?: "locked";
  }> {
    const token = randomUUID();
    const locked = await this.redis.set(
      KEYS.flushLock,
      token,
      "PX",
      FLUSH_LOCK_MS,
      "NX",
    );
    const written = {
      windows: 0,
      pageviewRows: 0,
      eventRows: 0,
      failed: 0,
      visitsClosed: 0,
    };
    if (locked !== "OK") return { ...written, skipped: "locked" };
    try {
      // Visits idle past their time first: their exits go into the windows of
      // their last hits, which this same run then writes.
      written.visitsClosed = await this.closeIdleVisits(now);
      const before = flushWindowOf(new Date(now.getTime() - FLUSH_GRACE_MS));
      const windows = (await this.redis.smembers(KEYS.windows))
        .filter((window) => window < before)
        .sort();
      for (const window of windows) {
        try {
          const rows = await this.flushWindow(window);
          if (rows) {
            written.windows += 1;
            written.pageviewRows += rows.pageviewRows;
            written.eventRows += rows.eventRows;
          }
        } catch (error) {
          written.failed += 1;
          await this.failed(window, error as Error);
        }
      }
    } finally {
      if ((await this.redis.get(KEYS.flushLock)) === token) {
        await this.redis.del(KEYS.flushLock);
      }
    }
    return written;
  }

  /**
   * Ends every visit idle for {@link VISIT_IDLE_SECONDS}: one exit, under its
   * last page view and how the visit began, and the seconds from its first hit
   * to its last, added to the window of its last hit, after which its key and
   * its place among the open visits go. Each close is one script that checks
   * the visit is still where it was read, so a hit arriving meanwhile keeps
   * the visit open. A visit whose key is already gone (the flusher stopped
   * past its lifetime) loses its exit, and is counted as dropped.
   */
  async closeIdleVisits(now = new Date()): Promise<number> {
    const cutoff = Math.floor(now.getTime() / 1000) - VISIT_IDLE_SECONDS;
    const members = await this.redis.zrangebyscore(
      KEYS.openVisits,
      "-inf",
      cutoff,
      "LIMIT",
      0,
      MAX_VISITS_CLOSED_PER_RUN,
    );
    if (members.length === 0) return 0;
    const read = this.redis.pipeline();
    for (const member of members) read.hmget(KEYS.visit(member), "l", "s", "x");
    const visits = (await read.exec()) ?? [];

    const close = this.redis.pipeline();
    const closes: number[] = [];
    let queued = 0;
    let lost = 0;
    members.forEach((member, i) => {
      const [error, fields] = visits[i] ?? [];
      const [last, start, exit] = (fields as Array<string | null>) ?? [];
      if (error || !last || !exit) {
        close.zrem(KEYS.openVisits, member);
        queued += 1;
        if (!error && !last) lost += 1;
        return;
      }
      const window = flushWindowOf(new Date(Number(last) * 1000));
      const span = Math.max(0, Number(last) - Number(start ?? last));
      close.siteAnalyticsCloseVisit(
        KEYS.visit(member),
        KEYS.openVisits,
        KEYS.buffer(window),
        KEYS.windows,
        last,
        member,
        window,
        exit,
        span,
      );
      closes.push(queued++);
    });
    if (lost > 0) {
      close.hincrby(KEYS.drops(utcDay(now)), DROP_REASONS.exit, lost);
    }
    const results = (await close.exec()) ?? [];
    return closes.filter((at) => !results[at]?.[0] && results[at]?.[1] === 1)
      .length;
  }

  /** Claims, writes and releases one window; null when it held nothing. */
  private async flushWindow(
    window: string,
  ): Promise<{ pageviewRows: number; eventRows: number } | null> {
    const claimed = await this.claim(window);
    let rows: { pageviewRows: number; eventRows: number } | null = null;
    if (claimed) {
      rows = await this.dataSource.transaction((manager) =>
        this.writeWindow(manager, window, claimed.fields, claimed.claim),
      );
      await this.redis.del(KEYS.flushing(window));
    }
    await this.redis.srem(KEYS.windows, window);
    // A count that reached the window after its claim made a new key whose
    // id this SREM just took out of the set; put it back for the next run.
    if (await this.redis.exists(KEYS.buffer(window))) {
      await this.redis.sadd(KEYS.windows, window);
    }
    await this.redis.hdel(KEYS.failures, window);
    return rows;
  }

  /**
   * Counts one failed attempt at a window and sets the window aside after
   * {@link MAX_FLUSH_ATTEMPTS}: its claimed key is renamed to a parked one
   * that expires after a week, and its id leaves the set the flusher reads.
   */
  private async failed(window: string, error: Error): Promise<void> {
    try {
      const attempts = await this.redis.hincrby(KEYS.failures, window, 1);
      if (attempts < MAX_FLUSH_ATTEMPTS) {
        this.logger.warn(
          `Window ${window} not written (attempt ${attempts}): ${error.message}`,
        );
        return;
      }
      for (const key of [KEYS.flushing(window), KEYS.buffer(window)]) {
        if (await this.redis.exists(key)) {
          await this.redis.rename(key, KEYS.parked(window));
          await this.redis.expire(
            KEYS.parked(window),
            PARKED_WINDOW_TTL_SECONDS,
          );
          break;
        }
      }
      await this.redis.sadd(KEYS.parkedWindows, window);
      await this.redis.srem(KEYS.windows, window);
      await this.redis.hdel(KEYS.failures, window);
      this.logger.error(
        `Window ${window} set aside after ${attempts} failed writes: ${error.message}`,
      );
    } catch (bookkeeping) {
      this.logger.error(
        `Window ${window} failed and could not be noted: ${(bookkeeping as Error).message}`,
      );
    }
  }

  /**
   * A window's counts and the claim they are written under: from its claimed
   * key if a previous run left one, else by renaming the live key to the
   * claimed one and naming a new claim in it. Null when there is nothing.
   */
  private async claim(
    window: string,
  ): Promise<{ claim: string; fields: Record<string, string> } | null> {
    const flushing = KEYS.flushing(window);
    if (!(await this.redis.exists(flushing))) {
      try {
        await this.redis.rename(KEYS.buffer(window), flushing);
      } catch {
        return null;
      }
    }
    await this.redis.hsetnx(flushing, CLAIM_FIELD, randomUUID());
    const fields: Record<string, string> = {};
    let cursor = "0";
    do {
      const [nextCursor, entries] = await this.redis.hscan(
        flushing,
        cursor,
        "COUNT",
        HSCAN_COUNT,
      );
      for (let i = 0; i + 1 < entries.length; i += 2) {
        fields[entries[i]] = entries[i + 1];
      }
      cursor = nextCursor;
    } while (cursor !== "0");
    const claim = fields[CLAIM_FIELD];
    delete fields[CLAIM_FIELD];
    if (!claim || Object.keys(fields).length === 0) {
      await this.redis.del(flushing);
      return null;
    }
    return { claim, fields };
  }

  /**
   * What the collector held in memory while Redis was away: the same rows,
   * written without a marker, beside whatever the flusher writes for that
   * window.
   */
  async writeDirect(window: string, increments: ReadonlyMap<string, number>) {
    const fields = Object.fromEntries(
      [...increments].map(([field, count]) => [field, String(count)]),
    );
    return this.dataSource.transaction((manager) =>
      this.writeWindow(manager, window, fields, null),
    );
  }

  /**
   * Inserts one window's rows under the window's hour, page views and events
   * each in statements fed by arrays. With a `claim`, the claim's marker goes
   * first and a claim that has one is skipped.
   */
  private async writeWindow(
    manager: EntityManager,
    window: string,
    fields: Record<string, string>,
    claim: string | null,
  ): Promise<{ pageviewRows: number; eventRows: number }> {
    const { pageviews, events } = rowsOfFields(fields);
    if (claim) {
      const inserted: unknown[] = await manager.query(
        `INSERT INTO site_analytics_flushes
           (bucket, claim, pageview_rows, event_rows)
         VALUES ($1::timestamptz, $2::uuid, $3, $4)
         ON CONFLICT (bucket, claim) DO NOTHING
         RETURNING bucket`,
        [window, claim, pageviews.length, events.length],
      );
      if (inserted.length === 0) {
        this.logger.warn(
          `Window ${window} claim ${claim} written before; skipped`,
        );
        return { pageviewRows: 0, eventRows: 0 };
      }
    }
    const bucket = bucketOf(window);
    for (let at = 0; at < pageviews.length; at += INSERT_CHUNK_ROWS) {
      const chunk = pageviews.slice(at, at + INSERT_CHUNK_ROWS);
      const column = (i: number) => chunk.map((row) => row.dims[i]);
      const count = (key: keyof PageviewCounts) =>
        chunk.map((row) => row.counts[key]);
      await manager.query(
        `INSERT INTO site_pageviews
           (bucket, locale, path, page_type, viewport, browser, os, country,
            browser_language, entry, referrer, channel, campaign, views,
            seconds, visits, engaged, visit_seconds, exits,
            visit_span_seconds)
         SELECT $1::timestamptz, *
           FROM unnest($2::text[], $3::text[], $4::text[], $5::text[],
                       $6::text[], $7::text[], $8::text[], $9::text[],
                       $10::text[], $11::text[], $12::text[], $13::text[],
                       $14::int[], $15::int[], $16::int[], $17::int[],
                       $18::int[], $19::int[], $20::int[])`,
        [
          bucket,
          ...Array.from({ length: 12 }, (_, i) => column(i)),
          count("views"),
          count("seconds"),
          count("visits"),
          count("engaged"),
          count("visitSeconds"),
          count("exits"),
          count("visitSpanSeconds"),
        ],
      );
    }
    for (let at = 0; at < events.length; at += INSERT_CHUNK_ROWS) {
      const chunk = events.slice(at, at + INSERT_CHUNK_ROWS);
      const column = (i: number) => chunk.map((row) => row.dims[i]);
      await manager.query(
        `INSERT INTO site_events
           (bucket, name, locale, path, page_type, viewport, browser, os,
            country, props_key, props, entity, count)
         SELECT $1::timestamptz, name, locale, path, page_type, viewport,
                browser, os, country, props_key, props::jsonb,
                NULLIF(entity, '')::uuid, count
           FROM unnest($2::text[], $3::text[], $4::text[], $5::text[],
                       $6::text[], $7::text[], $8::text[], $9::text[],
                       $10::text[], $11::text[], $12::text[], $13::int[])
             AS e(name, locale, path, page_type, viewport, browser, os,
                  country, props_key, props, entity, count)`,
        [
          bucket,
          ...Array.from({ length: 9 }, (_, i) => column(i)),
          chunk.map((row) => JSON.stringify(propsOfKey(row.dims[8]))),
          column(9),
          chunk.map((row) => row.count),
        ],
      );
    }
    return { pageviewRows: pageviews.length, eventRows: events.length };
  }

  /**
   * Writes yesterday's visitors from its HyperLogLogs (all languages, and
   * each language), once: a day already written stays as it is, and a key
   * that is gone (expired, or lost with Redis) is skipped rather than written
   * as zero. Runs four times in the hour after midnight, the hour a day's key
   * outlives its day. Returns the keys found and the rows written.
   */
  async snapshotVisitors(
    now = new Date(),
  ): Promise<{ found: number; written: number }> {
    const day = addIsoDays(utcDay(now), -1);
    const keys = [ALL_LOCALES_SCOPE, ...SITE_LOCALES].map((scope) => ({
      day,
      scope,
      key: KEYS.visitors(day, scope),
    }));
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
    if (rows.length === 0) return { found: 0, written: 0 };

    const inserted: unknown[] = await this.dataSource.query(
      `INSERT INTO site_visitor_days (day, scope, visitors)
       SELECT * FROM unnest($1::date[], $2::text[], $3::int[])
       ON CONFLICT (day, scope) DO NOTHING
       RETURNING day`,
      [
        rows.map((row) => row.day),
        rows.map((row) => row.scope),
        rows.map((row) => row.visitors),
      ],
    );
    return { found: rows.length, written: inserted.length };
  }

  /**
   * Drops the chunks of the statistic's hypertables that lie wholly before
   * {@link RETENTION_DAYS}. A dropped chunk is a dropped table: nothing for
   * VACUUM. Each drop waits at most {@link DROP_LOCK_TIMEOUT} for its lock
   * and otherwise leaves the chunk for the next night, so a long admin read
   * of an old chunk never queues the API behind it
   * (docs/troubleshooting/timescaledb-lock-queues.md). Returns the chunks
   * dropped per table.
   */
  async dropOldChunks(): Promise<Record<string, number>> {
    const dropped: Record<string, number> = {};
    for (const table of SITE_ANALYTICS_HYPERTABLES) {
      try {
        const chunks: unknown[] = await this.dataSource.transaction(
          async (manager) => {
            await manager.query(
              `SELECT set_config('lock_timeout', '${DROP_LOCK_TIMEOUT}', true)`,
            );
            return manager.query(
              `SELECT drop_chunks($1::regclass, older_than => $2::interval)`,
              [table, `${RETENTION_DAYS} days`],
            );
          },
        );
        dropped[table] = chunks.length;
      } catch (error) {
        this.logger.warn(
          `Could not drop old ${table} chunks, next night: ${(error as Error).message}`,
        );
        dropped[table] = 0;
      }
    }
    return dropped;
  }

  /** What waits in Redis and what was dropped; see {@link readSiteAnalyticsBacklog}. */
  backlog(now = new Date()): Promise<SiteAnalyticsBacklog> {
    return readSiteAnalyticsBacklog(this.redis, this.dataSource, now);
  }

  /**
   * Copies `page_view_days` (the most-read counter before it moved onto this
   * pipeline) into `site_events` as `page_read` rows, one per page, language
   * and day, once: the marker in `site_analytics_imports` is written in the
   * same transaction, and a second instance waits on it and then skips. The
   * old table stays until a later release drops it. A copied row has no path
   * or device (`''`, `unknown`, `zz`), only what the old table knew.
   */
  async importPageViewDays(): Promise<number> {
    const exists: Array<{ table: string | null }> = await this.dataSource.query(
      `SELECT to_regclass('page_view_days')::text AS table`,
    );
    if (!exists[0]?.table) return 0;
    return this.dataSource.transaction(async (manager) => {
      const marker: unknown[] = await manager.query(
        `INSERT INTO site_analytics_imports (name) VALUES ($1)
         ON CONFLICT (name) DO NOTHING RETURNING name`,
        [PAGE_VIEW_DAYS_IMPORT],
      );
      if (marker.length === 0) return 0;
      const result: unknown[] = await manager.query(
        `WITH copied AS (
           INSERT INTO site_events
             (bucket, name, locale, path, page_type, viewport, browser, os,
              country, props_key, props, entity, count)
           SELECT day::timestamp AT TIME ZONE 'UTC', $1, locale, '',
                  CASE kind WHEN 'article' THEN 'guide' ELSE kind END,
                  'unknown', 'unknown', 'unknown', 'zz',
                  CASE WHEN kind IN ('park', 'attraction') THEN 'kind=' || kind
                       ELSE 'key=' || key || '&kind=' || kind END,
                  CASE WHEN kind IN ('park', 'attraction')
                       THEN jsonb_build_object('kind', kind)
                       ELSE jsonb_build_object('key', key, 'kind', kind) END,
                  CASE WHEN kind IN ('park', 'attraction') THEN key::uuid END,
                  views
             FROM page_view_days
            WHERE kind NOT IN ('park', 'attraction')
               OR key ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           RETURNING 1
         )
         SELECT count(*)::int AS rows FROM copied`,
        [PAGE_READ_EVENT],
      );
      const rows = Number((result[0] as { rows?: number })?.rows ?? 0);
      await manager.query(
        `UPDATE site_analytics_imports SET rows = $2 WHERE name = $1`,
        [PAGE_VIEW_DAYS_IMPORT, rows],
      );
      this.logger.log(`Copied ${rows} page_view_days rows into site_events`);
      return rows;
    });
  }
}

/**
 * The site statistic's backlog for the system-health report: the windows
 * Redis still holds, how old the oldest is, the ones set aside, the last
 * write, and the day's drops. A standalone function over a Redis client and a
 * data source, so the health report needs no part of this module.
 */
export async function readSiteAnalyticsBacklog(
  redis: Redis,
  dataSource: Pick<DataSource, "query">,
  now = new Date(),
): Promise<SiteAnalyticsBacklog> {
  const today = utcDay(now);
  const [windows, parked, openVisits, dropsToday, dropsYesterday, flushes] =
    await Promise.all([
      redis.smembers(KEYS.windows),
      redis.scard(KEYS.parkedWindows),
      redis.zcard(KEYS.openVisits),
      redis.hgetall(KEYS.drops(today)),
      redis.hgetall(KEYS.drops(addIsoDays(today, -1))),
      dataSource.query(
        `SELECT max(flushed_at) AS last FROM site_analytics_flushes
          WHERE bucket > now() - interval '2 days'`,
      ) as Promise<Array<{ last: Date | string | null }>>,
    ]);
  const ready = flushWindowOf(new Date(now.getTime() - FLUSH_GRACE_MS));
  const oldest = [...windows].sort()[0];
  const oldestPendingMinutes = oldest
    ? Math.floor((now.getTime() - Date.parse(oldest)) / 60_000)
    : null;
  const readyWindows = windows.filter((window) => window < ready).length;
  const counts = (raw: Record<string, string>) =>
    Object.fromEntries(
      Object.entries(raw).map(([reason, count]) => [reason, Number(count)]),
    );
  const last = flushes[0]?.last;
  return {
    pendingWindows: windows.length,
    readyWindows,
    oldestPendingMinutes,
    parkedWindows: Number(parked),
    openVisits: Number(openVisits),
    lastFlushAt: last ? new Date(last).toISOString() : null,
    dropsToday: counts(dropsToday),
    dropsYesterday: counts(dropsYesterday),
    healthy:
      Number(parked) === 0 &&
      (oldestPendingMinutes ?? 0) <= (3 * FLUSH_WINDOW_SECONDS) / 60 + 5,
  };
}

/** The page-view and event rows a window's fields stand for. */
export function rowsOfFields(fields: Record<string, string>): {
  pageviews: Array<{ dims: PageviewDims; counts: PageviewCounts }>;
  events: Array<{ dims: EventDims; count: number }>;
} {
  const pageviews = new Map<
    string,
    { dims: PageviewDims; counts: PageviewCounts }
  >();
  const events: Array<{ dims: EventDims; count: number }> = [];
  for (const [field, raw] of Object.entries(fields)) {
    const count = Number(raw);
    if (!Number.isFinite(count) || count <= 0) continue;
    const measure = field[0];
    const json = field.slice(1);
    let dims: unknown;
    try {
      dims = JSON.parse(json);
    } catch {
      continue;
    }
    if (!Array.isArray(dims) || dims.some((d) => typeof d !== "string"))
      continue;
    if (measure === BUFFER_MEASURES.events) {
      if (dims.length === 10) events.push({ dims: dims as EventDims, count });
      continue;
    }
    const column = PAGEVIEW_COLUMNS[measure as PageviewMeasure];
    if (!column || dims.length !== 12) continue;
    const row = pageviews.get(json) ?? {
      dims: dims as PageviewDims,
      counts: {
        views: 0,
        seconds: 0,
        visits: 0,
        engaged: 0,
        visitSeconds: 0,
        exits: 0,
        visitSpanSeconds: 0,
      },
    };
    row.counts[column] += count;
    pageviews.set(json, row);
  }
  return { pageviews: [...pageviews.values()], events };
}
