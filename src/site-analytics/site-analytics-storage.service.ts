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
  PAGE_READ_EVENT,
  RETENTION_DAYS,
  SITE_ANALYTICS_KEYS as KEYS,
  SITE_LOCALES,
  flushWindowOf,
} from "./site-analytics.constants";
import { propsOfKey } from "./site-analytics.hits";

/** A buffered page-view field: measure letter, then the row's dimensions. */
export type PageviewDims = [
  locale: string,
  path: string,
  pageType: string,
  viewport: string,
  browser: string,
  os: string,
  country: string,
  entry: string,
  referrer: string,
  channel: string,
  campaign: string,
];

/** A buffered event field's dimensions. */
export type EventDims = [
  name: string,
  locale: string,
  path: string,
  pageType: string,
  viewport: string,
  browser: string,
  os: string,
  country: string,
  entry: string,
  referrer: string,
  channel: string,
  campaign: string,
  propsKey: string,
  entity: string,
];

/**
 * The measure a buffer field counts: page views, visits started, visits that
 * reached a second page view, events. The field is the letter followed by the
 * row's dimensions as JSON, so the flusher can rebuild the row from the field
 * alone.
 */
export const BUFFER_MEASURES = {
  views: "v",
  visits: "s",
  engaged: "e",
  events: "n",
} as const;

export const pageviewField = (
  measure: "v" | "s" | "e",
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

/** How long a chunk drop waits for its lock before trying again tomorrow. */
const DROP_LOCK_TIMEOUT = "5s";

/** The name of the one-off copy of the old most-read table. */
export const PAGE_VIEW_DAYS_IMPORT = "page_view_days";

/**
 * The statistic's Postgres side, all of it insert-only: the flusher that moves
 * finished Redis windows into the hypertables, the direct write the collector
 * falls back to when Redis is away, the nightly visitor snapshot, the nightly
 * chunk drop, and the one-off import of `page_view_days`. Nothing here updates
 * or deletes a counted row, which is what keeps the tables free of dead
 * tuples. docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteAnalyticsStorageService implements OnApplicationBootstrap {
  private readonly logger = new Logger(SiteAnalyticsStorageService.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

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
   * Writes every finished window from Redis to Postgres, oldest first. One
   * instance at a time (a Redis lock); a window is renamed to a claimed key
   * before it is read, written in one transaction together with its marker in
   * `site_analytics_flushes`, and deleted only after the commit. A crash
   * leaves the claimed key, which the next run writes; a window whose marker
   * already exists is not written again. Returns what was written.
   */
  async flush(now = new Date()): Promise<{
    windows: number;
    pageviewRows: number;
    eventRows: number;
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
    if (locked !== "OK") {
      return { windows: 0, pageviewRows: 0, eventRows: 0, skipped: "locked" };
    }
    const written = { windows: 0, pageviewRows: 0, eventRows: 0 };
    try {
      const before = flushWindowOf(new Date(now.getTime() - FLUSH_GRACE_MS));
      const windows = (await this.redis.smembers(KEYS.windows))
        .filter((window) => window < before)
        .sort();
      for (const window of windows) {
        const fields = await this.claim(window);
        if (fields) {
          const rows = await this.dataSource.transaction((manager) =>
            this.writeWindow(manager, window, fields, true),
          );
          written.windows += 1;
          written.pageviewRows += rows.pageviewRows;
          written.eventRows += rows.eventRows;
          await this.redis.del(KEYS.flushing(window));
        }
        await this.redis.srem(KEYS.windows, window);
      }
    } finally {
      if ((await this.redis.get(KEYS.flushLock)) === token) {
        await this.redis.del(KEYS.flushLock);
      }
    }
    return written;
  }

  /**
   * A window's counts, from its claimed key if a previous run left one, else
   * by renaming the live key to the claimed one. Null when there is nothing.
   */
  private async claim(window: string): Promise<Record<string, string> | null> {
    const flushing = KEYS.flushing(window);
    if (!(await this.redis.exists(flushing))) {
      try {
        await this.redis.rename(KEYS.buffer(window), flushing);
      } catch {
        return null;
      }
    }
    const fields = await this.redis.hgetall(flushing);
    return Object.keys(fields).length > 0 ? fields : null;
  }

  /**
   * The collector's fallback when Redis cannot take a request's counts: the
   * same rows, written straight away and without a marker, beside whatever
   * the flusher writes for that window later.
   */
  async writeDirect(window: string, increments: Map<string, number>) {
    const fields = Object.fromEntries(
      [...increments].map(([field, count]) => [field, String(count)]),
    );
    return this.dataSource.transaction((manager) =>
      this.writeWindow(manager, window, fields, false),
    );
  }

  /**
   * Inserts one window's rows, page views and events each in one statement
   * fed by arrays. With `marked`, the window's marker goes first and a window
   * that has one is skipped.
   */
  private async writeWindow(
    manager: EntityManager,
    window: string,
    fields: Record<string, string>,
    marked: boolean,
  ): Promise<{ pageviewRows: number; eventRows: number }> {
    const { pageviews, events } = rowsOfFields(fields);
    if (marked) {
      const inserted: unknown[] = await manager.query(
        `INSERT INTO site_analytics_flushes (bucket, pageview_rows, event_rows)
         VALUES ($1::timestamptz, $2, $3)
         ON CONFLICT (bucket) DO NOTHING
         RETURNING bucket`,
        [window, pageviews.length, events.length],
      );
      if (inserted.length === 0) {
        this.logger.warn(`Window ${window} was written before; skipped`);
        return { pageviewRows: 0, eventRows: 0 };
      }
    }
    if (pageviews.length > 0) {
      const column = (i: number) => pageviews.map((row) => row.dims[i]);
      await manager.query(
        `INSERT INTO site_pageviews
           (bucket, locale, path, page_type, viewport, browser, os, country,
            entry, referrer, channel, campaign, views, visits, engaged)
         SELECT $1::timestamptz, *
           FROM unnest($2::text[], $3::text[], $4::text[], $5::text[],
                       $6::text[], $7::text[], $8::text[], $9::text[],
                       $10::text[], $11::text[], $12::text[],
                       $13::int[], $14::int[], $15::int[])`,
        [
          window,
          ...Array.from({ length: 11 }, (_, i) => column(i)),
          pageviews.map((row) => row.views),
          pageviews.map((row) => row.visits),
          pageviews.map((row) => row.engaged),
        ],
      );
    }
    if (events.length > 0) {
      const column = (i: number) => events.map((row) => row.dims[i]);
      await manager.query(
        `INSERT INTO site_events
           (bucket, name, locale, path, page_type, viewport, browser, os,
            country, entry, referrer, channel, campaign, props_key, props,
            entity, count)
         SELECT $1::timestamptz, name, locale, path, page_type, viewport,
                browser, os, country, entry, referrer, channel, campaign,
                props_key, props::jsonb, NULLIF(entity, '')::uuid, count
           FROM unnest($2::text[], $3::text[], $4::text[], $5::text[],
                       $6::text[], $7::text[], $8::text[], $9::text[],
                       $10::text[], $11::text[], $12::text[], $13::text[],
                       $14::text[], $15::text[], $16::text[], $17::int[])
             AS e(name, locale, path, page_type, viewport, browser, os,
                  country, entry, referrer, channel, campaign, props_key,
                  props, entity, count)`,
        [
          window,
          ...Array.from({ length: 13 }, (_, i) => column(i)),
          events.map((row) => JSON.stringify(propsOfKey(row.dims[12]))),
          column(13),
          events.map((row) => row.count),
        ],
      );
    }
    return { pageviewRows: pageviews.length, eventRows: events.length };
  }

  /**
   * Writes the visitors of the last two finished UTC days from their
   * HyperLogLogs (all languages, and each language), once: a day already
   * written stays as it is, and a key that is gone (expired, or lost with
   * Redis) is skipped rather than written as zero. Returns the keys found
   * and the rows written.
   */
  async snapshotVisitors(
    now = new Date(),
  ): Promise<{ found: number; written: number }> {
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

  /**
   * Copies `page_view_days` (the most-read counter before it moved onto this
   * pipeline) into `site_events` as `page_read` rows, one per page, language
   * and day, once: the marker in `site_analytics_imports` is written in the
   * same transaction, and a second instance waits on it and then skips. The
   * old table stays until a later release drops it. A copied row has no path,
   * device or visit (`''`, `unknown`, `zz`), only what the old table knew.
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
              country, entry, referrer, channel, campaign, props_key, props,
              entity, count)
           SELECT day::timestamp AT TIME ZONE 'UTC', $1, locale, '',
                  CASE kind WHEN 'article' THEN 'guide' ELSE kind END,
                  'unknown', 'unknown', 'unknown', 'zz', '', '', '', '',
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

/** The page-view and event rows a window's fields stand for. */
export function rowsOfFields(fields: Record<string, string>): {
  pageviews: Array<{
    dims: PageviewDims;
    views: number;
    visits: number;
    engaged: number;
  }>;
  events: Array<{ dims: EventDims; count: number }>;
} {
  const pageviews = new Map<
    string,
    { dims: PageviewDims; views: number; visits: number; engaged: number }
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
      if (dims.length === 14) events.push({ dims: dims as EventDims, count });
      continue;
    }
    if (dims.length !== 11) continue;
    const row = pageviews.get(json) ?? {
      dims: dims as PageviewDims,
      views: 0,
      visits: 0,
      engaged: 0,
    };
    if (measure === BUFFER_MEASURES.views) row.views += count;
    else if (measure === BUFFER_MEASURES.visits) row.visits += count;
    else if (measure === BUFFER_MEASURES.engaged) row.engaged += count;
    else continue;
    pageviews.set(json, row);
  }
  return { pageviews: [...pageviews.values()], events };
}
