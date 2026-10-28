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
  DAILY_RETENTION_DAYS,
  DROP_REASONS,
  FLUSH_WINDOW_SECONDS,
  HOURLY_RETENTION_DAYS,
  HYPERTABLE_RETENTION_DAYS,
  MAX_FLUSH_ATTEMPTS,
  MAX_VISITS_CLOSED_PER_RUN,
  PAGE_READ_EVENT,
  PARKED_WINDOW_TTL_SECONDS,
  ROLLUP_MARKER_RETENTION_DAYS,
  ROLLUP_RUN_BUDGET_MS,
  ROLLUP_SETTLE_SECONDS,
  SITE_ANALYTICS_KEYS as KEYS,
  SITE_LOCALES,
  VISIT_IDLE_SECONDS,
  bucketOf,
  flushWindowOf,
  hourKey,
} from "./site-analytics.constants";
import { propsOfKey } from "./site-analytics.hits";
import { daysBetween } from "./site-analytics.query";
import {
  ROLLUP_TABLES,
  rollupSql,
  type Cuboid,
  type RollupSource,
} from "./site-analytics.rollup";
import {
  retentionCutoff,
  unrolledDays,
  type RollupMarker,
} from "./site-analytics.retention";
import { defineCloseVisitCommand } from "./site-analytics-scripts";
import { parseVisitorDimMember } from "./visitor-dims.util";

/**
 * A buffered page-view field's dimensions. A page view's row leaves the last
 * three (the visit's) empty; a visit's start row carries the first page view's
 * and the visit's, `entry` being the first page view's path; its exit row the
 * last page view's and the visit's.
 */
export type PageviewDims = [
  locale: string,
  path: string,
  pageType: string,
  device: string,
  browser: string,
  os: string,
  country: string,
  browserLanguage: string,
  entry: string,
  referrer: string,
  channel: string,
];

/** A buffered event field's dimensions: the page's, never the visit's. */
export type EventDims = [
  name: string,
  locale: string,
  path: string,
  pageType: string,
  device: string,
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

/** The hourly tables, whose chunks wait for their roll-up before they go. */
const HOURLY_TABLES: ReadonlySet<string> = new Set(
  Object.values(ROLLUP_TABLES).map((table) => table.hourly),
);

/**
 * The markers a rolled-up day has: every cuboid of both sources, which one
 * transaction writes together. A cuboid added later has none for the days
 * before it, until the job has filled them in.
 */
const WANTED_MARKERS: ReadonlyArray<{ source: string; cuboid: number }> =
  Object.values(ROLLUP_TABLES).flatMap((table) =>
    table.cuboids.map((cuboid) => ({
      source: table.source,
      cuboid: cuboid.id,
    })),
  );

/** A chunk's range, `[start, end)`. */
interface ChunkRange {
  start: Date;
  end: Date;
}

/** An hourly chunk the drop left in place, and the days that kept it. */
export interface SkippedChunk {
  table: string;
  /** The chunk's first UTC day. */
  from: string;
  /** Its days with rows that miss a roll-up marker. */
  days: string[];
}

/** Rows deleted from the small plain tables. */
export interface PrunedRows {
  visitorDays: number;
  visitorHours: number;
  visitorDimDays: number;
  rollupMarkers: number;
}

/** What one run of the nightly retention did. */
export interface DropOldChunksResult {
  /** Chunks dropped per hypertable. */
  dropped: Record<string, number>;
  /** Hourly chunks kept because a day in them is not rolled up. */
  skipped: SkippedChunk[];
  pruned: PrunedRows;
}

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
 * snapshot, the roll-up of finished days into the daily tables, the nightly
 * chunk drop, and the one-off import of `page_view_days`. Nothing here updates or deletes a counted row, which is
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
      const failures: Array<[string, Error]> = [];
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
          failures.push([window, error as Error]);
        }
      }
      // A run that wrote nothing says the database is away, not that a window
      // is bad, so it counts no attempt and an outage parks nothing.
      if (written.windows > 0) {
        for (const [window, error] of failures)
          await this.failed(window, error);
      } else if (failures.length > 0) {
        this.logger.warn(
          `No window written, ${failures.length} waiting: ${failures[0][1].message}`,
        );
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
   * Counts one failed attempt at a window (only in a run that wrote another one) and sets the window aside after
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
           (bucket, locale, path, page_type, device, browser, os, country,
            browser_language, entry, referrer, channel, views,
            seconds, visits, engaged, visit_seconds, exits,
            visit_span_seconds)
         SELECT $1::timestamptz, *
           FROM unnest($2::text[], $3::text[], $4::text[], $5::text[],
                       $6::text[], $7::text[], $8::text[], $9::text[],
                       $10::text[], $11::text[], $12::text[],
                       $13::int[], $14::int[], $15::int[], $16::int[],
                       $17::int[], $18::int[], $19::int[])`,
        [
          bucket,
          ...Array.from({ length: 11 }, (_, i) => column(i)),
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
           (bucket, name, locale, path, page_type, device, browser, os,
            country, props_key, props, entity, count)
         SELECT $1::timestamptz, name, locale, path, page_type, device,
                browser, os, country, props_key, props::jsonb,
                NULLIF(entity, '')::uuid, count
           FROM unnest($2::text[], $3::text[], $4::text[], $5::text[],
                       $6::text[], $7::text[], $8::text[], $9::text[],
                       $10::text[], $11::text[], $12::text[], $13::int[])
             AS e(name, locale, path, page_type, device, browser, os,
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
   * each language), once, and each hour's beside them: a day already written
   * stays as it is, and a key that is gone (expired, or lost with Redis) is
   * skipped rather than written as zero. An hour nobody visited has no key, so
   * the 24 hours of every language are written (with zeros) only when the
   * day's all-languages key was found: then the day was counted, and an hour
   * or a language without a key had no visitors. Runs every quarter hour from midnight to 04:00, well inside the
   * time a day's and its hours' keys outlive their day. Returns the day keys found, and the
   * day and hour rows written.
   */
  async snapshotVisitors(
    now = new Date(),
  ): Promise<{ found: number; written: number; hoursWritten: number }> {
    const day = addIsoDays(utcDay(now), -1);
    const scopes = [ALL_LOCALES_SCOPE, ...SITE_LOCALES];
    const keys = scopes.map((scope) => ({
      day,
      scope,
      key: KEYS.visitors(day, scope),
    }));
    const hours = Array.from({ length: 24 }, (_, h) => {
      const start = new Date(`${day}T${String(h).padStart(2, "0")}:00:00Z`);
      return { start, key: hourKey(start) };
    });
    const pipeline = this.redis.pipeline();
    for (const { key } of keys) pipeline.exists(key).pfcount(key);
    for (const { scope } of keys) {
      for (const hour of hours) {
        pipeline.pfcount(KEYS.hourVisitors(hour.key, scope));
      }
    }
    const results = (await pipeline.exec()) ?? [];
    const rows = keys
      .map((entry, i) => ({
        ...entry,
        exists: Number(results[2 * i]?.[1] ?? 0) === 1,
        visitors: Number(results[2 * i + 1]?.[1] ?? 0),
      }))
      .filter((row) => row.exists && Number.isFinite(row.visitors));
    if (rows.length === 0) return { found: 0, written: 0, hoursWritten: 0 };

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

    // Without the all-languages key nothing says the day was counted.
    const counted = rows.some((row) => row.scope === ALL_LOCALES_SCOPE);
    const base = 2 * keys.length;
    const wanted = !counted
      ? []
      : keys
          .flatMap((entry, s) =>
            hours.map((hour, h) => ({
              hour: hour.start.toISOString(),
              scope: entry.scope,
              visitors: Number(results[base + s * hours.length + h]?.[1] ?? 0),
            })),
          )
          .filter((row) => Number.isFinite(row.visitors));
    const hoursInserted: unknown[] = await this.dataSource.query(
      `INSERT INTO site_visitor_hours (hour, scope, visitors)
       SELECT * FROM unnest($1::timestamptz[], $2::text[], $3::int[])
       ON CONFLICT (hour, scope) DO NOTHING
       RETURNING hour`,
      [
        wanted.map((row) => row.hour),
        wanted.map((row) => row.scope),
        wanted.map((row) => row.visitors),
      ],
    );
    return {
      found: rows.length,
      written: inserted.length,
      hoursWritten: hoursInserted.length,
    };
  }

  /**
   * Writes yesterday's visitors by country, device, browser, OS and channel
   * (`site_visitor_dim_days`) from the HyperLogLogs the day's index lists,
   * once: a row already written stays, and a key that is gone, or counted
   * nobody, is skipped rather than written as zero. Runs with
   * {@link snapshotVisitors}, every quarter hour from midnight to 04:00. Returns
   * the keys the index listed and the rows written.
   */
  async snapshotVisitorDims(
    now = new Date(),
  ): Promise<{ found: number; written: number }> {
    const day = addIsoDays(utcDay(now), -1);
    const keys = (await this.redis.smembers(KEYS.visitorDimIndex(day))).flatMap(
      (member) => parseVisitorDimMember(member) ?? [],
    );
    if (keys.length === 0) return { found: 0, written: 0 };
    const pipeline = this.redis.pipeline();
    for (const { scope, dim, value } of keys) {
      pipeline.pfcount(KEYS.visitorDim(day, scope, dim, value));
    }
    const results = (await pipeline.exec()) ?? [];
    const rows = keys
      .map((key, i) => ({ ...key, visitors: Number(results[i]?.[1] ?? 0) }))
      .filter((row) => row.visitors > 0);
    if (rows.length === 0) return { found: keys.length, written: 0 };
    const inserted: unknown[] = await this.dataSource.query(
      `INSERT INTO site_visitor_dim_days (day, scope, dim, value, visitors)
       SELECT $1::date, * FROM unnest($2::text[], $3::text[], $4::text[], $5::int[])
       ON CONFLICT (day, scope, dim, value) DO NOTHING
       RETURNING day`,
      [
        day,
        rows.map((row) => row.scope),
        rows.map((row) => row.dim),
        rows.map((row) => row.value),
        rows.map((row) => row.visitors),
      ],
    );
    return { found: keys.length, written: inserted.length };
  }

  /**
   * Rolls the finished days up into the daily tables (`site_pageviews_daily`,
   * `site_events_daily`), oldest first, for every cuboid of a day that has no
   * marker yet: the first run backfills what the hourly tables hold, later
   * runs add the day that ended, and a cuboid added to the lists later is
   * filled in for the old days the same way. A day is rolled only when it
   * ended `ROLLUP_SETTLE_SECONDS` ago and no flush window of it waits in
   * Redis any more, so no hourly row can still arrive for it. One instance
   * at a time (a Redis lock), within `ROLLUP_RUN_BUDGET_MS`; what is left is
   * the next run's. A day is one transaction, which checks that every cuboid
   * it wrote adds up to the hourly rows' totals before it commits. Returns
   * what was written.
   */
  async rollupDays(now = new Date()): Promise<{
    days: number;
    cuboids: number;
    rows: number;
    skipped?: "locked";
  }> {
    const done = { days: 0, cuboids: 0, rows: 0 };
    const token = randomUUID();
    const locked = await this.redis.set(
      KEYS.rollupLock,
      token,
      "PX",
      ROLLUP_RUN_BUDGET_MS + FLUSH_LOCK_MS,
      "NX",
    );
    if (locked !== "OK") return { ...done, skipped: "locked" };
    try {
      const last = await this.lastRollableDay(now);
      const first = await this.firstRollableDay(now);
      if (!first || first > last) return done;
      const rolled = await this.rolledMarkers(first, last);
      const deadline = Date.now() + ROLLUP_RUN_BUDGET_MS;
      for (const day of daysBetween(first, last)) {
        if (Date.now() > deadline) break;
        const missing = (Object.keys(ROLLUP_TABLES) as RollupSource[])
          .map((source) => ({
            source,
            cuboids: ROLLUP_TABLES[source].cuboids.filter(
              (cuboid) => !rolled.has(`${source}:${cuboid.id}:${day}`),
            ),
          }))
          .filter((entry) => entry.cuboids.length > 0);
        if (missing.length === 0) continue;
        const written = await this.rollupDay(day, missing);
        done.days += 1;
        done.cuboids += written.cuboids;
        done.rows += written.rows;
      }
    } finally {
      if ((await this.redis.get(KEYS.rollupLock)) === token) {
        await this.redis.del(KEYS.rollupLock);
      }
    }
    return done;
  }

  /**
   * The last day that is safe to roll up: ended `ROLLUP_SETTLE_SECONDS` ago,
   * and before the oldest flush window still waiting in Redis.
   */
  private async lastRollableDay(now: Date): Promise<string> {
    const settled = addIsoDays(
      utcDay(new Date(now.getTime() - ROLLUP_SETTLE_SECONDS * 1000)),
      -1,
    );
    const oldest = (await this.redis.smembers(KEYS.windows)).sort()[0];
    return oldest
      ? minDay(settled, addIsoDays(oldest.slice(0, 10), -1))
      : settled;
  }

  /**
   * The first day to look at: the day of the oldest hourly row, at most
   * `DAILY_RETENTION_DAYS` back, since an older day would go with the daily
   * rows' next chunk drop; null with no rows.
   */
  private async firstRollableDay(now: Date): Promise<string | null> {
    const rows: Array<{ day: string | null }> = await this.dataSource.query(
      `SELECT to_char(LEAST(
                (SELECT min(bucket) FROM site_pageviews),
                (SELECT min(bucket) FROM site_events)) AT TIME ZONE 'UTC',
              'YYYY-MM-DD') AS day`,
    );
    const oldest = rows[0]?.day;
    if (!oldest) return null;
    const floor = addIsoDays(utcDay(now), -DAILY_RETENTION_DAYS);
    return oldest > floor ? oldest : floor;
  }

  /** `source:cuboid:day` of every marker from `first` to `last`. */
  private async rolledMarkers(
    first: string,
    last: string,
  ): Promise<Set<string>> {
    const rows: Array<{ source: string; cuboid: number; day: string }> =
      await this.dataSource.query(
        `SELECT source, cuboid, to_char(day, 'YYYY-MM-DD') AS day
           FROM site_rollup_days
          WHERE day BETWEEN $1::date AND $2::date`,
        [first, last],
      );
    return new Set(rows.map((r) => `${r.source}:${r.cuboid}:${r.day}`));
  }

  /**
   * Writes the missing cuboids of one day in one transaction, serialised
   * with any other roller by an advisory lock, and checks that each adds up to
   * the day's hourly totals before it commits: a cuboid that does not is a
   * bug, and rolling it back leaves the day unrolled, read hourly.
   */
  private async rollupDay(
    day: string,
    missing: Array<{ source: RollupSource; cuboids: readonly Cuboid[] }>,
  ): Promise<{ cuboids: number; rows: number }> {
    const bucket = `${day}T00:00:00.000Z`;
    return this.dataSource.transaction(async (manager) => {
      await manager.query(
        `SELECT pg_advisory_xact_lock(hashtext('site-analytics-rollup'))`,
      );
      let cuboids = 0;
      let rows = 0;
      for (const { source, cuboids: wanted } of missing) {
        const written: number[] = [];
        for (const cuboid of wanted) {
          const result: Array<{ rows: number }> = await manager.query(
            rollupSql(source, cuboid),
            [bucket, day, cuboid.id],
          );
          if (result.length === 0) continue;
          written.push(cuboid.id);
          cuboids += 1;
          rows += result[0].rows;
        }
        await this.checkRolledTotals(manager, source, bucket, written);
      }
      return { cuboids, rows };
    });
  }

  /** Throws when a cuboid written for the day does not sum to the hourly rows. */
  private async checkRolledTotals(
    manager: EntityManager,
    source: RollupSource,
    bucket: string,
    cuboids: number[],
  ): Promise<void> {
    if (cuboids.length === 0) return;
    const table = ROLLUP_TABLES[source];
    const sums = table.measures
      .map((m) => `COALESCE(SUM(${m}), 0)::text AS ${m}`)
      .join(", ");
    const [hourly]: Array<Record<string, string>> = await manager.query(
      `SELECT ${sums} FROM ${table.hourly}
        WHERE bucket >= $1::timestamptz
          AND bucket < $1::timestamptz + interval '1 day'`,
      [bucket],
    );
    const daily: Array<Record<string, string>> = await manager.query(
      `SELECT cuboid::text AS cuboid, ${sums} FROM ${table.daily}
        WHERE bucket = $1::timestamptz AND cuboid = ANY($2::smallint[])
        GROUP BY cuboid`,
      [bucket, cuboids],
    );
    const seen = new Map(daily.map((row) => [Number(row.cuboid), row]));
    for (const id of cuboids) {
      for (const measure of table.measures) {
        const rolled = BigInt(seen.get(id)?.[measure] ?? "0");
        if (rolled !== BigInt(hourly[measure])) {
          throw new Error(
            `${source} cuboid ${id} of ${bucket.slice(0, 10)} adds up to ${rolled} ${measure}, the hourly rows to ${hourly[measure]}`,
          );
        }
      }
    }
  }

  /**
   * Applies the retention of every table the statistic keeps: the chunks of
   * its hypertables that lie wholly before their cutoff
   * (`HYPERTABLE_RETENTION_DAYS`), and the rows of the small plain tables
   * past theirs (`pruneSmallTables`). A dropped chunk is a dropped table:
   * nothing for VACUUM. Each drop waits at most {@link DROP_LOCK_TIMEOUT} for
   * its lock and otherwise leaves the chunk for the next night, so a long
   * admin read of an old chunk never queues the API behind it
   * (docs/troubleshooting/timescaledb-lock-queues.md).
   *
   * An hourly chunk goes only when every day with rows in it is rolled up:
   * has the marker of every cuboid of both sources. What the daily rows
   * lack, the hourly rows are the only copy of. A chunk with a day that is
   * not rolled up stays and is returned in `skipped`, for the job to fail on.
   */
  async dropOldChunks(now = new Date()): Promise<DropOldChunksResult> {
    const dropped: Record<string, number> = {};
    const skipped: SkippedChunk[] = [];
    for (const table of SITE_ANALYTICS_HYPERTABLES) {
      dropped[table] = 0;
      const cutoff = retentionCutoff(now, HYPERTABLE_RETENTION_DAYS[table]);
      try {
        for (const chunk of await this.chunksBefore(table, cutoff)) {
          const missing = HOURLY_TABLES.has(table)
            ? await this.unrolledDaysOf(table, chunk)
            : [];
          if (missing.length > 0) {
            skipped.push({
              table,
              from: utcDay(chunk.start),
              days: missing,
            });
            continue;
          }
          try {
            dropped[table] += await this.dropChunk(table, chunk);
          } catch (error) {
            this.logger.warn(
              `Could not drop the ${table} chunk of ${utcDay(chunk.start)}, next night: ${(error as Error).message}`,
            );
          }
        }
      } catch (error) {
        this.logger.warn(
          `Could not check old ${table} chunks, next night: ${(error as Error).message}`,
        );
      }
    }
    if (skipped.length > 0) {
      this.logger.warn(
        `Kept ${skipped.length} hourly chunk(s) past ${HOURLY_RETENTION_DAYS} days: ` +
          `${skipped.reduce((sum, chunk) => sum + chunk.days.length, 0)} day(s) have no roll-up marker yet`,
      );
    }
    return { dropped, skipped, pruned: await this.pruneSmallTables(now) };
  }

  /** The chunks of a hypertable that lie wholly before `cutoff`, oldest first. */
  private async chunksBefore(
    table: string,
    cutoff: string,
  ): Promise<ChunkRange[]> {
    const rows: Array<{ start: Date | string; end: Date | string }> =
      await this.dataSource.query(
        `SELECT range_start AS start, range_end AS end
           FROM timescaledb_information.chunks
          WHERE hypertable_schema = 'public' AND hypertable_name = $1
            AND range_end <= $2::timestamptz
          ORDER BY range_start`,
        [table, cutoff],
      );
    return rows.map((row) => ({
      start: new Date(row.start),
      end: new Date(row.end),
    }));
  }

  /**
   * The days of an hourly chunk that have rows and miss a roll-up marker.
   * A day without rows needs none: the first chunk of a table reaches back
   * before its first row.
   */
  private async unrolledDaysOf(
    table: string,
    chunk: ChunkRange,
  ): Promise<string[]> {
    const days: Array<{ day: string }> = await this.dataSource.query(
      `SELECT DISTINCT to_char(bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS day
         FROM ${table}
        WHERE bucket >= $1::timestamptz AND bucket < $2::timestamptz`,
      [chunk.start.toISOString(), chunk.end.toISOString()],
    );
    if (days.length === 0) return [];
    const markers: RollupMarker[] = await this.dataSource.query(
      `SELECT source, cuboid, to_char(day, 'YYYY-MM-DD') AS day
         FROM site_rollup_days
        WHERE day = ANY($1::date[])`,
      [days.map((row) => row.day)],
    );
    return unrolledDays(
      days.map((row) => row.day).sort(),
      markers,
      WANTED_MARKERS,
    );
  }

  /**
   * Drops exactly one chunk: `drop_chunks` takes the chunks wholly inside
   * `[newer_than, older_than]`, which for a chunk's own range is that chunk.
   */
  private async dropChunk(table: string, chunk: ChunkRange): Promise<number> {
    const dropped: unknown[] = await this.dataSource.transaction(
      async (manager) => {
        await manager.query(
          `SELECT set_config('lock_timeout', '${DROP_LOCK_TIMEOUT}', true)`,
        );
        return manager.query(
          `SELECT drop_chunks($1::regclass, older_than => $2::timestamptz,
                              newer_than => $3::timestamptz)`,
          [table, chunk.end.toISOString(), chunk.start.toISOString()],
        );
      },
    );
    return dropped.length;
  }

  /**
   * Deletes what the small plain tables hold past their retention: the
   * visitor counts, by hour, day and dimension value, after
   * {@link DAILY_RETENTION_DAYS}, the roll-up markers
   * after {@link ROLLUP_MARKER_RETENTION_DAYS}. They are small, so a plain
   * delete that autovacuum keeps up with does. A table that fails keeps its
   * rows for the next night.
   */
  private async pruneSmallTables(now: Date): Promise<PrunedRows> {
    const visitorCutoff = retentionCutoff(now, DAILY_RETENTION_DAYS);
    const markerCutoff = retentionCutoff(now, ROLLUP_MARKER_RETENTION_DAYS);
    // A statement that selects from the delete, so the driver answers with
    // the count rather than its own pair of rows and affected count.
    const prune = async (table: string, test: string, cutoff: string) => {
      try {
        const [row]: Array<{ deleted: number }> = await this.dataSource.query(
          `WITH gone AS (DELETE FROM ${table} WHERE ${test} RETURNING 1)
           SELECT count(*)::int AS deleted FROM gone`,
          [cutoff],
        );
        return Number(row?.deleted ?? 0);
      } catch (error) {
        this.logger.warn(
          `Could not prune old ${table} rows, next night: ${(error as Error).message}`,
        );
        return 0;
      }
    };
    return {
      visitorDays: await prune(
        "site_visitor_days",
        "day < $1::date",
        visitorCutoff,
      ),
      visitorHours: await prune(
        "site_visitor_hours",
        "hour < $1::timestamptz",
        visitorCutoff,
      ),
      visitorDimDays: await prune(
        "site_visitor_dim_days",
        "day < $1::date",
        visitorCutoff,
      ),
      rollupMarkers: await prune(
        "site_rollup_days",
        "day < $1::date",
        markerCutoff,
      ),
    };
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
             (bucket, name, locale, path, page_type, device, browser, os,
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

/** The earlier of two `YYYY-MM-DD` days. */
function minDay(a: string, b: string): string {
  return a < b ? a : b;
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
    if (!column || dims.length !== 11) continue;
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
