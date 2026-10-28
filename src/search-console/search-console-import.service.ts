import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { randomUUID } from "crypto";
import type Redis from "ioredis";
import { DataSource } from "typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import {
  getSearchConsoleImportDays,
  type SearchConsoleSetup,
} from "../config/search-console.config";
import { daysBetween } from "../site-analytics/site-analytics.query";
import {
  SearchConsoleApiError,
  SearchConsoleClient,
  type GscApiRow,
} from "./search-console.client";
import {
  BACKFILL_DAYS,
  DROP_LOCK_TIMEOUT,
  IMPORT_LOCK_KEY,
  IMPORT_LOCK_TTL_MS,
  INSERT_CHUNK_ROWS,
  MAX_CONSECUTIVE_FAILURES,
  RETENTION_DAYS,
  RUN_RETENTION_DAYS,
} from "./search-console.constants";
import { currentBatches, type CurrentBatch } from "./search-console-batches";
import {
  localeOfPath,
  normalizeQuery,
  pagePath,
} from "./search-console-url.util";

/** A row as it is stored, before it has a batch. */
export interface StoredRow {
  page: string;
  query: string | null;
  locale: string;
  country: string;
  device: string;
  clicks: number;
  impressions: number;
  positionSum: number;
}

/** What a day's download adds up to; the next run compares a fresh one with it. */
export interface DayTotals {
  rows: number;
  clicks: number;
  impressions: number;
  positionSum: number;
}

export interface DayBatch {
  rows: StoredRow[];
  totals: DayTotals;
  /** API rows on a page the property does not count, or no real path. */
  skipped: number;
}

export interface ImportSummary {
  /** Why nothing ran, when nothing did. */
  skipped?: "not-configured" | "running";
  daysChecked: number;
  batchesWritten: number;
  unchanged: number;
  rowsWritten: number;
  skippedRows: number;
  failedDays: string[];
}

const emptySummary = (): ImportSummary => ({
  daysChecked: 0,
  batchesWritten: 0,
  unchanged: 0,
  rowsWritten: 0,
  skippedRows: 0,
  failedDays: [],
});

/**
 * One day's API rows as stored ones. `pageRows` have the dimensions page,
 * country, device and become the rows without a query, whose sums are the
 * property's own; `queryRows` add the query. Pages are mapped to paths
 * (several URLs of one page, `http`, `www` and a trailing slash, merge), the
 * country and device lowered, the position weighted by the impressions.
 */
export function buildBatch(
  pageRows: readonly GscApiRow[],
  queryRows: readonly GscApiRow[],
  hosts: readonly string[],
): DayBatch {
  const merged = new Map<string, StoredRow>();
  let skipped = 0;
  const add = (row: GscApiRow, withQuery: boolean): StoredRow | null => {
    const [url, ...rest] = row.keys;
    const path = pagePath(url ?? "", hosts);
    if (!path) {
      skipped++;
      return null;
    }
    const query = withQuery ? normalizeQuery(rest.shift() ?? "") : null;
    const [country = "zzz", device = ""] = rest;
    const key = [path, query ?? "\u0000", country, device].join("\u0001");
    const stored = merged.get(key) ?? {
      page: path,
      query,
      locale: localeOfPath(path),
      country: country.toLowerCase().slice(0, 3),
      device: device.toLowerCase().slice(0, 8),
      clicks: 0,
      impressions: 0,
      positionSum: 0,
    };
    stored.clicks += Math.round(row.clicks);
    stored.impressions += Math.round(row.impressions);
    stored.positionSum += row.position * row.impressions;
    merged.set(key, stored);
    return stored;
  };
  const totals: DayTotals = {
    rows: 0,
    clicks: 0,
    impressions: 0,
    positionSum: 0,
  };
  for (const row of pageRows) {
    const stored = add(row, false);
    if (!stored) continue;
    totals.clicks += Math.round(row.clicks);
    totals.impressions += Math.round(row.impressions);
    totals.positionSum += row.position * row.impressions;
  }
  for (const row of queryRows) add(row, true);
  const rows = [...merged.values()];
  totals.rows = rows.length;
  return { rows, totals, skipped };
}

/** Whether a fresh download says nothing the day's current batch does not. */
export function isUnchanged(
  current: CurrentBatch | undefined,
  fresh: DayTotals,
): boolean {
  if (!current) return false;
  // A download that came back empty changes nothing: either the day was asked
  // before and was empty, or it must not erase a day that had data.
  if (fresh.rows === 0) return true;
  return (
    current.rows === fresh.rows &&
    current.clicks === fresh.clicks &&
    current.impressions === fresh.impressions &&
    Math.abs(current.positionSum - fresh.positionSum) < 1
  );
}

/** The days a run looks at, newest first: the recent ones again, then those never fetched. */
export function planDays(
  now: Date,
  recentDays: number,
  fetched: ReadonlySet<string>,
): string[] {
  const end = addIsoDays(utcDay(now), -1);
  const recent = daysBetween(addIsoDays(end, -(recentDays - 1)), end).reverse();
  const older = daysBetween(
    addIsoDays(end, -(BACKFILL_DAYS - 1)),
    addIsoDays(end, -recentDays),
  )
    .filter((day) => !fetched.has(day))
    .reverse();
  return [...recent, ...older];
}

/**
 * The import of Search Console's numbers into `search_console_rows`: the last
 * few days again on every run (Google finalises late), the days it has never
 * fetched back to 16 months (the first run is the backfill), one day at a time
 * and one transaction per day, so a run that stops resumes where it was.
 *
 * Insert-only: a day that changed is a new batch and the reports read the
 * newest batch of each day, so a re-import leaves no dead tuple and no
 * UPDATE or DELETE touches the hypertable. A day that did not change writes
 * nothing. docs/frontend/search-console.md.
 */
@Injectable()
export class SearchConsoleImportService {
  private readonly logger = new Logger(SearchConsoleImportService.name);

  constructor(
    @InjectDataSource() private readonly db: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly client: SearchConsoleClient,
  ) {}

  /**
   * One run. Nothing happens without credentials, or while another run holds
   * the lock. Throws once every day it could was written, if some days failed
   * or Google refused the account, so the job shows as failed and the status
   * carries the reason.
   */
  async run(
    triggeredBy: "cron" | "manual",
    now = new Date(),
  ): Promise<ImportSummary> {
    const setup = this.client.setup();
    if (!setup.configured) {
      return { ...emptySummary(), skipped: "not-configured" };
    }
    const lock = await this.acquireLock();
    if (!lock) return { ...emptySummary(), skipped: "running" };

    const runId = await this.startRun(triggeredBy);
    const summary = emptySummary();
    let failure: Error | null = null;
    try {
      await this.importDays(setup, now, summary);
      if (summary.failedDays.length > 0) {
        failure = new Error(
          `${summary.failedDays.length} of ${summary.daysChecked} day(s) failed to import: ${summary.failedDays.slice(0, 5).join(", ")}`,
        );
      }
    } catch (error) {
      failure = error as Error;
    } finally {
      await this.finishRun(runId, summary, failure);
      await this.releaseLock(lock);
    }
    if (failure) throw failure;
    return summary;
  }

  private async importDays(
    setup: Extract<SearchConsoleSetup, { configured: true }>,
    now: Date,
    summary: ImportSummary,
  ): Promise<void> {
    const end = addIsoDays(utcDay(now), -1);
    const current = await currentBatches(
      this.db,
      addIsoDays(end, -(BACKFILL_DAYS - 1)),
      end,
    );
    const days = planDays(
      now,
      getSearchConsoleImportDays(),
      new Set(current.keys()),
    );
    let consecutive = 0;
    let lastMessage = "";
    for (const day of days) {
      summary.daysChecked++;
      try {
        await this.importDay(setup, day, current.get(day), summary);
        consecutive = 0;
      } catch (error) {
        lastMessage = (error as Error).message;
        if (error instanceof SearchConsoleApiError && error.fatal) {
          // The account, the property or the key: every day would fail alike.
          throw error;
        }
        summary.failedDays.push(day);
        this.logger.warn(`Search Console ${day}: ${lastMessage}`);
        if (++consecutive >= MAX_CONSECUTIVE_FAILURES) {
          throw new Error(
            `Search Console import stopped after ${consecutive} failed days in a row: ${lastMessage}`,
          );
        }
      }
    }
    if (summary.batchesWritten > 0) {
      this.logger.log(
        `Search Console: ${summary.batchesWritten} day(s) written (${summary.rowsWritten} rows), ${summary.unchanged} unchanged, ${summary.skippedRows} rows off the site skipped`,
      );
    }
  }

  private async importDay(
    setup: Extract<SearchConsoleSetup, { configured: true }>,
    day: string,
    current: CurrentBatch | undefined,
    summary: ImportSummary,
  ): Promise<void> {
    const pageRows = await this.client.fetchDay(day, [
      "page",
      "country",
      "device",
    ]);
    const queryRows = await this.client.fetchDay(day, [
      "page",
      "query",
      "country",
      "device",
    ]);
    const batch = buildBatch(pageRows, queryRows, setup.hosts);
    summary.skippedRows += batch.skipped;
    if (isUnchanged(current, batch.totals)) {
      summary.unchanged++;
      return;
    }
    await this.writeBatch(day, batch);
    summary.batchesWritten++;
    summary.rowsWritten += batch.rows.length;
  }

  /**
   * The batch and its rows in one transaction, so a reader sees the day's
   * old batch or its new one and never half of either.
   */
  private async writeBatch(day: string, batch: DayBatch): Promise<void> {
    await this.db.transaction(async (manager) => {
      const [{ id }] = (await manager.query(
        `INSERT INTO search_console_batches (day, rows, clicks, impressions, position_sum)
         VALUES ($1::date, $2, $3, $4, $5) RETURNING id`,
        [
          day,
          batch.rows.length,
          batch.totals.clicks,
          batch.totals.impressions,
          batch.totals.positionSum,
        ],
      )) as Array<{ id: number }>;
      for (let i = 0; i < batch.rows.length; i += INSERT_CHUNK_ROWS) {
        const chunk = batch.rows.slice(i, i + INSERT_CHUNK_ROWS);
        await manager.query(
          `INSERT INTO search_console_rows
             (day, batch_id, page, query, locale, country, device, clicks, impressions, position_sum)
           SELECT $1::date, $2::int, * FROM unnest(
             $3::text[], $4::text[], $5::text[], $6::text[], $7::text[],
             $8::int[], $9::int[], $10::float8[])`,
          [
            day,
            id,
            chunk.map((row) => row.page),
            chunk.map((row) => row.query),
            chunk.map((row) => row.locale),
            chunk.map((row) => row.country),
            chunk.map((row) => row.device),
            chunk.map((row) => row.clicks),
            chunk.map((row) => row.impressions),
            chunk.map((row) => row.positionSum),
          ],
        );
      }
    });
  }

  /**
   * Retention: `drop_chunks` on chunks wholly older than `RETENTION_DAYS`,
   * under a `lock_timeout` so a long read of an old chunk delays the drop
   * rather than queueing the API behind it
   * (docs/troubleshooting/timescaledb-lock-queues.md), then the bookkeeping
   * rows that name nothing any more. Returns the chunks dropped.
   */
  async dropOldChunks(): Promise<number> {
    let dropped = 0;
    try {
      const chunks: unknown[] = await this.db.transaction(async (manager) => {
        await manager.query(
          `SELECT set_config('lock_timeout', '${DROP_LOCK_TIMEOUT}', true)`,
        );
        return manager.query(
          `SELECT drop_chunks('search_console_rows'::regclass, older_than => $1::interval)`,
          [`${RETENTION_DAYS} days`],
        );
      });
      dropped = chunks.length;
    } catch (error) {
      this.logger.warn(
        `Could not drop old search_console_rows chunks, next night: ${(error as Error).message}`,
      );
    }
    await this.db.query(
      `DELETE FROM search_console_batches WHERE day < (now() AT TIME ZONE 'UTC')::date - $1::int`,
      [RETENTION_DAYS],
    );
    await this.db.query(
      `DELETE FROM search_console_runs WHERE started_at < now() - ($1::int * interval '1 day')`,
      [RUN_RETENTION_DAYS],
    );
    return dropped;
  }

  private async startRun(triggeredBy: string): Promise<number | null> {
    try {
      const [row] = (await this.db.query(
        `INSERT INTO search_console_runs (triggered_by) VALUES ($1) RETURNING id`,
        [triggeredBy],
      )) as Array<{ id: number }>;
      return row.id;
    } catch (error) {
      this.logger.warn(`Could not record the run: ${(error as Error).message}`);
      return null;
    }
  }

  private async finishRun(
    id: number | null,
    summary: ImportSummary,
    failure: Error | null,
  ): Promise<void> {
    if (id === null) return;
    try {
      await this.db.query(
        `UPDATE search_console_runs
            SET finished_at = now(), ok = $2, days_checked = $3, batches_written = $4,
                rows_written = $5, error = $6
          WHERE id = $1`,
        [
          id,
          failure === null,
          summary.daysChecked,
          summary.batchesWritten,
          summary.rowsWritten,
          failure ? failure.message.slice(0, 1000) : null,
        ],
      );
    } catch (error) {
      this.logger.warn(`Could not close the run: ${(error as Error).message}`);
    }
  }

  /** A token for the lock, or null when a run holds it. Redis trouble does not stop a run. */
  private async acquireLock(): Promise<string | null> {
    const token = randomUUID();
    try {
      const got = await this.redis.set(
        IMPORT_LOCK_KEY,
        token,
        "PX",
        IMPORT_LOCK_TTL_MS,
        "NX",
      );
      return got === "OK" ? token : null;
    } catch (error) {
      this.logger.warn(
        `Import lock unavailable, running without it: ${(error as Error).message}`,
      );
      return token;
    }
  }

  private async releaseLock(token: string): Promise<void> {
    try {
      await this.redis.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end return 0",
        1,
        IMPORT_LOCK_KEY,
        token,
      );
    } catch {
      // The lock expires on its own.
    }
  }
}
