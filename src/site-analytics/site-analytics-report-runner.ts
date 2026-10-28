import {
  Injectable,
  Logger,
  ServiceUnavailableException,
} from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import {
  REPORT_CONCURRENCY,
  REPORT_QUEUE_LIMIT,
  REPORT_QUEUE_WAIT_MS,
  REPORT_STATEMENT_TIMEOUT_MS,
  REPORT_WORK_MEM_MB,
  ROLLED_DAYS_CACHE_MS,
} from "./site-analytics.constants";
import type { RollupSource } from "./site-analytics.rollup";

/** What the reports need to run their statements; the real one is {@link SiteReportRunner}. */
export interface ReportRunner {
  /** Runs a whole report within the per-process limit. */
  run<T>(report: () => Promise<T>): Promise<T>;
  /** Runs one statement of a report under the statement timeout. */
  query(sql: string, params?: unknown[]): Promise<any[]>;
  /** The days of a cuboid the nightly job has rolled up; what it cannot read is none. */
  rolledDays(
    source: RollupSource,
    cuboid: number,
  ): Promise<ReadonlySet<string>>;
}

/** A positive integer from the environment, else the default. */
export function envInt(name: string, fallback: number): number {
  const value = Number(process.env[name]);
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

/** Whether a Postgres error is a statement the timeout cancelled. */
function isStatementTimeout(error: unknown): boolean {
  const e = error as {
    code?: string;
    driverError?: { code?: string };
    message?: string;
  };
  return (
    (e?.driverError?.code ?? e?.code) === "57014" &&
    /statement timeout/i.test(e?.message ?? "")
  );
}

/**
 * How many reports may run at once in this process, and how many may wait: a
 * report past both is refused at once, and one that waits too long is refused
 * when its time is up. A request must not wait without end, unlike the
 * background work's `DbJobBudget`.
 */
export class ReportGate {
  private running = 0;
  private readonly waiting: Array<() => void> = [];

  constructor(
    private readonly limit: number,
    private readonly queueLimit: number,
    private readonly waitMs: number,
  ) {}

  /** Reports running now and waiting for a place. */
  get load(): { running: number; waiting: number } {
    return { running: this.running, waiting: this.waiting.length };
  }

  async run<T>(report: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await report();
    } finally {
      this.release();
    }
  }

  private acquire(): Promise<void> {
    if (this.running < this.limit) {
      this.running += 1;
      return Promise.resolve();
    }
    if (this.waiting.length >= this.queueLimit) {
      return Promise.reject(busy());
    }
    return new Promise<void>((resolve, reject) => {
      // The releasing report hands its place over, so `running` already
      // counts this one when it resumes.
      const take = () => {
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(() => {
        const at = this.waiting.indexOf(take);
        if (at >= 0) this.waiting.splice(at, 1);
        reject(busy());
      }, this.waitMs);
      this.waiting.push(take);
    });
  }

  private release(): void {
    const next = this.waiting.shift();
    if (next) next();
    else this.running -= 1;
  }
}

function busy(): ServiceUnavailableException {
  return new ServiceUnavailableException({
    statusCode: 503,
    error: "Service Unavailable",
    reason: "busy",
    message:
      "Too many statistic reports are running at once. Try again in a few seconds.",
  });
}

/**
 * What every admin report of the site statistic runs through: a per-process
 * limit on reports at once (`SITE_ANALYTICS_REPORT_CONCURRENCY`, with a bounded
 * queue), a timeout and a sort and hash memory on every statement
 * (`SITE_ANALYTICS_STATEMENT_TIMEOUT_MS`, `SITE_ANALYTICS_REPORT_WORK_MEM_MB`),
 * and the days of each cuboid the nightly job has rolled up. A
 * report that waits too long, or whose statement runs past its limit, answers
 * 503 with a message that says what to try. docs/frontend/site-analytics.md.
 */
@Injectable()
export class SiteReportRunner implements ReportRunner {
  private readonly logger = new Logger(SiteReportRunner.name);
  private readonly gate: ReportGate;
  private readonly timeoutMs: number;
  private readonly workMemMb: number;
  private readonly rolled = new Map<
    string,
    { at: number; days: Promise<ReadonlySet<string>> }
  >();

  constructor(@InjectDataSource() private readonly db: DataSource) {
    this.timeoutMs = envInt(
      "SITE_ANALYTICS_STATEMENT_TIMEOUT_MS",
      REPORT_STATEMENT_TIMEOUT_MS,
    );
    this.workMemMb = envInt(
      "SITE_ANALYTICS_REPORT_WORK_MEM_MB",
      REPORT_WORK_MEM_MB,
    );
    this.gate = new ReportGate(
      envInt("SITE_ANALYTICS_REPORT_CONCURRENCY", REPORT_CONCURRENCY),
      envInt("SITE_ANALYTICS_REPORT_QUEUE", REPORT_QUEUE_LIMIT),
      envInt("SITE_ANALYTICS_REPORT_QUEUE_WAIT_MS", REPORT_QUEUE_WAIT_MS),
    );
  }

  run<T>(report: () => Promise<T>): Promise<T> {
    return this.gate.run(report);
  }

  async query(sql: string, params: unknown[] = []): Promise<any[]> {
    try {
      return await this.db.transaction(async (manager) => {
        // For this transaction only: the pool's other work keeps its own.
        // Chunk-wise aggregation builds a hash table per chunk and merges
        // them, which a grouping by path (125,000 values a year) pays for
        // fifty times over; one table over all chunks was 2 to 3 times faster
        // on every report.
        await manager.query(
          `SELECT set_config('statement_timeout', $1, true),
                  set_config('work_mem', $2, true),
                  set_config('timescaledb.enable_chunkwise_aggregation', 'off', true)`,
          [String(this.timeoutMs), `${this.workMemMb}MB`],
        );
        return manager.query(sql, params);
      });
    } catch (error) {
      if (!isStatementTimeout(error)) throw error;
      this.logger.warn(
        `A report statement ran past ${this.timeoutMs} ms and was stopped: ${sql.replace(/\s+/g, " ").slice(0, 160)}`,
      );
      throw new ServiceUnavailableException({
        statusCode: 503,
        error: "Service Unavailable",
        reason: "timeout",
        message:
          `The report took longer than ${Math.round(this.timeoutMs / 1000)} s ` +
          "and was stopped. Try a shorter window, a coarser interval or a filter.",
      });
    }
  }

  /**
   * The days of one cuboid with a marker, from a copy kept for a minute: a
   * day is rolled once and never taken back, so an old copy only reads a day
   * hourly that could have been read daily. A failed read fails the report: an
   * empty set would read old days as complete zeros.
   */
  rolledDays(
    source: RollupSource,
    cuboid: number,
  ): Promise<ReadonlySet<string>> {
    const key = `${source}:${cuboid}`;
    const cached = this.rolled.get(key);
    if (cached && Date.now() - cached.at < ROLLED_DAYS_CACHE_MS) {
      return cached.days;
    }
    const days = (
      this.db.query(
        `SELECT to_char(day, 'YYYY-MM-DD') AS day
           FROM site_rollup_days
          WHERE source = $1 AND cuboid = $2::smallint`,
        [source, cuboid],
      ) as Promise<Array<{ day: string }>>
    )
      .then(
        (rows) => new Set(rows.map((row) => row.day)) as ReadonlySet<string>,
      )
      .catch((error: Error) => {
        this.logger.warn(`Rolled days of ${key} unreadable: ${error.message}`);
        this.rolled.delete(key);
        throw error;
      });
    this.rolled.set(key, { at: Date.now(), days });
    return days;
  }
}
