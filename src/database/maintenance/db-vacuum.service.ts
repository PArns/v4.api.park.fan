import { Injectable, Logger } from "@nestjs/common";
import { Client } from "pg";
import { getDatabaseConfig } from "../../config/database.config";
import {
  mayStartPcnReindex,
  mayStartTftPruneBatch,
  mayStartVacuum,
  PCN_REINDEX_INDEX,
  PCN_REINDEX_LOCK_TIMEOUT,
  PCN_REINDEX_TABLE,
  pcnReindexStatementTimeoutMs,
  quoteIdent,
  TFT_FORECAST_RETENTION_DAYS,
  TFT_PRUNE_BATCH_ROWS,
  TFT_PRUNE_COUNT_TIMEOUT_MS,
  TFT_PRUNE_MAX_BATCHES,
  TFT_PRUNE_TABLES,
  tftPruneRemainingSql,
  tftPruneSql,
  tftPruneStatementTimeoutMs,
  VACUUM_LOCK_TIMEOUT,
  VACUUM_MAINTENANCE_WORK_MEM,
  VACUUM_TARGETS,
  VacuumTarget,
  vacuumStatementTimeoutMs,
} from "./vacuum-plan";

/** The slice of `pg.Client` the job uses — a seam for the spec. */
export interface VacuumClient {
  connect(): Promise<unknown>;
  query(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: Record<string, unknown>[]; rowCount?: number | null }>;
  end(): Promise<unknown>;
  /** pg.Client is an EventEmitter: a connection lost while idle is emitted, not thrown. */
  on?(event: "error", listener: (err: Error) => void): unknown;
}

export type VacuumOutcome =
  | "vacuumed"
  | "skipped_window_closed"
  | "skipped_missing"
  | "skipped_in_progress"
  | "failed";

export interface TftPruneResult {
  table: string;
  deleted: number;
  batches: number;
  /** false when the batch cap or the 04:48 UTC cut-off stopped it; the next night continues. */
  done: boolean;
  /** Expired rows still there when it stopped early; null when the count failed. */
  remaining?: number | null;
  error?: string;
}

export type ReindexOutcome =
  | "reindexed"
  | "skipped_window_closed"
  | "skipped_missing"
  | "skipped_busy"
  | "failed";

export interface ReindexResult {
  outcome: ReindexOutcome;
  durationMs: number;
  bytesBefore: number | null;
  bytesAfter: number | null;
  reason?: string;
}

/**
 * Invalid leftovers of an interrupted REINDEX CONCURRENTLY: `<index>_ccnew`
 * (the half-built copy, still maintained by every write) or `<index>_ccold`.
 */
const LEFTOVER_INDEXES = `
  SELECT c.relname AS name
    FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
   WHERE i.indrelid = to_regclass($1) AND NOT i.indisvalid
     AND starts_with(c.relname, $2 || '_cc')
`;

export interface VacuumTableResult {
  table: string;
  outcome: VacuumOutcome;
  durationMs: number;
  deadBefore: number | null;
  deadAfter: number | null;
  error?: string;
}

/**
 * Children are included so that a hypertable (whose chunks inherit from it)
 * reports its chunks' dead tuples and its chunks' running vacuums. The tables
 * on the list today are plain, where the child set is empty.
 */
const RELATION_AND_CHILDREN = `
  relid = to_regclass($1)
  OR relid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = to_regclass($1))
`;

/**
 * `VACUUM (ANALYZE)` on the high-churn tables before the 06:00 UTC CatBoost
 * training (PAR-868) — see `vacuum-plan.ts` for the window, the tables and why.
 *
 * Runs on ONE dedicated connection outside the TypeORM pool, for three reasons:
 * - VACUUM cannot run inside a transaction block. A plain `pg.Client` sends each
 *   statement on its own; nothing here ever issues BEGIN.
 * - The session settings below (`lock_timeout`, `statement_timeout`,
 *   `maintenance_work_mem`) die with the connection instead of leaking into a
 *   pooled one the API reuses.
 * - The job runs for up to an hour; it must not hold one of the API's pool slots.
 *
 * Never throws: a table that fails is logged and the next one runs.
 */
@Injectable()
export class DbVacuumService {
  private readonly logger = new Logger(DbVacuumService.name);

  /** Overridden in the spec. */
  protected createClient(): VacuumClient {
    const cfg = getDatabaseConfig();
    return new Client({
      host: cfg.host,
      port: cfg.port,
      user: cfg.username,
      password: cfg.password,
      database: cfg.database,
      // Same naming rule as the pool (docs/troubleshooting/timescaledb-lock-queues.md):
      // a lock holder in
      // pg_stat_activity must say whose session it is.
      application_name: "parkfan-db-maintenance",
    });
  }

  /** Clock seam for the spec. */
  protected now(): Date {
    return new Date();
  }

  async vacuumChurnTables(
    targets: readonly VacuumTarget[] = VACUUM_TARGETS,
  ): Promise<VacuumTableResult[]> {
    const results: VacuumTableResult[] = [];

    if (!mayStartVacuum(this.now())) {
      this.logger.warn(
        `🧽 VACUUM window closed at ${this.now().toISOString()} (tables start only 04:50–05:50 UTC) — skipping all ${targets.length} tables`,
      );
      return targets.map((t) => ({
        table: t.table,
        outcome: "skipped_window_closed",
        durationMs: 0,
        deadBefore: null,
        deadAfter: null,
      }));
    }

    const client = await this.openClient("VACUUM", VACUUM_LOCK_TIMEOUT);
    if (!client) return results;

    try {
      for (const target of targets) {
        const result = await this.vacuumOne(client, target);
        results.push(result);
      }
    } finally {
      await client.end().catch(() => undefined);
    }

    const done = results.filter((r) => r.outcome === "vacuumed").length;
    const totalMs = results.reduce((s, r) => s + r.durationMs, 0);
    this.logger.log(
      `🧽 VACUUM window done: ${done}/${targets.length} tables vacuumed in ${Math.round(totalMs / 1000)} s` +
        ` (${results
          .filter((r) => r.outcome !== "vacuumed")
          .map((r) => `${r.table}: ${r.outcome}`)
          .join(", ")})`,
    );
    return results;
  }

  /**
   * The session every maintenance step runs on: own connection, `lock_timeout`
   * and the 256 MB work memory (a REINDEX sorts in `maintenance_work_mem` too).
   * Null when it cannot be opened — logged, never thrown.
   */
  private async openClient(
    what: string,
    lockTimeout: string,
  ): Promise<VacuumClient | null> {
    try {
      const client = this.createClient();
      // A dropped connection between statements (Postgres restart, terminated backend)
      // arrives as an 'error' event; unhandled, Node would crash the whole API process.
      client.on?.("error", (err) =>
        this.logger.error(`🧽 ${what} connection error: ${describe(err)}`),
      );
      await client.connect();
      await client.query(`SET lock_timeout = '${lockTimeout}'`);
      await client.query(
        `SET maintenance_work_mem = '${VACUUM_MAINTENANCE_WORK_MEM}'`,
      );
      return client;
    } catch (err) {
      this.logger.error(
        `🧽 ${what} job could not open its connection: ${describe(err)}`,
      );
      return null;
    }
  }

  /**
   * Batched retention DELETE on the TFT forecast tables by `forecast_date`
   * (PAR-1054, the PAR-873 pattern): each batch is its own autocommitted
   * statement of at most {@link TFT_PRUNE_BATCH_ROWS} rows; a table stops at
   * its first short batch, when the run has used {@link TFT_PRUNE_MAX_BATCHES}
   * (one budget for all tables, not one each), or when the
   * clock leaves 04:35–04:48 UTC (each batch's timeout capped to 04:50, when
   * the VACUUM starts) — a stopped run logs the rows left and the next night
   * continues. A missing
   * table (the shadow before its first write) is skipped. Never throws.
   */
  async pruneTftForecasts(
    tables: readonly string[] = TFT_PRUNE_TABLES,
    retentionDays: number = TFT_FORECAST_RETENTION_DAYS,
    batchRows: number = TFT_PRUNE_BATCH_ROWS,
    maxBatches: number = TFT_PRUNE_MAX_BATCHES,
  ): Promise<TftPruneResult[]> {
    const results: TftPruneResult[] = [];
    let used = 0; // batches of the whole run: the tables share one budget
    const client = await this.openClient("TFT prune", VACUUM_LOCK_TIMEOUT);
    if (!client) return results;
    try {
      for (const table of tables) {
        const r: TftPruneResult = {
          table,
          deleted: 0,
          batches: 0,
          done: false,
        };
        results.push(r);
        try {
          const exists = await client.query(
            "SELECT to_regclass($1) IS NOT NULL AS present",
            [table],
          );
          if (!exists.rows[0]?.present) {
            this.logger.debug(`🗑️  ${table}: not there yet, skipped`);
            r.done = true;
            continue;
          }
          const sql = tftPruneSql(table);
          while (used < maxBatches) {
            const now = this.now();
            if (!mayStartTftPruneBatch(now)) {
              this.logger.warn(
                `🗑️  ${table}: no new batch at ${now.toISOString()} (batches start only 04:35–04:48 UTC)`,
              );
              break;
            }
            await client.query(
              `SET statement_timeout = ${tftPruneStatementTimeoutMs(now)}`,
            );
            const res = await client.query(sql, [retentionDays, batchRows]);
            const n = Number(res.rowCount ?? 0);
            r.deleted += n;
            r.batches++;
            used++;
            if (n < batchRows) {
              r.done = true;
              break;
            }
          }
        } catch (err) {
          r.error = describe(err);
          this.logger.error(`🗑️  ${table}: prune failed: ${r.error}`);
        }
        // Counted only after a run that worked: a late or stopped-before-start run must not seq-scan inside 05:50–07:30.
        if (!r.done && r.batches > 0)
          r.remaining = await this.countExpired(client, table, retentionDays);
      }
    } catch (err) {
      this.logger.error(`🗑️  TFT prune aborted: ${describe(err)}`);
    } finally {
      await client.end().catch(() => undefined);
    }
    this.logger.log(
      `🗑️  TFT forecasts older than ${retentionDays} days: ` +
        results
          .map(
            (r) =>
              `${r.table} ${r.deleted.toLocaleString("en-US")} in ${r.batches} batch(es)` +
              (r.done
                ? ""
                : ` (not finished, ${r.remaining == null ? "unknown" : r.remaining.toLocaleString("en-US")} rows left, continuing next run)`),
          )
          .join(", "),
    );
    return results;
  }

  /** Expired rows left in `table`; null on any failure (only logged, never thrown). */
  private async countExpired(
    client: VacuumClient,
    table: string,
    retentionDays: number,
  ): Promise<number | null> {
    try {
      await client.query(
        `SET statement_timeout = ${TFT_PRUNE_COUNT_TIMEOUT_MS}`,
      );
      const res = await client.query(tftPruneRemainingSql(table), [
        retentionDays,
      ]);
      return Number(res.rows[0]?.n ?? 0);
    } catch (err) {
      this.logger.warn(
        `🗑️  ${table}: could not count the rows left: ${describe(err)}`,
      );
      return null;
    }
  }

  /**
   * Weekly `REINDEX INDEX CONCURRENTLY pcn_forecasts_pkey` (PAR-1054): the
   * rolling prune leaves the PK at twice its packed size (10.1 GB against a
   * 5.2 GB heap on 2026-10-24) — VACUUM marks pages reusable but never shrinks
   * an index. CONCURRENTLY works because the table is a plain one (not in
   * `HYPERTABLES`); it takes SHARE UPDATE EXCLUSIVE, never ACCESS EXCLUSIVE,
   * so reads and writes go on (docs/troubleshooting/timescaledb-lock-queues.md).
   *
   * Skipped when another index build is running anywhere, when the table is
   * being vacuumed, or when another maintenance session is active. An
   * interrupted earlier run's invalid `_ccnew`/`_ccold` copy is dropped first,
   * and again after a failure, because every write keeps maintaining it.
   * Never throws.
   */
  async reindexPcnForecastsPkey(): Promise<ReindexResult> {
    const base = { durationMs: 0, bytesBefore: null, bytesAfter: null };
    const now = this.now();
    if (!mayStartPcnReindex(now)) {
      this.logger.warn(
        `🧽 REINDEX ${PCN_REINDEX_INDEX}: not started at ${now.toISOString()} (only Sunday 19:10–19:30 UTC)`,
      );
      return { ...base, outcome: "skipped_window_closed" };
    }
    const client = await this.openClient("REINDEX", PCN_REINDEX_LOCK_TIMEOUT);
    if (!client) {
      return { ...base, outcome: "failed", reason: "no connection" };
    }

    try {
      const exists = await client.query(
        "SELECT to_regclass($1) IS NOT NULL AS present",
        [PCN_REINDEX_INDEX],
      );
      if (!exists.rows[0]?.present) {
        return { ...base, outcome: "skipped_missing" };
      }

      const busy = await this.reindexBlocker(client);
      if (busy) {
        this.logger.log(`🧽 REINDEX ${PCN_REINDEX_INDEX}: skipped — ${busy}`);
        return { ...base, outcome: "skipped_busy", reason: busy };
      }

      const timeoutMs = pcnReindexStatementTimeoutMs(now);
      await client.query(`SET statement_timeout = ${timeoutMs}`);
      await this.dropLeftoverIndexes(client);
      const bytesBefore = await this.indexBytes(client);
      const started = Date.now();
      try {
        await client.query(
          `REINDEX INDEX CONCURRENTLY ${quoteIdent(PCN_REINDEX_INDEX)}`,
        );
      } catch (err) {
        const durationMs = Date.now() - started;
        this.logger.error(
          `🧽 REINDEX ${PCN_REINDEX_INDEX} failed after ${Math.round(durationMs / 1000)} s (timeout ${Math.round(timeoutMs / 1000)} s): ${describe(err)}`,
        );
        await this.dropLeftoverIndexes(client).catch((e: unknown) =>
          this.logger.error(
            `🧽 could not drop the leftover index, next run retries: ${describe(e)}`,
          ),
        );
        return {
          ...base,
          outcome: "failed",
          durationMs,
          bytesBefore,
          reason: describe(err),
        };
      }
      const durationMs = Date.now() - started;
      const bytesAfter = await this.indexBytes(client);
      this.logger.log(
        `🧽 REINDEX ${PCN_REINDEX_INDEX} in ${Math.round(durationMs / 1000)} s — ${fmtBytes(bytesBefore)} → ${fmtBytes(bytesAfter)}`,
      );
      return { outcome: "reindexed", durationMs, bytesBefore, bytesAfter };
    } catch (err) {
      this.logger.error(
        `🧽 REINDEX ${PCN_REINDEX_INDEX}: skipped after error: ${describe(err)}`,
      );
      return { ...base, outcome: "failed", reason: describe(err) };
    } finally {
      await client.end().catch(() => undefined);
    }
  }

  /** Why the REINDEX must not start now, or null. */
  private async reindexBlocker(client: VacuumClient): Promise<string | null> {
    const building = await client.query(
      "SELECT pid, relid::regclass::text AS rel FROM pg_stat_progress_create_index LIMIT 1",
    );
    if (building.rows.length > 0) {
      return `an index build is running (pid ${String(building.rows[0].pid)} on ${String(building.rows[0].rel)})`;
    }
    const vacuum = await client.query(
      `SELECT pid FROM pg_stat_progress_vacuum WHERE ${RELATION_AND_CHILDREN} LIMIT 1`,
      [PCN_REINDEX_TABLE],
    );
    if (vacuum.rows.length > 0) {
      return `${PCN_REINDEX_TABLE} is being vacuumed (pid ${String(vacuum.rows[0].pid)})`;
    }
    const other = await client.query(
      `SELECT pid FROM pg_stat_activity
        WHERE application_name = 'parkfan-db-maintenance'
          AND pid <> pg_backend_pid() AND state <> 'idle' LIMIT 1`,
    );
    if (other.rows.length > 0) {
      return `another maintenance session is active (pid ${String(other.rows[0].pid)})`;
    }
    return null;
  }

  private async dropLeftoverIndexes(client: VacuumClient): Promise<void> {
    const left = await client.query(LEFTOVER_INDEXES, [
      PCN_REINDEX_TABLE,
      PCN_REINDEX_INDEX,
    ]);
    for (const row of left.rows) {
      const name = String(row.name);
      this.logger.warn(`🧽 dropping invalid leftover index ${name}`);
      await client.query(
        `DROP INDEX CONCURRENTLY IF EXISTS ${quoteIdent(name)}`,
      );
    }
  }

  private async indexBytes(client: VacuumClient): Promise<number | null> {
    const res = await client.query(
      "SELECT pg_relation_size(to_regclass($1))::bigint AS bytes",
      [PCN_REINDEX_INDEX],
    );
    const b = res.rows[0]?.bytes;
    return b === undefined || b === null ? null : Number(b);
  }

  private async vacuumOne(
    client: VacuumClient,
    target: VacuumTarget,
  ): Promise<VacuumTableResult> {
    const { table } = target;
    const base = { table, durationMs: 0, deadBefore: null, deadAfter: null };

    const now = this.now();
    if (!mayStartVacuum(now)) {
      this.logger.warn(
        `🧽 ${table}: not started — it is ${now.toISOString()}, past the 05:50 UTC last start`,
      );
      return { ...base, outcome: "skipped_window_closed" };
    }

    try {
      const exists = await client.query(
        "SELECT to_regclass($1) IS NOT NULL AS present",
        [table],
      );
      if (!exists.rows[0]?.present) {
        this.logger.warn(`🧽 ${table}: no such table — skipped`);
        return { ...base, outcome: "skipped_missing" };
      }

      const running = await client.query(
        `SELECT pid FROM pg_stat_progress_vacuum WHERE ${RELATION_AND_CHILDREN} LIMIT 1`,
        [table],
      );
      if (running.rows.length > 0) {
        this.logger.log(
          `🧽 ${table}: a vacuum is already running (pid ${String(running.rows[0].pid)}) — skipped`,
        );
        return { ...base, outcome: "skipped_in_progress" };
      }

      const deadBefore = await this.deadTuples(client, table);
      const timeoutMs = vacuumStatementTimeoutMs(now, target.maxStatementMs);
      await client.query(`SET statement_timeout = ${timeoutMs}`);

      const started = Date.now();
      try {
        await client.query(`VACUUM (ANALYZE) ${quoteIdent(table)}`);
      } catch (err) {
        const durationMs = Date.now() - started;
        this.logger.error(
          `🧽 ${table}: VACUUM failed after ${Math.round(durationMs / 1000)} s (timeout ${Math.round(timeoutMs / 1000)} s): ${describe(err)}`,
        );
        return {
          ...base,
          outcome: "failed",
          durationMs,
          deadBefore,
          error: describe(err),
        };
      } finally {
        await client.query("RESET statement_timeout").catch(() => undefined);
      }
      const durationMs = Date.now() - started;

      const deadAfter = await this.deadTuples(client, table);
      this.logger.log(
        `🧽 ${table}: VACUUM (ANALYZE) in ${Math.round(durationMs / 1000)} s — dead tuples ${fmt(deadBefore)} → ${fmt(deadAfter)}`,
      );
      return {
        ...base,
        outcome: "vacuumed",
        durationMs,
        deadBefore,
        deadAfter,
      };
    } catch (err) {
      this.logger.error(`🧽 ${table}: skipped after error: ${describe(err)}`);
      return { ...base, outcome: "failed", error: describe(err) };
    }
  }

  private async deadTuples(
    client: VacuumClient,
    table: string,
  ): Promise<number | null> {
    const res = await client.query(
      `SELECT COALESCE(SUM(n_dead_tup), 0)::bigint AS dead FROM pg_stat_all_tables WHERE ${RELATION_AND_CHILDREN}`,
      [table],
    );
    const dead = res.rows[0]?.dead;
    return dead === undefined || dead === null ? null : Number(dead);
  }
}

function describe(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function fmtBytes(n: number | null): string {
  return n === null ? "?" : `${(n / 1024 ** 3).toFixed(2)} GB`;
}

function fmt(n: number | null): string {
  return n === null ? "?" : n.toLocaleString("en-US");
}
