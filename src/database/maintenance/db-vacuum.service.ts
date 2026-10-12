import { Injectable, Logger } from "@nestjs/common";
import { Client } from "pg";
import { getDatabaseConfig } from "../../config/database.config";
import {
  mayStartVacuum,
  quoteIdent,
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
  ): Promise<{ rows: Record<string, unknown>[] }>;
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
      // Same naming rule as the pool (runbook §0b): a lock holder in
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

    let client: VacuumClient;
    try {
      client = this.createClient();
      // A dropped connection between statements (Postgres restart, terminated backend)
      // arrives as an 'error' event; unhandled, Node would crash the whole API process.
      client.on?.("error", (err) =>
        this.logger.error(`🧽 VACUUM connection error: ${describe(err)}`),
      );
      await client.connect();
      await client.query(`SET lock_timeout = '${VACUUM_LOCK_TIMEOUT}'`);
      await client.query(
        `SET maintenance_work_mem = '${VACUUM_MAINTENANCE_WORK_MEM}'`,
      );
    } catch (err) {
      this.logger.error(
        `🧽 VACUUM job could not open its connection: ${describe(err)}`,
      );
      return results;
    }

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

function fmt(n: number | null): string {
  return n === null ? "?" : n.toLocaleString("en-US");
}
