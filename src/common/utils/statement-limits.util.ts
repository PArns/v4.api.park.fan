import { DataSource, EntityManager } from "typeorm";

/**
 * Deadlines (and, where a statement needs them, planner settings) for ONE
 * transaction, applied with `SET LOCAL` so they end with it.
 *
 * The application pool has no `statement_timeout` of its own, so a background
 * statement that goes wrong runs until it finishes — and the server keeps
 * running it after the Node process that sent it has died, because Postgres
 * only notices a closed client when it next writes to it. PAR-820: the nightly
 * downtime reconstruction hung for days on one statement whose plan had
 * collapsed, blocking the queue's only slot and being killed only by deploys.
 *
 * `statement_timeout` is enforced by the server, so it also bounds the
 * orphaned case. `lock_timeout` keeps the statement from queueing indefinitely
 * behind an ACCESS EXCLUSIVE request — and, more to the point on a hypertable,
 * from becoming the head of a queue everyone else then waits behind (the
 * 2026-09-28 compression-policy outage, PAR-563).
 */
export interface StatementLimits {
  /** Per statement, server-enforced. */
  statementTimeoutMs: number;
  /** How long any one lock request may wait. */
  lockTimeoutMs: number;
  /** Ends the session if the transaction sits idle between statements. */
  idleInTransactionTimeoutMs?: number;
  /** Extra `SET LOCAL name = value` planner settings, e.g. `enable_nestloop`. */
  planner?: Readonly<Record<string, string>>;
}

const GUC_NAME = /^[a-z_][a-z0-9_.]*$/;
const GUC_VALUE = /^[A-Za-z0-9_.-]+$/;

/**
 * The `SET LOCAL` statements for a set of limits, in the order they are issued.
 *
 * Built from numbers and a fixed allowlist of characters, never from input, and
 * validated anyway: `SET` takes no bind parameters, so this is the one place
 * text is spliced into a statement.
 */
export function statementLimitSql(limits: StatementLimits): string[] {
  const ms = (value: number, name: string): number => {
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error(`${name} must be a positive number of ms, got ${value}`);
    }
    return Math.round(value);
  };

  const out = [
    `SET LOCAL statement_timeout = ${ms(limits.statementTimeoutMs, "statementTimeoutMs")}`,
    `SET LOCAL lock_timeout = ${ms(limits.lockTimeoutMs, "lockTimeoutMs")}`,
  ];
  if (limits.idleInTransactionTimeoutMs !== undefined) {
    out.push(
      `SET LOCAL idle_in_transaction_session_timeout = ${ms(
        limits.idleInTransactionTimeoutMs,
        "idleInTransactionTimeoutMs",
      )}`,
    );
  }
  for (const [name, value] of Object.entries(limits.planner ?? {})) {
    if (!GUC_NAME.test(name) || !GUC_VALUE.test(value)) {
      throw new Error(`Refusing planner setting ${name} = ${value}`);
    }
    out.push(`SET LOCAL ${name} = ${value}`);
  }
  return out;
}

/** Apply limits at the start of a transaction the caller already holds. */
export async function applyStatementLimits(
  manager: EntityManager,
  limits: StatementLimits,
): Promise<void> {
  for (const sql of statementLimitSql(limits)) await manager.query(sql);
}

/**
 * Run one read under its own limits, in a transaction of its own.
 *
 * The transaction exists only to scope `SET LOCAL`: a single statement under
 * READ COMMITTED takes the same snapshot either way, and the settings go back
 * to the pool's defaults when it commits — a plain `SET` would leak into
 * whichever request borrows the connection next.
 */
export async function queryWithLimits<T>(
  dataSource: DataSource,
  sql: string,
  params: unknown[],
  limits: StatementLimits,
): Promise<T> {
  return dataSource.transaction(async (manager) => {
    await applyStatementLimits(manager, limits);
    return (await manager.query(sql, params)) as T;
  });
}
