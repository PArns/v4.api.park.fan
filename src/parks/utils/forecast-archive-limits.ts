import { EntityManager } from "typeorm";

/**
 * Runs `fn` in a transaction whose statements carry their own deadline and
 * lock deadline (`SET LOCAL`, so nothing leaks onto the pooled connection).
 *
 * The forward archive's reads touch `queue_data`, `attraction_hourly_history`
 * and its own tables from a background job; a job query with no deadline is
 * how the 2026-09-28 and 2026-10-09 lock queues formed
 * (db-health-runbook §0, §0b). A timed-out statement raises 57014 / 55P03,
 * which the caller treats as "try again tomorrow", never as data.
 *
 * Shaped to be swapped for `src/common/utils/statement-limits.util.ts` once
 * PR #442 lands: the `limits` argument is that file's `StatementLimits` (its
 * two required fields), and every call site goes through this one function.
 */
export async function withStatementLimits<T>(
  manager: EntityManager,
  limits: { statementTimeoutMs: number; lockTimeoutMs: number },
  fn: (em: EntityManager) => Promise<T>,
): Promise<T> {
  return manager.transaction(async (em) => {
    await em.query(
      `SELECT set_config('statement_timeout', $1, true),
              set_config('lock_timeout', $2, true)`,
      [
        `${Math.max(1, Math.round(limits.statementTimeoutMs))}ms`,
        `${Math.max(1, Math.round(limits.lockTimeoutMs))}ms`,
      ],
    );
    return fn(em);
  });
}

/** `YYYY-MM-DD` plus n calendar days, without touching a timezone. */
export function addIsoDays(isoDate: string, days: number): string {
  const d = new Date(`${isoDate}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}
