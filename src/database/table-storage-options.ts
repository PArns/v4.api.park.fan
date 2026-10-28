import type { Logger } from "@nestjs/common";

/** What `ensureTableStorageOptions` needs from a repository or a DataSource. */
interface Queryable {
  query(sql: string, params?: unknown[]): Promise<unknown>;
}

const TABLE_NAME = /^[a-z_][a-z0-9_]*$/;
const STORAGE_OPTION = /^[a-z_]+=[0-9.]+$/;

/**
 * Writes storage settings `synchronize` cannot express (`fillfactor`, the
 * autovacuum thresholds) onto `table`, once, when `pg_class.reloptions` lacks
 * any of them. `ALTER TABLE … SET` takes a lock that blocks neither reads nor
 * writes, and a fill factor only shapes pages written from then on. A failure
 * is logged, not thrown: the table works without them.
 *
 * `options` are spelled as `pg_class.reloptions` spells them (`fillfactor=80`),
 * so the comparison needs no parsing.
 */
export async function ensureTableStorageOptions(
  db: Queryable,
  table: string,
  options: readonly string[],
  logger: Pick<Logger, "log" | "warn">,
): Promise<void> {
  if (!TABLE_NAME.test(table) || !options.every((o) => STORAGE_OPTION.test(o)))
    throw new Error(`Refusing storage options ${options} for ${table}`);
  try {
    const rows = (await db.query(
      `SELECT reloptions FROM pg_class WHERE oid = '${table}'::regclass`,
    )) as Array<{ reloptions: string[] | null }>;
    const current = rows[0]?.reloptions ?? [];
    if (options.every((o) => current.includes(o))) return;
    await db.query(`ALTER TABLE ${table} SET (${options.join(", ")})`);
    logger.log(`${table}: ${options.join(", ")}`);
  } catch (error) {
    logger.warn(
      `Could not set ${table} storage options: ${(error as Error).message}`,
    );
  }
}
