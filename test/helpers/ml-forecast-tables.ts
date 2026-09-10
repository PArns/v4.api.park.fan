import { DataSource } from "typeorm";
import * as fs from "fs";
import * as path from "path";

/**
 * The merge tables that no TypeORM entity owns, and where their DDL lives.
 *
 * `ATTRACTION_DEPENDENCIES`, `PARK_DEPENDENCIES`, `PARK_INLINE_DEPENDENCIES`
 * and `PARK_TABLES_HANDLED_INLINE` name 34 tables between them. Thirty are
 * entities, so `synchronize: true` in `global-setup.ts` creates them. These
 * four are created at runtime by the service that writes them — three Python
 * sub-services and one NestJS processor — and the E2E schema therefore never
 * had them. `applyMergeDependencies` walks its list straight through, so the
 * first one it reaches aborts the whole merge with 42P01:
 *
 *     QueryFailedError: relation "pcn_forecasts" does not exist
 *       at applyMergeDependencies (src/parks/utils/merge-dependencies.ts:392)
 *
 * That is why the merge path had no E2E coverage at all — not oversight, a
 * wall anyone writing one hits on the first run.
 *
 * The DDL is READ FROM THOSE SOURCES rather than copied here. A copy is a
 * second definition of a schema this repo does not own the migration for, and
 * it would drift the first time a sub-service adds a column: the suite would
 * keep passing against a table production no longer has. Reading the statement
 * the writer itself issues cannot drift — if the source changes, the test
 * schema changes with it, and if the statement moves or is renamed the
 * extraction finds nothing and fails loudly (see `extractCreateTable`).
 */
const ML_FORECAST_TABLE_SOURCES: ReadonlyArray<{
  table: string;
  source: string;
}> = [
  { table: "pcn_forecasts", source: "pcn-service/db.py" },
  { table: "shape_forecasts", source: "shape-service/db.py" },
  { table: "tft_forecasts", source: "nf-service/db.py" },
  {
    table: "catboost_daily_forecasts",
    source: "src/queues/processors/nf-forecast.processor.ts",
  },
];

/** Table names only — `setup-e2e.ts` truncates these alongside the entities. */
export const ML_FORECAST_TABLES: ReadonlyArray<string> =
  ML_FORECAST_TABLE_SOURCES.map((t) => t.table);

const REPO_ROOT = path.resolve(__dirname, "../..");

/**
 * Pulls one `CREATE TABLE IF NOT EXISTS <table> (...)` statement out of a
 * source file, whatever language holds it.
 *
 * The SQL sits verbatim inside a Python `text("""…""")` block or a TypeScript
 * template literal, so a plain text scan reads both. Parentheses are matched by
 * counting, because the column list contains its own — `varchar(16)`,
 * `(now() AT TIME ZONE 'UTC')::date`. Line comments are stripped first: they
 * are legal in the extracted SQL, but an unbalanced parenthesis inside one
 * would throw the count off.
 *
 * Throws rather than returning undefined. A silently missing table would come
 * back as a 42P01 inside the merge transaction, which reads like a bug in the
 * merge rather than a stale path in this file.
 */
function extractCreateTable(table: string, source: string): string {
  const file = path.join(REPO_ROOT, source);
  if (!fs.existsSync(file)) {
    throw new Error(
      `E2E schema: ${source} is gone, so the DDL for "${table}" cannot be read. ` +
        `Point ML_FORECAST_TABLE_SOURCES at wherever the CREATE TABLE moved.`,
    );
  }

  const withoutLineComments = fs
    .readFileSync(file, "utf8")
    .replace(/--[^\n]*/g, "");

  const header = new RegExp(
    `CREATE\\s+TABLE\\s+IF\\s+NOT\\s+EXISTS\\s+${table}\\s*\\(`,
    "i",
  ).exec(withoutLineComments);
  if (!header) {
    throw new Error(
      `E2E schema: no "CREATE TABLE IF NOT EXISTS ${table}" in ${source}. ` +
        `The writer's DDL is the one the test schema mirrors — find where it went.`,
    );
  }

  const open = header.index + header[0].length - 1;
  let depth = 0;
  for (let i = open; i < withoutLineComments.length; i++) {
    const char = withoutLineComments[i];
    if (char === "(") depth++;
    else if (char === ")") {
      depth--;
      if (depth === 0) {
        return `CREATE TABLE IF NOT EXISTS ${table} ${withoutLineComments.slice(open, i + 1)}`;
      }
    }
  }

  throw new Error(
    `E2E schema: the column list of "${table}" in ${source} never closes.`,
  );
}

/**
 * Creates the four entity-less merge tables on an already-migrated test schema.
 *
 * Called from `global-setup.ts` after `synchronize`, so the tables exist before
 * any suite connects — the same place the hypertable and the extensions are set
 * up, and deliberately not inside a single spec: a spec that creates its own
 * schema is a spec nothing else can rely on, and the next merge test would
 * either duplicate this or silently run against four missing tables.
 *
 * No foreign keys, matching production: these tables are written by services
 * that do not share this schema's entity graph, which is exactly why
 * `ATTRACTION_DEPENDENCIES` calls them "no FK, so silent orphans if forgotten".
 * Adding one here would make the test schema stricter than production and turn
 * a forgotten dependency into a loud 23503 instead of the quiet orphan the
 * merge code is written to prevent.
 */
export async function createMlForecastTables(
  dataSource: DataSource,
): Promise<void> {
  for (const { table, source } of ML_FORECAST_TABLE_SOURCES) {
    await dataSource.query(extractCreateTable(table, source));
  }
}
