import { DataSource } from "typeorm";
import Redis from "ioredis";
import * as dotenv from "dotenv";
import * as path from "path";
import { ML_FORECAST_TABLES } from "./helpers/ml-forecast-tables";

// Load .env.test BEFORE any other modules
dotenv.config({ path: path.resolve(__dirname, "../.env.test") });

// Force NODE_ENV=test
process.env.NODE_ENV = "test";

let dataSource: DataSource;
let redis: Redis;

/**
 * Per-file E2E setup.
 *
 * The containers are NOT started here — `global-setup.ts` starts one pair for
 * the whole run and puts their connection details in the environment. This file
 * only opens a connection to what is already running, because the truncation
 * below needs one and every test file gets a fresh module registry.
 *
 * `synchronize` is off: global setup created the schema, and a second migrator
 * per file would race the app's own `synchronize: true` connections.
 */
beforeAll(async () => {
  dataSource = new DataSource({
    type: "postgres",
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT as string, 10),
    username: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    entities: [__dirname + "/../src/**/*.entity{.ts,.js}"],
    synchronize: false,
    logging: false,
  });

  await dataSource.initialize();

  // `lazyConnect` + an awaited `connect()`, so that a Redis this file cannot
  // reach fails the run here. Constructing eagerly and gating the cleanup on
  // `status === "ready"` would let a client that never connects turn the
  // between-tests flush into a silent no-op — and ioredis swallows connection
  // errors while nothing listens for them.
  redis = new Redis({
    host: process.env.REDIS_HOST,
    port: parseInt(process.env.REDIS_PORT as string, 10),
    maxRetriesPerRequest: 3,
    enableOfflineQueue: false,
    lazyConnect: true,
  });

  await redis.connect();
}, 120000);

afterAll(async () => {
  if (dataSource?.isInitialized) {
    await dataSource.destroy();
  }

  if (redis) {
    await redis.quit().catch(() => undefined);
    redis.disconnect();
  }
}, 60000);

/**
 * Cleanup between tests
 * - Truncates all tables (preserves schema)
 * - Empties Redis
 *
 * Redis has to be emptied for the same reason the tables do, and it did not
 * have to be while every file had its own container. One Redis for the whole
 * run means one file's leftovers are the next file's starting state: cached
 * responses, admin sessions, popularity counters.
 *
 * It is deliberately not the last line of defence against a leftover Bull
 * REPEATABLE job — one written between the final flush and `app.close()` would
 * slip past this. `SKIP_QUEUE_BOOTSTRAP=true` in `.env.test` is what keeps
 * those from being written at all.
 */
afterEach(async () => {
  // `finally`, so a Redis that refuses to flush does not also cost the run its
  // table cleanup: this hook is the only isolation left between two test FILES,
  // and returning early from it hands the next file this file's rows.
  try {
    if (redis) {
      await redis.flushall();
    }
  } finally {
    await truncateAllTables();
  }
});

async function truncateAllTables(): Promise<void> {
  if (!dataSource?.isInitialized) {
    return;
  }

  // `entityMetadatas` is every table TypeORM knows, which is every table the
  // schema had until the merge tables that no entity owns were added to it.
  // Those four are not in the metadata and would therefore never be emptied —
  // one spec's forecast rows would be the next spec's starting state, and the
  // merge assertions here count rows.
  const tableNames = [
    ...dataSource.entityMetadatas.map((entity) => entity.tableName),
    ...ML_FORECAST_TABLES,
  ];

  // Disable foreign key checks temporarily for faster truncation
  await dataSource.query("SET session_replication_role = replica;");

  try {
    for (const tableName of tableNames) {
      // A failure here used to be a `console.warn` about tables that might not
      // exist. Global setup creates all of these — the entities from this same
      // metadata, the four others from their writers' own DDL — so every table
      // does exist and a failure means something else — a lock lost
      // to a background query, say. Swallowing it was survivable while each
      // file had its own database; with one shared database it silently seeds
      // every later file, so it fails the test that caused it instead.
      await dataSource.query(`TRUNCATE TABLE "${tableName}" CASCADE;`);
    }
  } finally {
    // Re-enable foreign key checks
    await dataSource.query("SET session_replication_role = DEFAULT;");
  }
}
