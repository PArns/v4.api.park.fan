import { DataSource } from "typeorm";
import Redis from "ioredis";
import * as dotenv from "dotenv";
import * as path from "path";

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

  redis = new Redis({
    host: process.env.REDIS_HOST,
    port: parseInt(process.env.REDIS_PORT as string, 10),
    maxRetriesPerRequest: 3,
    enableOfflineQueue: false,
  });
}, 120000);

afterAll(async () => {
  if (dataSource?.isInitialized) {
    await dataSource.destroy();
  }

  if (redis) {
    if (redis.status === "ready") {
      await redis.quit().catch(() => undefined);
    }
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
 * run means one file's leftovers are the next file's starting state — and the
 * worst of those leftovers are not cache keys but Bull's REPEATABLE jobs:
 * `QueueSchedulerService` registers `wait-times-cron` on a five-minute cron
 * five seconds after an AppModule spec boots, and a surviving registration lets
 * `WaitTimesProcessor` fire in the middle of a later, unrelated test.
 */
afterEach(async () => {
  if (redis?.status === "ready") {
    await redis.flushall();
  }

  if (dataSource?.isInitialized) {
    const entities = dataSource.entityMetadatas;

    // Disable foreign key checks temporarily for faster truncation
    await dataSource.query("SET session_replication_role = replica;");

    for (const entity of entities) {
      const tableName = entity.tableName;
      try {
        await dataSource.query(`TRUNCATE TABLE "${tableName}" CASCADE;`);
      } catch (error) {
        // Ignore errors for tables that don't exist
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        console.warn(`Warning: Could not truncate ${tableName}:`, errorMessage);
      }
    }

    // Re-enable foreign key checks
    await dataSource.query("SET session_replication_role = DEFAULT;");
  }
});
