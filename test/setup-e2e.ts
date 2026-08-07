import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { DataSource } from "typeorm";
import * as dotenv from "dotenv";
import * as path from "path";

// Load .env.test BEFORE any other modules
dotenv.config({ path: path.resolve(__dirname, "../.env.test") });

// Force NODE_ENV=test
process.env.NODE_ENV = "test";

let container: StartedPostgreSqlContainer;
let redisContainer: StartedRedisContainer;
let dataSource: DataSource;

/**
 * Global setup for E2E tests
 * - Starts TimescaleDB and Redis containers
 * - Initializes database with schema
 * - Runs before all test suites
 *
 * Redis is a container rather than a developer's local server for the same reason
 * Postgres is: without it every endpoint that reads through the cache answers 500
 * ("Stream isn't writeable and enableOfflineQueue options is false"), and the suite
 * only passes on machines that happen to run Redis on :6379.
 */
beforeAll(async () => {
  console.log("🐳 Starting Redis test container...");

  redisContainer = await new RedisContainer("redis:7-alpine").start();

  // Set before the app modules read config — dotenv above does not override these.
  process.env.REDIS_HOST = redisContainer.getHost();
  process.env.REDIS_PORT = redisContainer.getPort().toString();
  delete process.env.REDIS_PASSWORD;
  delete process.env.SKIP_REDIS;

  console.log(
    `✅ Test Redis running at ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
  );

  console.log("🐳 Starting TimescaleDB test container...");

  container = await new PostgreSqlContainer("timescale/timescaledb:latest-pg16")
    .withDatabase("parkfan_test")
    .withUsername("test_user")
    .withPassword("test_password")
    .withExposedPorts(5432)
    .start();

  // Override env vars with container connection details
  process.env.DB_HOST = container.getHost();
  process.env.DB_PORT = container.getPort().toString();
  process.env.DB_USERNAME = container.getUsername();
  process.env.DB_PASSWORD = container.getPassword();
  process.env.DB_DATABASE = container.getDatabase();

  console.log(
    `✅ Test database running at ${process.env.DB_HOST}:${process.env.DB_PORT}`,
  );

  // Create DataSource for schema initialization
  dataSource = new DataSource({
    type: "postgres",
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT, 10),
    username: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    entities: [__dirname + "/../src/**/*.entity{.ts,.js}"],
    synchronize: true, // Auto-create schema for tests
    logging: false,
  });

  await dataSource.initialize();

  // Enable TimescaleDB extension
  try {
    await dataSource.query(
      "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;",
    );
    console.log("✅ TimescaleDB extension enabled");

    // Enable pg_trgm extension (for fuzzy text search)
    await dataSource.query("CREATE EXTENSION IF NOT EXISTS pg_trgm;");
    await dataSource.query("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;");
    console.log("✅ pg_trgm extension enabled");

    // Lower the word-similarity threshold the `<%` operator uses, the same way
    // SearchService.initializeFuzzySearchIndices does at boot.
    //
    // ALTER DATABASE only reaches connections opened AFTER it runs. In production
    // that is every connection, because the setting was applied long ago. On a
    // container created seconds before the app connects it is nobody: the service
    // sets it on a pool that is already open, so `<%` keeps the 0.6 default and
    // every single-character typo ("rokburg", "orlndo", "bruhl") stops matching.
    // Setting it here, before the app starts, is what production actually looks like.
    await dataSource.query(
      `ALTER DATABASE "${container.getDatabase()}" SET pg_trgm.word_similarity_threshold = 0.4;`,
    );
    console.log("✅ pg_trgm word_similarity_threshold set to 0.4");
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.warn("⚠️  Could not enable extensions:", errorMessage);
    console.warn("⚠️  Continuing without some extensions");
  }

  // Convert time-series tables to hypertables
  try {
    await dataSource.query(`
      SELECT create_hypertable('queue_data', 'timestamp',
        chunk_time_interval => INTERVAL '1 day',
        if_not_exists => TRUE
      );
    `);
    console.log("✅ queue_data converted to hypertable");
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.warn(
      "⚠️  Could not create hypertable for queue_data:",
      errorMessage,
    );
  }

  console.log("✅ Test database schema created");
}, 120000); // 120s timeout for container startup (first run pulls image)

/**
 * Global teardown
 * - Destroys database connection
 * - Stops and removes container
 */
afterAll(async () => {
  if (dataSource?.isInitialized) {
    await dataSource.destroy();
    console.log("🧹 Database connection closed");
  }

  if (container) {
    await container.stop();
    console.log("🧹 Test container stopped");
  }

  if (redisContainer) {
    await redisContainer.stop();
    console.log("🧹 Redis container stopped");
  }
}, 60000); // 60s timeout for cleanup (two containers)

/**
 * Cleanup between test suites
 * - Truncates all tables (preserves schema)
 */
afterEach(async () => {
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
