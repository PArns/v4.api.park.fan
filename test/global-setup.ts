import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { DataSource } from "typeorm";
import * as dotenv from "dotenv";
import * as path from "path";
import { createMlForecastTables } from "./helpers/ml-forecast-tables";

/**
 * One TimescaleDB and one Redis for the whole E2E run.
 *
 * This used to live in `setupFilesAfterEnv`, which Jest evaluates once per test
 * FILE — nine files meant nine TimescaleDB containers and nine Redis containers,
 * started and stopped in sequence, for a suite whose assertions take a fraction
 * of that. `globalSetup` runs once for the run instead.
 *
 * The schema is created here too, so the containers a suite connects to are
 * already migrated, extensions and hypertable included. Per-file isolation is
 * unaffected: `setup-e2e.ts` still truncates every table and empties Redis
 * after each test.
 *
 * The started containers are parked on `globalThis` because `globalTeardown` is
 * a separate module and cannot see this file's bindings.
 */

export interface E2eContainers {
  postgres: StartedPostgreSqlContainer;
  redis: StartedRedisContainer;
}

export const E2E_CONTAINERS = Symbol.for("parkfan.e2e.containers");

export default async function globalSetup(): Promise<void> {
  // Load .env.test BEFORE any other module reads config
  dotenv.config({ path: path.resolve(__dirname, "../.env.test") });
  process.env.NODE_ENV = "test";

  let redis: StartedRedisContainer | undefined;
  let postgres: StartedPostgreSqlContainer | undefined;

  try {
    console.log("🐳 Starting Redis test container...");
    redis = await new RedisContainer("redis:7-alpine").start();

    // Set before the app modules read config — dotenv above does not override these.
    process.env.REDIS_HOST = redis.getHost();
    process.env.REDIS_PORT = redis.getPort().toString();
    delete process.env.REDIS_PASSWORD;
    delete process.env.SKIP_REDIS;

    console.log(
      `✅ Test Redis running at ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
    );

    console.log("🐳 Starting TimescaleDB test container...");
    postgres = await new PostgreSqlContainer(
      "timescale/timescaledb:latest-pg16",
    )
      .withDatabase("parkfan_test")
      .withUsername("test_user")
      .withPassword("test_password")
      .withExposedPorts(5432)
      .start();

    process.env.DB_HOST = postgres.getHost();
    process.env.DB_PORT = postgres.getPort().toString();
    process.env.DB_USERNAME = postgres.getUsername();
    process.env.DB_PASSWORD = postgres.getPassword();
    process.env.DB_DATABASE = postgres.getDatabase();

    console.log(
      `✅ Test database running at ${process.env.DB_HOST}:${process.env.DB_PORT}`,
    );

    await createSchema(postgres.getDatabase());
  } catch (error) {
    // Jest does not run globalTeardown when globalSetup throws, so whatever is
    // already up has to be taken down here or it outlives the run — with no
    // code path left that knows about it.
    await postgres?.stop().catch(() => undefined);
    await redis?.stop().catch(() => undefined);
    throw error;
  }

  (globalThis as Record<symbol, unknown>)[E2E_CONTAINERS] = {
    postgres,
    redis,
  } satisfies E2eContainers;
}

async function createSchema(database: string): Promise<void> {
  const dataSource = new DataSource({
    type: "postgres",
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT as string, 10),
    username: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    entities: [__dirname + "/../src/**/*.entity{.ts,.js}"],
    synchronize: true, // Auto-create schema for tests
    logging: false,
  });

  // Inside the `try`, not before it: `synchronize: true` does its schema work
  // during `initialize()`, and a failure there leaves a connected pool that
  // nothing else would ever close.
  try {
    await dataSource.initialize();

    try {
      await dataSource.query(
        "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;",
      );
      console.log("✅ TimescaleDB extension enabled");

      // pg_trgm + fuzzystrmatch for the fuzzy search suite
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
      // Setting it here, before any app starts, is what production actually looks like.
      await dataSource.query(
        `ALTER DATABASE "${database}" SET pg_trgm.word_similarity_threshold = 0.4;`,
      );
      console.log("✅ pg_trgm word_similarity_threshold set to 0.4");
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.warn("⚠️  Could not enable extensions:", errorMessage);
      console.warn("⚠️  Continuing without some extensions");
    }

    try {
      await dataSource.query(`
        SELECT create_hypertable('queue_data', 'timestamp',
          chunk_time_interval => INTERVAL '1 day',
          if_not_exists => TRUE
        );
      `);
      console.log("✅ queue_data converted to hypertable");
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.warn(
        "⚠️  Could not create hypertable for queue_data:",
        errorMessage,
      );
    }

    // The four merge tables no entity owns, so `synchronize` above never made
    // them. Unguarded on purpose, unlike the extensions and the hypertable: a
    // suite that starts without these does not degrade, it reports a missing
    // relation from inside the merge transaction and blames the merge.
    await createMlForecastTables(dataSource);
    console.log("✅ ML forecast tables created (no entity owns these)");
  } finally {
    // Guarded, because `initialize()` itself may be what threw, and destroying
    // a DataSource that never connected throws in its own right — which would
    // replace the real error with a misleading one.
    if (dataSource.isInitialized) {
      await dataSource.destroy();
    }
  }

  console.log("✅ Test database schema created");
}
