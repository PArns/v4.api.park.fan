import { DataSource } from "typeorm";
import Redis from "ioredis";
import * as dotenv from "dotenv";
import * as path from "path";
import { ML_FORECAST_TABLES } from "./helpers/ml-forecast-tables";
import {
  BULL_QUEUE_REGISTRATIONS,
  bullPrefix,
} from "../src/queues/queue-registrations";

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
    protocol: 2,
    maxRetriesPerRequest: 3,
    enableOfflineQueue: false,
    lazyConnect: true,
  });

  await redis.connect();
  await pauseBullQueues();
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
 *
 * No Bull job runs in an E2E suite unless its spec resumes the queue: every
 * queue is paused before the app boots and again after each flush, which
 * clears the pause. With `SKIP_QUEUE_BOOTSTRAP=false` the jobs the boot chains
 * queue (children metadata, P50 baselines) otherwise ran against the
 * truncation below and lost with a deadlock (40P01) or wrote a park against a
 * destination it had just emptied (23503), PAR-893. Waiting for them is no
 * option: the children metadata sync runs for minutes. A job a spec resumed
 * its queue for is allowed to finish before the truncation.
 */
afterEach(async () => {
  // `finally`, so neither a job that will not finish nor a Redis that refuses
  // to flush costs the run its table cleanup: this hook is the only isolation
  // left between two test FILES, and returning early from it hands the next
  // file this file's rows.
  try {
    await pauseBullQueues();
    await waitForActiveBullJobs();
  } finally {
    try {
      await truncateAllTables();
    } finally {
      await redis?.flushall();
      await pauseBullQueues();
    }
  }
});

/**
 * What Bull's own global `queue.pause()` does (pause-5.lua in bull@4.16.5):
 * the waiting jobs move to `paused` and `meta-paused` sends every later add
 * there too, so no worker takes another job. One script for every queue
 * instead of a Bull `Queue` per queue, each with its own connections.
 */
const PAUSE_QUEUES = `
for _, queue in ipairs(KEYS) do
  if redis.call("EXISTS", queue .. ":wait") == 1 then
    redis.call("RENAME", queue .. ":wait", queue .. ":paused")
  end
  redis.call("SET", queue .. ":meta-paused", 1)
end`;
const ACTIVE_JOBS_TIMEOUT_MS = 30_000;

function bullQueueKeys(): string[] {
  return BULL_QUEUE_REGISTRATIONS.map(({ name }) => `${bullPrefix()}:${name}`);
}

async function pauseBullQueues(): Promise<void> {
  const queues = bullQueueKeys();
  await redis.eval(PAUSE_QUEUES, queues.length, ...queues);
}

async function waitForActiveBullJobs(): Promise<void> {
  const queues = bullQueueKeys();
  const deadline = Date.now() + ACTIVE_JOBS_TIMEOUT_MS;
  for (;;) {
    const pipeline = redis.pipeline();
    for (const queue of queues) {
      pipeline.llen(`${queue}:active`);
    }
    const lengths = (await pipeline.exec()) ?? [];
    const busy = queues.filter((_, i) => Number(lengths[i]?.[1]) > 0);
    if (busy.length === 0) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(
        `Bull jobs still active after ${ACTIVE_JOBS_TIMEOUT_MS} ms, the truncation would run under them: ${busy.join(", ")}`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

async function truncateAllTables(): Promise<void> {
  if (!dataSource?.isInitialized) {
    return;
  }

  // `entityMetadatas` is every table TypeORM knows, which is every table the
  // schema had until the merge tables that no entity owns were added to it.
  // Those two are not in the metadata and would therefore never be emptied —
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
      // metadata, the two others from their writers' own DDL — so every table
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
