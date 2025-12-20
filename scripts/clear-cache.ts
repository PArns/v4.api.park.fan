import * as Redis from "ioredis";
import * as dotenv from "dotenv";
import * as path from "path";

// Load .env file from root
dotenv.config({ path: path.resolve(__dirname, "../.env") });

const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379", 10);
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;
const REDIS_DB = parseInt(process.env.REDIS_DB || "0", 10);
const BULL_PREFIX = process.env.BULL_PREFIX || "parkfan"; // Not used for cache, but good for context

async function clearIntegratedCache() {
  console.log("🧹 Clearing Park Integrated Response Cache...");
  console.log(`   Redis: ${REDIS_HOST}:${REDIS_PORT}`);

  const redis = new Redis.default({
    host: REDIS_HOST,
    port: REDIS_PORT,
    password: REDIS_PASSWORD,
    db: REDIS_DB,
  });

  const matchPattern = "park:integrated:*";
  const stream = redis.scanStream({
    match: matchPattern,
    count: 100,
  });

  let deletedCount = 0;

  stream.on("data", async (keys) => {
    if (keys.length) {
      // Keys might be prefixed if ioredis was initialized with prefix?
      // NestJS redis module often uses prefix?
      // But here we use raw connection.
      // NOTE: We assume the keys in redis match the pattern directly (or have global prefix).
      // Given we scan with match, we get the full key.

      const pipeline = redis.pipeline();
      keys.forEach((key) => {
        pipeline.del(key);
      });
      await pipeline.exec();
      deletedCount += keys.length;
      process.stdout.write(`\rDeleted ${deletedCount} keys...`);
    }
  });

  stream.on("end", () => {
    console.log("\n✅ Cache clear complete!");
    console.log(`Total keys deleted: ${deletedCount}`);
    redis.quit();
    process.exit(0);
  });

  stream.on("error", (err) => {
    console.error("\n❌ Scan error:", err);
    redis.quit();
    process.exit(1);
  });
}

clearIntegratedCache();
