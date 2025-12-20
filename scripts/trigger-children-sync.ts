import Queue from "bull";
import * as dotenv from "dotenv";
import * as path from "path";

// Load .env file from root
dotenv.config({ path: path.resolve(__dirname, "../.env") });

const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379", 10);
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;
const REDIS_DB = parseInt(process.env.REDIS_DB || "0", 10);
const BULL_PREFIX = process.env.BULL_PREFIX || "parkfan";

async function triggerChildrenSync() {
  console.log("🎢 Triggering children metadata sync job...");
  console.log(`   Redis: ${REDIS_HOST}:${REDIS_PORT}`);
  console.log(`   Prefix: ${BULL_PREFIX}`);

  // Connect to the specific queue
  const childrenQueue = new Queue("children-metadata", {
    redis: {
      host: REDIS_HOST,
      port: REDIS_PORT,
      password: REDIS_PASSWORD,
      db: REDIS_DB,
    },
    prefix: BULL_PREFIX,
  });

  try {
    // Add job to queue
    const job = await childrenQueue.add(
      "fetch-all-children",
      {},
      {
        removeOnComplete: true,
        attempts: 3,
      },
    );

    console.log(`✅ Job added successfully with ID: ${job.id}`);
    console.log(
      "📊 The job will sync attractions, shows, AND restaurants for all parks.",
    );
    console.log(
      "⏳ This may take several minutes. Check application logs for progress.",
    );
  } catch (error) {
    console.error("❌ Failed to trigger sync job:", error);
    process.exit(1);
  } finally {
    await childrenQueue.close();
    process.exit(0);
  }
}

triggerChildrenSync();
