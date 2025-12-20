import Queue from "bull";
import * as dotenv from "dotenv";
import * as path from "path";

// Load .env file from root
dotenv.config({ path: path.resolve(__dirname, "../.env") });

const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379", 10);
const BULL_PREFIX = process.env.BULL_PREFIX || "parkfan";

async function triggerTraining() {
  console.log("🚀 Triggering manual ML training job...");
  console.log(`   Redis: ${REDIS_HOST}:${REDIS_PORT}`);
  console.log(`   Prefix: ${BULL_PREFIX}`);

  const queue = new Queue("ml-training", {
    redis: {
      host: REDIS_HOST,
      port: REDIS_PORT,
    },
    prefix: BULL_PREFIX,
  });

  try {
    // Add job with explicit ID to verify deduplication behavior or random ID
    // We don't force jobId here to allow multiple manual triggers if needed,
    // though the processor might handle deduplication/locking.
    const job = await queue.add(
      "train-model",
      {},
      {
        removeOnComplete: true,
        removeOnFail: false,
      },
    );

    console.log(`✅ Job added successfully!`);
    console.log(`   Job ID: ${job.id}`);
    console.log(
      "   Check application logs or Bull Board (http://localhost:3001) for progress.",
    );
  } catch (error) {
    console.error("❌ Failed to add job:", error);
    process.exit(1);
  } finally {
    await queue.close();
    process.exit(0);
  }
}

triggerTraining();
