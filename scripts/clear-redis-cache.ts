
import { Redis } from "ioredis";

async function clearCache() {
    const redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");

    console.log("Connecting to Redis...");

    // Find keys matching 'park:integrated:*'
    const keys = await redis.keys("park:integrated:*");

    if (keys.length > 0) {
        console.log(`Found ${keys.length} cached park responses.`);
        const deleted = await redis.del(...keys);
        console.log(`✅ Deleted ${deleted} keys.`);
    } else {
        console.log("No cached park responses found.");
    }

    // Also clear park status/schedule caches just in case
    const scheduleKeys = await redis.keys("schedule:*");
    if (scheduleKeys.length > 0) {
        await redis.del(...scheduleKeys);
        console.log(`✅ Cleared ${scheduleKeys.length} schedule cache keys.`);
    }

    redis.disconnect();
}

clearCache().catch(console.error);
