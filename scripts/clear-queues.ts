import Queue from 'bull';
import * as dotenv from 'dotenv';
import { getRedisConfig } from '../src/config/redis.config';

// Load environment variables
dotenv.config();

const queues = [
    'wait-times',
    'park-metadata',
    'children-metadata',
    'attractions-metadata',
    'shows-metadata',
    'restaurants-metadata',
    'occupancy-calculation',
    'weather',
    'weather-historical',
    'holidays',
    'ml-training',
    'prediction-accuracy',
];

async function clearQueues() {
    const redisConfig = getRedisConfig();
    const prefix = process.env.BULL_PREFIX || 'parkfan';

    console.log('🚀 Clearing all queues...');
    console.log(`🔌 Redis: ${redisConfig.host}:${redisConfig.port}`);
    console.log(`🏷️  Prefix: ${prefix}`);

    for (const queueName of queues) {
        const queue = new Queue(queueName, {
            redis: {
                host: redisConfig.host,
                port: redisConfig.port,
            },
            prefix,
        });

        try {
            await queue.obliterate({ force: true });
            console.log(`✅ Cleared queue: ${queueName}`);
        } catch (error) {
            console.error(`❌ Failed to clear queue ${queueName}:`, error);
        } finally {
            await queue.close();
        }
    }

    console.log('✨ All queues cleared!');
    process.exit(0);
}

clearQueues().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
});
