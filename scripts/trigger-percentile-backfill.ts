import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { Queue } from 'bull';

/**
 * Trigger Queue Percentile Backfill
 *
 * Backfills percentiles for the last N days.
 * Run manually after deploying percentile aggregates.
 *
 * Usage:
 *   ts-node scripts/trigger-percentile-backfill.ts [days]
 *
 * Examples:
 *   ts-node scripts/trigger-percentile-backfill.ts 90  # Last 90 days
 *   ts-node scripts/trigger-percentile-backfill.ts 7   # Last 7 days
 */
async function triggerBackfill() {
    const days = parseInt(process.argv[2] || '90', 10);

    console.log(`🚀 Triggering percentile backfill for last ${days} days...`);

    const app = await NestFactory.createApplicationContext(AppModule);
    const analyticsQueue = app.get<Queue>('BullQueue_analytics');

    try {
        const job = await analyticsQueue.add('backfill-percentiles', { days });
        console.log(`✅ Job queued: ${job.id}`);
        console.log(`   Days: ${days}`);
        console.log(`\n⏳ Monitor progress in Bull Board or logs...`);
    } catch (error) {
        console.error('❌ Failed to queue backfill job:', error);
        process.exit(1);
    } finally {
        await app.close();
    }
}

triggerBackfill();
