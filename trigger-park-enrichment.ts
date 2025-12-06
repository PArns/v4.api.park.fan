/**
 * Trigger Park Enrichment
 * 
 * Manually triggers the park enrichment process to populate:
 * - countryCode (from country name)
 * - influencingCountries (from country code)
 */

import { NestFactory } from '@nestjs/core';
import { AppModule } from './src/app.module';
import { getQueueToken } from '@nestjs/bull';
import { Queue } from 'bull';

async function bootstrap() {
    console.log('🚀 Triggering park enrichment...\n');

    const app = await NestFactory.createApplicationContext(AppModule);

    try {
        const enrichmentQueue = app.get<Queue>(getQueueToken('park-enrichment'));

        console.log('📋 Adding enrichment job to queue...');
        const job = await enrichmentQueue.add('enrich-all', {}, {
            priority: 10,
            attempts: 3,
        });

        console.log(`✅ Job added: ${job.id}`);
        console.log('⏳ Waiting for job to complete...\n');

        // Wait for job to finish
        const result = await job.finished();

        console.log('\n✅ Enrichment completed successfully!');
        console.log('📊 Check database for updated countryCode and influencingCountries');

    } catch (error) {
        console.error('❌ Enrichment failed:', error);
        process.exit(1);
    } finally {
        await app.close();
    }
}

bootstrap();
