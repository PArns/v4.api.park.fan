import { NestFactory } from '@nestjs/core';
import { AppModule } from './src/app.module';
import { BullModule, getQueueToken } from '@nestjs/bull';
import { Queue } from 'bull';

async function bootstrap() {
    const app = await NestFactory.createApplicationContext(AppModule);
    const parkQueue = app.get<Queue>(getQueueToken('park-metadata'));

    console.log('🚀 Triggering park-metadata job...');

    // Clean old jobs first to be clean
    await parkQueue.clean(0, 'active');
    await parkQueue.clean(0, 'wait');
    await parkQueue.clean(0, 'failed');

    await parkQueue.add(
        'fetch-all-parks',
        {},
        {
            priority: 1,
            jobId: `manual-geocoding-${Date.now()}`,
            removeOnComplete: true,
        },
    );

    console.log('✅ Job triggered!');
    await app.close();
    process.exit(0);
}

bootstrap();
