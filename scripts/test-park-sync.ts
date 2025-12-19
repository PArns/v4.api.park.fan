import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { ParksService } from '../src/parks/parks.service';

/**
 * Test script to run syncParks and observe merge behavior
 */
async function main() {
    console.log('🚀 Testing Park Sync with Smart Merge...\n');

    const app = await NestFactory.createApplicationContext(AppModule);
    const parksService = app.get(ParksService);

    try {
        // Run park sync
        console.log('⏳ Starting park sync...\n');
        const syncedCount = await parksService.syncParks();

        console.log(`\n✅ Sync complete! Synced ${syncedCount} parks`);
        console.log('\n📊 Check logs above for merge operations');
        console.log('   Look for: 🔗 Found potential duplicate');
        console.log('   Look for: 🔀 Migrating child entities');
        console.log('   Look for: 🗑️  Deleted empty losing park');

    } catch (error) {
        console.error('❌ Sync failed:', error);
    }

    await app.close();
}

main().catch(console.error);
