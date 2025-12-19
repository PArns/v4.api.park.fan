import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { ParksService } from '../src/parks/parks.service';
import { DataSource } from 'typeorm';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../src/common/redis/redis.module';

/**
 * Comprehensive diagnostic script for Epic Universe status
 */
async function main() {
    console.log('🔍 Epic Universe Live Status Debug\n');
    console.log(`Current Time (UTC): ${new Date().toISOString()}`);
    console.log(`Current Time (Local): ${new Date().toLocaleString()}\n`);

    const app = await NestFactory.createApplicationContext(AppModule);
    const parksService = app.get(ParksService);
    const dataSource = app.get(DataSource);
    const redis = app.get<Redis>(REDIS_CLIENT);

    try {
        // 1. Find Epic Universe
        console.log('📍 Step 1: Finding Epic Universe...');
        const parks = await dataSource.query(`
            SELECT id, name, timezone, queue_times_entity_id, wiki_entity_id
            FROM parks
            WHERE name ILIKE '%epic universe%'
        `);

        if (parks.length === 0) {
            console.error('❌ Epic Universe not found!');
            await app.close();
            return;
        }

        const park = parks[0];
        console.log(`✅ Found: ${park.name}`);
        console.log(`   ID: ${park.id}`);
        console.log(`   Timezone: ${park.timezone}`);
        console.log(`   Queue-Times ID: ${park.queue_times_entity_id}`);
        console.log(`   Wiki ID: ${park.wiki_entity_id}\n`);

        // 2. Check current time in park timezone
        console.log('🕐 Step 2: Timezone Verification...');
        const now = new Date();
        const parkLocalTime = now.toLocaleString('en-US', {
            timeZone: park.timezone,
            dateStyle: 'full',
            timeStyle: 'long',
        });
        console.log(`   Park Local Time: ${parkLocalTime}\n`);

        // 3. Check schedule entries
        console.log('📅 Step 3: Checking Schedule Entries...');
        const schedules = await dataSource.query(`
            SELECT date, "scheduleType", "openingTime", "closingTime"
            FROM schedule_entries
            WHERE "parkId" = $1
            AND date >= CURRENT_DATE - INTERVAL '1 day'
            AND date <= CURRENT_DATE + INTERVAL '7 days'
            ORDER BY date
        `, [park.id]);

        console.log(`   Total schedule entries (last 1 day to next 7 days): ${schedules.length}`);
        if (schedules.length > 0) {
            schedules.forEach((s: any) => {
                console.log(`   - ${s.date}: ${s.scheduleType} (${s.openingTime || 'N/A'} - ${s.closingTime || 'N/A'})`);
            });
        } else {
            console.log('   ⚠️  No schedule entries found!');
        }
        console.log();

        // 4. Check recent queue data
        console.log('🎢 Step 4: Checking Recent Queue Data...');
        const queueData = await dataSource.query(`
            SELECT 
                COUNT(DISTINCT a.id) as total_attractions,
                COUNT(DISTINCT CASE WHEN q.status = 'OPERATING' THEN a.id END) as operating_attractions,
                MAX(q.timestamp) as latest_timestamp
            FROM attractions a
            LEFT JOIN LATERAL (
                SELECT status, timestamp
                FROM queue_data qd
                WHERE qd."attractionId" = a.id
                AND qd.timestamp > NOW() - INTERVAL '30 minutes'
                ORDER BY timestamp DESC
                LIMIT 1
            ) q ON true
            WHERE a."parkId" = $1
        `, [park.id]);

        console.log(`   Total attractions with recent data: ${queueData[0].total_attractions}`);
        console.log(`   Operating attractions: ${queueData[0].operating_attractions}`);
        console.log(`   Latest queue timestamp: ${queueData[0].latest_timestamp || 'None'}\n`);

        // 5. Check Redis cache
        console.log('💾 Step 5: Checking Redis Cache...');
        const cacheKeys = await redis.keys(`park:*${park.id}*`);
        console.log(`   Cache keys found: ${cacheKeys.length}`);
        for (const key of cacheKeys) {
            const ttl = await redis.ttl(key);
            const value = await redis.get(key);
            console.log(`   - ${key}`);
            console.log(`     TTL: ${ttl}s`);
            console.log(`     Value: ${value?.substring(0, 100)}...\n`);
        }

        // 6. Test getBatchParkStatus
        console.log('🧪 Step 6: Testing getBatchParkStatus...');
        const statusMap = await parksService.getBatchParkStatus([park.id]);
        const status = statusMap.get(park.id);
        console.log(`   Calculated Status: ${status}\n`);

        // 7. Test isParkOperatingToday
        console.log('🔬 Step 7: Testing isParkOperatingToday...');
        const isOperatingToday = await parksService.isParkOperatingToday(park.id);
        console.log(`   Is Operating Today: ${isOperatingToday}\n`);

        // 8. Summary
        console.log('📊 Summary:');
        console.log(`   Park: ${park.name}`);
        console.log(`   Status: ${status}`);
        console.log(`   Expected: OPERATING (has ${queueData[0].operating_attractions} operating attractions)`);

        if (status === 'OPERATING') {
            console.log('\n✅ SUCCESS: Park correctly shows as OPERATING');
        } else {
            console.log('\n❌ ISSUE: Park shows as CLOSED but should be OPERATING');
            console.log('\n🔍 Possible causes:');
            if (schedules.length === 0) {
                console.log('   1. No schedule entries found');
            }
            if (queueData[0].operating_attractions < 10) {
                console.log(`   2. Only ${queueData[0].operating_attractions} operating attractions (threshold is 10)`);
            }
            if (cacheKeys.length > 0) {
                console.log('   3. Stale cache might be serving old data');
            }
        }

    } catch (error) {
        console.error('❌ Error:', error);
    }

    await app.close();
}

main().catch(console.error);
