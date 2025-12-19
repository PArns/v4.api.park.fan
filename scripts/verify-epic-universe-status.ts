import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { ParksService } from '../src/parks/parks.service';
import { DataSource } from 'typeorm';

/**
 * Verification script to check:
 * 1. Epic Universe is recognized as OPERATING via heuristic
 * 2. Timezone handling is correct for park status calculations
 */
async function main() {
    const app = await NestFactory.createApplicationContext(AppModule);
    const parksService = app.get(ParksService);
    const dataSource = app.get(DataSource);

    console.log('🔍 Epic Universe Status Verification\n');

    // Find Epic Universe
    const epicUniverse = await dataSource.query(`
        SELECT id, name, timezone, queue_times_entity_id as "queueTimesEntityId", wiki_entity_id as "wikiEntityId"
        FROM parks
        WHERE name ILIKE '%epic universe%'
        LIMIT 1
    `);

    if (epicUniverse.length === 0) {
        console.error('❌ Epic Universe not found!');
        await app.close();
        return;
    }

    const park = epicUniverse[0];
    console.log(`✅ Found: ${park.name}`);
    console.log(`   ID: ${park.id}`);
    console.log(`   Timezone: ${park.timezone}`);
    console.log(`   Queue-Times ID: ${park.queueTimesEntityId}`);
    console.log(`   Wiki ID: ${park.wikiEntityId}`);

    // Check schedule
    const schedule = await parksService.getUpcomingSchedule(park.id, 1);
    console.log(`\n📅 Schedule entries: ${schedule.length}`);

    // Check park status via service method
    const statusMap = await parksService.getBatchParkStatus([park.id]);
    const status = statusMap.get(park.id);
    console.log(`\n🏛️  Calculated Status: ${status}`);

    // Check recent queue data
    const recentData = await dataSource.query(`
        SELECT 
            COUNT(DISTINCT a.id) as attraction_count,
            COUNT(DISTINCT CASE WHEN q.status = 'OPERATING' THEN a.id END) as operating_count
        FROM attractions a
        JOIN LATERAL (
            SELECT status
            FROM queue_data qd
            WHERE qd."attractionId" = a.id
            AND qd.timestamp > NOW() - INTERVAL '20 minutes'
            ORDER BY timestamp DESC
            LIMIT 1
        ) q ON true
        WHERE a."parkId" = $1
    `, [park.id]);

    console.log(`\n🎢 Attraction Status (last 20 min):`);
    console.log(`   Total attractions with data: ${recentData[0].attraction_count}`);
    console.log(`   Operating attractions: ${recentData[0].operating_count}`);

    // Timezone verification
    console.log(`\n🌍 Timezone Verification:`);
    const now = new Date();
    const utcTime = now.toISOString();
    const parkLocalTime = now.toLocaleString('en-US', {
        timeZone: park.timezone,
        dateStyle: 'full',
        timeStyle: 'long',
    });

    console.log(`   UTC Time: ${utcTime}`);
    console.log(`   Park Local Time: ${parkLocalTime}`);

    // Verify isParkOperatingToday
    const isOperatingToday = await parksService.isParkOperatingToday(park.id);
    console.log(`   isParkOperatingToday: ${isOperatingToday}`);

    // Final verdict
    console.log(`\n📊 Verification Result:`);
    if (status === 'OPERATING' && parseInt(recentData[0].operating_count) > 0) {
        console.log(`   ✅ PASS - Epic Universe correctly recognized as OPERATING`);
        console.log(`   ✅ Heuristic working: ${recentData[0].operating_count} operating attractions`);
    } else if (status === 'CLOSED' && schedule.length === 0 && parseInt(recentData[0].operating_count) === 0) {
        console.log(`   ⚠️  CLOSED - No schedule and no operating attractions (expected if park is actually closed)`);
    } else {
        console.log(`   ❌ FAIL - Status mismatch!`);
        console.log(`      Expected: OPERATING (has ${recentData[0].operating_count} operating rides)`);
        console.log(`      Got: ${status}`);
    }

    await app.close();
}

main().catch(console.error);
