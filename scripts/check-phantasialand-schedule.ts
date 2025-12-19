import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { ParksService } from '../src/parks/parks.service';

async function main() {
    const app = await NestFactory.createApplicationContext(AppModule);
    const parksService = app.get(ParksService);

    console.log('🔍 Checking Phantasialand schedule...\n');

    const park = await parksService.findBySlug('phantasialand');
    if (!park) {
        console.error('❌ Phantasialand not found in DB!');
        await app.close();
        return;
    }

    console.log(`✅ Found Park: ${park.name} (${park.timezone})`);

    // Get schedule for next 7 days
    const upcomingSchedule = await parksService.getUpcomingSchedule(park.id, 7);

    console.log(`📅 Found ${upcomingSchedule.length} schedule entries for next 7 days:`);
    upcomingSchedule.forEach(s => {
        console.log(`   - ${s.date}: ${s.scheduleType} (${s.openingTime?.toISOString()} - ${s.closingTime?.toISOString()})`);
    });

    // Check "Current" Status Logic
    const now = new Date();
    console.log(`\n⌚ Current Time (UTC): ${now.toISOString()}`);

    const operatingSchedule = upcomingSchedule.find(
        (s) =>
            s.scheduleType === "OPERATING" &&
            s.openingTime &&
            s.closingTime &&
            now >= s.openingTime &&
            now < s.closingTime,
    );

    const status = operatingSchedule ? "OPERATING" : "CLOSED";
    console.log(`🏛️  Calculated Park Status: ${status}`);

    if (operatingSchedule) {
        console.log(`   (Matched Schedule: ${operatingSchedule.date})`);
    } else {
        console.log(`   (No matching OPERATING schedule for current time)`);
    }

    await app.close();
}

main().catch(console.error);
