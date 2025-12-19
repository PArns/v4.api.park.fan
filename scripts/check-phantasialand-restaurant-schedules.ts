import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { ParksService } from '../src/parks/parks.service';
import { RestaurantsService } from '../src/restaurants/restaurants.service';

async function main() {
    const app = await NestFactory.createApplicationContext(AppModule);
    const parksService = app.get(ParksService);
    const restaurantsService = app.get(RestaurantsService);

    console.log('🔍 Checking Phantasialand Restaurant Schedules...\n');

    const park = await parksService.findBySlug('phantasialand');
    if (!park) {
        console.error('❌ Phantasialand not found in DB!');
        await app.close();
        return;
    }

    // Get current status map
    const liveDataMap = await restaurantsService.findCurrentStatusByPark(park.id);
    const restaurants = await restaurantsService.findByParkId(park.id);

    console.log(`🍽️  Found ${restaurants.length} restaurants in Phantasialand.`);

    let withHours = 0;
    let withoutHours = 0;

    restaurants.forEach(r => {
        const live = liveDataMap.get(r.id);
        const hours = live?.operatingHours;
        const hoursCount = hours?.length || 0;

        if (hoursCount > 0) {
            withHours++;
            console.log(`   ✅ ${r.name}: ${hoursCount} schedule entries`);
            // console.log(JSON.stringify(hours));
        } else {
            withoutHours++;
            // console.log(`   ❌ ${r.name}: No schedule`);
        }
    });

    console.log(`\nSummary:`);
    console.log(`✅ With Schedule: ${withHours}`);
    console.log(`❌ Without Schedule: ${withoutHours}`);

    if (withHours === 0) {
        console.log(`\n⚠️  It appears we are receiving NO restaurant schedules from the upstream API for Phantasialand.`);
    }

    await app.close();
}

main().catch(console.error);
