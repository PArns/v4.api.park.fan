import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { DataSource } from "typeorm";

async function run() {
    console.log('🔄 Triggering park-metadata sync on fresh DB...\n');

    const app = await NestFactory.createApplicationContext(AppModule);
    const queue: any = app.get('BullQueue_park-metadata');

    const job = await queue.add('sync-park-mappings', { forceSync: true }, { priority: 1 });
    console.log(`✅ Job queued: ${job.id}`);
    console.log('⏳ Waiting for sync to complete...\n');

    await job.finished();
    console.log('✅ Sync completed!\n');

    // Verify Phantasialand
    const dataSource = app.get(DataSource);

    const park = await dataSource.query(`
    SELECT id, name, data_sources, primary_data_source
    FROM parks WHERE slug = 'phantasialand'
  `);

    if (park.length === 0) {
        console.log('❌ Phantasialand not found!');
        await app.close();
        return;
    }

    console.log('📊 Phantasialand Status:');
    console.log(`  Name: ${park[0].name}`);
    console.log(`  Data sources: ${park[0].data_sources}`);
    console.log(`  Primary: ${park[0].primary_data_source}\n`);

    const mappings = await dataSource.query(`
    SELECT external_source, external_entity_id
    FROM external_entity_mapping
    WHERE internal_entity_id = $1 AND internal_entity_type = 'park'
    ORDER BY external_source
  `, [park[0].id]);

    console.log(`Park Mappings: ${mappings.length}`);
    console.table(mappings);

    const attractions = await dataSource.query(`
    SELECT COUNT(*) as total, COUNT(land_name) as with_land
    FROM attractions WHERE "parkId" = $1
  `, [park[0].id]);

    console.log(`\nAttractions: ${attractions[0].total} total, ${attractions[0].with_land} with land data`);

    if (attractions[0].with_land > 0) {
        const samples = await dataSource.query(`
      SELECT name, land_name
      FROM attractions 
      WHERE "parkId" = $1 AND land_name IS NOT NULL
      LIMIT 5
    `, [park[0].id]);

        console.log('\nSample:');
        console.table(samples);
        console.log(`\n✅ SUCCESS! Land data populated for ${attractions[0].with_land}/${attractions[0].total} attractions`);
    } else {
        console.log('\n❌ No land data populated');
    }

    await app.close();
}

run().catch(console.error);
