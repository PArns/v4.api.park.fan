import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { DataSource } from "typeorm";

async function run() {
    const app = await NestFactory.createApplicationContext(AppModule);
    const dataSource = app.get(DataSource);

    const parkId = '740406a5-7ef0-4612-ba63-2413da969596'; // Phantasialand
    const queueTimesId = '56';

    console.log('🔍 Checking current mapping status for Phantasialand...');

    const mappings = await dataSource.query(`
    SELECT external_source, external_entity_id 
    FROM external_entity_mapping 
    WHERE internal_entity_id = $1 AND internal_entity_type = 'park'
  `, [parkId]);

    console.log('Current park-level mappings:', mappings);

    const hasQT = mappings.some((m: any) => m.external_source === 'queue-times');

    if (!hasQT) {
        console.log('❌ Missing Queue-Times park mapping! Creating it...');

        await dataSource.query(`
      INSERT INTO external_entity_mapping (
        internal_entity_id,
        internal_entity_type,
        external_source,
        external_entity_id,
        match_confidence,
        match_method,
        verified
      ) VALUES ($1, 'park', 'queue-times', $2, 0.99, 'fuzzy', true)
      ON CONFLICT DO NOTHING
    `, [parkId, queueTimesId]);

        console.log('✅ Queue-Times mapping created!');
    } else {
        console.log('✅ Queue-Times mapping already exists');
    }

    // Verify final state
    const finalMappings = await dataSource.query(`
    SELECT external_source, external_entity_id 
    FROM external_entity_mapping 
    WHERE internal_entity_id = $1 AND internal_entity_type = 'park'
    ORDER BY external_source
  `, [parkId]);

    console.log('\n📊 Final park-level mappings:');
    console.table(finalMappings);

    console.log('\n✅ Done! Now trigger park mappings sync to populate land data.');

    await app.close();
}

run().catch(console.error);
