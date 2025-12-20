import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { Queue } from "bull";
import { InjectQueue } from "@nestjs/bull";
import { Injectable } from "@nestjs/common";

@Injectable()
class SyncTrigger {
    constructor(
        @InjectQueue("park-metadata") private parkQueue: Queue,
    ) { }

    async triggerSync() {
        console.log("🔄 Triggering sync-park-mappings job...");

        const job = await this.parkQueue.add(
            "sync-park-mappings",
            { forceSync: true },
            { priority: 1 }
        );

        console.log(`✅ Job queued: ${job.id}`);
        console.log(`   Waiting for completion...`);

        const result = await job.finished();
        console.log(`✅ Job completed!`);
        console.log(result);
    }
}

async function run() {
    const app = await NestFactory.createApplicationContext(AppModule);
    const trigger = app.get(SyncTrigger);

    await trigger.triggerSync();

    console.log("\n📊 Checking land data...");
    const dataSource = app.get("DataSource");

    const result = await dataSource.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(land_external_id) as with_land_data
    FROM attractions 
    WHERE "parkId" = (SELECT id FROM parks WHERE slug = 'phantasialand')
  `);

    console.log(`Phantasialand: ${result[0].total} attractions, ${result[0].with_land_data} with land data`);

    if (result[0].with_land_data > 0) {
        const sample = await dataSource.query(`
      SELECT name, land_name, land_external_id 
      FROM attractions 
      WHERE "parkId" = (SELECT id FROM parks WHERE slug = 'phantasialand') 
        AND land_name IS NOT NULL 
      LIMIT 5
    `);
        console.log("\nSample attractions with land data:");
        console.table(sample);
    }

    await app.close();
}

run().catch(console.error);
