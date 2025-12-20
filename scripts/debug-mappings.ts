
import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { EntityMappingsProcessor } from "../src/queues/processors/entity-mappings.processor";

async function run() {
    console.log("🚀 Initializing Debug Mappings Script...");

    const app = await NestFactory.createApplicationContext(AppModule);
    const processor = app.get(EntityMappingsProcessor);

    const parkId = '740406a5-7ef0-4612-ba63-2413da969596'; // Phantasialand

    console.log(`\n🔗 Syncing mappings for park ${parkId}...`);
    try {
        // Accessing private method via 'any' cast for debugging
        const result = await (processor as any).syncParkEntityMappings(parkId);
        console.log(`✅ Result: ${result} mappings processed.`);
    } catch (err) {
        console.error("❌ Error syncing mappings:", err);
    }

    await app.close();
}

run();
