
import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { ParksService } from "../src/parks/parks.service";
import { ExternalEntityMapping } from "../src/database/entities/external-entity-mapping.entity";
import { getRepository } from "typeorm";
import { DataSource } from "typeorm";

async function run() {
    console.log("🚀 Initializing Repair Park Mappings Script (Recoverable)...");

    const app = await NestFactory.createApplicationContext(AppModule);
    const parksService = app.get(ParksService);
    const dataSource = app.get(DataSource);
    const mappingRepo = dataSource.getRepository(ExternalEntityMapping);

    const parks = await parksService.findAll();
    console.log(`Checking ${parks.length} parks for missing QT mappings...`);

    let fixedCount = 0;

    for (const park of parks) {
        if (park.queueTimesEntityId) {
            // Check if mapping exists by Internal ID
            const existing = await mappingRepo.findOne({
                where: {
                    internalEntityId: park.id,
                    internalEntityType: 'park',
                    externalSource: 'queue-times'
                }
            });

            if (!existing) {
                console.log(`⚠️ Missing QT mapping for park ${park.name} (QT ID: ${park.queueTimesEntityId}). Creating...`);

                try {
                    await mappingRepo.save({
                        internalEntityId: park.id,
                        internalEntityType: 'park',
                        externalSource: 'queue-times',
                        externalEntityId: park.queueTimesEntityId,
                        matchConfidence: 1.0,
                        matchMethod: 'manual'
                    });
                    fixedCount++;
                    console.log(`   ✅ Created QT mapping for ${park.name}`);
                } catch (err: any) {
                    console.error(`   ❌ Failed to create mapping for ${park.name} (likely duplicate):`, err.message);
                }
            }

            // Also check Wiki mapping while we are here
            if (park.wikiEntityId) {
                const existingWiki = await mappingRepo.findOne({
                    where: {
                        internalEntityId: park.id,
                        internalEntityType: 'park',
                        externalSource: 'themeparks-wiki'
                    }
                });

                if (!existingWiki) {
                    console.log(`⚠️ Missing Wiki mapping for park ${park.name} (Wiki ID: ${park.wikiEntityId}). Creating...`);
                    try {
                        await mappingRepo.save({
                            internalEntityId: park.id,
                            internalEntityType: 'park',
                            externalSource: 'themeparks-wiki',
                            externalEntityId: park.wikiEntityId,
                            matchConfidence: 1.0,
                            matchMethod: 'manual'
                        });
                        fixedCount++;
                        console.log(`   ✅ Created Wiki mapping for ${park.name}`);
                    } catch (err: any) {
                        console.error(`   ❌ Failed to create Wiki mapping for ${park.name}:`, err.message);
                    }
                }
            }
        }
    }

    console.log(`✅ Fixed ${fixedCount} missing mappings.`);
    await app.close();
}

run();
