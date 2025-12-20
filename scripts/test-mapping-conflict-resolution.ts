import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { DataSource } from "typeorm";

async function run() {
  console.log("🧪 Testing automatic mapping conflict resolution...\n");

  const app = await NestFactory.createApplicationContext(AppModule);
  const dataSource = app.get(DataSource);

  // Find a test park (using Phantasialand)
  const parkId = "740406a5-7ef0-4612-ba63-2413da969596";
  const queueTimesId = "56";

  console.log("Step 1: Check current Phantasialand mappings");
  const currentMappings = await dataSource.query(
    `
    SELECT external_source, external_entity_id, internal_entity_id
    FROM external_entity_mapping 
    WHERE (internal_entity_id = $1 AND internal_entity_type = 'park')
       OR (external_source = 'queue-times' AND external_entity_id = $2)
    ORDER BY external_source
  `,
    [parkId, queueTimesId],
  );

  console.table(currentMappings);

  // Simulate conflict: Create a stale mapping (queue-times:56 pointing to wrong park)
  const fakeWrongParkId = "00000000-0000-0000-0000-000000000000";

  console.log("\nStep 2: Simulating conflict - Creating stale mapping...");
  console.log(`  queue-times:${queueTimesId} → ${fakeWrongParkId} (WRONG)`);

  await dataSource.query(
    `
    DELETE FROM external_entity_mapping 
    WHERE external_source = 'queue-times' AND external_entity_id = $1
  `,
    [queueTimesId],
  );

  await dataSource.query(
    `
    INSERT INTO external_entity_mapping (
      internal_entity_id, internal_entity_type, external_source, 
      external_entity_id, match_confidence, match_method, verified
    ) VALUES ($1, 'park', 'queue-times', $2, 0.99, 'fuzzy', false)
  `,
    [fakeWrongParkId, queueTimesId],
  );

  console.log("  ✅ Stale mapping created");

  // Now trigger park-metadata sync which should auto-resolve the conflict
  console.log("\nStep 3: Triggering park-metadata sync...");
  console.log(
    "  This will call ParkMetadataProcessor which will detect and resolve the conflict\n",
  );

  const queue: any = app.get("BullQueue_park-metadata");

  const job = await queue.add(
    "sync-park-mappings",
    { forceSync: true },
    { priority: 1 },
  );
  console.log(`  📋 Job queued: ${job.id}`);
  console.log("  ⏳ Waiting for job to complete...\n");

  // Wait for job completion
  const result = await job.finished();

  console.log("Step 4: Verifying conflict was resolved...");
  const finalMappings = await dataSource.query(
    `
    SELECT external_source, external_entity_id, internal_entity_id
    FROM external_entity_mapping 
    WHERE (internal_entity_id = $1 AND internal_entity_type = 'park')
       OR (external_source = 'queue-times' AND external_entity_id = $2)
    ORDER BY external_source
  `,
    [parkId, queueTimesId],
  );

  console.table(finalMappings);

  // Verify queue-times mapping now points to correct park
  const qtMapping = finalMappings.find(
    (m: any) =>
      m.external_source === "queue-times" &&
      m.external_entity_id === queueTimesId,
  );

  if (qtMapping && qtMapping.internal_entity_id === parkId) {
    console.log("\n✅ SUCCESS! Conflict automatically resolved.");
    console.log(
      `   queue-times:${queueTimesId} now correctly points to Phantasialand`,
    );
  } else {
    console.log("\n❌ FAILED! Conflict not resolved.");
    console.log("   Expected queue-times:56 to point to", parkId);
    console.log("   But got:", qtMapping);
  }

  // Check if land data was populated
  console.log("\nStep 5: Checking if land data was populated...");
  const landDataCount = await dataSource.query(
    `
    SELECT COUNT(*) as count
    FROM attractions 
    WHERE "parkId" = $1 AND land_name IS NOT NULL
  `,
    [parkId],
  );

  console.log(`  Attractions with land data: ${landDataCount[0].count}`);

  if (landDataCount[0].count > 0) {
    const samples = await dataSource.query(
      `
      SELECT name, land_name
      FROM attractions 
      WHERE "parkId" = $1 AND land_name IS NOT NULL
      LIMIT 3
    `,
      [parkId],
    );

    console.log("\n  Sample attractions:");
    console.table(samples);
  }

  await app.close();
}

run().catch(console.error);
