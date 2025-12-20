import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { DataSource } from "typeorm";

async function run() {
  console.log("🧪 Testing Auto-Fix with Fresh Database\n");
  console.log("=".repeat(60));

  const app = await NestFactory.createApplicationContext(AppModule);
  const dataSource = app.get(DataSource);

  // Step 1: Reset database
  console.log("\n📦 Step 1: Resetting database...");

  await dataSource.query("DELETE FROM queue_data");
  console.log("  ✅ Deleted all queue data");

  await dataSource.query("DELETE FROM external_entity_mapping");
  console.log("  ✅ Deleted all external entity mappings");

  await dataSource.query("DELETE FROM attractions");
  console.log("  ✅ Deleted all attractions");

  await dataSource.query("DELETE FROM parks");
  console.log("  ✅ Deleted all parks");

  await dataSource.query("DELETE FROM destinations");
  console.log("  ✅ Deleted all destinations");

  // Step 2: Run seeder via npm
  console.log("\n🌱 Step 2: Seeding fresh data...");
  console.log("  Running: npm run seed");

  const { execSync } = require("child_process");
  execSync("npm run seed", {
    cwd: "/Users/patrick/Repos/v4.api.park.fan",
    stdio: "inherit",
  });

  // Step 3: Trigger sync with auto-fix
  console.log("\n🔄 Step 3: Triggering park-metadata sync...");
  console.log("  This will test automatic conflict resolution");

  const queue: any = app.get("BullQueue_park-metadata");
  const job = await queue.add(
    "sync-park-mappings",
    { forceSync: true },
    { priority: 1 },
  );

  console.log(`  📋 Job queued: ${job.id}`);
  console.log("  ⏳ Waiting for sync to complete...\n");

  await job.finished();

  // Step 4: Verify Phantasialand
  console.log("\n✅ Step 4: Verifying Phantasialand...");

  const park = await dataSource.query(`
    SELECT id, name, slug, data_sources, primary_data_source
    FROM parks WHERE slug = 'phantasialand'
  `);

  if (park.length === 0) {
    console.log("  ❌ Phantasialand not found!");
    await app.close();
    return;
  }

  console.log(`  Park: ${park[0].name}`);
  console.log(`  Data sources: ${park[0].data_sources}`);
  console.log(`  Primary source: ${park[0].primary_data_source}`);

  // Check mappings
  const mappings = await dataSource.query(
    `
    SELECT external_source, external_entity_id
    FROM external_entity_mapping
    WHERE internal_entity_id = $1 AND internal_entity_type = 'park'
    ORDER BY external_source
  `,
    [park[0].id],
  );

  console.log(`\n  Park-level mappings: ${mappings.length}`);
  console.table(mappings);

  // Check attractions
  const attractions = await dataSource.query(
    `
    SELECT COUNT(*) as total, COUNT(land_name) as with_land
    FROM attractions WHERE "parkId" = $1
  `,
    [park[0].id],
  );

  console.log(`  Total attractions: ${attractions[0].total}`);
  console.log(`  With land data: ${attractions[0].with_land}`);

  if (attractions[0].with_land > 0) {
    const samples = await dataSource.query(
      `
      SELECT name, land_name, land_external_id
      FROM attractions 
      WHERE "parkId" = $1 AND land_name IS NOT NULL
      LIMIT 5
    `,
      [park[0].id],
    );

    console.log("\n  Sample attractions with land data:");
    console.table(samples);
  }

  // Final verdict
  console.log("\n" + "=".repeat(60));

  if (mappings.length === 2 && attractions[0].with_land > 0) {
    console.log("✅ SUCCESS! Auto-fix works correctly:");
    console.log(
      "   - Both park mappings created (themeparks-wiki + queue-times)",
    );
    console.log("   - No conflicts detected (or automatically resolved)");
    console.log(
      `   - Land data populated for ${attractions[0].with_land}/${attractions[0].total} attractions`,
    );
  } else {
    console.log("❌ ISSUES DETECTED:");
    if (mappings.length < 2) {
      console.log(`   - Only ${mappings.length} park mapping(s) (expected 2)`);
    }
    if (attractions[0].with_land === 0) {
      console.log("   - No land data populated");
    }
  }

  console.log("=".repeat(60));

  await app.close();
}

run().catch(console.error);
