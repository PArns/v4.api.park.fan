import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { MultiSourceOrchestrator } from "../src/external-apis/data-sources/multi-source-orchestrator.service";
import { Repository } from "typeorm";
import { Park } from "../src/parks/entities/park.entity";
import { getRepositoryToken } from "@nestjs/typeorm";

/**
 * Quick test: Fetch Phantasialand live data from all 3 sources
 * and verify crowd level is saved
 */
async function testWartezeiten() {
  console.log("🧪 Testing Wartezeiten Integration\n");

  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ["log", "error", "warn"],
  });

  const orchestrator = app.get(MultiSourceOrchestrator);
  const parkRepo = app.get<Repository<Park>>(getRepositoryToken(Park));

  // Find Phantasialand
  const phantasialand = await parkRepo.findOne({
    where: { name: "Phantasialand" },
  });

  if (!phantasialand) {
    console.log("❌ Phantasialand not found in database");
    await app.close();
    return;
  }

  console.log(`✅ Found: ${phantasialand.name} (${phantasialand.id})`);
  console.log(`   Wiki ID: ${phantasialand.wikiEntityId || "N/A"}`);
  console.log(`   QT ID: ${phantasialand.queueTimesEntityId || "N/A"}`);
  console.log(
    `   Wartezeiten ID: ${phantasialand.wartezeitenEntityId || "N/A"}\n`,
  );

  // Build external ID map
  const parkExternalIdMap = new Map<string, string>();
  if (phantasialand.wikiEntityId) {
    parkExternalIdMap.set("themeparks-wiki", phantasialand.wikiEntityId);
  }
  if (phantasialand.queueTimesEntityId) {
    parkExternalIdMap.set("queue-times", phantasialand.queueTimesEntityId);
  }

  // Auto-seed Wartezeiten ID if missing (for testing)
  let wzId = phantasialand.wartezeitenEntityId;
  if (!wzId) {
    console.log("🛠️  Seeding Wartezeiten ID for testing...");
    wzId = "3a48bc99-3a51-4730-9fb1-be485f0c2742";
    await parkRepo.update(phantasialand.id, {
      wartezeitenEntityId: wzId,
    });
    console.log(`✅ Seeded Wartezeiten ID: ${wzId}`);
  }

  if (wzId) {
    parkExternalIdMap.set("wartezeiten-app", wzId);
  }

  console.log("📡 Fetching live data from all sources...");
  const liveData = await orchestrator.fetchParkLiveData(
    phantasialand.id,
    parkExternalIdMap,
  );

  console.log(`\n✅ Live Data Response:`);
  console.log(`   Source: ${liveData.source}`);
  console.log(`   Entities: ${liveData.entities.length}`);
  console.log(`   Lands: ${liveData.lands?.length || 0}`);
  console.log(`   Crowd Level: ${liveData.crowdLevel ?? "N/A"}`);

  if (liveData.operatingHours && liveData.operatingHours.length > 0) {
    const hours = liveData.operatingHours[0];
    console.log(
      `   🕒 Operating Hours: ${hours.open} - ${hours.close} (${hours.type})`,
    );
  } else {
    console.log(`   🕒 Operating Hours: N/A`);
  }

  // Show first few entities
  console.log(`\n📊 Sample Entities (first 5):`);
  for (let i = 0; i < Math.min(5, liveData.entities.length); i++) {
    const entity = liveData.entities[i];
    console.log(
      `   ${i + 1}. ${entity.name} - ${entity.waitTime || 0} min (${entity.status}) [${entity.source || "unknown"}]`,
    );
  }

  // Save crowd level if available
  if (liveData.crowdLevel !== undefined && liveData.crowdLevel !== null) {
    console.log(`\n💾 Saving crowd level: ${liveData.crowdLevel.toFixed(1)}`);
    await parkRepo.update(phantasialand.id, {
      currentCrowdLevel: liveData.crowdLevel,
    });

    // Verify
    const updated = await parkRepo.findOne({ where: { id: phantasialand.id } });
    const level =
      updated?.currentCrowdLevel !== null
        ? Number(updated?.currentCrowdLevel)
        : 0;
    console.log(`✅ Verified: ${level.toFixed(1)}`);
  } else {
    console.log("\n⚠️  No crowd level data from Wartezeiten");
  }

  console.log("\n✅ Test complete!");
  await app.close();
}

testWartezeiten().catch((error) => {
  console.error("❌ Test failed:", error);
  process.exit(1);
});
