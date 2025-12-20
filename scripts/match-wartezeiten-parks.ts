import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { WartezeitenDataSource } from "../src/external-apis/wartezeiten/wartezeiten-data-source";
import { Repository } from "typeorm";
import { Park } from "../src/parks/entities/park.entity";
import { getRepositoryToken } from "@nestjs/typeorm";
import { Logger } from "@nestjs/common";

/**
 * Normalizes a string for comparison
 */
function normalize(str: string): string {
  return str
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "") // remove all non-alphanumeric
    .trim();
}

async function matchParks() {
  const logger = new Logger("WartezeitenMatcher");
  logger.log("🔄 Starting Wartezeiten Park Matching...");

  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ["log", "error", "warn"],
  });

  try {
    const parkRepo = app.get<Repository<Park>>(getRepositoryToken(Park));
    const wartezeitenSource = app.get(WartezeitenDataSource);

    // 1. Fetch all local parks
    const localParks = await parkRepo.find();
    logger.log(`📚 Found ${localParks.length} local parks in DB`);

    // 2. Fetch all Wartezeiten parks
    logger.log("📡 [Wartezeiten API] Fetching parks list...");
    const wzParks = await wartezeitenSource.fetchAllParks();
    logger.log(`🌍 [Wartezeiten API] Found ${wzParks.length} parks available`);

    let matchCount = 0;
    let updateCount = 0;

    // 3. Match and Update
    for (const wzPark of wzParks) {
      if (!wzPark.name) continue;

      const normalizedWzName = normalize(wzPark.name);

      // Find match in local parks
      let match = localParks.find((p) => p.name === wzPark.name);

      if (!match) {
        match = localParks.find((p) => normalize(p.name) === normalizedWzName);
      }

      // Try specific known mappings if auto-match fails
      if (!match) {
        if (wzPark.name === "Disney's Magic Kingdom")
          match = localParks.find((p) => p.name === "Magic Kingdom Park");
        if (wzPark.name === "Disney's Animal Kingdom")
          match = localParks.find(
            (p) => p.name === "Disney's Animal Kingdom Theme Park",
          );
        if (wzPark.name === "Disney's Hollywood Studios")
          match = localParks.find(
            (p) => p.name === "Disney's Hollywood Studios",
          );
        if (wzPark.name === "Universal Studios Florida")
          match = localParks.find(
            (p) => p.name === "Universal Studios Florida",
          );
        if (wzPark.name === "Universal's Islands of Adventure")
          match = localParks.find((p) => p.name === "Islands of Adventure");
      }

      if (match) {
        matchCount++;

        // Update if ID is missing or different
        if (match.wartezeitenEntityId !== wzPark.externalId) {
          await parkRepo.update(match.id, {
            wartezeitenEntityId: wzPark.externalId,
          });
          logger.log(
            `✅ [DB UPDATE] Matched: "${match.name}" ↔️ "${wzPark.name}" (Wartezeiten ID: ${wzPark.externalId})`,
          );
          updateCount++;
        }
      }
    }

    logger.log("\n📊 MATCHING SUMMARY 📊");
    logger.log(`Matched Parks: ${matchCount} / ${wzParks.length}`);
    logger.log(`Updated Records: ${updateCount}`);

    // 4. Check for Crowd Levels (Live Data Check)
    if (matchCount > 0) {
      logger.log(
        "\n🧪 [Live Data Check] Verifying data gain for updated parks (First 10 distinct matches)...",
      );

      // Fetch fresh from DB to get updated IDs
      const updatedParks = await parkRepo.find({
        where: {
          wartezeitenEntityId: require("typeorm").Not(
            require("typeorm").IsNull(),
          ),
        },
      });

      let retrievedCrowdLevels = 0;
      let processed = 0;

      for (const park of updatedParks) {
        if (processed >= 10) break;
        processed++;

        try {
          logger.log(
            `   📡 [Wartezeiten API] Fetching live data for "${park.name}"...`,
          );
          const live = await wartezeitenSource.fetchParkLiveData(
            park.wartezeitenEntityId!,
          );

          const entityCount = live.entities.length;
          const crowd = live.crowdLevel;

          if (crowd !== undefined) {
            retrievedCrowdLevels++;
            logger.log(`   🎉 [DATA GAIN] Crowd Level: ${crowd.toFixed(1)}`);
          } else {
            logger.log(`   🔸 [DATA GAIN] Crowd Level: N/A`);
          }

          // Fetch Opening Times (using 'any' to access private client for verification)
          try {
            const client = (wartezeitenSource as any).client;
            const opening = await client.getOpeningTimes(
              park.wartezeitenEntityId!,
            );
            if (opening && opening.length > 0) {
              const hours = opening[0];
              const status = hours.opened_today ? "OPEN" : "CLOSED";
              const timeRange = hours.opened_today
                ? `(${hours.open_from} - ${hours.closed_from})`
                : "";
              logger.log(
                `   🎉 [DATA GAIN] Opening Times: ${status} ${timeRange}`,
              );
            } else {
              logger.log(`   🔸 [DATA GAIN] Opening Times: None`);
            }
          } catch (err) {
            logger.log(`   🔸 [DATA GAIN] Opening Times: Fetch failed`);
          }

          if (entityCount > 0) {
            logger.log(
              `   🎉 [DATA GAIN] Wait Times: ${entityCount} attractions`,
            );
            // Show sample
            const sample = live.entities[0];
            logger.log(
              `      > Sample: "${sample.name}" = ${sample.waitTime ?? 0} min (${sample.status})`,
            );
          } else {
            logger.log(`   🔸 [DATA GAIN] Wait Times: None`);
          }

          // small delay to be nice to API
          await new Promise((r) => setTimeout(r, 500));
        } catch (e) {
          logger.error(
            `   ❌ [ERROR] Failed to fetch live data for ${park.name}`,
          );
        }
      }
    }
  } catch (error) {
    logger.error(error);
  } finally {
    await app.close();
  }
}

matchParks();

matchParks();
