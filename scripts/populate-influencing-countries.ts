import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { ParksService } from "../src/parks/parks.service";
import { COUNTRY_INFLUENCES } from "../src/common/country-influences";

async function main() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const parksService = app.get(ParksService);

  console.log("🌍 Populating influencing countries for parks...\n");

  const parks = await parksService.findAll();
  let updated = 0;

  for (const park of parks) {
    if (park.country) {
      const influences = COUNTRY_INFLUENCES[park.country];

      if (influences && influences.length > 0) {
        // Take top 3 most important neighbors
        const topInfluences = influences.slice(0, 3);

        await parksService.update(park.id, {
          influencingCountries: topInfluences,
        });

        console.log(
          `✅ ${park.name} (${park.country}): ${topInfluences.join(", ")}`,
        );
        updated++;
      }
    }
  }

  console.log(`\n🎉 Updated ${updated} parks with influencing countries!`);
  await app.close();
}

main().catch(console.error);
