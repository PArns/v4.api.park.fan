import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { ParksService } from "../src/parks/parks.service";

async function run() {
  console.log("🚀 Initializing Repair Script against DB:", process.env.DB_HOST);

  const app = await NestFactory.createApplicationContext(AppModule);
  const parksService = app.get(ParksService);

  console.log("🔧 Triggering Repair Duplicates...");
  await parksService.repairDuplicates();
  console.log("✅ Repair Complete.");

  await app.close();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
