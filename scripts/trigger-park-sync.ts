import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { ParksService } from "../src/parks/parks.service";

async function run() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const parksService = app.get(ParksService);

  console.log("Triggering Park Sync...");
  await parksService.syncParks();
  console.log("Sync Complete.");

  await app.close();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
