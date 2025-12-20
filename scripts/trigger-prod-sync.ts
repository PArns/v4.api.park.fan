import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { Queue } from "bull";

async function run() {
  console.log("🔄 Connecting to LIVE production...");
  const app = await NestFactory.createApplicationContext(AppModule);
  const queue: Queue = app.get("BullQueue_park-metadata");

  console.log("📋 Triggering park-metadata sync on LIVE production...");
  const job = await queue.add(
    "sync-park-mappings",
    { forceSync: true },
    { priority: 1 },
  );
  console.log(`✅ Job queued on LIVE: ${job.id}`);
  console.log(
    '⏳ Job will execute shortly. Monitor logs for "Mapping conflict" messages.',
  );

  await app.close();
}

run().catch(console.error);
