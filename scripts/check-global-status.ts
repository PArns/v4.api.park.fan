
import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { DataSource } from "typeorm";

async function run() {
  console.log("🚀 Checking Global Park Status...");

  const app = await NestFactory.createApplicationContext(AppModule);
  const dataSource = app.get(DataSource);

  // Check specific major parks
  const majorParks = ['Magic Kingdom', 'Europa-Park', 'Disneyland Park', 'Universal Studios Florida', 'Phantasialand'];

  for (const name of majorParks) {
    const park = await dataSource.query(`SELECT id, name, timezone FROM parks WHERE name = $1`, [name]);
    if (park.length > 0) {
      const p = park[0];
      console.log(`\n=== ${p.name} (${p.timezone}) ===`);

      // Check schedule for today
      const schedule = await dataSource.query(`
            SELECT date, "scheduleType", "openingTime", "closingTime", "description" 
            FROM schedule_entries 
            WHERE "parkId" = $1 
            AND date >= NOW() - INTERVAL '3 days'
            AND date <= NOW() + INTERVAL '2 days'
            ORDER BY date ASC
          `, [p.id]);
      console.table(schedule);
    } else {
      console.log(`\n❌ Park not found: ${name}`);
    }
  }

  await app.close();
}

run();
