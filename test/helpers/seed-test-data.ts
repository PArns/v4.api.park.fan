import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { Destination } from "../../src/destinations/entities/destination.entity";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import {
  createTestDestination,
  createTestParks,
} from "../fixtures/park.fixtures";
import { createTestAttractions } from "../fixtures/attraction.fixtures";

/**
 * Seeds minimal test data for E2E tests
 * Creates: 1 destination, 2 parks, 10 attractions (5 per park)
 *
 * This is MUCH faster than the production seeder which:
 * - Fetches 105 parks + 4000+ attractions from ThemeParks.wiki API
 * - Triggers Bull queue jobs
 * - Processes weather data
 *
 * Test seeding is:
 * - Predictable (same data every time)
 * - Fast (direct DB insert, no API calls)
 * - Minimal (only what's needed for tests)
 */
export async function seedMinimalTestData(app: INestApplication): Promise<{
  destination: Destination;
  parks: Park[];
  attractions: Attraction[];
}> {
  const dataSource = app.get(DataSource);

  // 1. Create destination
  const destinationRepo = dataSource.getRepository(Destination);
  const destinationData = createTestDestination();
  const destination = await destinationRepo.save(destinationData);

  // 2. Create 2 parks
  const parkRepo = dataSource.getRepository(Park);
  const parksData = createTestParks();
  parksData[0].destinationId = destination.id;
  parksData[1].destinationId = destination.id;
  const parks = await parkRepo.save(parksData);

  // 3. Create 5 attractions per park (10 total)
  const attractionRepo = dataSource.getRepository(Attraction);
  const allAttractions: Attraction[] = [];

  for (let i = 0; i < parks.length; i++) {
    const attractionsData = createTestAttractions(parks[i].id, i);
    const attractions = await attractionRepo.save(attractionsData);
    allAttractions.push(...attractions);
  }

  console.log(
    `✅ Test data seeded: 1 destination, ${parks.length} parks, ${allAttractions.length} attractions`,
  );

  return {
    destination,
    parks,
    attractions: allAttractions,
  };
}

/**
 * Clears all data from test database
 * Truncates tables in correct order (respects foreign keys)
 */
export async function clearTestData(app: INestApplication): Promise<void> {
  const dataSource = app.get(DataSource);

  const entities = dataSource.entityMetadatas;

  // Truncate tables in reverse order (children before parents)
  const tableNames = entities
    .map((entity) => `"${entity.tableName}"`)
    .reverse();

  for (const tableName of tableNames) {
    try {
      await dataSource.query(`TRUNCATE ${tableName} CASCADE;`);
    } catch (error) {
      // Ignore errors for tables that don't exist yet
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.warn(`Warning: Could not truncate ${tableName}:`, errorMessage);
    }
  }

  console.log("🧹 Test data cleared");
}
