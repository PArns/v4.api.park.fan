import { Test, TestingModule } from "@nestjs/testing";
import { WartezeitenClient } from "./wartezeiten.client";
import { WartezeitenDataSource } from "./wartezeiten-data-source";
import { EntityType } from "../data-sources/interfaces/data-source.interface";

// Simple integration test - comment out if running full test suite
describe.skip("WartezeitenDataSource (Integration)", () => {
  let dataSource: WartezeitenDataSource;
  let client: WartezeitenClient;

  beforeAll(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [WartezeitenClient, WartezeitenDataSource],
    }).compile();

    client = module.get<WartezeitenClient>(WartezeitenClient);
    dataSource = module.get<WartezeitenDataSource>(WartezeitenDataSource);
  });

  it("should be defined", () => {
    expect(dataSource).toBeDefined();
    expect(client).toBeDefined();
  });

  it("should have correct properties", () => {
    expect(dataSource.name).toBe("wartezeiten-app");
    expect(dataSource.completeness).toBe(6);
  });

  it("should support only attractions", () => {
    expect(dataSource.supportsEntityType(EntityType.ATTRACTION)).toBe(true);
    expect(dataSource.supportsEntityType(EntityType.SHOW)).toBe(false);
    expect(dataSource.supportsEntityType(EntityType.RESTAURANT)).toBe(false);
  });

  it("should have correct data richness", () => {
    const richness = dataSource.getDataRichness();

    expect(richness.hasSchedules).toBe(false);
    expect(richness.hasShows).toBe(false);
    expect(richness.hasRestaurants).toBe(false);
    expect(richness.hasLands).toBe(false);
    expect(richness.hasForecasts).toBe(false);
    expect(richness.hasMultipleQueueTypes).toBe(false);
  });

  describe("Live API Tests", () => {
    it("should fetch all parks", async () => {
      const parks = await dataSource.fetchAllParks();

      expect(parks.length).toBeGreaterThan(0);
      expect(parks[0]).toHaveProperty("externalId");
      expect(parks[0]).toHaveProperty("source", "wartezeiten-app");
      expect(parks[0]).toHaveProperty("name");

      console.log(`✅ Fetched ${parks.length} parks`);
      console.log("Sample park:", parks[0]);
    }, 15000);

    it("should fetch live data for Phantasialand", async () => {
      // Use Phantasialand UUID
      const parkUuid = "3a48bc99-3a51-4730-9fb1-be485f0c2742";

      const liveData = await dataSource.fetchParkLiveData(parkUuid);

      expect(liveData.source).toBe("wartezeiten-app");
      expect(liveData.parkExternalId).toBe(parkUuid);
      expect(liveData.entities.length).toBeGreaterThan(0);
      expect(liveData.crowdLevel).toBeDefined();

      const firstEntity = liveData.entities[0];
      expect(firstEntity).toHaveProperty("externalId");
      expect(firstEntity).toHaveProperty("name");
      expect(firstEntity).toHaveProperty("status");
      expect(firstEntity.entityType).toBe(EntityType.ATTRACTION);

      console.log(`✅ Fetched ${liveData.entities.length} entities`);
      console.log(`✅ Crowd level: ${liveData.crowdLevel}`);
      console.log("Sample entity:", firstEntity);
    }, 15000);

    it("should fetch park entities for matching", async () => {
      const parkUuid = "3a48bc99-3a51-4730-9fb1-be485f0c2742";

      const entities = await dataSource.fetchParkEntities(parkUuid);

      expect(entities.length).toBeGreaterThan(0);
      expect(entities[0]).toHaveProperty("externalId");
      expect(entities[0]).toHaveProperty("source", "wartezeiten-app");
      expect(entities[0]).toHaveProperty("name");
      expect(entities[0].entityType).toBe(EntityType.ATTRACTION);

      console.log(`✅ Fetched ${entities.length} entities for matching`);
    }, 15000);

    it("should check health successfully", async () => {
      const isHealthy = await dataSource.isHealthy();

      expect(isHealthy).toBe(true);
      console.log("✅ Health check passed");
    }, 10000);
  });
});
