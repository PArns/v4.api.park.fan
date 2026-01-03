import request from "supertest";
import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import { AppModule } from "../../src/app.module";
import { Repository } from "typeorm";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParksService } from "../../src/parks/parks.service";
import { QueueDataService } from "../../src/queue-data/queue-data.service";
import { AnalyticsService } from "../../src/analytics/analytics.service";
import { ShowsService } from "../../src/shows/shows.service";

describe("Search Fuzzy (E2E)", () => {
  let app: INestApplication;
  let parkRepository: Repository<Park>;
  let attractionRepository: Repository<Attraction>;

  // Mocks to prevent background syncs and deadlocks
  const mockParksService = {
    syncParks: jest.fn().mockResolvedValue(0),
    getBatchParkStatus: jest.fn().mockResolvedValue(new Map()),
    findAll: jest.fn().mockResolvedValue([]),
    getSyncCountryCodes: jest.fn().mockResolvedValue([]),
    saveScheduleData: jest.fn().mockResolvedValue(undefined),
    findParksWithoutGeodata: jest.fn().mockResolvedValue([]),
    fillScheduleGaps: jest.fn().mockResolvedValue(undefined),
    onModuleInit: jest.fn(),
  };

  const mockQueueDataService = {
    findCurrentStatusByAttractionIds: jest.fn().mockResolvedValue(new Map()),
    findCurrentStatusByAttraction: jest.fn().mockResolvedValue([]),
    onModuleInit: jest.fn(),
  };

  const mockAnalyticsService = {
    calculateParkOccupancy: jest.fn().mockResolvedValue({ current: 0 }),
  };

  const mockShowsService = {
    findCurrentStatusByShow: jest.fn().mockResolvedValue(null),
    syncShows: jest.fn().mockResolvedValue(0),
  };

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(ParksService)
      .useValue(mockParksService)
      .overrideProvider(QueueDataService)
      .useValue(mockQueueDataService)
      .overrideProvider(AnalyticsService)
      .useValue(mockAnalyticsService)
      .overrideProvider(ShowsService)
      .useValue(mockShowsService)
      .compile();

    app = moduleFixture.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
      }),
    );
    await app.init();

    parkRepository = moduleFixture.get<Repository<Park>>(
      getRepositoryToken(Park),
    );
    attractionRepository = moduleFixture.get<Repository<Attraction>>(
      getRepositoryToken(Attraction),
    );
  });

  // Seed data before EACH test because setup-e2e.ts truncates tables after each test
  beforeEach(async () => {
    await seedTestData();
  });

  afterAll(async () => {
    await app.close();
  });

  async function seedTestData() {
    try {
      // 1. Phantasialand (Park)
      const phantasialand = await parkRepository.save(
        parkRepository.create({
          name: "Phantasialand",
          slug: "phantasialand",
          externalId: "phantasialand",
          timezone: "Europe/Berlin",
          city: "Brühl",
          country: "Germany",
          countryCode: "DE",
        }),
      );

      // 2. Attractions in Phantasialand
      await attractionRepository.save([
        attractionRepository.create({
          name: "F.L.Y.",
          slug: "fly",
          externalId: "fly",
          park: phantasialand,
          landName: "Rookburgh",
        }),
        attractionRepository.create({
          name: "Taron",
          slug: "taron",
          externalId: "taron",
          park: phantasialand,
          landName: "Klugheim",
        }),
        attractionRepository.create({
          name: "Black Mamba",
          slug: "black-mamba",
          externalId: "black-mamba",
          park: phantasialand,
          landName: "Deep in Africa",
        }),
        // Distractors for "fly" search
        attractionRepository.create({
          name: "Flying Eagle",
          slug: "flying-eagle",
          externalId: "flying-eagle",
          park: phantasialand,
          landName: "Fantasy",
        }),
        attractionRepository.create({
          name: "Flying Ninjago",
          slug: "flying-ninjago",
          externalId: "flying-ninjago",
          park: phantasialand,
          landName: "Mystery",
        }),
      ]);

      // 3. Walt Disney World - Magic Kingdom (Park) - for Orlando test
      await parkRepository.save(
        parkRepository.create({
          name: "Magic Kingdom",
          slug: "magic-kingdom",
          externalId: "magic-kingdom",
          timezone: "America/New_York",
          city: "Orlando",
          country: "United States",
          countryCode: "US",
          region: "Florida",
        }),
      );
    } catch (e) {
      console.error("❌ Seeding failed:", e);
    }
  }

  describe("Fuzzy Search Scenarios", () => {
    it('should find "F.L.Y." when searching for "fly" and Rank it FIRST (Generic Search)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "fly" })
        .expect(200);

      const results = response.body.results;
      expect(results.length).toBeGreaterThan(0);

      // Should be FIRST result because "fly" normalized equals "fly" exactly (Rank 1),
      // whereas "Flying..." only matches prefix (Rank 3)
      expect(results[0].name).toBe("F.L.Y.");
      expect(results[0].type).toBe("attraction");
    });

    it('should find "F.L.Y." when searching for "fly" (Normalization/Exact-ish)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "fly", type: "attraction" })
        .expect(200);

      const results = response.body.results;
      expect(results.length).toBeGreaterThan(0);
      expect(results[0].name).toBe("F.L.Y.");
    });

    it('should find "Phantasialand" when searching for "phantasuland" (Typo)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "phantasuland", type: "park" })
        .expect(200);

      const results = response.body.results;
      expect(results.length).toBeGreaterThan(0);
      expect(results[0].name).toBe("Phantasialand");
    });

    it('should find "Taron" when searching for "taron" (Exact)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "taron", type: "attraction" })
        .expect(200);

      const results = response.body.results;
      expect(results.length).toBeGreaterThan(0);
      expect(results[0].name).toBe("Taron");
    });

    it('should find attractions in "Rookburgh" when searching for "rookhburgh" (Land Typo)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "rookhburgh", type: "attraction" })
        .expect(200);

      const results = response.body.results;
      expect(results.length).toBeGreaterThan(0);
      const fly = results.find((r: any) => r.name === "F.L.Y.");
      expect(fly).toBeDefined();
    });

    it('should find attractions in "Rookburgh" when searching for "rokburg" (User Land Typo)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "rokburg", type: "attraction" })
        .expect(200);

      const results = response.body.results;
      expect(results.length).toBeGreaterThan(0);
      const fly = results.find((r: any) => r.name === "F.L.Y.");
      expect(fly).toBeDefined();
    });

    it('should find "Phantasialand" when searching for "fantasialand" (User Typo)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "fantasialand", type: "park" })
        .expect(200);

      expect(response.body.results.length).toBeGreaterThan(0);
      expect(response.body.results[0].name).toBe("Phantasialand");
    });

    it('should find "Phantasialand" when searching for "bruhl" (City - umlaut handling)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "bruhl", type: "park" })
        .expect(200);

      expect(response.body.results.length).toBeGreaterThan(0);
      expect(response.body.results[0].name).toBe("Phantasialand");
    });

    it('should find "Magic Kingdom" when searching for "orlndo" (City Typo)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "orlndo", type: "park" })
        .expect(200);

      const results = response.body.results;
      expect(results.length).toBeGreaterThan(0);
      expect(results[0].name).toBe("Magic Kingdom");
    });

    it('should find "Magic Kingdom" when searching for "orlando" (Exact City)', async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "orlando", type: "park" })
        .expect(200);

      const results = response.body.results;
      expect(results.length).toBeGreaterThan(0);
      expect(results[0].name).toBe("Magic Kingdom");
    });

    it("should return empty results for complete gibberish", async () => {
      const response = await request(app.getHttpServer())
        .get("/search")
        .query({ q: "xyzabc12345", type: "park" })
        .expect(200);

      expect(response.body.results.length).toBe(0);
    });
  });
});
