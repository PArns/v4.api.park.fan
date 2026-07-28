import { Test, TestingModule } from "@nestjs/testing";
import { DiscoveryService } from "./discovery.service";
import { getRepositoryToken } from "@nestjs/typeorm";
import { Park } from "../parks/entities/park.entity";
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { ParksService } from "../parks/parks.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("DiscoveryService Deduplication", () => {
  let service: DiscoveryService;

  const mockParkRepository = {
    find: jest.fn(),
    query: jest.fn().mockResolvedValue([]),
  };

  const mockRedis = {
    get: jest.fn(),
    setex: jest.fn(),
    del: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DiscoveryService,
        {
          provide: getRepositoryToken(Park),
          useValue: mockParkRepository,
        },
        {
          provide: getRepositoryToken(ParkDailyStats),
          useValue: {
            find: jest.fn().mockResolvedValue([]),
            findOne: jest.fn().mockResolvedValue(null),
          },
        },
        {
          provide: ParksService,
          useValue: {
            getBatchParkStatus: jest.fn().mockResolvedValue(new Map()),
            getBatchHasOperatingSchedule: jest
              .fn()
              .mockResolvedValue(new Map()),
            getBatchSchedules: jest
              .fn()
              .mockResolvedValue({ today: new Map(), next: new Map() }),
          },
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
      ],
    }).compile();

    service = module.get<DiscoveryService>(DiscoveryService);
  });

  it("should merge countries with same name but different slugs", async () => {
    mockRedis.get.mockResolvedValue(null);
    mockParkRepository.find.mockResolvedValue([
      {
        id: "1",
        name: "Disneyland Paris",
        slug: "disneyland-paris",
        continent: "Europe",
        continentSlug: "europe",
        country: "France",
        countrySlug: "france",
        city: "Marne-la-Vallée",
        citySlug: "marne-la-vallee",
        countryCode: "FR",
        attractions: [],
      },
      {
        id: "2",
        name: "Parc Astérix",
        slug: "parc-asterix",
        continent: "Europe",
        continentSlug: "europe",
        country: "FR", // Different name (simulating DB inconsistency)
        countrySlug: "fr",
        countryCode: "FR", // Common key
        city: "Plailly",
        citySlug: "plailly",
        attractions: [],
      },
    ]);

    const result = await service.getGeoStructure();
    const europe = result.continents.find((c) => c.slug === "europe");

    expect(europe).toBeDefined();
    if (!europe) return;

    // Expect only one country entry for "France"
    // This assertion will fail BEFORE the fix
    expect(europe.countries.length).toBe(1);
    expect(europe.countries[0].name).toBe("France");

    // Expect both parks to be in that single country entry
    expect(europe.countries[0].parkCount).toBe(2);
  });

  it("exposes park coordinates as finite numbers, null when unset", async () => {
    mockRedis.get.mockResolvedValue(null);
    mockParkRepository.find.mockResolvedValue([
      {
        id: "1",
        name: "Europa-Park",
        slug: "europa-park",
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Rust",
        citySlug: "rust",
        // `decimal` columns arrive as strings from the pg driver.
        latitude: "48.2682000",
        longitude: "7.7216000",
        attractions: [],
      },
      {
        id: "2",
        name: "Never Geocoded",
        slug: "never-geocoded",
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Nowhere",
        citySlug: "nowhere",
        latitude: null,
        longitude: null,
        attractions: [],
      },
    ]);

    const result = await service.getGeoStructure();
    const germany = result.continents
      .find((c) => c.slug === "europe")
      ?.countries.find((c) => c.slug === "germany");

    const geocoded = germany?.cities.find((c) => c.slug === "rust")?.parks[0];
    expect(geocoded?.latitude).toBe(48.2682);
    expect(geocoded?.longitude).toBe(7.7216);

    const ungeocoded = germany?.cities.find((c) => c.slug === "nowhere")
      ?.parks[0];
    expect(ungeocoded?.latitude).toBeNull();
    expect(ungeocoded?.longitude).toBeNull();
  });
});
