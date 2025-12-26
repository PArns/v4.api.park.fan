import { Test, TestingModule } from "@nestjs/testing";
import { DiscoveryService } from "./discovery.service";
import { getRepositoryToken } from "@nestjs/typeorm";
import { Park } from "../parks/entities/park.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("DiscoveryService Deduplication", () => {
  let service: DiscoveryService;

  const mockParkRepository = {
    find: jest.fn(),
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
        attractions: [],
      },
      {
        id: "2",
        name: "Parc Astérix",
        slug: "parc-asterix",
        continent: "Europe",
        continentSlug: "europe",
        country: "France",
        countrySlug: "fr", // Different slug, same name
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
});
