import { Test, TestingModule } from "@nestjs/testing";
import { DiscoveryService } from "./discovery.service";
import { getRepositoryToken } from "@nestjs/typeorm";
import { Park } from "../parks/entities/park.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { ParksService } from "../parks/parks.service";

describe("DiscoveryService Deduplication", () => {
  let service: DiscoveryService;

  // QueryBuilder mock that supports the chained API used in getGeoStructure()
  const makeQbMock = (parks: object[]) => ({
    select: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    loadRelationCountAndMap: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    addOrderBy: jest.fn().mockReturnThis(),
    getMany: jest.fn().mockResolvedValue(parks),
  });

  const mockParkRepository = {
    createQueryBuilder: jest.fn(),
    // Used by getLiveStats()
    query: jest.fn().mockResolvedValue([]),
  };

  const mockParksService = {
    getBatchSchedules: jest
      .fn()
      .mockResolvedValue({ today: new Map(), next: new Map() }),
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
          provide: ParksService,
          useValue: mockParksService,
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
    const parkData = [
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
        attractionCount: 0,
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
        attractionCount: 0,
      },
    ];

    mockRedis.get.mockResolvedValue(null);
    mockParkRepository.createQueryBuilder.mockReturnValue(makeQbMock(parkData));

    const result = await service.getGeoStructure();
    const europe = result.continents.find((c) => c.slug === "europe");

    expect(europe).toBeDefined();
    if (!europe) return;

    // Expect only one country entry for "France"
    expect(europe.countries.length).toBe(1);
    expect(europe.countries[0].name).toBe("France");

    // Expect both parks to be in that single country entry
    expect(europe.countries[0].parkCount).toBe(2);
  });
});
