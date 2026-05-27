import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { SearchService } from "./search.service";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { ParksService } from "../parks/parks.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { ShowsService } from "../shows/shows.service";
import { PopularityService } from "../popularity/popularity.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

// Chainable QueryBuilder mock — every builder method returns the same object
// so the fluent chain works; getMany resolves whatever a test configures.
function createQueryBuilderMock() {
  const qb: Record<string, jest.Mock> = {};
  const chainMethods = [
    "select",
    "leftJoin",
    "leftJoinAndSelect",
    "where",
    "andWhere",
    "orWhere",
    "orderBy",
    "addOrderBy",
    "setParameter",
    "limit",
    "offset",
    "skip",
    "take",
  ];
  for (const m of chainMethods) qb[m] = jest.fn().mockReturnValue(qb);
  qb.getMany = jest.fn().mockResolvedValue([]);
  qb.getCount = jest.fn().mockResolvedValue(0);
  return qb;
}

describe("SearchService", () => {
  let service: SearchService;

  const parkQB = createQueryBuilderMock();
  const attractionQB = createQueryBuilderMock();
  const showQB = createQueryBuilderMock();
  const restaurantQB = createQueryBuilderMock();
  const scheduleQB = createQueryBuilderMock();

  const mockParkRepository = { createQueryBuilder: jest.fn(() => parkQB) };
  const mockAttractionRepository = {
    createQueryBuilder: jest.fn(() => attractionQB),
  };
  const mockShowRepository = { createQueryBuilder: jest.fn(() => showQB) };
  const mockRestaurantRepository = {
    createQueryBuilder: jest.fn(() => restaurantQB),
  };
  const mockScheduleRepository = {
    findOne: jest.fn(),
    createQueryBuilder: jest.fn(() => scheduleQB),
  };

  const mockPipeline = {
    set: jest.fn().mockReturnThis(),
    exec: jest.fn().mockResolvedValue([]),
  };
  const mockRedis = {
    get: jest.fn().mockResolvedValue(null),
    set: jest.fn().mockResolvedValue("OK"),
    mget: jest.fn().mockResolvedValue([]),
    pipeline: jest.fn(() => mockPipeline),
  };

  const mockParksService = {
    getBatchParkStatus: jest.fn().mockResolvedValue(new Map()),
  };
  const mockAnalyticsService = {
    getBatchAttractionP50s: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionP90Baselines: jest.fn().mockResolvedValue(new Map()),
    getBatchParkOccupancy: jest.fn().mockResolvedValue(new Map()),
    getAttractionCrowdLevel: jest.fn().mockReturnValue("moderate"),
    getParkCrowdLevel: jest.fn().mockReturnValue("moderate"),
  };
  const mockQueueDataService = {
    findCurrentStatusByAttraction: jest.fn(),
    findCurrentStatusByAttractionIds: jest.fn().mockResolvedValue(new Map()),
  };
  const mockShowsService = {
    findBatchCurrentStatusByShows: jest.fn().mockResolvedValue(new Map()),
  };
  const mockPopularityService = {
    recordParkHit: jest.fn(),
    recordAttractionHit: jest.fn(),
    recordSearchHit: jest.fn(),
    getTopParks: jest.fn().mockResolvedValue([]),
    getTopAttractions: jest.fn().mockResolvedValue([]),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SearchService,
        { provide: getRepositoryToken(Park), useValue: mockParkRepository },
        {
          provide: getRepositoryToken(Attraction),
          useValue: mockAttractionRepository,
        },
        { provide: getRepositoryToken(Show), useValue: mockShowRepository },
        {
          provide: getRepositoryToken(Restaurant),
          useValue: mockRestaurantRepository,
        },
        {
          provide: getRepositoryToken(ScheduleEntry),
          useValue: mockScheduleRepository,
        },
        { provide: ParksService, useValue: mockParksService },
        { provide: AnalyticsService, useValue: mockAnalyticsService },
        { provide: QueueDataService, useValue: mockQueueDataService },
        { provide: ShowsService, useValue: mockShowsService },
        { provide: PopularityService, useValue: mockPopularityService },
        { provide: REDIS_CLIENT, useValue: mockRedis },
      ],
    }).compile();

    service = module.get<SearchService>(SearchService);

    jest.clearAllMocks();

    // Restore default behaviours after clearing call history.
    parkQB.getMany.mockResolvedValue([]);
    attractionQB.getMany.mockResolvedValue([]);
    showQB.getMany.mockResolvedValue([]);
    restaurantQB.getMany.mockResolvedValue([]);
    scheduleQB.getMany.mockResolvedValue([]);
    mockRedis.get.mockResolvedValue(null);
    mockRedis.mget.mockResolvedValue([]);
    mockRedis.pipeline.mockReturnValue(mockPipeline);
    mockParksService.getBatchParkStatus.mockResolvedValue(new Map());
    mockAnalyticsService.getBatchParkOccupancy.mockResolvedValue(new Map());
    mockAnalyticsService.getBatchAttractionP50s.mockResolvedValue(new Map());
    mockAnalyticsService.getBatchAttractionP90Baselines.mockResolvedValue(
      new Map(),
    );
    mockAnalyticsService.getAttractionCrowdLevel.mockReturnValue("moderate");
    mockAnalyticsService.getParkCrowdLevel.mockReturnValue("moderate");
    mockQueueDataService.findCurrentStatusByAttractionIds.mockResolvedValue(
      new Map(),
    );
    mockShowsService.findBatchCurrentStatusByShows.mockResolvedValue(new Map());
    mockPopularityService.getTopParks.mockResolvedValue([]);
    mockPopularityService.getTopAttractions.mockResolvedValue([]);
  });

  const park = (overrides: Record<string, unknown> = {}) => ({
    id: "p1",
    name: "Phantasialand",
    slug: "phantasialand",
    latitude: 50.8,
    longitude: 6.88,
    continent: "Europe",
    country: "Germany",
    countryCode: "DE",
    city: "Bruhl",
    continentSlug: "europe",
    countrySlug: "germany",
    citySlug: "bruhl",
    timezone: "Europe/Berlin",
    destination: null,
    ...overrides,
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("search", () => {
    it("should return empty results when no matches found", async () => {
      const result = await service.search({ q: "test", type: ["park"] });

      expect(result.query).toBe("test");
      expect(result.results).toEqual([]);
      expect(result.counts.park.returned).toBe(0);
      expect(result.counts.park.total).toBe(0);
    });

    it("returns cached payload without touching the DB", async () => {
      const cachedPayload = { query: "disney", results: [], counts: {} };
      mockRedis.get.mockResolvedValueOnce(JSON.stringify(cachedPayload));

      const result = await service.search({ q: "disney", type: ["park"] });

      expect(result).toEqual(cachedPayload);
      expect(parkQB.getMany).not.toHaveBeenCalled();
      expect(mockParksService.getBatchParkStatus).not.toHaveBeenCalled();
    });

    it("deduplicates same-named attractions under the same park and only enriches survivors", async () => {
      const parkRef = park();
      attractionQB.getMany.mockResolvedValueOnce([
        { id: "a1", name: "Taron", slug: "taron", landName: null, park: parkRef },
        { id: "a2", name: "Taron", slug: "taron", landName: null, park: parkRef },
      ]);
      mockParksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "OPERATING"]]),
      );

      const result = await service.search({ q: "taron", type: ["attraction"] });

      // Both raw matches counted, but only one survives dedup.
      expect(result.counts.attraction.total).toBe(2);
      expect(result.counts.attraction.returned).toBe(1);
      expect(result.results).toHaveLength(1);
      expect(result.results[0].id).toBe("a1");

      // Enrichment ran for the single survivor only, not both raw matches.
      expect(
        mockQueueDataService.findCurrentStatusByAttractionIds,
      ).toHaveBeenCalledWith(["a1"]);
    });

    it("ranks OPERATING parks ahead of closed ones", async () => {
      parkQB.getMany.mockResolvedValueOnce([
        park({ id: "pClosed", name: "Closed Park", slug: "closed" }),
        park({ id: "pOpen", name: "Open Park", slug: "open" }),
      ]);
      mockParksService.getBatchParkStatus.mockResolvedValue(
        new Map([
          ["pClosed", "CLOSED"],
          ["pOpen", "OPERATING"],
        ]),
      );

      const result = await service.search({ q: "park", type: ["park"] });

      expect(result.results).toHaveLength(2);
      expect(result.results[0].id).toBe("pOpen");
      expect(result.results[0].status).toBe("OPERATING");
      expect(result.results[1].id).toBe("pClosed");
    });
  });
});
