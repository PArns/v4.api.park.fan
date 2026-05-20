import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { AttractionsService } from "./attractions.service";
import { Attraction } from "./entities/attraction.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { QueueTimesClient } from "../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../external-apis/wartezeiten/wartezeiten.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import { createTestAttraction } from "../../test/fixtures/attraction.fixtures";

describe("AttractionsService", () => {
  let service: AttractionsService;
  let attractionRepository: Repository<Attraction>;

  // Mock repository
  const mockAttractionRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    count: jest.fn(),
    findAndCount: jest.fn().mockResolvedValue([[], 0]),
    createQueryBuilder: jest.fn(() => ({
      leftJoinAndSelect: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
      getOne: jest.fn().mockResolvedValue(null),
    })),
  };

  // Mock services
  const mockThemeParksClient = {
    getEntityChildren: jest.fn(),
    getEntity: jest.fn(),
  };

  const mockThemeParksMapper = {
    mapAttraction: jest.fn(),
  };

  const mockParksService = {
    findAll: jest.fn(),
    syncParks: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AttractionsService,
        {
          provide: getRepositoryToken(Attraction),
          useValue: mockAttractionRepository,
        },
        {
          provide: ThemeParksClient,
          useValue: mockThemeParksClient,
        },
        {
          provide: QueueTimesClient,
          useValue: {
            getParks: jest.fn().mockResolvedValue([]),
            getParkQueueTimes: jest
              .fn()
              .mockResolvedValue({ lands: [], rides: [] }),
          },
        },
        {
          provide: WartezeitenClient,
          useValue: {
            getParks: jest.fn().mockResolvedValue([]),
            getWaitTimes: jest.fn().mockResolvedValue([]),
            getOpeningTimes: jest.fn().mockResolvedValue([]),
          },
        },
        {
          provide: ThemeParksMapper,
          useValue: mockThemeParksMapper,
        },
        {
          provide: ParksService,
          useValue: mockParksService,
        },
      ],
    }).compile();

    service = module.get<AttractionsService>(AttractionsService);
    attractionRepository = module.get<Repository<Attraction>>(
      getRepositoryToken(Attraction),
    );

    // Clear all mocks before each test
    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("findAll", () => {
    it("should return an array of attractions", async () => {
      const testAttractions = [
        createTestAttraction("park-1", {
          name: "Attraction 1",
          slug: "attraction-1",
        }),
        createTestAttraction("park-1", {
          name: "Attraction 2",
          slug: "attraction-2",
        }),
      ];

      mockAttractionRepository.find.mockResolvedValue(testAttractions);

      const result = await service.findAll();

      expect(result).toEqual(testAttractions);
      expect(mockAttractionRepository.find).toHaveBeenCalledTimes(1);
    });

    it("should return empty array when no attractions exist", async () => {
      mockAttractionRepository.find.mockResolvedValue([]);

      const result = await service.findAll();

      expect(result).toEqual([]);
      expect(mockAttractionRepository.find).toHaveBeenCalledTimes(1);
    });
  });

  describe("findBySlug", () => {
    it("should return an attraction by slug", async () => {
      const testAttraction = createTestAttraction("park-1", {
        slug: "test-space-mountain",
      });

      mockAttractionRepository.findOne.mockResolvedValue(testAttraction);

      const result = await service.findBySlug("test-space-mountain");

      expect(result).toEqual(testAttraction);
      expect(mockAttractionRepository.findOne).toHaveBeenCalled();
    });

    it("should return null if attraction not found", async () => {
      mockAttractionRepository.findOne.mockResolvedValue(null);

      const result = await service.findBySlug("non-existent");

      expect(result).toBeNull();
      expect(mockAttractionRepository.findOne).toHaveBeenCalled();
    });
  });

  describe("findByParkId", () => {
    // findByParkId now returns a {data, total} pagination shape via
    // findAndCount, not a raw array via find().
    it("should return paginated attractions for a given park", async () => {
      const parkId = "park-123";
      const testAttractions = [
        createTestAttraction(parkId, { name: "Attraction 1" }),
        createTestAttraction(parkId, { name: "Attraction 2" }),
      ];

      mockAttractionRepository.findAndCount.mockResolvedValue([
        testAttractions,
        2,
      ]);

      const result = await service.findByParkId(parkId);

      expect(result).toEqual({ data: testAttractions, total: 2 });
      expect(mockAttractionRepository.findAndCount).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { parkId },
          order: { name: "ASC" },
        }),
      );
    });

    it("should return empty data when park has no attractions", async () => {
      mockAttractionRepository.findAndCount.mockResolvedValue([[], 0]);

      const result = await service.findByParkId("empty-park");

      expect(result).toEqual({ data: [], total: 0 });
    });
  });

  describe("getRepository", () => {
    it("should return the repository instance", () => {
      const repo = service.getRepository();
      expect(repo).toBe(attractionRepository);
    });
  });
});
