import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { QueueDataService } from "./queue-data.service";
import { QueueData } from "./entities/queue-data.entity";
import { ForecastData } from "./entities/forecast-data.entity";

describe("QueueDataService", () => {
  let service: QueueDataService;

  // Mock repositories
  const mockQueueDataRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    findAndCount: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      select: jest.fn().mockReturnThis(),
      leftJoin: jest.fn().mockReturnThis(),
      innerJoin: jest.fn().mockReturnThis(),
      innerJoinAndSelect: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      take: jest.fn().mockReturnThis(),
      groupBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
      getOne: jest.fn().mockResolvedValue(null),
      getRawMany: jest.fn().mockResolvedValue([]),
      getCount: jest.fn().mockResolvedValue(0),
    })),
    query: jest.fn(),
  };

  const mockForecastDataRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      select: jest.fn().mockReturnThis(),
      leftJoin: jest.fn().mockReturnThis(),
      leftJoinAndSelect: jest.fn().mockReturnThis(),
      innerJoin: jest.fn().mockReturnThis(),
      innerJoinAndSelect: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      addOrderBy: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      take: jest.fn().mockReturnThis(),
      groupBy: jest.fn().mockReturnThis(),
      distinctOn: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
      getOne: jest.fn().mockResolvedValue(null),
      getRawMany: jest.fn().mockResolvedValue([]),
      getManyAndCount: jest.fn().mockResolvedValue([[], 0]),
      getCount: jest.fn().mockResolvedValue(0),
    })),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        QueueDataService,
        {
          provide: getRepositoryToken(QueueData),
          useValue: mockQueueDataRepository,
        },
        {
          provide: getRepositoryToken(ForecastData),
          useValue: mockForecastDataRepository,
        },
      ],
    }).compile();

    service = module.get<QueueDataService>(QueueDataService);

    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("findCurrentStatusByAttraction", () => {
    it("should return current queue data for all queue types", async () => {
      const attractionId = "attr-123";
      const mockQueueData = {
        id: "qd-1",
        attractionId,
        queueType: "STANDBY",
        status: "OPERATING",
        waitTime: 30,
        timestamp: new Date(),
      };

      // Mock findOne to return data when called
      mockQueueDataRepository.findOne.mockResolvedValue(mockQueueData);

      const result = await service.findCurrentStatusByAttraction(attractionId);

      // Should return array of results (one per queue type found)
      expect(result).toHaveLength(6); // 6 queue types checked
      expect(result[0]).toEqual(mockQueueData);
      expect(mockQueueDataRepository.findOne).toHaveBeenCalled();
    });

    it("should return empty array when no data available", async () => {
      mockQueueDataRepository.findOne.mockResolvedValue(null);

      const result = await service.findCurrentStatusByAttraction("attr-999");

      expect(result).toEqual([]);
    });
  });

  describe("findForecastsByAttraction", () => {
    it("should return forecasts for next N hours", async () => {
      const attractionId = "attr-123";
      const hours = 24;

      const mockForecasts = [
        {
          id: "fc-1",
          attractionId,
          predictedTime: new Date(),
          predictedWaitTime: 35,
        },
      ];

      mockForecastDataRepository.find.mockResolvedValue(mockForecasts);

      const result = await service.findForecastsByAttraction(
        attractionId,
        hours,
      );

      expect(result).toEqual(mockForecasts);
    });
  });

  describe("findWaitTimesByAttraction", () => {
    it("should return wait times with pagination", async () => {
      const attractionId = "attr-123";

      mockQueueDataRepository.findAndCount.mockResolvedValue([[], 0]);

      const result = await service.findWaitTimesByAttraction(attractionId);

      expect(result).toHaveProperty("data");
      expect(result).toHaveProperty("total");
    });
  });

  describe("findWaitTimesByPark", () => {
    it("should return current wait times for all attractions in park", async () => {
      const parkId = "park-123";

      mockQueueDataRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue([]);

      const result = await service.findWaitTimesByPark(parkId);

      expect(result).toEqual([]);
    });
  });
});
