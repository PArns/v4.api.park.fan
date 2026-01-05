import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { AnalyticsService } from "./analytics.service";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { WeatherData } from "../parks/entities/weather-data.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { RestaurantLiveData } from "../restaurants/entities/restaurant-live-data.entity";
import { ShowLiveData } from "../shows/entities/show-live-data.entity";
import { PredictionAccuracy } from "../ml/entities/prediction-accuracy.entity";
import { WaitTimePrediction } from "../ml/entities/wait-time-prediction.entity";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("AnalyticsService", () => {
  let service: AnalyticsService;

  // Mock Redis
  const mockRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
    setex: jest.fn(),
  };

  // Create standard query builder mock
  const createMockQueryBuilder = () => ({
    select: jest.fn().mockReturnThis(),
    addSelect: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    orWhere: jest.fn().mockReturnThis(),
    groupBy: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    addOrderBy: jest.fn().mockReturnThis(),
    leftJoin: jest.fn().mockReturnThis(),
    innerJoin: jest.fn().mockReturnThis(),
    leftJoinAndSelect: jest.fn().mockReturnThis(),
    limit: jest.fn().mockReturnThis(),
    offset: jest.fn().mockReturnThis(),
    skip: jest.fn().mockReturnThis(),
    take: jest.fn().mockReturnThis(),
    getRawMany: jest.fn().mockResolvedValue([]),
    getRawOne: jest.fn().mockResolvedValue(null),
    getMany: jest.fn().mockResolvedValue([]),
    getOne: jest.fn().mockResolvedValue(null),
    getCount: jest.fn().mockResolvedValue(0),
  });

  // Repository mocks
  const mockQueueDataRepository = {
    query: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
  };

  const mockAttractionRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
    find: jest.fn(),
    findOne: jest.fn(),
  };

  const mockParkRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
    find: jest.fn(),
    findOne: jest.fn(),
    manager: {
      query: jest.fn(),
    },
  };

  const mockShowRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
  };

  const mockRestaurantRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
  };

  const mockWeatherDataRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
  };

  const mockScheduleEntryRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
  };

  const mockRestaurantLiveDataRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
  };

  const mockShowLiveDataRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
  };

  const mockPredictionAccuracyRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
  };

  const mockWaitTimePredictionRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
  };

  const mockQueueDataAggregateRepository = {
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
    find: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AnalyticsService,
        {
          provide: getRepositoryToken(QueueData),
          useValue: mockQueueDataRepository,
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: mockAttractionRepository,
        },
        {
          provide: getRepositoryToken(Park),
          useValue: mockParkRepository,
        },
        {
          provide: getRepositoryToken(Show),
          useValue: mockShowRepository,
        },
        {
          provide: getRepositoryToken(Restaurant),
          useValue: mockRestaurantRepository,
        },
        {
          provide: getRepositoryToken(WeatherData),
          useValue: mockWeatherDataRepository,
        },
        {
          provide: getRepositoryToken(ScheduleEntry),
          useValue: mockScheduleEntryRepository,
        },
        {
          provide: getRepositoryToken(RestaurantLiveData),
          useValue: mockRestaurantLiveDataRepository,
        },
        {
          provide: getRepositoryToken(ShowLiveData),
          useValue: mockShowLiveDataRepository,
        },
        {
          provide: getRepositoryToken(PredictionAccuracy),
          useValue: mockPredictionAccuracyRepository,
        },
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: mockWaitTimePredictionRepository,
        },
        {
          provide: getRepositoryToken(QueueDataAggregate),
          useValue: mockQueueDataAggregateRepository,
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
      ],
    }).compile();

    service = module.get<AnalyticsService>(AnalyticsService);

    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("calculateParkOccupancy", () => {
    it("should return default occupancy when no current data", async () => {
      // Mock no current average wait time
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne.mockResolvedValue(null);
      mockQueueDataRepository.createQueryBuilder.mockReturnValue(queryBuilder);

      const result = await service.calculateParkOccupancy("park-123");

      expect(result).toHaveProperty("current");
      expect(result).toHaveProperty("trend");
      expect(result).toHaveProperty("comparedToTypical");
      expect(result.current).toBe(0);
    });

    it("should calculate occupancy percentage correctly", async () => {
      // Mock current average wait time
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne
        .mockResolvedValueOnce({ avgWait: "30" }) // Current wait
        .mockResolvedValueOnce(null) // Previous data for trend
        .mockResolvedValueOnce(null); // Typical data

      mockQueueDataRepository.createQueryBuilder.mockReturnValue(queryBuilder);
      mockQueueDataRepository.query.mockResolvedValue([
        { operating_count: "5" },
      ]);

      const result = await service.calculateParkOccupancy("park-123");

      expect(result).toHaveProperty("current");
      expect(result).toHaveProperty("breakdown");
      expect(result.breakdown?.currentAvgWait).toBe(30);
    });
  });

  describe("getParkPercentilesToday", () => {
    it("should return null when no data available", async () => {
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne.mockResolvedValue(null);
      mockQueueDataAggregateRepository.createQueryBuilder.mockReturnValue(
        queryBuilder,
      );

      const result = await service.getParkPercentilesToday("park-123");

      expect(result).toBeNull();
    });

    it("should return percentile data when available", async () => {
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne.mockResolvedValue({
        p50: "25.5",
        p75: "40.2",
        p90: "55.8",
        p95: "65.3",
      });
      mockQueueDataAggregateRepository.createQueryBuilder.mockReturnValue(
        queryBuilder,
      );

      const result = await service.getParkPercentilesToday("park-123");

      expect(result).toEqual({
        p50: 26,
        p75: 40,
        p90: 56,
        p95: 65,
      });
    });
  });

  describe("getAttractionPercentilesToday", () => {
    it("should return null when no data available", async () => {
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne.mockResolvedValue(null);
      mockQueueDataAggregateRepository.createQueryBuilder.mockReturnValue(
        queryBuilder,
      );

      const result =
        await service.getAttractionPercentilesToday("attraction-123");

      expect(result).toBeNull();
    });

    it("should return full percentile distribution when available", async () => {
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne.mockResolvedValue({
        p25: "15.2",
        p50: "25.5",
        p75: "40.2",
        p90: "55.8",
        iqr: "25",
        sampleCount: "150",
      });
      mockQueueDataAggregateRepository.createQueryBuilder.mockReturnValue(
        queryBuilder,
      );

      const result =
        await service.getAttractionPercentilesToday("attraction-123");

      expect(result).toEqual({
        p25: 15,
        p50: 26,
        p75: 40,
        p90: 56,
        iqr: 25,
        sampleCount: 150,
      });
    });
  });

  describe("getAttractionRollingPercentiles", () => {
    it("should return null when no data available", async () => {
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne.mockResolvedValue(null);
      mockQueueDataAggregateRepository.createQueryBuilder.mockReturnValue(
        queryBuilder,
      );

      const result =
        await service.getAttractionRollingPercentiles("attraction-123");

      expect(result).toBeNull();
    });

    it("should return rolling percentiles for specified days", async () => {
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne.mockResolvedValue({
        p50: "25.5",
        p90: "55.8",
        iqr: "25",
      });
      mockQueueDataAggregateRepository.createQueryBuilder.mockReturnValue(
        queryBuilder,
      );

      const result = await service.getAttractionRollingPercentiles(
        "attraction-123",
        7,
      );

      expect(result).toEqual({
        p50: 26,
        p90: 56,
        iqr: 25,
      });
    });
  });

  describe("getAttractionStatistics", () => {
    it("should return statistics for an attraction", async () => {
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne
        .mockResolvedValueOnce({
          // Today's stats
          avg: "30",
          max: "60",
          min: "10",
          count: "50",
        })
        .mockResolvedValueOnce({ avgWait: "25" }) // Typical wait
        .mockResolvedValueOnce({ waitTime: "55" }); // P95

      mockQueueDataRepository.createQueryBuilder.mockReturnValue(queryBuilder);
      mockQueueDataRepository.query.mockResolvedValue([]);

      const startTime = new Date();
      startTime.setHours(0, 0, 0, 0);
      const result = await service.getAttractionStatistics(
        "attraction-123",
        startTime,
        "Europe/Berlin",
      );

      expect(result).toHaveProperty("avgWaitToday");
      expect(result).toHaveProperty("peakWaitToday");
      expect(result).toHaveProperty("minWaitToday");
      expect(result).toHaveProperty("typicalWaitThisHour");
      expect(result).toHaveProperty("dataPoints");
    });
  });

  describe("getBatchAttractionStatistics", () => {
    it("should return statistics for multiple attractions including peak timestamp", async () => {
      const attractionIds = ["a1", "a2"];
      const mockRows = [
        {
          attractionId: "a1",
          min_wait: "5",
          max_wait: "45",
          avg_wait: "20.5",
          count: "100",
          max_timestamp: "2023-01-01T10:00:00.000Z",
        },
        {
          attractionId: "a2",
          min_wait: "10",
          max_wait: "60",
          avg_wait: "35.2",
          count: "150",
          max_timestamp: "2023-01-01T14:30:00.000Z",
        },
      ];

      mockQueueDataRepository.query.mockResolvedValue(mockRows);

      const startTime = new Date();
      startTime.setHours(0, 0, 0, 0);
      const result = await service.getBatchAttractionStatistics(
        attractionIds,
        startTime,
      );

      expect(result.size).toBe(2);

      const stats1 = result.get("a1");
      expect(stats1).toBeDefined();
      expect(stats1?.min).toBe(5);
      expect(stats1?.max).toBe(45);
      expect(stats1?.avg).toBe(21); // Rounded
      expect(stats1?.count).toBe(100);
      expect(stats1?.maxTimestamp).toEqual(
        new Date("2023-01-01T10:00:00.000Z"),
      );

      const stats2 = result.get("a2");
      expect(stats2?.max).toBe(60);
      expect(stats2?.maxTimestamp).toEqual(
        new Date("2023-01-01T14:30:00.000Z"),
      );
    });

    it("should return empty map if no attractions provided", async () => {
      const startTime = new Date();
      startTime.setHours(0, 0, 0, 0);
      const result = await service.getBatchAttractionStatistics([], startTime);
      expect(result.size).toBe(0);
      expect(mockQueueDataRepository.query).not.toHaveBeenCalled();
    });
  });

  describe("detectAttractionTrend", () => {
    it("should return stable when no data available", async () => {
      const queryBuilder = createMockQueryBuilder();
      queryBuilder.getRawOne.mockResolvedValue(null);
      mockQueueDataRepository.createQueryBuilder.mockReturnValue(queryBuilder);

      const result = await service.detectAttractionTrend("attraction-123");

      expect(result.trend).toBe("stable");
      expect(result.recentAverage).toBeNull();
      expect(result.previousAverage).toBeNull();
    });
  });
});
