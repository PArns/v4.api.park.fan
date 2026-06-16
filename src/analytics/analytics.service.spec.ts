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
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { HeadlinerAttraction } from "./entities/headliner-attraction.entity";
import { ParkP50Baseline } from "./entities/park-p50-baseline.entity";
import { AttractionP50Baseline } from "./entities/attraction-p50-baseline.entity";
import { ParkP90Baseline } from "./entities/park-p90-baseline.entity";
import { AttractionP90Baseline } from "./entities/attraction-p90-baseline.entity";
import { AttractionHourlyHistory } from "./entities/attraction-hourly-history.entity";
import { AttractionRopeDrop } from "./entities/attraction-rope-drop.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("AnalyticsService", () => {
  let service: AnalyticsService;

  // Mock Redis
  const mockRedis = {
    get: jest.fn().mockResolvedValue(null),
    set: jest.fn().mockResolvedValue("OK"),
    del: jest.fn().mockResolvedValue(1),
    setex: jest.fn().mockResolvedValue("OK"),
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
    query: jest.fn().mockResolvedValue([]),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
  };

  const mockAttractionRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
    find: jest.fn().mockResolvedValue([]),
    findOne: jest.fn().mockResolvedValue({ timezone: "UTC" }),
  };

  const mockParkRepository = {
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => createMockQueryBuilder()),
    find: jest.fn().mockResolvedValue([]),
    findOne: jest.fn().mockResolvedValue({ timezone: "UTC" }),
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
    findOne: jest.fn().mockResolvedValue(null),
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
    manager: {
      query: jest.fn().mockResolvedValue([]),
    },
  };

  const mockParkDailyStatsRepository = {
    findOne: jest.fn().mockResolvedValue(null),
    find: jest.fn().mockResolvedValue([]),
    save: jest.fn(),
  };

  const mockHeadlinerAttractionRepository = {
    find: jest.fn().mockResolvedValue([]),
    save: jest.fn(),
    delete: jest.fn(),
  };

  const mockParkP50BaselineRepository = {
    findOne: jest.fn().mockResolvedValue({ p50Baseline: 30 }),
    upsert: jest.fn(),
  };

  const mockAttractionP50BaselineRepository = {
    findOne: jest.fn().mockResolvedValue(null),
    upsert: jest.fn(),
  };

  const mockParkP90BaselineRepository = {
    findOne: jest.fn().mockResolvedValue({ p90Baseline: 50 }),
    upsert: jest.fn(),
  };

  const mockAttractionP90BaselineRepository = {
    findOne: jest.fn().mockResolvedValue(null),
    upsert: jest.fn(),
    find: jest.fn().mockResolvedValue([]),
  };

  const mockAttractionHourlyHistoryRepository = {
    findOne: jest.fn().mockResolvedValue(null),
    upsert: jest.fn(),
    find: jest.fn().mockResolvedValue([]),
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
    })),
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
          provide: getRepositoryToken(ParkDailyStats),
          useValue: mockParkDailyStatsRepository,
        },
        {
          provide: getRepositoryToken(HeadlinerAttraction),
          useValue: mockHeadlinerAttractionRepository,
        },
        {
          provide: getRepositoryToken(ParkP50Baseline),
          useValue: mockParkP50BaselineRepository,
        },
        {
          provide: getRepositoryToken(AttractionP50Baseline),
          useValue: mockAttractionP50BaselineRepository,
        },
        {
          provide: getRepositoryToken(ParkP90Baseline),
          useValue: mockParkP90BaselineRepository,
        },
        {
          provide: getRepositoryToken(AttractionP90Baseline),
          useValue: mockAttractionP90BaselineRepository,
        },
        {
          provide: getRepositoryToken(AttractionHourlyHistory),
          useValue: mockAttractionHourlyHistoryRepository,
        },
        {
          provide: getRepositoryToken(AttractionRopeDrop),
          useValue: mockAttractionHourlyHistoryRepository,
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
      ],
    }).compile();

    service = module.get<AnalyticsService>(AnalyticsService);

    jest.clearAllMocks();

    // Force legacy park-wide path; per-ride-ratio path uses
    // getBatchAttractionP50s (Redis pipeline) and is exercised by
    // dedicated tests, not by this top-level suite.
    (service as any).getPerHeadlinerRatios = jest.fn().mockResolvedValue(null);
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
      // Mock headliners to trigger headliner path
      mockHeadlinerAttractionRepository.find.mockResolvedValueOnce([
        { attractionId: "a1" },
      ]);

      // Now driven by getCurrentParkPeakWait (per-headliner MAX in 60min,
      // averaged) — the SQL returns rows shaped {attractionId, latest_wait}.
      mockQueueDataRepository.query
        .mockResolvedValueOnce([{ attractionId: "a1", latest_wait: "30" }])
        .mockResolvedValueOnce([]) // trend bucket 1
        .mockResolvedValueOnce([]) // trend bucket 2
        .mockResolvedValueOnce([{ operating_count: "5" }]); // getActiveAttractionsCount

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
      expect(stats1?.avg).toBe(20); // Rounded 20.25 -> 20
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

  describe("getAttractionTypicalWaits", () => {
    it("computes weekday/weekend, per-weekday and record peak from daily peaks", async () => {
      mockRedis.get.mockResolvedValueOnce(null);
      // Per-day peaks: 3 Mondays (dow 1) + 2 Saturdays (dow 6). DE weekend = Sat/Sun.
      mockQueueDataAggregateRepository.manager.query.mockResolvedValueOnce([
        { day: "2025-06-02", dow: 1, day_peak: "20" },
        { day: "2025-06-09", dow: 1, day_peak: "30" },
        { day: "2025-06-16", dow: 1, day_peak: "40" },
        { day: "2025-06-07", dow: 6, day_peak: "50" },
        { day: "2025-06-14", dow: 6, day_peak: "60" },
      ]);

      const result = await service.getAttractionTypicalWaits(
        "attraction-123",
        "Europe/Berlin",
        "DE",
      );

      // Weekday peaks [20,30,40]: P50=30, P90=38→round5→40
      expect(result.weekday).toEqual({
        typical: 30,
        busy: 40,
        sampleDays: 3,
      });
      // Weekend peaks [50,60]: P50=55, P90=59→round5→60
      expect(result.weekend).toEqual({ typical: 55, busy: 60, sampleDays: 2 });

      // Per day-of-week, ordered ascending, with isWeekend flags
      expect(result.byDayOfWeek).toEqual([
        {
          dayOfWeek: 1,
          isWeekend: false,
          typical: 30,
          busy: 40,
          sampleDays: 3,
        },
        { dayOfWeek: 6, isWeekend: true, typical: 55, busy: 60, sampleDays: 2 },
      ]);

      // Record peak = highest daily peak, with its date
      expect(result.peak).toEqual({ value: 60, date: "2025-06-14" });

      expect(result.windowDays).toBe(365);
      expect(result.displayable).toBe(false); // 5 days < 20 threshold

      // Result is cached for re-use (v2 key)
      expect(mockRedis.set).toHaveBeenCalledWith(
        expect.stringContaining("attraction:typical-waits:v2:attraction-123"),
        expect.any(String),
        "EX",
        24 * 60 * 60,
      );
    });

    it("returns empty buckets, no peak and non-displayable when there is no data", async () => {
      mockRedis.get.mockResolvedValueOnce(null);
      mockQueueDataAggregateRepository.manager.query.mockResolvedValueOnce([]);

      const result = await service.getAttractionTypicalWaits(
        "attraction-123",
        "Europe/Berlin",
        "DE",
      );

      expect(result.weekday).toEqual({
        typical: null,
        busy: null,
        sampleDays: 0,
      });
      expect(result.weekend).toEqual({
        typical: null,
        busy: null,
        sampleDays: 0,
      });
      expect(result.byDayOfWeek).toEqual([]);
      expect(result.peak).toBeNull();
      expect(result.displayable).toBe(false);
    });

    it("classifies weekend country-aware (Fri+Sat for the UAE)", async () => {
      mockRedis.get.mockResolvedValueOnce(null);
      mockQueueDataAggregateRepository.manager.query.mockResolvedValueOnce([
        { day: "2025-06-05", dow: 4, day_peak: "10" }, // Thursday → weekday
        { day: "2025-06-06", dow: 5, day_peak: "100" }, // Friday → weekend
        { day: "2025-06-07", dow: 6, day_peak: "110" }, // Saturday → weekend
      ]);

      const result = await service.getAttractionTypicalWaits(
        "attraction-123",
        "Asia/Dubai",
        "AE",
      );

      expect(result.weekday.sampleDays).toBe(1);
      expect(result.weekend.sampleDays).toBe(2);
      expect(result.byDayOfWeek.map((d) => [d.dayOfWeek, d.isWeekend])).toEqual(
        [
          [4, false],
          [5, true],
          [6, true],
        ],
      );
      expect(result.peak).toEqual({ value: 110, date: "2025-06-07" });
    });

    it("serves a cached result without querying the database", async () => {
      const cached = {
        weekday: { typical: 20, busy: 40, sampleDays: 100 },
        weekend: { typical: 30, busy: 50, sampleDays: 40 },
        byDayOfWeek: [],
        peak: { value: 80, date: "2025-08-09" },
        windowDays: 365,
        dataFrom: "2025-06-16",
        dataTo: "2026-06-15",
        displayable: true,
        generatedAt: "2026-06-16T03:00:00.000Z",
      };
      mockRedis.get.mockResolvedValueOnce(JSON.stringify(cached));

      const result = await service.getAttractionTypicalWaits(
        "attraction-123",
        "Europe/Berlin",
        "DE",
      );

      expect(result).toEqual(cached);
      expect(
        mockQueueDataAggregateRepository.manager.query,
      ).not.toHaveBeenCalled();
    });
  });
});
