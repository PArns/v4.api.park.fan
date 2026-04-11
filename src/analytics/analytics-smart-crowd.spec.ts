import { Test, TestingModule } from "@nestjs/testing";
import { AnalyticsService } from "./analytics.service";
import { getRepositoryToken } from "@nestjs/typeorm";
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
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("Smart Crowd Level Logic", () => {
  let service: AnalyticsService;
  let queueDataRepo: any;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AnalyticsService,
        {
          provide: getRepositoryToken(QueueData),
          useValue: {
            query: jest.fn(),
            createQueryBuilder: jest.fn(() => ({
              select: jest.fn().mockReturnThis(),
              innerJoin: jest.fn().mockReturnThis(),
              where: jest.fn().mockReturnThis(),
              andWhere: jest.fn().mockReturnThis(),
              getRawOne: jest.fn().mockResolvedValue({ avgWait: 10 }),
            })),
          },
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: { find: jest.fn().mockResolvedValue([]) },
        },
        {
          provide: getRepositoryToken(Park),
          useValue: {
            findOne: jest.fn().mockResolvedValue({ timezone: "UTC" }),
          },
        },
        {
          provide: getRepositoryToken(Show),
          useValue: { count: jest.fn().mockResolvedValue(0) },
        },
        {
          provide: getRepositoryToken(Restaurant),
          useValue: { count: jest.fn().mockResolvedValue(0) },
        },
        {
          provide: getRepositoryToken(WeatherData),
          useValue: { count: jest.fn().mockResolvedValue(0) },
        },
        {
          provide: getRepositoryToken(ScheduleEntry),
          useValue: { findOne: jest.fn().mockResolvedValue(null) },
        },
        {
          provide: getRepositoryToken(RestaurantLiveData),
          useValue: { count: jest.fn().mockResolvedValue(0) },
        },
        {
          provide: getRepositoryToken(ShowLiveData),
          useValue: { count: jest.fn().mockResolvedValue(0) },
        },
        {
          provide: getRepositoryToken(PredictionAccuracy),
          useValue: { count: jest.fn().mockResolvedValue(0) },
        },
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: { count: jest.fn().mockResolvedValue(0) },
        },
        {
          provide: getRepositoryToken(QueueDataAggregate),
          useValue: {
            createQueryBuilder: jest.fn(() => ({
              getRawOne: jest.fn().mockResolvedValue(null),
            })),
          },
        },
        {
          provide: getRepositoryToken(ParkDailyStats),
          useValue: { findOne: jest.fn().mockResolvedValue(null) },
        },
        {
          provide: getRepositoryToken(HeadlinerAttraction),
          useValue: { find: jest.fn().mockResolvedValue([]) },
        },
        {
          provide: getRepositoryToken(ParkP50Baseline),
          useValue: {
            findOne: jest.fn().mockResolvedValue({ p50Baseline: 30 }),
          },
        },
        {
          provide: getRepositoryToken(AttractionP50Baseline),
          useValue: { findOne: jest.fn().mockResolvedValue(null) },
        },
        {
          provide: REDIS_CLIENT,
          useValue: {
            get: jest.fn(),
            set: jest.fn(),
            mget: jest.fn().mockResolvedValue([]),
          },
        },
      ],
    }).compile();

    service = module.get<AnalyticsService>(AnalyticsService);
    queueDataRepo = module.get(getRepositoryToken(QueueData));
    const headlinerRepo = module.get(getRepositoryToken(HeadlinerAttraction));

    // Mock headliners
    headlinerRepo.find.mockResolvedValue([
      { attractionId: "ride1" },
      { attractionId: "ride2" },
      { attractionId: "ride3" },
      { attractionId: "ride4" },
    ]);

    // Mock Redis explicitly
    (service as any).redis = module.get(REDIS_CLIENT);

    // Mock getDailyAverageWaitTime to return 20
    (service as any).getDailyAverageWaitTime = jest.fn().mockResolvedValue(20);

    // Mock getBatchAttractionP90s
    service.getBatchAttractionP90s = jest
      .fn()
      .mockImplementation(async (ids) => {
        const map = new Map();
        ids.forEach((id: string) => map.set(id, 60)); // Default P90 = 60 mins
        return map;
      });

    // Mock detectTrend
    (service as any).detectTrend = jest.fn().mockResolvedValue("stable");
  });

  it("should calculate Smart Occupancy using only rides >= 10m when available", async () => {
    // Setup: 2 rides >= 10m, 2 rides < 10m
    const mockData = [
      { attractionId: "ride1", avg_wait: 45 }, // >= 10
      { attractionId: "ride2", avg_wait: 30 }, // >= 10
    ];

    queueDataRepo.query.mockResolvedValueOnce(mockData); // for getCurrentSpotWaitTime
    queueDataRepo.query.mockResolvedValueOnce([]); // for trends bucket 1
    queueDataRepo.query.mockResolvedValueOnce([]); // for trends bucket 2

    const result = await service.calculateParkOccupancy("park1");

    // Expectation:
    // Only ride1 and ride2 used.
    // Sum Current: 45 + 30 = 75. Avg = 37.5. Round(37.5) = 38.
    // Baseline is mocked at 30
    // Occupancy: (38 / 30) * 100 = 126.66... -> 127%

    expect(result.current).toBe(127);
  });

  it("should validly fallback to ALL rides if none are >= 10m", async () => {
    // Setup: All rides < 10m
    const mockDataFallback = [
      { attractionId: "ride3", avg_wait: 5 },
      { attractionId: "ride4", avg_wait: 5 },
    ];

    queueDataRepo.query
      .mockResolvedValueOnce([]) // 1st call: getCurrentSpotWaitTime with threshold 10
      .mockResolvedValueOnce(mockDataFallback) // 2nd call: getCurrentSpotWaitTime fallback with threshold 0
      .mockResolvedValue([]); // 3rd+ calls: trends bucket 1, bucket 2, active count...

    const result = await service.calculateParkOccupancy("park1");

    // Expectation:
    // Fallback used. Avg wait = 5. Baseline = 30.
    // Occupancy: (5 / 30) * 100 = 16.66% -> 17%

    expect(result.current).toBe(17);
  });

  it("should explicitly filter STANDBY queues in the query", async () => {
    queueDataRepo.query.mockResolvedValue([]);
    await service.calculateParkOccupancy("park-id");

    // Verify SQL contains STANDBY filter
    const sqlCall = queueDataRepo.query.mock.calls[0][0];
    expect(sqlCall).toContain("qd.\"queueType\" = 'STANDBY'");
  });
});
