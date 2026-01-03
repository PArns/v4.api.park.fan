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
        { provide: getRepositoryToken(Attraction), useValue: {} },
        { provide: getRepositoryToken(Park), useValue: {} },
        { provide: getRepositoryToken(Show), useValue: {} },
        { provide: getRepositoryToken(Restaurant), useValue: {} },
        { provide: getRepositoryToken(WeatherData), useValue: {} },
        { provide: getRepositoryToken(ScheduleEntry), useValue: {} },
        { provide: getRepositoryToken(RestaurantLiveData), useValue: {} },
        { provide: getRepositoryToken(ShowLiveData), useValue: {} },
        { provide: getRepositoryToken(PredictionAccuracy), useValue: {} },
        { provide: getRepositoryToken(WaitTimePrediction), useValue: {} },
        { provide: getRepositoryToken(QueueDataAggregate), useValue: {} },
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

  it("should calculate Smart Occupancy using only rides > 10m when available", async () => {
    // Setup: 2 rides > 10m, 2 rides < 10m
    const mockData = [
      { id: "ride1", waitTime: 45 }, // > 10
      { id: "ride2", waitTime: 30 }, // > 10
      { id: "ride3", waitTime: 5 }, // < 10
      { id: "ride4", waitTime: 5 }, // < 10
    ];

    queueDataRepo.query.mockResolvedValue(mockData);

    const result = await service.calculateParkOccupancy("park1");

    // Expectation:
    // Only ride1 and ride2 used.
    // Sum Current: 45 + 30 = 75
    // Sum P90 (mocked at 60 each): 60 + 60 = 120
    // Occupancy: (75 / 120) * 100 = 62.5% -> 63%

    // Breakdown Avg: (45+30+5+5) / 4 = 21.25 -> 21

    expect(result.current).toBe(63);
    expect(result.breakdown?.currentAvgWait).toBe(38);
    expect(queueDataRepo.query).toHaveBeenCalledWith(
      expect.stringContaining("qd.\"queueType\" = 'STANDBY'"),
      expect.any(Array),
    );
  });

  it("should validly fallback to ALL rides if none are > 10m", async () => {
    // Setup: All rides < 10m
    const mockData = [
      { id: "ride3", waitTime: 5 },
      { id: "ride4", waitTime: 5 },
    ];

    queueDataRepo.query.mockResolvedValue(mockData);

    const result = await service.calculateParkOccupancy("park1");

    // Expectation:
    // Both used.
    // Sum Current: 10
    // Sum P90: 120
    // Occupancy: (10 / 120) * 100 = 8.33% -> 8%

    expect(result.current).toBe(8);
  });

  it("should explicitly filter STANDBY queues in the query", async () => {
    queueDataRepo.query.mockResolvedValue([]);
    await service.calculateParkOccupancy("park-id");

    // Verify SQL contains STANDBY filter
    const sqlCall = queueDataRepo.query.mock.calls[0][0];
    expect(sqlCall).toContain("qd.\"queueType\" = 'STANDBY'");
  });
});
