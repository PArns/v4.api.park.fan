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
import { ParkP90Baseline } from "./entities/park-p90-baseline.entity";
import { AttractionP90Baseline } from "./entities/attraction-p90-baseline.entity";
import { AttractionHourlyHistory } from "./entities/attraction-hourly-history.entity";
import { AttractionRopeDrop } from "./entities/attraction-rope-drop.entity";
import { AttractionTypicalWaits } from "./entities/attraction-typical-waits.entity";
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
          provide: getRepositoryToken(ParkP90Baseline),
          useValue: {
            findOne: jest.fn().mockResolvedValue({ p90Baseline: 50 }),
          },
        },
        {
          provide: getRepositoryToken(AttractionP90Baseline),
          useValue: {
            findOne: jest.fn().mockResolvedValue(null),
            find: jest.fn().mockResolvedValue([]),
          },
        },
        {
          provide: getRepositoryToken(AttractionHourlyHistory),
          useValue: {
            findOne: jest.fn().mockResolvedValue(null),
            find: jest.fn().mockResolvedValue([]),
            upsert: jest.fn(),
            createQueryBuilder: jest.fn(() => ({
              where: jest.fn().mockReturnThis(),
              andWhere: jest.fn().mockReturnThis(),
              getMany: jest.fn().mockResolvedValue([]),
            })),
          },
        },
        {
          provide: getRepositoryToken(AttractionTypicalWaits),
          useValue: {
            find: jest.fn().mockResolvedValue([]),
            count: jest.fn().mockResolvedValue(0),
            upsert: jest.fn(),
            delete: jest.fn(),
          },
        },
        {
          provide: getRepositoryToken(AttractionRopeDrop),
          useValue: {
            findOne: jest.fn().mockResolvedValue(null),
            find: jest.fn().mockResolvedValue([]),
          },
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

    // Force legacy park-wide path; per-ride-ratio path is tested elsewhere.
    (service as any).getPerHeadlinerRatios = jest.fn().mockResolvedValue(null);

    // Mock getDailyAverageWaitTime to return 20
    (service as any).getDailyAverageWaitTime = jest.fn().mockResolvedValue(20);

    // Mock getBatchAttractionP90Baselines (cache-backed; replaces the old
    // live-aggregation getBatchAttractionP90s).
    service.getBatchAttractionP90Baselines = jest
      .fn()
      .mockImplementation(async (ids) => {
        const map = new Map();
        ids.forEach((id: string) => map.set(id, 60));
        return map;
      });

    // Park-level baseline lookups: P50 first (median, primary), P90 only
    // as fallback. calculateParkOccupancy reads P50 then P90.
    service.getP50BaselineFromCache = jest.fn().mockResolvedValue(50);
    service.getP90BaselineFromCache = jest.fn().mockResolvedValue(70);
    (service as any).getP50BaselineWithConfidence = jest
      .fn()
      .mockResolvedValue({ value: 50, confidence: "high" });

    // Mock detectTrend
    (service as any).detectTrend = jest.fn().mockResolvedValue("stable");
  });

  it("should calculate Smart Occupancy using only rides >= 10m when available", async () => {
    // Setup: 2 headliners report a recent MAX wait (peak in last 60 min).
    // getCurrentParkPeakWait returns rows shaped { attractionId, latest_wait }.
    const mockData = [
      { attractionId: "ride1", latest_wait: 60 },
      { attractionId: "ride2", latest_wait: 40 },
    ];

    queueDataRepo.query.mockResolvedValueOnce(mockData); // getCurrentParkPeakWait
    queueDataRepo.query.mockResolvedValueOnce([]); // trends bucket 1
    queueDataRepo.query.mockResolvedValueOnce([]); // trends bucket 2

    const result = await service.calculateParkOccupancy("park1");

    // Park-peak avg = (60 + 40) / 2 = 50.
    // P50 baseline mocked at 50 (typical median wait).
    // Occupancy = (50 / 50) * 100 = 100 (= current peak matches typical).
    expect(result.current).toBe(100);
  });

  it("should validly fallback to ALL rides if none are >= 10m", async () => {
    const mockDataFallback = [
      { attractionId: "ride3", latest_wait: 5 },
      { attractionId: "ride4", latest_wait: 5 },
    ];

    queueDataRepo.query
      .mockResolvedValueOnce([]) // 1st call: peak query with threshold 10
      .mockResolvedValueOnce(mockDataFallback) // 2nd: threshold=0 retry
      .mockResolvedValue([]); // trends + active count

    const result = await service.calculateParkOccupancy("park1");

    // Avg peak = 5. P50 baseline = 50.
    // Occupancy = (5 / 50) * 100 = 10%.
    expect(result.current).toBe(10);
  });

  it("should explicitly filter STANDBY queues in the query", async () => {
    queueDataRepo.query.mockResolvedValue([]);
    await service.calculateParkOccupancy("park-id");

    // Verify SQL contains STANDBY filter
    const sqlCall = queueDataRepo.query.mock.calls[0][0];
    expect(sqlCall).toContain("qd.\"queueType\" = 'STANDBY'");
  });
});
