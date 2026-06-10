import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ConfigService } from "@nestjs/config";
import { MLService } from "./ml.service";
import { WaitTimePrediction } from "./entities/wait-time-prediction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../parks/entities/schedule-entry.entity";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { WeatherService } from "../parks/weather.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { HolidaysService } from "../holidays/holidays.service";
import { ParksService } from "../parks/parks.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("MLService", () => {
  let service: MLService;

  // Mock Redis
  const mockRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
    ttl: jest.fn(),
    incr: jest.fn(),
    expire: jest.fn(),
  };

  // Mock Config Service
  const mockConfigService = {
    get: jest.fn((key: string) => {
      if (key === "ML_SERVICE_URL") return "http://localhost:8000";
      return undefined;
    }),
  };

  // Mock Repositories
  const mockPredictionRepository = {
    find: jest.fn(),
    save: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
      delete: jest.fn().mockReturnThis(),
      execute: jest.fn().mockResolvedValue({ affected: 0 }),
    })),
  };

  const mockQueueDataRepository = {
    findOne: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      select: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      addOrderBy: jest.fn().mockReturnThis(),
      distinctOn: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      getRawMany: jest.fn().mockResolvedValue([]),
      getMany: jest.fn().mockResolvedValue([]),
    })),
  };

  const mockParkRepository = {
    findOne: jest.fn(),
  };

  const mockAttractionRepository = {
    findOne: jest.fn(),
    find: jest.fn(),
  };

  const mockScheduleEntryRepository = {
    findOne: jest.fn(),
    find: jest.fn(),
  };

  // Mock Services
  const mockPredictionAccuracyService = {
    recordPredictions: jest.fn().mockResolvedValue(0),
  };

  const mockWeatherService = {
    getHourlyForecast: jest.fn(),
    getCurrentAndForecast: jest.fn(),
    getWeatherData: jest.fn(),
  };

  const mockAnalyticsService = {
    getCurrentOccupancy: jest.fn(),
    getP50BaselineFromCache: jest.fn(),
  };

  const mockHolidaysService = {
    isBridgeDay: jest.fn(),
    isEffectiveSchoolHoliday: jest.fn(),
    getHolidays: jest.fn(),
  };

  const mockParksService = {
    getOperatingDateRange: jest.fn(),
    isParkSeasonal: jest.fn(),
    getBatchParkStatus: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLService,
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: mockPredictionRepository,
        },
        {
          provide: getRepositoryToken(QueueData),
          useValue: mockQueueDataRepository,
        },
        {
          provide: getRepositoryToken(Park),
          useValue: mockParkRepository,
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: mockAttractionRepository,
        },
        {
          provide: getRepositoryToken(ScheduleEntry),
          useValue: mockScheduleEntryRepository,
        },
        {
          provide: ConfigService,
          useValue: mockConfigService,
        },
        {
          provide: PredictionAccuracyService,
          useValue: mockPredictionAccuracyService,
        },
        {
          provide: WeatherService,
          useValue: mockWeatherService,
        },
        {
          provide: AnalyticsService,
          useValue: mockAnalyticsService,
        },
        {
          provide: HolidaysService,
          useValue: mockHolidaysService,
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

    service = module.get<MLService>(MLService);

    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("isHealthy", () => {
    it("should return true when ML service is healthy", async () => {
      const mlClient = (service as any).mlClient;
      mlClient.get = jest.fn().mockResolvedValue({
        status: 200,
        data: { status: "healthy" },
      });

      const result = await service.isHealthy();

      expect(result).toBe(true);
    });

    it("should return false when ML service is unreachable", async () => {
      const mlClient = (service as any).mlClient;
      mlClient.get = jest
        .fn()
        .mockRejectedValue(new Error("Connection refused"));

      const result = await service.isHealthy();

      expect(result).toBe(false);
    });
  });

  describe("getParkPredictions", () => {
    const parkId = "park-123";

    it("should return cached predictions when available", async () => {
      const cachedData = {
        predictions: [
          {
            attractionId: "attr-1",
            predictedTime: new Date().toISOString(),
            predictedWaitTime: 30,
            confidence: 0.9,
          },
        ],
      };

      // Park lookup is now done before the cache read for timezone-aware
      // cache keys; mock both calls.
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        timezone: "UTC",
        countryCode: "US",
        regionCode: null,
      });
      mockRedis.get.mockResolvedValue(JSON.stringify(cachedData));

      const result = await service.getParkPredictions(parkId, "hourly");

      expect(result).toEqual(cachedData);
      expect(mockRedis.get).toHaveBeenCalled();
    });
  });

  describe("getServingDailyPredictions (TFT near-term merge)", () => {
    const parkId = "park-123";
    const mkPred = (aid: string, date: string, wait: number, model: string) =>
      ({
        attractionId: aid,
        predictedTime: `${date}T12:00:00`,
        predictedWaitTime: wait,
        predictionType: "daily",
        confidence: 0.7,
        crowdLevel: "moderate",
        baseline: 0,
        modelVersion: model,
      }) as any;

    afterEach(() => jest.restoreAllMocks());

    it("overrides CatBoost with TFT for covered (attraction,date), keeps CatBoost for the rest", async () => {
      const near = "2026-05-25";
      const far = "2026-08-01";
      jest.spyOn(service, "getParkPredictions").mockResolvedValue({
        predictions: [
          mkPred("attr-1", near, 20, "catboost"),
          mkPred("attr-1", far, 25, "catboost"),
          mkPred("attr-2", near, 99, "catboost"),
        ],
        count: 3,
        modelVersion: "catboost-v1",
      } as any);
      jest
        .spyOn(service, "getTftDailyPredictions")
        .mockResolvedValue([mkPred("attr-1", near, 45, "tft")]);

      const res = await service.getServingDailyPredictions(parkId, 30);
      const byKey = new Map(
        res.predictions.map((p) => [
          `${p.attractionId}|${p.predictedTime.slice(0, 10)}`,
          p,
        ]),
      );
      // attr-1 near → TFT (45), not CatBoost (20)
      expect(byKey.get(`attr-1|${near}`)?.predictedWaitTime).toBe(45);
      expect(byKey.get(`attr-1|${near}`)?.modelVersion).toBe("tft");
      // attr-1 far + non-headliner attr-2 near → CatBoost untouched
      expect(byKey.get(`attr-1|${far}`)?.predictedWaitTime).toBe(25);
      expect(byKey.get(`attr-2|${near}`)?.predictedWaitTime).toBe(99);
      expect(res.count).toBe(3);
      expect(res.modelVersion).toContain("tft30");
    });

    it("falls back to pure CatBoost when TFT has no forecasts", async () => {
      jest.spyOn(service, "getParkPredictions").mockResolvedValue({
        predictions: [mkPred("attr-1", "2026-05-25", 20, "catboost")],
        count: 1,
        modelVersion: "catboost-v1",
      } as any);
      jest.spyOn(service, "getTftDailyPredictions").mockResolvedValue([]);

      const res = await service.getServingDailyPredictions(parkId, 30);
      expect(res.predictions).toHaveLength(1);
      expect(res.predictions[0].predictedWaitTime).toBe(20);
      expect(res.modelVersion).toBe("catboost-v1");
    });
  });

  describe("storePredictions", () => {
    const parkId = "park-1";
    const attractionId = "attr-1";
    const today = "2026-04-17";
    const tomorrow = "2026-04-18";
    const nextWeek = "2026-04-24";

    const predictions = [
      {
        attractionId,
        predictedTime: `${today}T12:00:00Z`,
        predictedWaitTime: 30,
        confidence: 0.9,
        crowdLevel: "moderate" as const,
        baseline: 28,
        trend: "stable",
        modelVersion: "v1.0.0",
        predictionType: "hourly" as const,
      },
      {
        attractionId,
        predictedTime: `${tomorrow}T12:00:00Z`,
        predictedWaitTime: 10,
        confidence: 0.95,
        crowdLevel: "low" as const,
        baseline: 28,
        trend: "stable",
        modelVersion: "v1.0.0",
        predictionType: "daily" as const,
      },
      {
        attractionId,
        predictedTime: `${nextWeek}T12:00:00Z`,
        predictedWaitTime: 50,
        confidence: 0.85,
        crowdLevel: "high" as const,
        baseline: 28,
        trend: "stable",
        modelVersion: "v1.0.0",
        predictionType: "daily" as const,
      },
    ];

    beforeEach(() => {
      mockAttractionRepository.find.mockResolvedValue([
        { id: attractionId, parkId },
      ]);
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        timezone: "UTC",
      });
      mockPredictionRepository.save.mockImplementation((entities) =>
        Promise.resolve(entities),
      );
    });

    it("should store all predictions when park has no schedule history (e.g. Hellendoorn)", async () => {
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: null,
        maxDate: null,
      });
      mockParksService.isParkSeasonal.mockResolvedValue(false);
      mockScheduleEntryRepository.find.mockResolvedValue([]);

      await service.storePredictions(predictions);

      expect(mockPredictionRepository.save).toHaveBeenCalledWith(
        expect.arrayContaining([
          expect.objectContaining({
            predictedTime: new Date(`${today}T12:00:00Z`),
          }),
          expect.objectContaining({
            predictedTime: new Date(`${tomorrow}T12:00:00Z`),
          }),
          expect.objectContaining({
            predictedTime: new Date(`${nextWeek}T12:00:00Z`),
          }),
        ]),
      );
    });

    it("should filter out predictions for explicitly CLOSED days", async () => {
      // For a seasonal park the gap filter also skips dates without an
      // explicit OPERATING entry, so we have to mark the days we want
      // kept as OPERATING and the day we want dropped as CLOSED.
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: "2026-01-01",
        maxDate: "2026-12-31",
      });
      mockParksService.isParkSeasonal.mockResolvedValue(true);
      mockScheduleEntryRepository.find.mockResolvedValue([
        {
          date: new Date(`${today}T12:00:00Z`),
          scheduleType: ScheduleType.OPERATING,
        },
        {
          date: new Date(`${tomorrow}T12:00:00Z`),
          scheduleType: ScheduleType.CLOSED,
        },
        {
          date: new Date(`${nextWeek}T12:00:00Z`),
          scheduleType: ScheduleType.OPERATING,
        },
      ]);

      await service.storePredictions(predictions);

      const saved = mockPredictionRepository.save.mock.calls[0][0];
      expect(saved).toHaveLength(2);
      expect(
        saved.find((p: any) =>
          p.predictedTime.toISOString().startsWith(tomorrow),
        ),
      ).toBeUndefined();
    });

    it("should filter out predictions in seasonal gaps", async () => {
      // Seasonal park with operating history but no explicit schedule
      // entries for the predicted days → every prediction falls into a
      // gap and gets skipped, so save is never called.
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: "2026-01-01",
        maxDate: "2026-12-31",
      });
      mockParksService.isParkSeasonal.mockResolvedValue(true);
      mockScheduleEntryRepository.find.mockResolvedValue([]);

      await service.storePredictions(predictions);

      expect(mockPredictionRepository.save).not.toHaveBeenCalled();
    });
  });
});
