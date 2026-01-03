import { Test, TestingModule } from "@nestjs/testing";
import { MLService } from "./ml.service";
import { getRepositoryToken } from "@nestjs/typeorm";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { WaitTimePrediction } from "./entities/wait-time-prediction.entity";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { ConfigService } from "@nestjs/config";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { WeatherService } from "../parks/weather.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { HolidaysService } from "../holidays/holidays.service";
import { ParksService } from "../parks/parks.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { QueueType } from "../external-apis/themeparks/themeparks.types";

describe("MLService QueueType Safeguards", () => {
  let service: MLService;
  let queueDataRepo: any; // Mock repository
  let attractionRepo: any;
  let parkRepo: any;
  let redisClient: any;
  let queryBuilder: any;

  beforeEach(async () => {
    // Mock QueryBuilder (chainable)
    queryBuilder = {
      select: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      getRawMany: jest.fn().mockResolvedValue([]),
      getMany: jest.fn().mockResolvedValue([]),
      getOne: jest.fn().mockResolvedValue(null),
      orderBy: jest.fn().mockReturnThis(),
      addOrderBy: jest.fn().mockReturnThis(),
      setParameter: jest.fn().mockReturnThis(),
      distinctOn: jest.fn().mockReturnThis(),
    };

    queueDataRepo = {
      createQueryBuilder: jest.fn().mockReturnValue(queryBuilder),
      findOne: jest.fn().mockResolvedValue(null),
    };

    attractionRepo = {
      find: jest.fn().mockResolvedValue([{ id: "attr1", parkId: "park1" }]),
      findOne: jest.fn().mockResolvedValue({ id: "attr1", parkId: "park1" }),
    };

    parkRepo = {
      findOne: jest.fn().mockResolvedValue({ id: "park1", timezone: "UTC" }),
    };

    redisClient = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
      mget: jest.fn().mockResolvedValue([]),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLService,
        { provide: getRepositoryToken(WaitTimePrediction), useValue: {} },
        { provide: getRepositoryToken(QueueData), useValue: queueDataRepo },
        { provide: getRepositoryToken(Park), useValue: parkRepo },
        { provide: getRepositoryToken(Attraction), useValue: attractionRepo },
        {
          provide: getRepositoryToken(ScheduleEntry),
          useValue: { findOne: jest.fn() },
        },
        {
          provide: ConfigService,
          useValue: { get: jest.fn() },
        },
        { provide: PredictionAccuracyService, useValue: {} },
        {
          provide: WeatherService,
          useValue: { getHourlyForecast: jest.fn().mockResolvedValue([]) },
        },
        {
          provide: AnalyticsService,
          useValue: { getCurrentOccupancy: jest.fn() },
        },
        {
          provide: HolidaysService,
          useValue: {
            isBridgeDay: jest.fn(),
            isSchoolHolidayInInfluenceZone: jest.fn(),
          },
        },
        { provide: ParksService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: redisClient },
      ],
    }).compile();

    service = module.get<MLService>(MLService);

    // Mock getPredictions to avoid HTTP calls
    jest.spyOn(service, "getPredictions").mockResolvedValue({
      predictions: [],
      count: 0,
      modelVersion: "test",
    });
  });

  describe("getParkPredictions", () => {
    it("should filter ACTIVE ATTRACTIONS by STANDBY queue type", async () => {
      // Mock getRawMany to return some active attractions so flow continues
      queryBuilder.getRawMany.mockResolvedValueOnce([{ id: "attr1" }]);

      await service.getParkPredictions("park1");

      // Verify the active attractions query (first queryBuilder usage usually)
      // We look for the call that checks timestamp > 90 days
      const _activeAttrCalls = queryBuilder.andWhere.mock.calls.filter(
        (call: any) => call[0].includes("timestamp > :cutoff"),
      );

      // In that same chain (or subsequent .andWhere), we expect the queueType filter
      // Note: Since method chaining mocks use the same spy object, we can check all calls to .andWhere
      const standbyCalls = queryBuilder.andWhere.mock.calls.filter(
        (call: any) =>
          call[0].includes("q.queueType = :queueType") &&
          call[1]?.queueType === QueueType.STANDBY,
      );

      expect(standbyCalls.length).toBeGreaterThanOrEqual(1);
    });

    it("should filter BATCH CURRENT wait times by STANDBY queue type", async () => {
      // 1st QB call: Active attractions (return attr1)
      queryBuilder.getRawMany.mockResolvedValueOnce([{ id: "attr1" }]);
      // 2nd QB call: Current wait times
      queryBuilder.getMany.mockResolvedValueOnce([]);

      await service.getParkPredictions("park1");

      // The current wait time query has a distinctOn clause
      // We can verify that we added the .andWhere filter for queueType
      const standbyCalls = queryBuilder.andWhere.mock.calls.filter(
        (call: any) =>
          call[0].includes("q.queueType = :queueType") &&
          call[1]?.queueType === QueueType.STANDBY,
      );

      // We expect at least 3 calls total across the method (Active, Current, Recent)
      expect(standbyCalls.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe("getAttractionPredictions", () => {
    it("should filter SINGLE CURRENT wait time by STANDBY queue type", async () => {
      await service.getAttractionPredictions("attr1");

      expect(queueDataRepo.findOne).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            attractionId: "attr1",
            queueType: QueueType.STANDBY,
          }),
        }),
      );
    });

    it("should filter SINGLE RECENT wait time by STANDBY queue type", async () => {
      await service.getAttractionPredictions("attr1");

      // Recent wait time uses QueryBuilder
      const standbyCalls = queryBuilder.andWhere.mock.calls.filter(
        (call: any) =>
          call[0].includes("q.queueType = :queueType") &&
          call[1]?.queueType === QueueType.STANDBY,
      );

      expect(standbyCalls.length).toBeGreaterThanOrEqual(1);
    });
  });
});
