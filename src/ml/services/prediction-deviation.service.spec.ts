import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { PredictionDeviationService } from "./prediction-deviation.service";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

describe("PredictionDeviationService", () => {
  let service: PredictionDeviationService;
  let predictionRepository: jest.Mocked<Repository<WaitTimePrediction>>;
  let redisClient: any;

  beforeEach(async () => {
    const mockRedis = {
      get: jest.fn(),
      set: jest.fn(),
      del: jest.fn(),
    };

    const mockRepository = {
      findOne: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PredictionDeviationService,
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: mockRepository,
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
      ],
    }).compile();

    service = module.get<PredictionDeviationService>(
      PredictionDeviationService,
    );
    predictionRepository = module.get(getRepositoryToken(WaitTimePrediction));
    redisClient = module.get(REDIS_CLIENT);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("checkDeviation", () => {
    it("should detect deviation when absolute threshold exceeded", async () => {
      const attractionId = "attr-123";
      const actualWaitTime = 45;

      // Mock prediction: 30min
      predictionRepository.findOne.mockResolvedValue({
        attractionId,
        predictedWaitTime: 30,
        predictionType: "hourly",
        predictedTime: new Date(),
      } as WaitTimePrediction);

      const result = await service.checkDeviation(attractionId, actualWaitTime);

      expect(result.hasDeviation).toBe(true);
      expect(result.deviation).toBe(15); // 45 - 30
      expect(result.percentageDeviation).toBe(50); // 15/30 * 100
      expect(result.predictedWaitTime).toBe(30);
    });

    it("should detect deviation when percentage threshold exceeded", async () => {
      const attractionId = "attr-456";
      const actualWaitTime = 15;

      // Mock prediction: 10min → 50% deviation
      predictionRepository.findOne.mockResolvedValue({
        attractionId,
        predictedWaitTime: 10,
        predictionType: "hourly",
        predictedTime: new Date(),
      } as WaitTimePrediction);

      const result = await service.checkDeviation(attractionId, actualWaitTime);

      expect(result.hasDeviation).toBe(true);
      expect(result.deviation).toBe(5);
      expect(result.percentageDeviation).toBe(50);
    });

    it("should not detect deviation within thresholds", async () => {
      const attractionId = "attr-789";
      const actualWaitTime = 32;

      // Mock prediction: 30min → 2min deviation (6.7%)
      predictionRepository.findOne.mockResolvedValue({
        attractionId,
        predictedWaitTime: 30,
        predictionType: "hourly",
        predictedTime: new Date(),
      } as WaitTimePrediction);

      const result = await service.checkDeviation(attractionId, actualWaitTime);

      expect(result.hasDeviation).toBe(false);
      expect(result.deviation).toBe(2);
      expect(result.percentageDeviation).toBeCloseTo(6.67, 1);
    });

    it("should return false when no prediction exists", async () => {
      predictionRepository.findOne.mockResolvedValue(null);

      const result = await service.checkDeviation("attr-999", 50);

      expect(result.hasDeviation).toBe(false);
      expect(result.deviation).toBeUndefined();
    });

    it("should handle prediction repository errors gracefully", async () => {
      predictionRepository.findOne.mockRejectedValue(
        new Error("Database error"),
      );

      const result = await service.checkDeviation("attr-error", 50);

      expect(result.hasDeviation).toBe(false);
    });
  });

  describe("flagDeviation", () => {
    it("should store deviation metadata in Redis with TTL", async () => {
      const attractionId = "attr-123";
      const metadata = {
        actualWaitTime: 50,
        predictedWaitTime: 30,
        deviation: 20,
        percentageDeviation: 66.67,
        detectedAt: new Date(),
      };

      await service.flagDeviation(attractionId, metadata);

      expect(redisClient.set).toHaveBeenCalledWith(
        "prediction:deviation:attr-123",
        JSON.stringify(metadata),
        "EX",
        3600,
      );
    });

    it("should handle Redis errors gracefully", async () => {
      redisClient.set.mockRejectedValue(new Error("Redis error"));

      const metadata = {
        actualWaitTime: 50,
        predictedWaitTime: 30,
        deviation: 20,
        percentageDeviation: 66.67,
        detectedAt: new Date(),
      };

      await expect(
        service.flagDeviation("attr-error", metadata),
      ).resolves.not.toThrow();
    });
  });

  describe("getDeviationFlag", () => {
    it("should retrieve deviation metadata from Redis", async () => {
      const attractionId = "attr-123";
      const metadata = {
        actualWaitTime: 50,
        predictedWaitTime: 30,
        deviation: 20,
        percentageDeviation: 66.67,
        detectedAt: new Date().toISOString(),
      };

      redisClient.get.mockResolvedValue(JSON.stringify(metadata));

      const result = await service.getDeviationFlag(attractionId);

      expect(result).toEqual(metadata);
      expect(redisClient.get).toHaveBeenCalledWith(
        "prediction:deviation:attr-123",
      );
    });

    it("should return null when no flag exists", async () => {
      redisClient.get.mockResolvedValue(null);

      const result = await service.getDeviationFlag("attr-999");

      expect(result).toBeNull();
    });

    it("should handle Redis errors gracefully", async () => {
      redisClient.get.mockRejectedValue(new Error("Redis error"));

      const result = await service.getDeviationFlag("attr-error");

      expect(result).toBeNull();
    });
  });

  describe("clearDeviationFlag", () => {
    it("should delete deviation flag from Redis", async () => {
      const attractionId = "attr-123";

      await service.clearDeviationFlag(attractionId);

      expect(redisClient.del).toHaveBeenCalledWith(
        "prediction:deviation:attr-123",
      );
    });
  });
});
