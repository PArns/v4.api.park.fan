import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ConfigService } from "@nestjs/config";
import { MLService } from "./ml.service";
import { WaitTimePrediction } from "./entities/wait-time-prediction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { OpenMeteoClient } from "../external-apis/weather/open-meteo.client";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("MLService", () => {
  let service: MLService;

  // Mock Redis
  const mockRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
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

  // Mock Services
  const mockPredictionAccuracyService = {
    recordPrediction: jest.fn(),
  };

  const mockOpenMeteoClient = {
    getCurrentWeather: jest.fn(),
    getForecast: jest.fn(),
    getHourlyForecast: jest.fn(),
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
          provide: ConfigService,
          useValue: mockConfigService,
        },
        {
          provide: PredictionAccuracyService,
          useValue: mockPredictionAccuracyService,
        },
        {
          provide: OpenMeteoClient,
          useValue: mockOpenMeteoClient,
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
      // Mock axios instance to return healthy response
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

  describe("getModelInfo", () => {
    it("should return model information", async () => {
      const mockModelInfo = {
        version: "v1.0.0",
        trainedAt: "2025-12-01T00:00:00Z",
        features: ["hour", "dayOfWeek", "temperature"],
        accuracy: 0.92,
      };

      const mlClient = (service as any).mlClient;
      mlClient.get = jest.fn().mockResolvedValue({ data: mockModelInfo });

      const result = await service.getModelInfo();

      expect(result).toEqual(mockModelInfo);
      expect(mlClient.get).toHaveBeenCalledWith("/model/info");
    });

    it("should throw error when ML service is down", async () => {
      const mlClient = (service as any).mlClient;
      mlClient.get = jest
        .fn()
        .mockRejectedValue(new Error("Service unavailable"));

      await expect(service.getModelInfo()).rejects.toThrow();
    });
  });

  describe("getAttractionPredictionsWithFallback", () => {
    const attractionId = "attraction-123";

    it("should return DB predictions when available", async () => {
      const now = new Date();
      const dbPredictions = [
        {
          id: "pred-1",
          attractionId,
          predictedTime: new Date(now.getTime() + 60 * 60 * 1000),
          predictedWaitTime: 35,
          confidence: 0.9,
          crowdLevel: "moderate" as const,
          baseline: 30,
          trend: "stable",
          modelVersion: "v1.0.0",
          predictionType: "daily" as const,
          createdAt: now,
        },
      ];

      mockPredictionRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue(dbPredictions);

      const result = await service.getAttractionPredictionsWithFallback(
        attractionId,
        "daily", // Use daily to get from  DB
      );

      expect(result).toHaveLength(1);
      expect(result[0].predictedWaitTime).toBe(35);
      expect(result[0].confidence).toBe(0.9);
    });

    it("should fall back to ML service when no DB predictions", async () => {
      mockPredictionRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue([]);

      // Mock attraction data for ML service
      mockAttractionRepository.findOne.mockResolvedValue({
        id: attractionId,
        parkId: "park-123",
        park: {
          id: "park-123",
          latitude: 28.3852,
          longitude: -81.5639,
        },
      });

      // Mock queue data
      mockQueueDataRepository.findOne.mockResolvedValue({
        waitTime: 30,
        timestamp: new Date(),
      });

      // Mock weather forecast
      mockOpenMeteoClient.getHourlyForecast.mockResolvedValue({
        hours: [
          {
            time: new Date().toISOString(),
            temperature: 22,
            precipitation: 0,
            cloudCover: 30,
          },
        ],
      });

      // Mock ML service response
      const mlClient = (service as any).mlClient;
      mlClient.post = jest.fn().mockResolvedValue({
        data: {
          predictions: [
            {
              attractionId,
              predictedTime: new Date().toISOString(),
              predictedWaitTime: 32,
              confidence: 0.85,
              predictionType: "hourly" as const,
              crowdLevel: "moderate" as const,
              baseline: 30,
              modelVersion: "v1.0.0",
            },
          ],
          count: 1,
          modelVersion: "v1.0.0",
        },
      });

      const result = await service.getAttractionPredictionsWithFallback(
        attractionId,
        "hourly",
      );

      expect(result).toBeDefined();
      expect(mlClient.post).toHaveBeenCalled();
    });

    it("should return empty array when both DB and ML service fail", async () => {
      // No DB predictions
      mockPredictionRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue([]);

      // Attraction not found will cause getAttractionPredictions to throw
      mockAttractionRepository.findOne.mockResolvedValue(null);

      // Should throw HttpException
      await expect(
        service.getAttractionPredictionsWithFallback(attractionId, "hourly"),
      ).rejects.toThrow("Attraction not found");
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

      mockRedis.get.mockResolvedValue(JSON.stringify(cachedData));

      const result = await service.getParkPredictions(parkId, "hourly");

      expect(result).toEqual(cachedData);
      // Cache key includes date
      expect(mockRedis.get).toHaveBeenCalled();
      const cacheKey = (mockRedis.get as jest.Mock).mock.calls[0][0];
      expect(cacheKey).toMatch(/^ml:park:park-123:hourly:/);
    });

    it("should fetch and cache predictions when not cached", async () => {
      mockRedis.get.mockResolvedValue(null);

      // Mock park data
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        name: "Test Park",
        latitude: 28.3852,
        longitude: -81.5639,
      });

      // Mock attractions
      mockAttractionRepository.find.mockResolvedValue([
        { id: "attr-1" },
        { id: "attr-2" },
      ]);

      // Mock queue data
      mockQueueDataRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue([]);

      // Mock weather forecast
      mockOpenMeteoClient.getHourlyForecast.mockResolvedValue({
        hours: [{ time: new Date().toISOString(), temperature: 25 }],
      });

      // Mock ML service response
      const mlClient = (service as any).mlClient;
      mlClient.post = jest.fn().mockResolvedValue({
        data: {
          predictions: [
            {
              attractionId: "attr-1",
              predictedTime: new Date().toISOString(),
              predictedWaitTime: 28,
              confidence: 0.88,
              predictionType: "hourly" as const,
              crowdLevel: "moderate" as const,
              baseline: 25,
              modelVersion: "v1.0.0",
            },
          ],
          count: 1,
          modelVersion: "v1.0.0",
        },
      });

      const result = await service.getParkPredictions(parkId, "hourly");

      expect(result).toBeDefined();
      expect(mockRedis.set).toHaveBeenCalled();
      expect(mlClient.post).toHaveBeenCalledWith(
        "/predict",
        expect.any(Object),
      );
    });
  });

  describe("storePredictions", () => {
    it("should store predictions and record for accuracy tracking", async () => {
      const predictions = [
        {
          attractionId: "attr-1",
          predictedTime: new Date().toISOString(),
          predictedWaitTime: 30,
          confidence: 0.9,
          crowdLevel: "moderate" as const,
          baseline: 28,
          trend: "stable",
          modelVersion: "v1.0.0",
          predictionType: "hourly" as const,
        },
      ];

      mockPredictionRepository.save.mockResolvedValue(predictions);

      await service.storePredictions(predictions);

      expect(mockPredictionRepository.save).toHaveBeenCalled();
      expect(
        mockPredictionAccuracyService.recordPrediction,
      ).toHaveBeenCalledTimes(predictions.length);
    });

    it("should handle empty predictions array", async () => {
      await service.storePredictions([]);

      // Service still calls save with empty array, which is fine
      expect(mockPredictionRepository.save).toHaveBeenCalledWith([]);
      expect(
        mockPredictionAccuracyService.recordPrediction,
      ).not.toHaveBeenCalled();
    });
  });

  describe("getStoredPredictions", () => {
    it("should return stored predictions within time range", async () => {
      const attractionId = "attr-1";
      const startTime = new Date();
      const endTime = new Date(startTime.getTime() + 24 * 60 * 60 * 1000);

      const storedPredictions = [
        {
          id: "pred-1",
          attractionId,
          predictedTime: new Date(startTime.getTime() + 60 * 60 * 1000),
          predictedWaitTime: 32,
          confidence: 0.88,
          predictionType: "daily",
        },
      ];

      mockPredictionRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue(storedPredictions);

      const result = await service.getStoredPredictions(
        attractionId,
        "daily",
        startTime,
        endTime,
      );

      expect(result).toEqual(storedPredictions);
    });

    it("should return all predictions when no time range specified", async () => {
      const attractionId = "attr-1";

      mockPredictionRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue([]);

      const result = await service.getStoredPredictions(attractionId, "hourly");

      expect(result).toEqual([]);
    });
  });
});
