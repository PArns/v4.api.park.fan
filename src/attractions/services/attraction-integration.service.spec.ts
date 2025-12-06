import { Test, TestingModule } from "@nestjs/testing";
import { AttractionIntegrationService } from "./attraction-integration.service";
import { Attraction } from "../entities/attraction.entity";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { createTestAttraction } from "../../../test/fixtures/attraction.fixtures";

describe("AttractionIntegrationService", () => {
  let service: AttractionIntegrationService;

  // Mock Redis
  const mockRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
  };

  // Mock Services
  const mockQueueDataService = {
    findCurrentStatusByAttraction: jest.fn(),
    findForecastsByAttraction: jest.fn(),
  };

  const mockAnalyticsService = {
    detectAttractionTrend: jest.fn(),
    getAttractionStatistics: jest.fn(),
  };

  const mockMLService = {
    isHealthy: jest.fn(),
    getAttractionPredictionsWithFallback: jest.fn(),
  };

  const mockPredictionAccuracyService = {
    getAttractionAccuracyWithBadge: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AttractionIntegrationService,
        {
          provide: QueueDataService,
          useValue: mockQueueDataService,
        },
        {
          provide: AnalyticsService,
          useValue: mockAnalyticsService,
        },
        {
          provide: MLService,
          useValue: mockMLService,
        },
        {
          provide: PredictionAccuracyService,
          useValue: mockPredictionAccuracyService,
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
      ],
    }).compile();

    service = module.get<AttractionIntegrationService>(
      AttractionIntegrationService,
    );

    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("buildIntegratedResponse", () => {
    const testAttraction = createTestAttraction("park-1", {
      id: "attraction-123",
      name: "Test Attraction",
      slug: "test-attraction",
    });

    it("should return cached response if available", async () => {
      const cachedResponse = {
        id: testAttraction.id,
        name: testAttraction.name,
        slug: testAttraction.slug,
      };

      mockRedis.get.mockResolvedValue(JSON.stringify(cachedResponse));

      const result = await service.buildIntegratedResponse(testAttraction);

      expect(result).toEqual(cachedResponse);
      expect(mockRedis.get).toHaveBeenCalledWith(
        `attraction:integrated:${testAttraction.id}`,
      );
      expect(
        mockQueueDataService.findCurrentStatusByAttraction,
      ).not.toHaveBeenCalled();
    });

    it("should build integrated response when not cached", async () => {
      mockRedis.get.mockResolvedValue(null);

      // Mock queue data
      mockQueueDataService.findCurrentStatusByAttraction.mockResolvedValue([
        {
          queueType: "STANDBY",
          status: "OPERATING",
          waitTime: 30,
          state: null,
          returnStart: null,
          returnEnd: null,
          price: null,
          allocationStatus: null,
          currentGroupStart: null,
          currentGroupEnd: null,
          estimatedWait: null,
          lastUpdated: new Date(),
          timestamp: new Date(),
        },
      ]);

      // Mock forecasts
      mockQueueDataService.findForecastsByAttraction.mockResolvedValue([
        {
          predictedTime: new Date("2025-12-15T12:00:00Z"),
          predictedWaitTime: 35,
          confidencePercentage: 85,
          source: "themeparks",
        },
      ]);

      // Mock ML service (healthy)
      mockMLService.isHealthy.mockResolvedValue(true);
      mockMLService.getAttractionPredictionsWithFallback.mockResolvedValue([
        {
          predictedTime: "2025-12-15T14:00:00Z",
          predictedWaitTime: 40,
          confidence: 0.9,
          crowdLevel: "moderate",
          baseline: 30,
          trend: "increasing",
          modelVersion: "v1.0.0",
        },
      ]);

      // Mock analytics
      mockAnalyticsService.detectAttractionTrend.mockResolvedValue({
        trend: "increasing",
        changeRate: 5,
        recentAverage: 32,
        previousAverage: 27,
      });

      mockAnalyticsService.getAttractionStatistics.mockResolvedValue({
        avgWaitToday: 30,
        peakWaitToday: 50,
        minWaitToday: 15,
        typicalWaitThisHour: 28,
        percentile95ThisHour: 45,
        currentVsTypical: 7,
        dataPoints: 120,
        timestamp: new Date(),
      });

      // Mock prediction accuracy
      mockPredictionAccuracyService.getAttractionAccuracyWithBadge.mockResolvedValue(
        {
          badge: "gold",
          last30Days: {
            accuracy: 92.5,
            avgError: 5.2,
            sampleSize: 500,
          },
          message: "Highly accurate predictions",
        },
      );

      const result = await service.buildIntegratedResponse(testAttraction);

      // Verify DTO structure
      expect(result).toHaveProperty("id", testAttraction.id);
      expect(result).toHaveProperty("name", testAttraction.name);
      expect(result).toHaveProperty("queues");
      expect(result).toHaveProperty("forecasts");
      expect(result).toHaveProperty("predictions");
      expect(result).toHaveProperty("statistics");
      expect(result).toHaveProperty("predictionAccuracy");

      // Verify queues data
      expect(result.queues).toHaveLength(1);
      expect(result.queues?.[0].queueType).toBe("STANDBY");
      expect(result.queues?.[0].waitTime).toBe(30);
      expect(result.queues?.[0].trend).toBeDefined();
      expect(result.queues?.[0].trend?.direction).toBe("increasing");

      // Verify forecasts
      expect(result.forecasts).toHaveLength(1);
      expect(result.forecasts?.[0].predictedWaitTime).toBe(35);

      // Verify ML predictions
      expect(result.predictions).toHaveLength(1);
      expect(result.predictions?.[0].predictedWaitTime).toBe(40);
      expect(result.predictions?.[0].confidence).toBe(0.9);

      // Verify statistics
      expect(result.statistics?.avgWaitToday).toBe(30);
      expect(result.statistics?.peakWaitToday).toBe(50);

      // Verify prediction accuracy
      expect(result.predictionAccuracy?.badge).toBe("gold");

      // Verify cache was set
      expect(mockRedis.set).toHaveBeenCalled();
    });

    it("should handle ML service unavailable gracefully", async () => {
      mockRedis.get.mockResolvedValue(null);
      mockQueueDataService.findCurrentStatusByAttraction.mockResolvedValue([]);
      mockQueueDataService.findForecastsByAttraction.mockResolvedValue([]);

      // ML service is down
      mockMLService.isHealthy.mockResolvedValue(false);

      mockAnalyticsService.getAttractionStatistics.mockResolvedValue({
        avgWaitToday: 25,
        peakWaitToday: 40,
        minWaitToday: 10,
        typicalWaitThisHour: 22,
        percentile95ThisHour: 38,
        currentVsTypical: 14,
        dataPoints: 85,
        timestamp: new Date(),
      });

      mockPredictionAccuracyService.getAttractionAccuracyWithBadge.mockResolvedValue(
        {
          badge: "silver",
          last30Days: {
            accuracy: 88.0,
            avgError: 6.8,
            sampleSize: 300,
          },
          message: "Good predictions",
        },
      );

      const result = await service.buildIntegratedResponse(testAttraction);

      // Predictions should be undefined when ML service is down (not initialized)
      expect(result.predictions).toBeUndefined();

      // Other data should still be present
      expect(result.statistics).toBeDefined();
      expect(result.predictionAccuracy).toBeDefined();
    });

    it("should handle trend calculation errors gracefully", async () => {
      mockRedis.get.mockResolvedValue(null);

      mockQueueDataService.findCurrentStatusByAttraction.mockResolvedValue([
        {
          queueType: "STANDBY",
          status: "OPERATING",
          waitTime: 30,
          state: null,
          returnStart: null,
          returnEnd: null,
          price: null,
          allocationStatus: null,
          currentGroupStart: null,
          currentGroupEnd: null,
          estimatedWait: null,
          lastUpdated: new Date(),
          timestamp: new Date(),
        },
      ]);

      mockQueueDataService.findForecastsByAttraction.mockResolvedValue([]);
      mockMLService.isHealthy.mockResolvedValue(false);

      // Trend calculation throws error
      mockAnalyticsService.detectAttractionTrend.mockRejectedValue(
        new Error("Trend calculation failed"),
      );

      mockAnalyticsService.getAttractionStatistics.mockResolvedValue({
        avgWaitToday: 28,
        peakWaitToday: 45,
        minWaitToday: 12,
        typicalWaitThisHour: 25,
        percentile95ThisHour: 42,
        currentVsTypical: 12,
        dataPoints: 95,
        timestamp: new Date(),
      });

      mockPredictionAccuracyService.getAttractionAccuracyWithBadge.mockResolvedValue(
        {
          badge: "bronze",
          last30Days: {
            accuracy: 82.0,
            avgError: 8.5,
            sampleSize: 200,
          },
          message: "Moderate predictions",
        },
      );

      const result = await service.buildIntegratedResponse(testAttraction);

      // Should have queue data without trend
      expect(result.queues).toHaveLength(1);
      expect(result.queues?.[0].trend).toBeUndefined();

      // Other data should still work
      expect(result).toHaveProperty("statistics");
    });

    it("should handle statistics fetch error by returning null", async () => {
      mockRedis.get.mockResolvedValue(null);
      mockQueueDataService.findCurrentStatusByAttraction.mockResolvedValue([]);
      mockQueueDataService.findForecastsByAttraction.mockResolvedValue([]);
      mockMLService.isHealthy.mockResolvedValue(false);

      // Statistics fetch fails
      mockAnalyticsService.getAttractionStatistics.mockRejectedValue(
        new Error("Stats failed"),
      );

      mockPredictionAccuracyService.getAttractionAccuracyWithBadge.mockResolvedValue(
        {
          badge: "none",
          last30Days: null,
          message: "Insufficient data",
        },
      );

      const result = await service.buildIntegratedResponse(testAttraction);

      expect(result.statistics).toBeNull();
      expect(result.predictionAccuracy).toBeDefined();
    });

    it("should handle prediction accuracy fetch error gracefully", async () => {
      mockRedis.get.mockResolvedValue(null);
      mockQueueDataService.findCurrentStatusByAttraction.mockResolvedValue([]);
      mockQueueDataService.findForecastsByAttraction.mockResolvedValue([]);
      mockMLService.isHealthy.mockResolvedValue(false);

      mockAnalyticsService.getAttractionStatistics.mockResolvedValue({
        avgWaitToday: 22,
        peakWaitToday: 38,
        minWaitToday: 8,
        typicalWaitThisHour: 20,
        percentile95ThisHour: 35,
        currentVsTypical: 10,
        dataPoints: 75,
        timestamp: new Date(),
      });

      // Prediction accuracy fetch fails
      mockPredictionAccuracyService.getAttractionAccuracyWithBadge.mockRejectedValue(
        new Error("Accuracy fetch failed"),
      );

      const result = await service.buildIntegratedResponse(testAttraction);

      expect(result.statistics).toBeDefined();
      expect(result.predictionAccuracy).toBeNull();
    });
  });
});
