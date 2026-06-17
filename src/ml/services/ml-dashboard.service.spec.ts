import { Test, TestingModule } from "@nestjs/testing";
import { getQueueToken } from "@nestjs/bull";
import { MLDashboardService } from "./ml-dashboard.service";
import { MLModelService } from "./ml-model.service";
import { MLService } from "../ml.service";
import { PredictionAccuracyService } from "./prediction-accuracy.service";
import { PredictionDeviationService } from "./prediction-deviation.service";
import { MLDriftMonitoringService } from "./ml-drift-monitoring.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * MLDashboardService is the orchestrator behind /admin/ml/dashboard.
 * The dashboard surfaces model metadata, accuracy, drift, and
 * performance to operators. Tests pin down:
 *   1. No-active-model → throws (admin endpoint should 500 vs returning
 *      a half-shaped DTO).
 *   2. The badge threshold ladder (different from the
 *      prediction-accuracy ladder — these are dashboard-specific).
 *   3. `getNextScheduledTraining` math always lands on the next 06:00
 *      UTC slot, never the past.
 *   4. Drift metrics are catch-and-null'd so a drift failure doesn't
 *      take down the whole dashboard.
 */
describe("MLDashboardService", () => {
  let service: MLDashboardService;

  const mlModelService = {
    getActiveModel: jest.fn(),
    getModelComparison: jest.fn(),
  };
  const mlService = {
    getModelInfo: jest.fn(),
  };
  const accuracyService = {
    getSystemAccuracyStats: jest.fn(),
    getTopBottomPerformers: jest.fn(),
    getHourlyAccuracyPatterns: jest.fn(),
    getDayOfWeekAccuracyPatterns: jest.fn(),
  };
  const deviationService = {};
  const driftService = {
    getDriftMetrics: jest.fn(),
  };
  const mlTrainingQueue = { add: jest.fn() };
  const redis = {
    get: jest.fn().mockResolvedValue(null),
    set: jest.fn().mockResolvedValue("OK"),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLDashboardService,
        { provide: MLModelService, useValue: mlModelService },
        { provide: MLService, useValue: mlService },
        { provide: PredictionAccuracyService, useValue: accuracyService },
        { provide: PredictionDeviationService, useValue: deviationService },
        { provide: MLDriftMonitoringService, useValue: driftService },
        { provide: getQueueToken("ml-training"), useValue: mlTrainingQueue },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(MLDashboardService);
  });

  const happyPathSetup = () => {
    mlModelService.getActiveModel.mockResolvedValue({
      version: "v1",
      trainedAt: new Date("2026-05-18T06:00:00Z"),
      trainingDurationSeconds: 1200,
      modelType: "catboost",
      mae: 4.2,
      rmse: 6.1,
      mape: 12.3,
      r2Score: 0.85,
      trainSamples: 1000,
      validationSamples: 200,
      trainDataStartDate: new Date("2024-01-01"),
      trainDataEndDate: new Date("2026-05-15"),
      featuresUsed: ["a", "b", "c"],
      hyperparameters: { depth: 8 },
    });
    mlModelService.getModelComparison.mockResolvedValue({
      current: null,
      previous: null,
      improvement: null,
    });
    accuracyService.getSystemAccuracyStats.mockResolvedValue({
      overall: {
        mae: 5,
        mape: 10,
        coveragePercent: 90,
        totalPredictions: 1000,
      },
      byPredictionType: {},
    });
    accuracyService.getTopBottomPerformers.mockResolvedValue({
      topPerformers: [],
      bottomPerformers: [],
    });
    accuracyService.getHourlyAccuracyPatterns.mockResolvedValue([]);
    accuracyService.getDayOfWeekAccuracyPatterns.mockResolvedValue([]);
    driftService.getDriftMetrics.mockResolvedValue({
      currentDrift: 5,
      status: "healthy",
      trainingMae: 4,
      liveMae: 4.2,
      threshold: 20,
      dailyMetrics: [],
    });
    mlService.getModelInfo.mockResolvedValue({ file_size_mb: 2.3 });
  };

  describe("getDashboard", () => {
    it("returns the full dashboard DTO on the happy path", async () => {
      happyPathSetup();

      const result = await service.getDashboard();

      expect(result.model.current.version).toBe("v1");
      expect(result.model.current.fileSizeMB).toBe(2.3);
      expect(result.performance.live.badge).toBe("excellent"); // mae=5 → excellent
      expect(result.performance.drift!.status).toBe("healthy");
      expect(result.system.modelAge).toEqual(
        expect.objectContaining({
          days: expect.any(Number),
          hours: expect.any(Number),
        }),
      );
    });

    it("throws when no active model exists (admin endpoint must 500, not 200 with nulls)", async () => {
      happyPathSetup();
      mlModelService.getActiveModel.mockResolvedValue(null);

      await expect(service.getDashboard()).rejects.toThrow(/No active model/);
    });

    it("survives drift-service failure (admin dashboard stays up)", async () => {
      happyPathSetup();
      driftService.getDriftMetrics.mockRejectedValue(new Error("drift broken"));

      const result = await service.getDashboard();

      // Dashboard still resolves — drift just becomes null.
      expect(result.performance.drift).toBeNull();
    });

    it("survives ml-service `getModelInfo` failure (file size becomes null)", async () => {
      happyPathSetup();
      mlService.getModelInfo.mockRejectedValue(new Error("ML offline"));

      const result = await service.getDashboard();

      expect(result.model.current.fileSizeMB).toBeNull();
    });
  });

  describe("badge ladder (dashboard-specific thresholds)", () => {
    // Dashboard uses slightly different thresholds than the per-attraction
    // ladder: <8 excellent, <12 good, <18 fair, ≥18 poor. The thresholds
    // are dashboard-only — slides change what operators see.
    it.each([
      [3, "excellent"],
      [7.9, "excellent"],
      [8, "good"],
      [11.9, "good"],
      [12, "fair"],
      [17.9, "fair"],
      [18, "poor"],
      [100, "poor"],
    ])("MAE=%d → '%s'", async (mae, expected) => {
      happyPathSetup();
      accuracyService.getSystemAccuracyStats.mockResolvedValue({
        overall: { mae, mape: 10, coveragePercent: 90, totalPredictions: 1000 },
        byPredictionType: {},
      });

      const result = await service.getDashboard();

      expect(result.performance.live.badge).toBe(expected);
    });
  });

  describe("getNextScheduledTraining (private)", () => {
    it("returns 06:00 UTC the next day when called after 06:00", async () => {
      happyPathSetup();
      // Trained yesterday — used so we don't fail on missing data.
      const result = await service.getDashboard();
      const nextTraining = new Date(result.system.nextTraining);

      // The next training is at 06:00 UTC.
      expect(nextTraining.getUTCHours()).toBe(6);
      expect(nextTraining.getUTCMinutes()).toBe(0);
      // And it's in the future.
      expect(nextTraining.getTime()).toBeGreaterThan(Date.now());
    });
  });
});
