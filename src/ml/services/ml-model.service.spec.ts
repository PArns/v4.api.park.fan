import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import * as fs from "fs/promises";
import { MLModelService } from "./ml-model.service";
import { MLModel } from "../entities/ml-model.entity";

jest.mock("fs/promises");

/**
 * MLModelService surface ML metadata to admin dashboards. The
 * security-critical bit here is the file-path validation — admin
 * pages display `fileSizeBytes` derived from `fs.stat(model.filePath)`,
 * so a malicious filePath in the DB could escape MODEL_DIR. Other
 * concerns:
 *   1. `getModelAge` math (used by the "model is N days old" badge).
 *   2. `getModelComparison` improvement detection.
 *   3. Graceful degradation when the model file is missing on disk.
 */
describe("MLModelService", () => {
  let service: MLModelService;

  const mlModelRepo = {
    findOne: jest.fn(),
    find: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();
    (fs.stat as jest.Mock).mockResolvedValue({ size: 1024 * 1024 });

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLModelService,
        { provide: getRepositoryToken(MLModel), useValue: mlModelRepo },
      ],
    }).compile();

    service = module.get(MLModelService);
  });

  const baseModel = {
    version: "v2026.05.18_0600",
    trainedAt: new Date("2026-05-18T06:00:00Z"),
    trainingDurationSeconds: 1200,
    filePath: "/app/models/catboost_v2026.05.18_0600.cbm",
    modelType: "catboost",
    isActive: true,
    mae: 4.567,
    rmse: 6.123,
    mape: 12.345,
    r2Score: 0.876,
    trainSamples: 100_000,
    validationSamples: 20_000,
    trainDataStartDate: new Date("2024-01-01"),
    trainDataEndDate: new Date("2026-05-15"),
    featuresUsed: ["park_occupancy_pct", "hour_of_day", "weather_temp"],
    hyperparameters: { depth: 8, iterations: 1000 },
  };

  describe("getActiveModelWithDetails", () => {
    it("returns null when no active model exists (admin sees 'no model')", async () => {
      mlModelRepo.findOne.mockResolvedValue(null);

      const result = await service.getActiveModelWithDetails();
      expect(result).toBeNull();
    });

    it("enriches the active model with file size + duration in MB and days", async () => {
      mlModelRepo.findOne.mockResolvedValue(baseModel);
      (fs.stat as jest.Mock).mockResolvedValue({ size: 2.5 * 1024 * 1024 });

      const result = await service.getActiveModelWithDetails();

      expect(result).not.toBeNull();
      expect(result!.version).toBe(baseModel.version);
      expect(result!.fileSizeMB).toBe(2.5);
      expect(result!.modelSize).toBe("2.5 MB");
      // train duration in days = (2026-05-15 - 2024-01-01) ≈ 865 days
      expect(result!.trainingData.dataDurationDays).toBeGreaterThan(800);
      expect(result!.trainingData.totalSamples).toBe(120_000);
      // Hyperparameters carried through.
      expect(result!.configuration.featureCount).toBe(3);
    });

    it("rounds the training metrics to 2 decimals", async () => {
      mlModelRepo.findOne.mockResolvedValue(baseModel);

      const result = await service.getActiveModelWithDetails();

      expect(result!.trainingMetrics.mae).toBe(4.57);
      expect(result!.trainingMetrics.rmse).toBe(6.12);
      expect(result!.trainingMetrics.mape).toBe(12.35);
      expect(result!.trainingMetrics.r2Score).toBe(0.88);
    });

    it("gracefully degrades when the model file is missing on disk", async () => {
      mlModelRepo.findOne.mockResolvedValue(baseModel);
      (fs.stat as jest.Mock).mockRejectedValue(
        Object.assign(new Error("ENOENT"), { code: "ENOENT" }),
      );

      const result = await service.getActiveModelWithDetails();

      // Still returns the model — file size just becomes null.
      expect(result).not.toBeNull();
      expect(result!.fileSizeMB).toBeNull();
      expect(result!.modelSize).toBeNull();
    });

    it("rejects model file paths outside MODEL_DIR (path traversal protection)", async () => {
      mlModelRepo.findOne.mockResolvedValue({
        ...baseModel,
        filePath: "/etc/passwd",
      });

      const result = await service.getActiveModelWithDetails();

      // The unsafe path is detected — fs.stat is NOT called.
      expect(fs.stat).not.toHaveBeenCalled();
      // Model still returned, just without file size.
      expect(result).not.toBeNull();
      expect(result!.fileSizeMB).toBeNull();
    });
  });

  describe("getModelComparison", () => {
    it("returns an all-null comparison when no models exist", async () => {
      mlModelRepo.find.mockResolvedValue([]);
      const result = await service.getModelComparison();
      expect(result).toEqual({
        current: null,
        previous: null,
        improvement: null,
      });
    });

    it("returns just current when only one model exists (no previous)", async () => {
      mlModelRepo.find.mockResolvedValue([baseModel]);

      const result = await service.getModelComparison();

      expect(result.current).not.toBeNull();
      expect(result.previous).toBeNull();
      expect(result.improvement).toBeNull();
    });

    it("flags improvement when current MAE is lower than previous", async () => {
      mlModelRepo.find.mockResolvedValue([
        { ...baseModel, mae: 4 },
        { ...baseModel, version: "v_prev", mae: 6 },
      ]);

      const result = await service.getModelComparison();

      expect(result.improvement!.isImproving).toBe(true);
      expect(result.improvement!.maeDelta).toBe(-2); // 4 - 6
    });

    it("flags degradation when current MAE is higher than previous", async () => {
      mlModelRepo.find.mockResolvedValue([
        { ...baseModel, mae: 8 },
        { ...baseModel, version: "v_prev", mae: 4 },
      ]);

      const result = await service.getModelComparison();

      expect(result.improvement!.isImproving).toBe(false);
      expect(result.improvement!.maeDelta).toBe(4); // 8 - 4
      expect(result.improvement!.maePercentChange).toBe(100); // 4 / 4 = 100%
    });
  });

  describe("getModelAge", () => {
    it("breaks the age down into days, hours, minutes", () => {
      // 3 days, 4 hours, 5 minutes ago
      const ageMs =
        3 * 24 * 60 * 60 * 1000 + 4 * 60 * 60 * 1000 + 5 * 60 * 1000;
      const trainedAt = new Date(Date.now() - ageMs);

      const result = service.getModelAge(trainedAt);

      expect(result.days).toBe(3);
      expect(result.hours).toBe(4);
      // Allow ±1 min drift for test execution time.
      expect(result.minutes).toBeGreaterThanOrEqual(4);
      expect(result.minutes).toBeLessThanOrEqual(5);
    });

    it("returns zero everything for a 'just trained' model", () => {
      const result = service.getModelAge(new Date());
      expect(result.days).toBe(0);
      expect(result.hours).toBe(0);
    });
  });
});
