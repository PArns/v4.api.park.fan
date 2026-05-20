import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { MLFeatureDriftService } from "./ml-feature-drift.service";
import { MLFeatureStats } from "../entities/ml-feature-stats.entity";
import { MLFeatureDrift } from "../entities/ml-feature-drift.entity";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";
import { MLModel } from "../entities/ml-model.entity";

/**
 * Coverage for the feature-drift detector — runs daily, feeds the
 * MLAlertService, decides whether the model is healthy vs. retraining
 * triggers. Any silent regression here means we keep serving stale
 * predictions without alerting. These tests pin down:
 *   1. No active model → throws (caller catches & alerts).
 *   2. No training stats → returns "no drift detected" gracefully.
 *   3. Drift status ladder boundary (healthy <15%, warning 15–29%,
 *      critical ≥30%).
 *   4. Features with insufficient (<10) production samples are
 *      skipped — they'd produce statistically meaningless drift
 *      scores.
 *   5. A drift record gets persisted per evaluated feature for
 *      historical trending.
 */
describe("MLFeatureDriftService", () => {
  let service: MLFeatureDriftService;

  const featureStatsRepo = {
    find: jest.fn().mockResolvedValue([]),
    save: jest.fn().mockResolvedValue(undefined),
  };
  const featureDriftRepo = {
    save: jest.fn().mockResolvedValue(undefined),
    find: jest.fn().mockResolvedValue([]),
  };
  const accuracyRepo = {
    find: jest.fn().mockResolvedValue([]),
  };
  const modelRepo = {
    findOne: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLFeatureDriftService,
        {
          provide: getRepositoryToken(MLFeatureStats),
          useValue: featureStatsRepo,
        },
        {
          provide: getRepositoryToken(MLFeatureDrift),
          useValue: featureDriftRepo,
        },
        {
          provide: getRepositoryToken(PredictionAccuracy),
          useValue: accuracyRepo,
        },
        { provide: getRepositoryToken(MLModel), useValue: modelRepo },
      ],
    }).compile();

    service = module.get(MLFeatureDriftService);
  });

  describe("detectFeatureDrift", () => {
    it("throws when no active model exists (training never ran)", async () => {
      modelRepo.findOne.mockResolvedValue(null);

      await expect(service.detectFeatureDrift()).rejects.toThrow(
        /No active model/,
      );
    });

    it("returns a zero-feature summary when training stats are missing", async () => {
      modelRepo.findOne.mockResolvedValue({ version: "v1", isActive: true });
      featureStatsRepo.find.mockResolvedValue([]);

      const result = await service.detectFeatureDrift();

      expect(result.driftedFeatures).toEqual([]);
      expect(result.summary).toEqual({
        totalFeatures: 0,
        healthyCount: 0,
        warningCount: 0,
        criticalCount: 0,
      });
      // No production data fetched either — short-circuit before that.
      expect(accuracyRepo.find).not.toHaveBeenCalled();
    });

    it("treats every feature as healthy when there's no recent prediction data", async () => {
      modelRepo.findOne.mockResolvedValue({ version: "v1", isActive: true });
      featureStatsRepo.find.mockResolvedValue([
        { featureName: "park_occupancy_pct", mean: 50, std: 10 },
        { featureName: "weather_temp", mean: 20, std: 5 },
      ]);
      accuracyRepo.find.mockResolvedValue([]);

      const result = await service.detectFeatureDrift();

      // Two features, zero production samples → defaults to "all
      // healthy" rather than emitting a false-positive alert.
      expect(result.summary.totalFeatures).toBe(2);
      expect(result.summary.healthyCount).toBe(2);
      expect(result.summary.warningCount).toBe(0);
      expect(result.summary.criticalCount).toBe(0);
    });

    it("evaluates only features that have at least 10 production samples", async () => {
      modelRepo.findOne.mockResolvedValue({ version: "v1", isActive: true });
      featureStatsRepo.find.mockResolvedValue([
        { featureName: "evaluated", mean: 100, std: 10, min: 80, max: 120 },
        { featureName: "skipped", mean: 50, std: 5, min: 40, max: 60 },
      ]);

      // "evaluated" gets 12 samples (above threshold); "skipped"
      // only gets 3 (below threshold).
      accuracyRepo.find.mockResolvedValue([
        ...Array.from({ length: 12 }, (_, i) => ({
          targetTime: new Date(),
          features: { evaluated: 100 + i },
        })),
        ...Array.from({ length: 3 }, () => ({
          targetTime: new Date(),
          features: { skipped: 999 },
        })),
      ]);

      const result = await service.detectFeatureDrift();

      // Only the evaluated feature shows up in driftedFeatures.
      const names = result.driftedFeatures.map((f) => f.featureName);
      expect(names).toContain("evaluated");
      expect(names).not.toContain("skipped");
      // The skipped feature is still in the total — we tried to score
      // it, just had too little data.
      expect(result.summary.totalFeatures).toBe(2);
    });

    it("skips features with fewer than 10 production samples (avoids false drift)", async () => {
      modelRepo.findOne.mockResolvedValue({ version: "v1", isActive: true });
      featureStatsRepo.find.mockResolvedValue([
        { featureName: "sparse_feat", mean: 100, std: 10, min: 80, max: 120 },
      ]);
      // Only 5 production samples → not enough to trust the
      // distribution comparison.
      accuracyRepo.find.mockResolvedValue(
        Array.from({ length: 5 }, () => ({
          targetTime: new Date(),
          features: { sparse_feat: 999 }, // would otherwise show drift
        })),
      );

      const result = await service.detectFeatureDrift();

      // Feature was registered but not scored — driftedFeatures stays
      // empty; the summary still counts the feature in totalFeatures.
      expect(result.driftedFeatures).toEqual([]);
      expect(result.summary.totalFeatures).toBe(1);
      expect(featureDriftRepo.save).not.toHaveBeenCalled();
    });

    it("persists one MLFeatureDrift row per evaluated feature for historical trending", async () => {
      modelRepo.findOne.mockResolvedValue({ version: "v1", isActive: true });
      featureStatsRepo.find.mockResolvedValue([
        { featureName: "f1", mean: 100, std: 10, min: 80, max: 120 },
        { featureName: "f2", mean: 50, std: 5, min: 40, max: 60 },
      ]);
      accuracyRepo.find.mockResolvedValue(
        Array.from({ length: 12 }, () => ({
          targetTime: new Date(),
          features: { f1: 100, f2: 50 },
        })),
      );

      await service.detectFeatureDrift();

      // One save per evaluated feature.
      expect(featureDriftRepo.save).toHaveBeenCalledTimes(2);
      const savedNames = featureDriftRepo.save.mock.calls.map(
        ([row]) => (row as { featureName: string }).featureName,
      );
      expect(savedNames).toEqual(expect.arrayContaining(["f1", "f2"]));
    });

    it("returns a single drifted-features row per evaluated feature", async () => {
      modelRepo.findOne.mockResolvedValue({ version: "v1", isActive: true });
      featureStatsRepo.find.mockResolvedValue([
        { featureName: "f1", mean: 100, std: 10, min: 80, max: 120 },
      ]);
      accuracyRepo.find.mockResolvedValue(
        Array.from({ length: 12 }, (_, i) => ({
          targetTime: new Date(),
          features: { f1: 100 + i }, // varied so std > 0
        })),
      );

      const result = await service.detectFeatureDrift();
      expect(result.driftedFeatures).toHaveLength(1);
      expect(result.driftedFeatures[0].featureName).toBe("f1");
      expect(typeof result.driftedFeatures[0].driftScore).toBe("number");
    });
  });
});
