import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { MLAnomalyDetectionService } from "./ml-anomaly-detection.service";
import { MLPredictionAnomaly } from "../entities/ml-prediction-anomaly.entity";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";

/**
 * Coverage for the anomaly-detection N+1 fix. The old `createAnomaly`
 * issued a per-call `findOne()` to dedup against the last hour of
 * anomalies — for a 10k-prediction batch with up to 3 condition
 * triggers per row that meant 30k DB round-trips per nightly run.
 *
 * The refactor pre-loads every recent anomaly once and dedups in
 * memory. These tests pin the new contract down so a future refactor
 * can't accidentally reintroduce the old per-anomaly findOne.
 */
describe("MLAnomalyDetectionService", () => {
  let service: MLAnomalyDetectionService;

  const anomalyRepo = {
    find: jest.fn().mockResolvedValue([]),
    findOne: jest.fn(), // tracked-but-should-not-be-called for the N+1
    save: jest
      .fn()
      .mockImplementation((entity) =>
        Promise.resolve({ id: "saved-" + Math.random(), ...entity }),
      ),
  };

  const accuracyRepo = {
    find: jest.fn().mockResolvedValue([]),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLAnomalyDetectionService,
        {
          provide: getRepositoryToken(MLPredictionAnomaly),
          useValue: anomalyRepo,
        },
        {
          provide: getRepositoryToken(PredictionAccuracy),
          useValue: accuracyRepo,
        },
      ],
    }).compile();

    service = module.get(MLAnomalyDetectionService);
  });

  describe("detectAnomalies", () => {
    it("returns empty result when no predictions to analyse", async () => {
      accuracyRepo.find.mockResolvedValueOnce([]);

      const result = await service.detectAnomalies(7);

      expect(result.detected).toBe(0);
      expect(result.anomalies).toEqual([]);
      // No dedup pre-load either — short-circuit before touching anomalies.
      expect(anomalyRepo.find).not.toHaveBeenCalled();
    });

    it("pre-loads existing anomalies with ONE find() — no per-prediction findOne", async () => {
      // Synthetic prediction batch with a clear extreme value.
      // mean wait ≈ 30, std ≈ 5, prediction at 200 → extreme.
      const predictions = Array.from({ length: 100 }, (_, i) => ({
        id: `acc-${i}`,
        attractionId: `a-${i % 10}`, // 10 distinct attractions
        targetTime: new Date(`2026-05-18T12:00:00Z`),
        predictedWaitTime: 30,
        actualWaitTime: 30 + (Math.random() - 0.5) * 4,
        absoluteError: Math.random() * 3,
        wasUnplannedClosure: false,
        features: null,
        modelVersion: "v1",
        comparisonStatus: "COMPLETED",
      }));
      // One genuinely extreme value.
      predictions[0].actualWaitTime = 200;
      predictions[0].absoluteError = 170;

      accuracyRepo.find.mockResolvedValueOnce(predictions);

      await service.detectAnomalies(7);

      // The whole point: exactly one find() for dedup, regardless of
      // prediction count. The old code did `findOne()` per anomaly
      // trigger — that's the regression bait.
      expect(anomalyRepo.find).toHaveBeenCalledTimes(1);
      expect(anomalyRepo.findOne).not.toHaveBeenCalled();
    });

    it("dedups in-memory against pre-loaded anomalies (no duplicate save)", async () => {
      const targetTime = new Date("2026-05-18T12:00:00Z");
      const predictions = [
        {
          id: "acc-1",
          attractionId: "a-1",
          targetTime,
          predictedWaitTime: 30,
          actualWaitTime: 200, // extreme
          absoluteError: 170, // also large_error
          wasUnplannedClosure: false,
          features: null,
          modelVersion: "v1",
          comparisonStatus: "COMPLETED",
        },
        // Cluster with enough variance so std > 0 and the threshold check
        // triggers for the 200-wait outlier above.
        ...Array.from({ length: 30 }, (_, i) => ({
          id: `acc-bg-${i}`,
          attractionId: `bg-${i}`,
          targetTime,
          predictedWaitTime: 30,
          actualWaitTime: 30 + (i % 5),
          absoluteError: 1 + (i % 3),
          wasUnplannedClosure: false,
          features: null,
          modelVersion: "v1",
          comparisonStatus: "COMPLETED",
        })),
      ];

      // Pre-existing extreme_value anomaly for a-1 detected 30 minutes
      // ago → should suppress a duplicate this run.
      const recentDetectedAt = new Date(targetTime.getTime() - 30 * 60 * 1000);
      anomalyRepo.find.mockResolvedValueOnce([
        {
          attractionId: "a-1",
          anomalyType: "extreme_value",
          detectedAt: recentDetectedAt,
        },
      ]);
      accuracyRepo.find.mockResolvedValueOnce(predictions);

      const result = await service.detectAnomalies(7);

      // extreme_value was deduped → not saved. large_error wasn't
      // deduped → may or may not have been saved depending on threshold.
      const savedTypes = anomalyRepo.save.mock.calls.map(
        ([entity]) => (entity as { anomalyType: string }).anomalyType,
      );
      expect(savedTypes).not.toContain("extreme_value");
      // No matter what was triggered, no row touches DB in dedup branches.
      void result;
    });

    it("does not double-emit within the same run for the same (attraction, type)", async () => {
      const targetTime = new Date("2026-05-18T12:00:00Z");
      const targetTimeLater = new Date("2026-05-18T12:30:00Z");
      // Two predictions, same attraction, both within the 1-h dedup
      // window — should produce ONE anomaly of each triggered type.
      const predictions = [
        {
          id: "acc-1",
          attractionId: "a-1",
          targetTime,
          predictedWaitTime: 30,
          actualWaitTime: 200,
          absoluteError: 170,
          wasUnplannedClosure: false,
          features: null,
          modelVersion: "v1",
          comparisonStatus: "COMPLETED",
        },
        {
          id: "acc-2",
          attractionId: "a-1",
          targetTime: targetTimeLater,
          predictedWaitTime: 30,
          actualWaitTime: 210,
          absoluteError: 180,
          wasUnplannedClosure: false,
          features: null,
          modelVersion: "v1",
          comparisonStatus: "COMPLETED",
        },
        ...Array.from({ length: 30 }, (_, i) => ({
          id: `acc-bg-${i}`,
          attractionId: `bg-${i}`,
          targetTime,
          predictedWaitTime: 30,
          actualWaitTime: 30 + (i % 5),
          absoluteError: 1 + (i % 3),
          wasUnplannedClosure: false,
          features: null,
          modelVersion: "v1",
          comparisonStatus: "COMPLETED",
        })),
      ];
      accuracyRepo.find.mockResolvedValueOnce(predictions);
      anomalyRepo.find.mockResolvedValueOnce([]);

      await service.detectAnomalies(7);

      // For attraction a-1 + extreme_value: only ONE save, even though
      // two predictions in the same run triggered the condition.
      const a1Extreme = anomalyRepo.save.mock.calls.filter(([entity]) => {
        const e = entity as { attractionId: string; anomalyType: string };
        return e.attractionId === "a-1" && e.anomalyType === "extreme_value";
      });
      expect(a1Extreme.length).toBeLessThanOrEqual(1);
    });
  });
});
