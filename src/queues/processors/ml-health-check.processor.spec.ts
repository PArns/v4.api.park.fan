import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { getQueueToken } from "@nestjs/bull";
import { MLHealthCheckProcessor } from "./ml-health-check.processor";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";

/**
 * The ml-health-check processor runs daily at 2 AM and decides
 * whether to trigger an automated retraining job. It's the only
 * automatic feedback loop we have for model quality — if it falsely
 * decides "all good" we keep serving a degraded model; if it
 * over-triggers we burn ML training time. These tests pin both
 * directions.
 */
describe("MLHealthCheckProcessor", () => {
  let processor: MLHealthCheckProcessor;

  const predictionAccuracyService = {
    checkRetrainingNeeded: jest.fn(),
  };
  const mlTrainingQueue = {
    add: jest.fn().mockResolvedValue({ id: "job-1" }),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLHealthCheckProcessor,
        {
          provide: PredictionAccuracyService,
          useValue: predictionAccuracyService,
        },
        { provide: getQueueToken("ml-training"), useValue: mlTrainingQueue },
      ],
    }).compile();

    processor = module.get(MLHealthCheckProcessor);
  });

  describe("check-model-health (daily 2 AM)", () => {
    it("does NOT trigger retraining when health is good", async () => {
      predictionAccuracyService.checkRetrainingNeeded.mockResolvedValue({
        needed: false,
        reason: null,
        metrics: { mae: 4.2, coveragePercent: 95, mape: 8.5 },
      });

      await processor.handleHealthCheck({} as Job);

      expect(
        predictionAccuracyService.checkRetrainingNeeded,
      ).toHaveBeenCalledWith(7);
      expect(mlTrainingQueue.add).not.toHaveBeenCalled();
    });

    it("triggers training with the diagnostic payload when health degrades", async () => {
      const metrics = { mae: 12.5, coveragePercent: 78, mape: 18.2 };
      predictionAccuracyService.checkRetrainingNeeded.mockResolvedValue({
        needed: true,
        reason: "MAE exceeded threshold",
        metrics,
      });

      await processor.handleHealthCheck({} as Job);

      expect(mlTrainingQueue.add).toHaveBeenCalledTimes(1);
      const [jobName, payload] = mlTrainingQueue.add.mock.calls[0];
      expect(jobName).toBe("train-model");
      // The payload includes the reason + current metrics — operators
      // see exactly *why* the retrain was triggered.
      expect(payload).toMatchObject({
        reason: "MAE exceeded threshold",
        triggeredBy: "automated-health-check",
        currentMetrics: metrics,
      });
    });

    it("still triggers retraining when metrics are missing (gracefully)", async () => {
      predictionAccuracyService.checkRetrainingNeeded.mockResolvedValue({
        needed: true,
        reason: "Insufficient comparison data",
        metrics: null,
      });

      await processor.handleHealthCheck({} as Job);

      expect(mlTrainingQueue.add).toHaveBeenCalled();
      const [, payload] = mlTrainingQueue.add.mock.calls[0];
      expect(
        (payload as { currentMetrics: unknown }).currentMetrics,
      ).toBeNull();
    });

    it("rethrows on health-check failure so Bull retries", async () => {
      predictionAccuracyService.checkRetrainingNeeded.mockRejectedValueOnce(
        new Error("DB timeout"),
      );

      await expect(processor.handleHealthCheck({} as Job)).rejects.toThrow(
        /DB timeout/,
      );
      // No retrain triggered on a failed check — we'd be flying blind.
      expect(mlTrainingQueue.add).not.toHaveBeenCalled();
    });
  });
});
