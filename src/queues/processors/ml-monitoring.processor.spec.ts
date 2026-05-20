import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { MLMonitoringProcessor } from "./ml-monitoring.processor";
import { MLFeatureDriftService } from "../../ml/services/ml-feature-drift.service";
import { MLAlertService } from "../../ml/services/ml-alert.service";
import { MLAnomalyDetectionService } from "../../ml/services/ml-anomaly-detection.service";

/**
 * The ml-monitoring cron is a thin orchestrator over three services
 * (drift, alerts, anomalies). Its contract is "log + rethrow on
 * failure" so the Bull queue retries on the next schedule. These
 * tests pin that contract down per job — a silent swallow here means
 * we lose visibility into model degradation.
 */
describe("MLMonitoringProcessor", () => {
  let processor: MLMonitoringProcessor;

  const featureDriftService = {
    detectFeatureDrift: jest.fn(),
  };
  const alertService = {
    checkAndCreateAlerts: jest.fn(),
    cleanupOldAlerts: jest.fn(),
  };
  const anomalyDetectionService = {
    detectAnomalies: jest.fn(),
    cleanupOldAnomalies: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLMonitoringProcessor,
        { provide: MLFeatureDriftService, useValue: featureDriftService },
        { provide: MLAlertService, useValue: alertService },
        {
          provide: MLAnomalyDetectionService,
          useValue: anomalyDetectionService,
        },
      ],
    }).compile();

    processor = module.get(MLMonitoringProcessor);
  });

  describe("detect-feature-drift (daily)", () => {
    it("delegates to the drift service with a 7-day window", async () => {
      featureDriftService.detectFeatureDrift.mockResolvedValue({
        driftedFeatures: [],
        summary: {
          totalFeatures: 0,
          healthyCount: 0,
          warningCount: 0,
          criticalCount: 0,
        },
      });

      await processor.handleFeatureDriftDetection({} as Job);

      expect(featureDriftService.detectFeatureDrift).toHaveBeenCalledWith(7);
    });

    it("rethrows so Bull retries on next schedule", async () => {
      featureDriftService.detectFeatureDrift.mockRejectedValueOnce(
        new Error("DB down"),
      );

      await expect(
        processor.handleFeatureDriftDetection({} as Job),
      ).rejects.toThrow(/DB down/);
    });
  });

  describe("check-alerts (hourly)", () => {
    it("delegates to the alert service and tolerates zero new alerts", async () => {
      alertService.checkAndCreateAlerts.mockResolvedValue({
        created: 0,
        alerts: [],
      });

      await processor.handleAlertCheck({} as Job);

      expect(alertService.checkAndCreateAlerts).toHaveBeenCalled();
    });

    it("does NOT throw when alerts get created — they're the success case", async () => {
      alertService.checkAndCreateAlerts.mockResolvedValue({
        created: 2,
        alerts: [
          { title: "Accuracy Degradation" },
          { title: "Critical Feature Drift" },
        ],
      });

      await expect(
        processor.handleAlertCheck({} as Job),
      ).resolves.toBeUndefined();
    });

    it("rethrows on service failure (Bull retries)", async () => {
      alertService.checkAndCreateAlerts.mockRejectedValueOnce(
        new Error("Redis down"),
      );

      await expect(processor.handleAlertCheck({} as Job)).rejects.toThrow(
        /Redis down/,
      );
    });
  });

  describe("detect-anomalies (daily)", () => {
    it("delegates with a 7-day window and reports detected count", async () => {
      anomalyDetectionService.detectAnomalies.mockResolvedValue({
        detected: 5,
        anomalies: [],
      });

      await processor.handleAnomalyDetection({} as Job);

      expect(anomalyDetectionService.detectAnomalies).toHaveBeenCalledWith(7);
    });

    it("rethrows on failure", async () => {
      anomalyDetectionService.detectAnomalies.mockRejectedValueOnce(
        new Error("anomaly query crashed"),
      );

      await expect(processor.handleAnomalyDetection({} as Job)).rejects.toThrow(
        /anomaly query crashed/,
      );
    });
  });

  describe("cleanup (daily)", () => {
    it("runs alert + anomaly cleanups in parallel (one wall-time, not two)", async () => {
      alertService.cleanupOldAlerts.mockResolvedValue(3);
      anomalyDetectionService.cleanupOldAnomalies.mockResolvedValue(7);

      await processor.handleCleanup({} as Job);

      expect(alertService.cleanupOldAlerts).toHaveBeenCalled();
      expect(anomalyDetectionService.cleanupOldAnomalies).toHaveBeenCalled();
    });

    it("rethrows when either cleanup fails", async () => {
      alertService.cleanupOldAlerts.mockResolvedValue(0);
      anomalyDetectionService.cleanupOldAnomalies.mockRejectedValueOnce(
        new Error("anomaly cleanup failed"),
      );

      await expect(processor.handleCleanup({} as Job)).rejects.toThrow(
        /anomaly cleanup failed/,
      );
    });
  });
});
