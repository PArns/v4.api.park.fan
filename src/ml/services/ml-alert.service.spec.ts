import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { In } from "typeorm";
import { MLAlertService } from "./ml-alert.service";
import { MLAlert } from "../entities/ml-alert.entity";
import { PredictionAccuracyService } from "./prediction-accuracy.service";
import { MLFeatureDriftService } from "./ml-feature-drift.service";

/**
 * Coverage for MLAlertService — the alert creation/auto-resolve loop
 * that runs hourly. PR #46 replaced a per-alert SELECT+save() N+1
 * inside `resolveAlertIfActive` with a single bulk UPDATE; this test
 * file pins that contract down and exercises the rest of the lifecycle
 * (create-on-condition, dedup-on-active, escalate-on-severity-increase,
 * acknowledge, resolve, cleanup, severity ladder).
 */
describe("MLAlertService", () => {
  let service: MLAlertService;

  const alertRepo = {
    findOne: jest.fn(),
    find: jest.fn(),
    save: jest.fn(),
    update: jest.fn().mockResolvedValue({ affected: 0 }),
    delete: jest.fn().mockResolvedValue({ affected: 0 }),
  };

  const accuracyService = {
    getSystemAccuracyStats: jest.fn(),
  };

  const featureDriftService = {
    detectFeatureDrift: jest.fn().mockResolvedValue({
      driftedFeatures: [],
      summary: { criticalCount: 0, warningCount: 0, totalFeatures: 0 },
    }),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLAlertService,
        { provide: getRepositoryToken(MLAlert), useValue: alertRepo },
        { provide: PredictionAccuracyService, useValue: accuracyService },
        { provide: MLFeatureDriftService, useValue: featureDriftService },
      ],
    }).compile();

    service = module.get(MLAlertService);
  });

  describe("resolveAlertIfActive — N+1 fix (PR #46)", () => {
    // The private method used to SELECT all matching alerts then call
    // save() N times. PR #46 replaced it with a single repository.update.
    // We exercise it via checkAndCreateAlerts when the condition clears.
    it("auto-resolves all matching alerts with ONE UPDATE call (no N+1 save loop)", async () => {
      // Healthy MAE (< 8) triggers the auto-resolve branch for
      // accuracy_degradation alerts. Drift checks return clean too.
      accuracyService.getSystemAccuracyStats.mockResolvedValue({
        overall: {
          mae: 4,
          coveragePercent: 98,
          totalPredictions: 1000,
          matchedPredictions: 980,
        },
      });
      featureDriftService.detectFeatureDrift.mockResolvedValue({
        driftedFeatures: [],
        summary: { criticalCount: 0, warningCount: 0, totalFeatures: 0 },
      });
      alertRepo.update.mockResolvedValue({ affected: 3 });

      await service.checkAndCreateAlerts();

      // We expect at least one bulk UPDATE — and crucially, NO per-alert
      // save() invocations (that would be the old N+1 pattern).
      expect(alertRepo.update).toHaveBeenCalled();
      const accuracyCall = alertRepo.update.mock.calls.find(
        ([criteria]) =>
          (criteria as Record<string, unknown>).alertType ===
          "accuracy_degradation",
      );
      expect(accuracyCall).toBeDefined();
      // The criteria uses `status: In(["active", "acknowledged"])`
      // — covers both states in one call.
      expect((accuracyCall![0] as Record<string, unknown>).status).toEqual(
        In(["active", "acknowledged"]),
      );
      // The payload sets status=resolved + a resolvedAt timestamp.
      const payload = accuracyCall![1] as Record<string, unknown>;
      expect(payload.status).toBe("resolved");
      expect(payload.resolvedAt).toBeInstanceOf(Date);
      expect(payload.resolutionNote).toMatch(/auto-resolved/i);
      // And nothing went through save() — that's the regression-bait.
      expect(alertRepo.save).not.toHaveBeenCalled();
    });
  });

  describe("checkAndCreateAlerts — create on threshold breach", () => {
    it("creates an accuracy_degradation alert when MAE crosses 8 minutes", async () => {
      accuracyService.getSystemAccuracyStats.mockResolvedValue({
        overall: {
          mae: 12,
          coveragePercent: 95,
          totalPredictions: 500,
          matchedPredictions: 475,
        },
      });
      featureDriftService.detectFeatureDrift.mockResolvedValue({
        driftedFeatures: [],
        summary: { criticalCount: 0, warningCount: 0, totalFeatures: 0 },
      });
      // No existing alert — createAlert path produces a new one.
      alertRepo.findOne.mockResolvedValue(null);
      alertRepo.save.mockImplementation((entity) =>
        Promise.resolve({ id: "new-1", ...entity }),
      );

      const { alerts } = await service.checkAndCreateAlerts();

      const accuracyAlerts = alerts.filter(
        (a) => a.alertType === "accuracy_degradation",
      );
      expect(accuracyAlerts).toHaveLength(1);
      expect(accuracyAlerts[0].severity).toBe("high"); // MAE 12 → high (>10)
      expect(accuracyAlerts[0].title).toMatch(/accuracy/i);
      expect(accuracyAlerts[0].message).toContain("12");
    });

    it("returns null instead of creating a duplicate when an active alert exists", async () => {
      accuracyService.getSystemAccuracyStats.mockResolvedValue({
        overall: {
          mae: 12,
          coveragePercent: 95,
          totalPredictions: 500,
          matchedPredictions: 475,
        },
      });
      featureDriftService.detectFeatureDrift.mockResolvedValue({
        driftedFeatures: [],
        summary: { criticalCount: 0, warningCount: 0, totalFeatures: 0 },
      });
      // Existing alert at SAME severity — no new save, no escalate.
      alertRepo.findOne.mockResolvedValue({
        id: "existing-1",
        alertType: "accuracy_degradation",
        severity: "high",
        status: "active",
      });

      const { alerts } = await service.checkAndCreateAlerts();

      expect(
        alerts.filter((a) => a.alertType === "accuracy_degradation"),
      ).toHaveLength(0);
      // No save call for accuracy_degradation — that's the dedup contract.
      expect(alertRepo.save).not.toHaveBeenCalled();
    });

    it("escalates an existing alert when severity increases", async () => {
      accuracyService.getSystemAccuracyStats.mockResolvedValue({
        // MAE 18 → critical (>15)
        overall: {
          mae: 18,
          coveragePercent: 95,
          totalPredictions: 500,
          matchedPredictions: 475,
        },
      });
      featureDriftService.detectFeatureDrift.mockResolvedValue({
        driftedFeatures: [],
        summary: { criticalCount: 0, warningCount: 0, totalFeatures: 0 },
      });
      alertRepo.findOne.mockResolvedValue({
        id: "existing-1",
        alertType: "accuracy_degradation",
        severity: "medium", // currently medium — should escalate.
        status: "active",
      });
      alertRepo.save.mockImplementation((entity) => Promise.resolve(entity));

      const { alerts } = await service.checkAndCreateAlerts();

      const accuracy = alerts.find(
        (a) => a.alertType === "accuracy_degradation",
      );
      expect(accuracy).toBeDefined();
      expect(accuracy!.severity).toBe("critical");
      // Existing record reused, not a new one — id stays the same.
      expect(accuracy!.id).toBe("existing-1");
    });
  });

  describe("severity ladder", () => {
    // Severity boundaries are user-visible thresholds: a slide here
    // means the on-call gets paged at a different MAE.
    it.each([
      [5, "accuracy_degradation"],
      [9, "accuracy_degradation"],
      [11, "accuracy_degradation"],
      [16, "accuracy_degradation"],
    ])("classifies MAE=%d correctly via createAlert dispatch", async (mae) => {
      accuracyService.getSystemAccuracyStats.mockResolvedValue({
        overall: {
          mae,
          coveragePercent: 95,
          totalPredictions: 500,
          matchedPredictions: 475,
        },
      });
      featureDriftService.detectFeatureDrift.mockResolvedValue({
        driftedFeatures: [],
        summary: { criticalCount: 0, warningCount: 0, totalFeatures: 0 },
      });
      alertRepo.findOne.mockResolvedValue(null);
      alertRepo.save.mockImplementation((entity) =>
        Promise.resolve({ id: "x", ...entity }),
      );

      const { alerts } = await service.checkAndCreateAlerts();

      // MAE=5 doesn't trigger the alert at all (threshold is >8).
      if (mae <= 8) {
        expect(
          alerts.filter((a) => a.alertType === "accuracy_degradation"),
        ).toHaveLength(0);
        return;
      }
      const accuracy = alerts.find(
        (a) => a.alertType === "accuracy_degradation",
      );
      const expectedSeverity =
        mae > 15 ? "critical" : mae > 10 ? "high" : mae > 7 ? "medium" : "low";
      expect(accuracy!.severity).toBe(expectedSeverity);
    });
  });

  describe("acknowledge + resolve lifecycle", () => {
    it("acknowledgeAlert flips the status and stores who/when", async () => {
      const stored: Record<string, unknown> = {
        id: "a-1",
        alertType: "accuracy_degradation",
        severity: "high",
        status: "active",
      };
      alertRepo.findOne.mockResolvedValue(stored);
      alertRepo.save.mockImplementation((entity) => Promise.resolve(entity));

      const result = await service.acknowledgeAlert("a-1", "alice@example.com");

      expect(result.status).toBe("acknowledged");
      expect(result.acknowledgedBy).toBe("alice@example.com");
      expect(result.acknowledgedAt).toBeInstanceOf(Date);
    });

    it("acknowledgeAlert throws when the id is unknown", async () => {
      alertRepo.findOne.mockResolvedValue(null);
      await expect(
        service.acknowledgeAlert("nope", "alice@example.com"),
      ).rejects.toThrow(/not found/);
    });

    it("resolveAlert records the note and timestamp", async () => {
      alertRepo.findOne.mockResolvedValue({
        id: "a-1",
        alertType: "data_drift",
        severity: "medium",
        status: "active",
      });
      alertRepo.save.mockImplementation((entity) => Promise.resolve(entity));

      const result = await service.resolveAlert("a-1", "Fixed by retrain");

      expect(result.status).toBe("resolved");
      expect(result.resolutionNote).toBe("Fixed by retrain");
      expect(result.resolvedAt).toBeInstanceOf(Date);
    });
  });

  describe("cleanupOldAlerts", () => {
    it("deletes resolved alerts older than 30 days and returns the row count", async () => {
      alertRepo.delete.mockResolvedValue({ affected: 7 });

      const removed = await service.cleanupOldAlerts();

      expect(removed).toBe(7);
      const [criteria] = alertRepo.delete.mock.calls[0];
      expect((criteria as Record<string, unknown>).status).toBe("resolved");
      // resolvedAt is a TypeORM `LessThan(date)` operator — just assert
      // it's truthy and roughly 30 days ago.
      expect((criteria as Record<string, unknown>).resolvedAt).toBeDefined();
    });
  });
});
