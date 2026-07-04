import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { MLDriftMonitoringService } from "./ml-drift-monitoring.service";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";
import { MLModel } from "../entities/ml-model.entity";

/**
 * Coverage for the drift-monitoring service that decides whether the
 * model needs retraining based on the gap between trainingMae and the
 * recent live MAE. Two contracts to pin:
 *   1. The drift-status ladder boundaries (healthy ≤20%, warning 20-30%,
 *      critical >30%). A silent slide changes when the model gets
 *      retrained.
 *   2. `shouldRetrain` survives DB errors gracefully — returning
 *      "should=false" lets the cron skip without crashing the alert
 *      chain.
 */
describe("MLDriftMonitoringService", () => {
  let service: MLDriftMonitoringService;

  const accuracyRepo = {
    createQueryBuilder: jest.fn(),
  };
  const mlModelRepo = {
    findOne: jest.fn(),
  };

  /** Build a QB stub that resolves getRawMany to the provided rows. */
  const stubQB = (rows: unknown[]) => ({
    innerJoin: jest.fn().mockReturnThis(),
    leftJoin: jest.fn().mockReturnThis(),
    select: jest.fn().mockReturnThis(),
    addSelect: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    groupBy: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    getRawMany: jest.fn().mockResolvedValue(rows),
  });

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLDriftMonitoringService,
        {
          provide: getRepositoryToken(PredictionAccuracy),
          useValue: accuracyRepo,
        },
        { provide: getRepositoryToken(MLModel), useValue: mlModelRepo },
      ],
    }).compile();

    service = module.get(MLDriftMonitoringService);
  });

  describe("getDriftMetrics", () => {
    it("throws when no active model exists", async () => {
      mlModelRepo.findOne.mockResolvedValue(null);
      await expect(service.getDriftMetrics()).rejects.toThrow(
        /No active model/,
      );
    });

    it("reports 'healthy' when live MAE matches training MAE", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(
        stubQB(
          Array.from({ length: 7 }, (_, i) => ({
            date: `2026-05-${10 + i}`,
            mae: "5.0",
            count: "100",
          })),
        ),
      );

      const result = await service.getDriftMetrics();

      expect(result.status).toBe("healthy");
      expect(result.currentDrift).toBe(0);
      expect(result.liveMae).toBe(5);
      expect(result.trainingMae).toBe(5);
    });

    it("splits drift by horizon: hourly tracked (= top-level), daily untracked (§6a-2)", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(
        stubQB(
          Array.from({ length: 7 }, (_, i) => ({
            date: `2026-05-${10 + i}`,
            mae: "6.25",
            count: "100",
          })),
        ),
      );

      const result = await service.getDriftMetrics();

      const hourly = result.byHorizon.find((h) => h.horizon === "hourly")!;
      const daily = result.byHorizon.find((h) => h.horizon === "daily")!;
      // hourly mirrors the (only-scored) top-level drift
      expect(hourly.tracked).toBe(true);
      expect(hourly.currentDrift).toBe(result.currentDrift);
      expect(hourly.liveMae).toBe(result.liveMae);
      // far-daily is never scored → surfaced honestly as untracked, not a fake 0
      expect(daily.tracked).toBe(false);
      expect(daily.status).toBe("untracked");
      expect(daily.currentDrift).toBeNull();
      expect(daily.liveMae).toBeNull();
    });

    it("reports 'warning' when live MAE is 20-30% worse than training", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      // Live MAE = 6.25 → 25% worse than training (5) → "warning"
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(
        stubQB(
          Array.from({ length: 7 }, (_, i) => ({
            date: `2026-05-${10 + i}`,
            mae: "6.25",
            count: "100",
          })),
        ),
      );

      const result = await service.getDriftMetrics();

      expect(result.status).toBe("warning");
      expect(result.currentDrift).toBe(25);
    });

    it("reports 'critical' when live MAE is >30% worse than training", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      // Live MAE = 7 → 40% worse → "critical"
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(
        stubQB(
          Array.from({ length: 7 }, (_, i) => ({
            date: `2026-05-${10 + i}`,
            mae: "7.0",
            count: "100",
          })),
        ),
      );

      const result = await service.getDriftMetrics();

      expect(result.status).toBe("critical");
      expect(result.currentDrift).toBe(40);
    });

    it("only averages over the last 7 daily metrics, not the whole range", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      // 10 days of data — only the LAST 7 should count toward liveMae.
      const rows = [
        // First 3 days: very bad (avg 20) — should be IGNORED
        ...Array.from({ length: 3 }, (_, i) => ({
          date: `2026-05-${i + 1}`,
          mae: "20.0",
          count: "10",
        })),
        // Last 7 days: healthy (avg 5)
        ...Array.from({ length: 7 }, (_, i) => ({
          date: `2026-05-${i + 4}`,
          mae: "5.0",
          count: "100",
        })),
      ];
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(stubQB(rows));

      const result = await service.getDriftMetrics();

      // Only the last 7 days (avg 5) drive liveMae — so it equals
      // trainingMae and status is healthy.
      expect(result.liveMae).toBe(5);
      expect(result.status).toBe("healthy");
    });

    it("falls back to trainingMae when no recent metrics exist (cold-start safety)", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(stubQB([]));

      const result = await service.getDriftMetrics();

      // No live data → liveMae = trainingMae → 0% drift.
      expect(result.liveMae).toBe(5);
      expect(result.currentDrift).toBe(0);
      expect(result.dailyMetrics).toEqual([]);
    });
  });

  describe("shouldRetrain", () => {
    it("triggers retrain on critical drift", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(
        stubQB(
          Array.from({ length: 7 }, () => ({
            date: "2026-05-15",
            mae: "8.0", // 60% drift → critical
            count: "100",
          })),
        ),
      );

      const result = await service.shouldRetrain();

      expect(result.should).toBe(true);
      expect(result.reason).toMatch(/critical/i);
    });

    it("triggers retrain on warning drift that's >25% (not just >20%)", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      // 27% drift → warning bucket, AND > 25% → should retrain
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(
        stubQB(
          Array.from({ length: 7 }, () => ({
            date: "2026-05-15",
            mae: "6.35",
            count: "100",
          })),
        ),
      );

      const result = await service.shouldRetrain();
      expect(result.should).toBe(true);
    });

    it("does NOT trigger retrain when drift is in the warning band but below 25%", async () => {
      mlModelRepo.findOne.mockResolvedValue({ mae: 5, isActive: true });
      // 22% drift → warning, but ≤25% → no retrain
      accuracyRepo.createQueryBuilder.mockReturnValueOnce(
        stubQB(
          Array.from({ length: 7 }, () => ({
            date: "2026-05-15",
            mae: "6.1",
            count: "100",
          })),
        ),
      );

      const result = await service.shouldRetrain();
      expect(result.should).toBe(false);
    });

    it("returns should=false when drift check fails (don't trigger blind retrain)", async () => {
      mlModelRepo.findOne.mockRejectedValueOnce(new Error("DB down"));

      const result = await service.shouldRetrain();

      expect(result.should).toBe(false);
      expect(result.reason).toMatch(/error/i);
    });
  });
});
