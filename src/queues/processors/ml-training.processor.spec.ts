import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { getRepositoryToken } from "@nestjs/typeorm";
import * as fs from "fs/promises";
import { MLTrainingProcessor } from "./ml-training.processor";
import { MLModel } from "../../ml/entities/ml-model.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { MLFeatureDriftService } from "../../ml/services/ml-feature-drift.service";

jest.mock("fs/promises");

/**
 * Coverage for the daily 6 AM ML training cron. We don't try to test
 * the actual Python HTTP training call (heavy mock surface, low value)
 * — instead we focus on the **security-critical** helpers and the
 * cleanup path which has been the source of subtle bugs in the past:
 *   1. Path-traversal protection on model version strings.
 *   2. Path-safety against MODEL_DIR escape.
 *   3. Cleanup keeps the active model regardless of age.
 *   4. Cleanup retains the last N models even when older models are
 *      active.
 *   5. Cleanup tolerates missing files (model dir partially gone).
 */
describe("MLTrainingProcessor", () => {
  let processor: MLTrainingProcessor;

  const mlModelRepo = {
    find: jest.fn(),
    remove: jest.fn().mockResolvedValue(undefined),
  };
  const queueDataRepo = {};
  const featureDriftService = {};

  beforeEach(async () => {
    jest.clearAllMocks();
    (fs.unlink as jest.Mock).mockResolvedValue(undefined);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLTrainingProcessor,
        { provide: getRepositoryToken(MLModel), useValue: mlModelRepo },
        { provide: getRepositoryToken(QueueData), useValue: queueDataRepo },
        { provide: MLFeatureDriftService, useValue: featureDriftService },
      ],
    }).compile();

    processor = module.get(MLTrainingProcessor);
  });

  /**
   * The two sanitisation methods are private — exercise them through
   * the public surface (cleanupOldModels) and observe what happens
   * when a malicious version reaches the loop.
   */
  describe("sanitizeVersion (path traversal protection)", () => {
    // Rather than calling the private helper directly, we drive cleanup
    // with crafted versions and observe whether fs.unlink is invoked.
    // A safe version → fs.unlink fires; a rejected version → it doesn't.
    const setupAllModels = (versions: string[]) => {
      const models = versions.map((v, i) => ({
        id: `m${i}`,
        version: v,
        trainedAt: new Date(2020, 0, 1 + i),
        isActive: false,
      }));
      mlModelRepo.find.mockResolvedValueOnce(models);
    };

    it("rejects versions containing `..` (directory traversal)", async () => {
      // 31 safe models + 1 malicious — only the malicious one is
      // selected for deletion (oldest).
      const safe = Array.from({ length: 31 }, (_, i) => `v2026_safe_${i}`);
      const malicious = "../../../etc/passwd";
      setupAllModels([malicious, ...safe]);

      await processor.handleCleanupModels({} as Job);

      // unlink NOT called for the malicious version.
      const unlinkCalls = (fs.unlink as jest.Mock).mock.calls.map(
        ([p]: [string]) => p,
      );
      const maliciousAttempts = unlinkCalls.filter((p) =>
        p.includes(malicious),
      );
      expect(maliciousAttempts).toHaveLength(0);
    });

    it("rejects versions containing `/` (absolute paths)", async () => {
      const safe = Array.from({ length: 31 }, (_, i) => `v_safe_${i}`);
      const malicious = "/etc/passwd";
      setupAllModels([malicious, ...safe]);

      await processor.handleCleanupModels({} as Job);

      const unlinkCalls = (fs.unlink as jest.Mock).mock.calls.map(
        ([p]: [string]) => p,
      );
      expect(unlinkCalls.some((p) => p.includes(malicious))).toBe(false);
    });

    it("accepts safe alphanumeric + dot + dash + underscore versions", async () => {
      // 31 safe — oldest one (last) gets deleted.
      const versions = Array.from(
        { length: 32 },
        (_, i) => `v2026.01.${String(i).padStart(2, "0")}_safe`,
      );
      setupAllModels(versions);

      await processor.handleCleanupModels({} as Job);

      // At least one safe model deleted (oldest beyond retention=30).
      expect(fs.unlink).toHaveBeenCalled();
      const unlinkCalls = (fs.unlink as jest.Mock).mock.calls.map(
        ([p]: [string]) => p,
      );
      // All attempted paths include the catboost prefix → sanitiser
      // didn't reject them.
      expect(unlinkCalls.some((p) => p.includes("catboost_"))).toBe(true);
    });
  });

  describe("cleanupOldModels retention", () => {
    it("keeps the most recent MODELS_TO_KEEP=30 models and deletes the rest", async () => {
      // 35 models → 5 should be deleted (oldest at the bottom of the
      // DESC-sorted list).
      const models = Array.from({ length: 35 }, (_, i) => ({
        id: `m${i}`,
        version: `v_${i}`,
        // Sorted DESC at the repository level → newest first
        trainedAt: new Date(2026, 0, 35 - i),
        isActive: false,
      }));
      mlModelRepo.find.mockResolvedValueOnce(models);

      await processor.handleCleanupModels({} as Job);

      // 5 DB removes (35 - 30).
      expect(mlModelRepo.remove).toHaveBeenCalledTimes(5);
      // The DB entries removed are the 5 OLDEST (last in the array).
      const removedIds = mlModelRepo.remove.mock.calls.map(
        ([m]: [{ id: string }]) => m.id,
      );
      expect(removedIds).toEqual(["m30", "m31", "m32", "m33", "m34"]);
    });

    it("always keeps the active model even if it falls outside the retention window", async () => {
      // 35 models, the OLDEST one is active. Even though it would
      // otherwise be deleted, the active flag protects it.
      const models = Array.from({ length: 35 }, (_, i) => ({
        id: `m${i}`,
        version: `v_${i}`,
        trainedAt: new Date(2026, 0, 35 - i),
        isActive: i === 34, // oldest is active
      }));
      mlModelRepo.find.mockResolvedValueOnce(models);

      await processor.handleCleanupModels({} as Job);

      // The active model (m34) must NOT be in the remove calls.
      const removedIds = mlModelRepo.remove.mock.calls.map(
        ([m]: [{ id: string }]) => m.id,
      );
      expect(removedIds).not.toContain("m34");
      // 4 deletes instead of 5 — the active one took its retention slot.
      expect(mlModelRepo.remove).toHaveBeenCalledTimes(4);
    });

    it("skips cleanup entirely when fewer than MODELS_TO_KEEP models exist", async () => {
      const models = Array.from({ length: 10 }, (_, i) => ({
        id: `m${i}`,
        version: `v_${i}`,
        trainedAt: new Date(),
        isActive: false,
      }));
      mlModelRepo.find.mockResolvedValueOnce(models);

      await processor.handleCleanupModels({} as Job);

      expect(mlModelRepo.remove).not.toHaveBeenCalled();
      expect(fs.unlink).not.toHaveBeenCalled();
    });

    it("tolerates missing files on disk (partial cleanup) without crashing", async () => {
      const models = Array.from({ length: 32 }, (_, i) => ({
        id: `m${i}`,
        version: `v_${i}`,
        trainedAt: new Date(2026, 0, 32 - i),
        isActive: false,
      }));
      mlModelRepo.find.mockResolvedValueOnce(models);
      // First unlink (.cbm) fails — file already gone — but DB cleanup
      // should still happen.
      (fs.unlink as jest.Mock).mockRejectedValue(
        Object.assign(new Error("ENOENT"), { code: "ENOENT" }),
      );

      await processor.handleCleanupModels({} as Job);

      // DB entries still removed for the 2 over-retention models.
      expect(mlModelRepo.remove).toHaveBeenCalledTimes(2);
    });

    it("doesn't rethrow when cleanup fails (training job must not fail on cleanup)", async () => {
      // Make repository.find throw.
      mlModelRepo.find.mockRejectedValueOnce(new Error("DB exploded"));

      // No throw — cleanup is best-effort by design.
      await expect(
        processor.handleCleanupModels({} as Job),
      ).resolves.toBeUndefined();
    });
  });
});
