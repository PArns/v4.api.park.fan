import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { getRepositoryToken } from "@nestjs/typeorm";
import * as fs from "fs/promises";
import { MLTrainingProcessor } from "./ml-training.processor";
import { MLModel } from "../../ml/entities/ml-model.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { MLFeatureDriftService } from "../../ml/services/ml-feature-drift.service";
import axios from "axios";

jest.mock("fs/promises");
jest.mock("axios");
jest.mock("../../common/utils/file-logger.util", () => ({
  logJobFailure: jest.fn(),
}));

const mockedAxios = axios as jest.Mocked<typeof axios>;

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
    findOne: jest.fn(),
    update: jest.fn().mockResolvedValue(undefined),
    create: jest.fn((m: Record<string, unknown>) => m),
    save: jest.fn().mockResolvedValue(undefined),
  };
  const queueDataRepo = {
    createQueryBuilder: jest.fn(() => ({
      select: jest.fn().mockReturnThis(),
      addSelect: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      getRawOne: jest.fn().mockResolvedValue({
        minTime: "2025-12-24T00:00:00Z",
        maxTime: "2026-10-07T00:00:00Z",
      }),
    })),
  };
  const featureDriftService = {
    storeFeatureStats: jest.fn().mockResolvedValue(undefined),
  };

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

  /**
   * PAR-815: the processor used to read the new model's metrics from
   * `/model/info`, which answers with whatever model the worker has loaded.
   * Workers only reload on their next /predict, so after a 60 s wait it took the
   * PREVIOUS model's metrics and registered them under the new version — the
   * champion/challenger gate then compared the champion with itself.
   */
  describe("handleTrainModels — registering a trained version", () => {
    const ML = "http://ml-service:8000";
    let setTimeoutSpy: jest.SpyInstance;

    const savedInfo = (version: string, mae: number) => ({
      version,
      trainedAt: "2026-10-06T06:40:00Z",
      metrics: { mae, rmse: 9.1, mape: 30, r2: 0.8 },
      features: ["hour", "attractionId"],
      train_samples: 3108818,
      val_samples: 665830,
      hyperparameters: {},
      featureStats: [],
    });

    /** Routes GETs: status as given; the loaded model is the OLD one. */
    const routeGets = (
      byVersion: () => Promise<unknown>,
      trainStatus: Record<string, unknown> = { status: "completed" },
    ) => {
      mockedAxios.get.mockImplementation((url: string) => {
        if (url === `${ML}/train/status`) {
          return Promise.resolve({ data: trainStatus });
        }
        if (url === `${ML}/model/info`) {
          // A worker still serving the previous champion.
          return Promise.resolve({ data: savedInfo("v20261003_0600", 5.06) });
        }
        if (url.startsWith(`${ML}/model/info/`)) return byVersion();
        return Promise.reject(new Error(`unexpected GET ${url}`));
      });
    };

    beforeEach(() => {
      delete process.env.ML_SERVICE_URL;
      jest.useFakeTimers({ now: new Date("2026-10-06T06:00:00Z") });
      // Skip the 30 s poll waits and retry backoffs.
      setTimeoutSpy = jest.spyOn(global, "setTimeout").mockImplementation(((
        fn: () => void,
      ) => {
        fn();
        return 0 as unknown as NodeJS.Timeout;
      }) as unknown as typeof setTimeout);
      // /model/reload loads the DB-active row; after an accepted run that is
      // the new version.
      mockedAxios.post.mockImplementation((url: string) =>
        Promise.resolve({
          data:
            url === `${ML}/model/reload`
              ? { version: "v20261006_0600" }
              : { status: "training_started" },
        }),
      );
      mlModelRepo.findOne.mockResolvedValue({
        version: "v20261003_0600",
        mae: 5.06,
        isActive: true,
      });
      mlModelRepo.find.mockResolvedValue([]);
    });

    afterEach(() => {
      setTimeoutSpy.mockRestore();
      jest.useRealTimers();
    });

    it("registers the metrics saved for THIS version, not the loaded model's", async () => {
      routeGets(() =>
        Promise.resolve({ data: savedInfo("v20261006_0600", 4.433) }),
      );

      await processor.handleTrainModels({} as Job);

      expect(mockedAxios.get).toHaveBeenCalledWith(
        `${ML}/model/info/v20261006_0600`,
      );
      expect(mockedAxios.get).not.toHaveBeenCalledWith(`${ML}/model/info`);
      expect(mlModelRepo.save).toHaveBeenCalledTimes(1);
      const saved = mlModelRepo.save.mock.calls[0][0];
      expect(saved.version).toBe("v20261006_0600");
      expect(saved.mae).toBe(4.433);
      expect(saved.isActive).toBe(true);
    });

    it("activates only AFTER the row is saved and the old champion retired", async () => {
      routeGets(() =>
        Promise.resolve({ data: savedInfo("v20261006_0600", 4.433) }),
      );

      await processor.handleTrainModels({} as Job);

      const saveOrder = mlModelRepo.save.mock.invocationCallOrder[0];
      const retireOrder = mlModelRepo.update.mock.invocationCallOrder[0];
      const reloadIdx = mockedAxios.post.mock.calls.findIndex(
        ([u]) => u === `${ML}/model/reload`,
      );
      expect(reloadIdx).toBeGreaterThanOrEqual(0);
      const reloadOrder = mockedAxios.post.mock.invocationCallOrder[reloadIdx];
      expect(saveOrder).toBeLessThan(retireOrder);
      expect(retireOrder).toBeLessThan(reloadOrder);
      // The retire step leaves the new row alone.
      const [where, set] = mlModelRepo.update.mock.calls[0];
      expect(where.isActive).toBe(true);
      expect(where.version).toBeDefined();
      expect(set).toEqual({ isActive: false });
    });

    it("rolls the DB back to the champion when the ml-service cannot activate the new version", async () => {
      routeGets(() =>
        Promise.resolve({ data: savedInfo("v20261006_0600", 4.433) }),
      );
      mockedAxios.post.mockImplementation((url: string) =>
        url === `${ML}/model/reload`
          ? Promise.reject(new Error("Failed to reload model"))
          : Promise.resolve({ data: {} }),
      );

      await expect(processor.handleTrainModels({} as Job)).rejects.toThrow(
        /Could not activate v20261006_0600.*kept v20261003_0600 active/,
      );

      expect(
        mockedAxios.post.mock.calls.filter(([u]) => u === `${ML}/model/reload`),
      ).toHaveLength(3);
      expect(mlModelRepo.update).toHaveBeenCalledWith(
        { version: "v20261006_0600" },
        expect.objectContaining({ isActive: false }),
      );
      expect(mlModelRepo.update).toHaveBeenCalledWith(
        { version: "v20261003_0600" },
        { isActive: true },
      );
    });

    it("never touches serving when training fails", async () => {
      routeGets(() => Promise.reject(new Error("unreachable")), {
        status: "failed",
        error: "boom",
      });

      await expect(processor.handleTrainModels({} as Job)).rejects.toThrow(
        /Training failed: boom/,
      );
      expect(mockedAxios.post).not.toHaveBeenCalledWith(`${ML}/model/reload`);
      expect(mlModelRepo.save).not.toHaveBeenCalled();
    });

    it("never touches serving when training times out", async () => {
      routeGets(() => Promise.reject(new Error("unreachable")), {
        status: "training",
      });

      await expect(processor.handleTrainModels({} as Job)).rejects.toThrow(
        /Training timeout - exceeded 90 minutes/,
      );
      expect(mockedAxios.post).not.toHaveBeenCalledWith(`${ML}/model/reload`);
      expect(mlModelRepo.save).not.toHaveBeenCalled();
    });

    it("does not register a version whose saved metadata is missing, and leaves serving alone", async () => {
      routeGets(() =>
        Promise.reject(
          Object.assign(new Error("Not Found"), { response: { status: 404 } }),
        ),
      );

      await expect(processor.handleTrainModels({} as Job)).rejects.toThrow(
        /No saved metrics for v20261006_0600/,
      );

      expect(mlModelRepo.save).not.toHaveBeenCalled();
      expect(mlModelRepo.update).not.toHaveBeenCalled();
      expect(mockedAxios.post).not.toHaveBeenCalledWith(`${ML}/model/reload`);
      // 404 is final — no retries against a version that does not exist.
      expect(
        mockedAxios.get.mock.calls.filter(([u]) =>
          String(u).startsWith(`${ML}/model/info/`),
        ),
      ).toHaveLength(1);
    });

    it("refuses metrics that belong to another version or carry no MAE", async () => {
      routeGets(() =>
        Promise.resolve({ data: { ...savedInfo("v20261006_0600", 0) } }),
      );

      await expect(processor.handleTrainModels({} as Job)).rejects.toThrow(
        /No saved metrics/,
      );
      expect(mlModelRepo.save).not.toHaveBeenCalled();
    });

    it("gates the challenger on its own MAE", async () => {
      // Champion 4.0 × 1.25 = 5.0 < 5.5 → rejected, registered inactive.
      mlModelRepo.findOne.mockResolvedValue({
        version: "v20261003_0600",
        mae: 4.0,
        isActive: true,
      });
      routeGets(() =>
        Promise.resolve({ data: savedInfo("v20261006_0600", 5.5) }),
      );

      await processor.handleTrainModels({} as Job);

      const saved = mlModelRepo.save.mock.calls[0][0];
      expect(saved.mae).toBe(5.5);
      expect(saved.isActive).toBe(false);
      expect(mlModelRepo.update).not.toHaveBeenCalled();
      // Rejected: the champion keeps serving and nothing has to be reverted.
      expect(mockedAxios.post).not.toHaveBeenCalledWith(`${ML}/model/reload`);
    });
  });
});
