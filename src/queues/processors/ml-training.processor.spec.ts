import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { getRepositoryToken } from "@nestjs/typeorm";
import {
  MLTrainingProcessor,
  selectOrphanVersions,
} from "./ml-training.processor";
import { MLModel } from "../../ml/entities/ml-model.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { MLFeatureDriftService } from "../../ml/services/ml-feature-drift.service";
import axios from "axios";

jest.mock("axios");
jest.mock("../../common/utils/file-logger.util", () => ({
  logJobFailure: jest.fn(),
}));

const mockedAxios = axios as jest.Mocked<typeof axios>;
const ML = "http://ml-service:8000";

/**
 * Coverage for the daily 6 AM ML training cron and the model cleanup:
 *   1. Path-traversal protection on model version strings.
 *   2. Cleanup keeps the active model and the newest N regardless of age.
 *   3. Files are deleted through the ml-service (the API container does not
 *      mount the models volume), including orphans with no DB row.
 *   4. Registration reads the trained version's own metadata, and only an
 *      accepted, registered version is activated.
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
    delete process.env.ML_SERVICE_URL;

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

  const DAY_S = 24 * 60 * 60;
  const nowS = () => Date.now() / 1000;

  /** ml-service file listing + delete, recorded per version. */
  const mockModelFiles = (
    versions: Array<{ version: string; ageDays: number }>,
    protectedVersions: Record<string, string | null> = {
      sentinel: null,
      loaded: null,
      training: null,
    },
  ) => {
    mockedAxios.get.mockImplementation((url: string) =>
      url === `${ML}/models/files`
        ? Promise.resolve({
            data: {
              versions: versions.map((v) => ({
                version: v.version,
                files: [`catboost_${v.version}.cbm`],
                bytes: 60 * 1024 * 1024,
                mtime: nowS() - v.ageDays * DAY_S,
              })),
              protected: protectedVersions,
            },
          })
        : Promise.reject(new Error(`unexpected GET ${url}`)),
    );
    mockedAxios.delete.mockResolvedValue({ data: {} });
  };

  const deletedVersions = () =>
    mockedAxios.delete.mock.calls.map(([u]) =>
      decodeURIComponent(String(u).replace(`${ML}/models/files/`, "")),
    );

  const models = (n: number, activeIdx = -1) =>
    Array.from({ length: n }, (_, i) => ({
      id: `m${i}`,
      version: `v_${i}`,
      // Sorted DESC at the repository level → newest first
      trainedAt: new Date(2026, 0, n - i),
      isActive: i === activeIdx,
    }));

  describe("sanitizeVersion (path traversal protection)", () => {
    beforeEach(() => mockModelFiles([]));

    const setupAllModels = (versions: string[]) => {
      mlModelRepo.find.mockResolvedValueOnce(
        versions.map((v, i) => ({
          id: `m${i}`,
          version: v,
          trainedAt: new Date(2020, 0, versions.length - i),
          isActive: false,
        })),
      );
    };

    it("never sends a version containing `..` or `/` to the ml-service", async () => {
      const safe = Array.from({ length: 30 }, (_, i) => `v2026_safe_${i}`);
      setupAllModels([...safe, "../../../etc/passwd", "/etc/passwd"]);

      await processor.handleCleanupModels({} as Job);

      expect(deletedVersions()).toEqual([]);
      expect(mlModelRepo.remove).not.toHaveBeenCalled();
    });

    it("deletes safe alphanumeric + dot + dash + underscore versions", async () => {
      const versions = Array.from(
        { length: 31 },
        (_, i) => `v2026.01.${String(i).padStart(2, "0")}_safe`,
      );
      setupAllModels(versions);

      await processor.handleCleanupModels({} as Job);

      expect(deletedVersions()).toEqual(["v2026.01.30_safe"]);
    });
  });

  describe("cleanupOldModels retention", () => {
    beforeEach(() => mockModelFiles([]));

    it("keeps the most recent MODELS_TO_KEEP=30 models and deletes the rest", async () => {
      mlModelRepo.find.mockResolvedValueOnce(models(35));

      await processor.handleCleanupModels({} as Job);

      const removedIds = mlModelRepo.remove.mock.calls.map(
        ([m]: [{ id: string }]) => m.id,
      );
      expect(removedIds).toEqual(["m30", "m31", "m32", "m33", "m34"]);
      expect(deletedVersions()).toEqual([
        "v_30",
        "v_31",
        "v_32",
        "v_33",
        "v_34",
      ]);
    });

    it("always keeps the active model even if it falls outside the retention window", async () => {
      mlModelRepo.find.mockResolvedValueOnce(models(35, 34));

      await processor.handleCleanupModels({} as Job);

      const removedIds = mlModelRepo.remove.mock.calls.map(
        ([m]: [{ id: string }]) => m.id,
      );
      expect(removedIds).not.toContain("m34");
      expect(mlModelRepo.remove).toHaveBeenCalledTimes(4);
    });

    it("removes the DB row when the files are already gone (404)", async () => {
      mlModelRepo.find.mockResolvedValueOnce(models(32));
      mockedAxios.delete.mockRejectedValue(
        Object.assign(new Error("Not Found"), { response: { status: 404 } }),
      );

      await processor.handleCleanupModels({} as Job);

      expect(mlModelRepo.remove).toHaveBeenCalledTimes(2);
    });

    it("keeps the DB row when the ml-service could not delete or protects the files", async () => {
      mlModelRepo.find.mockResolvedValueOnce(models(32));
      mockedAxios.delete
        .mockRejectedValueOnce(new Error("ECONNREFUSED"))
        .mockRejectedValueOnce(
          Object.assign(new Error("Conflict"), { response: { status: 409 } }),
        );

      await processor.handleCleanupModels({} as Job);

      expect(mlModelRepo.remove).not.toHaveBeenCalled();
    });

    it("doesn't rethrow when cleanup fails (training job must not fail on cleanup)", async () => {
      mlModelRepo.find.mockRejectedValueOnce(new Error("DB exploded"));

      await expect(
        processor.handleCleanupModels({} as Job),
      ).resolves.toBeUndefined();
    });
  });

  describe("cleanupOldModels orphans", () => {
    it("deletes old files whose version has no DB row, through the ml-service", async () => {
      mlModelRepo.find.mockResolvedValueOnce(models(3));
      mockModelFiles([
        { version: "v_0", ageDays: 10 }, // registered
        { version: "v20260930_0600", ageDays: 9 }, // orphan
        { version: "v20261008_0600", ageDays: 1 }, // too young
      ]);

      await processor.handleCleanupModels({} as Job);

      expect(deletedVersions()).toEqual(["v20260930_0600"]);
    });

    it("never deletes the sentinel, loaded or in-training version", async () => {
      mlModelRepo.find.mockResolvedValueOnce([]);
      mockModelFiles(
        [
          { version: "v_sentinel", ageDays: 9 },
          { version: "v_loaded", ageDays: 9 },
          { version: "v_training", ageDays: 9 },
        ],
        { sentinel: "v_sentinel", loaded: "v_loaded", training: "v_training" },
      );

      await processor.handleCleanupModels({} as Job);

      expect(deletedVersions()).toEqual([]);
    });

    it("treats files of versions retired in the same run as handled, not orphans", async () => {
      mlModelRepo.find.mockResolvedValueOnce(models(31));
      mockModelFiles([{ version: "v_30", ageDays: 40 }]);

      await processor.handleCleanupModels({} as Job);

      // Deleted once as a retired model, not a second time as an orphan.
      expect(deletedVersions()).toEqual(["v_30"]);
    });
  });

  describe("selectOrphanVersions", () => {
    const now = Date.UTC(2026, 9, 9);
    const at = (days: number) => now / 1000 - days * DAY_S;

    it("selects unregistered, unprotected versions older than the minimum age", () => {
      const picked = selectOrphanVersions(
        [
          { version: "a", mtime: at(3) },
          { version: "b", mtime: at(3) },
          { version: "c", mtime: at(3) },
          { version: "d", mtime: at(1) },
        ],
        ["a"],
        ["b"],
        now,
        2 * DAY_S * 1000,
      ).map((v) => v.version);
      expect(picked).toEqual(["c"]);
    });
  });

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
