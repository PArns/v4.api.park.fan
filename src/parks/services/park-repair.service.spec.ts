import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import { ParkRepairService } from "./park-repair.service";

jest.mock("../../common/cache/park-cache-invalidation", () => ({
  invalidateParkCaches: jest.fn(),
}));

/**
 * The four ID repairs write park rows, so every path is pinned twice: once
 * where the write happens and the cache is dropped, once where the guard
 * refuses and nothing is written. `repairDuplicates` has its own spec.
 */
describe("ParkRepairService ID repairs", () => {
  const redis = {} as never;
  const invalidate = invalidateParkCaches as jest.Mock;

  const build = (find: jest.Mock = jest.fn().mockResolvedValue([])) => {
    const update = jest.fn().mockResolvedValue(undefined);
    const service = Object.create(
      ParkRepairService.prototype,
    ) as ParkRepairService;
    Object.assign(service, {
      logger: { log: jest.fn(), error: jest.fn(), warn: jest.fn() },
      parkRepository: { find, update },
      redis,
    });
    return { service, update };
  };

  beforeEach(() => {
    invalidate.mockReset().mockResolvedValue(undefined);
  });

  describe("fixMismatchedQueueTimesIds", () => {
    it("writes the new id and drops the park's caches", async () => {
      const { service, update } = build();

      const result = await service.fixMismatchedQueueTimesIds([
        { parkId: "p1", correctQtId: "qt-1" },
      ]);

      expect(update).toHaveBeenCalledWith("p1", { queueTimesEntityId: "qt-1" });
      expect(invalidate).toHaveBeenCalledWith(redis, "p1");
      expect(result.fixedQtMismatches).toBe(1);
      expect(result.errors).toEqual([]);
    });

    it("accepts an id the same park already holds", async () => {
      const find = jest
        .fn()
        .mockResolvedValue([
          { id: "p1", name: "One", queueTimesEntityId: "qt-1" },
        ]);
      const { service, update } = build(find);

      const result = await service.fixMismatchedQueueTimesIds([
        { parkId: "p1", correctQtId: "qt-1" },
      ]);

      expect(update).toHaveBeenCalledTimes(1);
      expect(result.fixedQtMismatches).toBe(1);
    });

    it("refuses an id owned by another park and writes nothing", async () => {
      const find = jest
        .fn()
        .mockResolvedValue([
          { id: "p2", name: "Two", queueTimesEntityId: "qt-1" },
        ]);
      const { service, update } = build(find);

      const result = await service.fixMismatchedQueueTimesIds([
        { parkId: "p1", correctQtId: "qt-1" },
      ]);

      expect(update).not.toHaveBeenCalled();
      expect(invalidate).not.toHaveBeenCalled();
      expect(result.fixedQtMismatches).toBe(0);
      expect(result.errors).toEqual([
        {
          parkId: "p1",
          error: 'QT-ID qt-1 is already used by park "Two" (p2)',
        },
      ]);
    });

    it("does nothing for an empty list", async () => {
      const { service, update } = build();

      const result = await service.fixMismatchedQueueTimesIds([]);

      expect(update).not.toHaveBeenCalled();
      expect(result.fixedQtMismatches).toBe(0);
      expect(result.errors).toEqual([]);
    });

    it("keeps going after a failed write and reports it by park", async () => {
      const { service, update } = build();
      update
        .mockRejectedValueOnce(new Error("db down"))
        .mockResolvedValueOnce(undefined);

      const result = await service.fixMismatchedQueueTimesIds([
        { parkId: "p1", correctQtId: "qt-1" },
        { parkId: "p2", correctQtId: "qt-2" },
      ]);

      expect(result.fixedQtMismatches).toBe(1);
      expect(result.errors).toEqual([{ parkId: "p1", error: "db down" }]);
    });

    it("still counts the fix when the cache invalidation fails", async () => {
      const { service, update } = build();
      invalidate.mockRejectedValue(new Error("redis down"));

      const result = await service.fixMismatchedQueueTimesIds([
        { parkId: "p1", correctQtId: "qt-1" },
      ]);

      expect(update).toHaveBeenCalledTimes(1);
      expect(result.fixedQtMismatches).toBe(1);
      expect(result.errors).toEqual([]);
    });
  });

  describe("fixMismatchedWartezeitenIds", () => {
    it("writes the new id and drops the park's caches", async () => {
      const { service, update } = build();

      const result = await service.fixMismatchedWartezeitenIds([
        { parkId: "p1", correctWzId: "wz-1" },
      ]);

      expect(update).toHaveBeenCalledWith("p1", {
        wartezeitenEntityId: "wz-1",
      });
      expect(invalidate).toHaveBeenCalledWith(redis, "p1");
      expect(result.fixedWzMismatches).toBe(1);
    });

    it("refuses an id owned by another park and writes nothing", async () => {
      const find = jest
        .fn()
        .mockResolvedValue([
          { id: "p2", name: "Two", wartezeitenEntityId: "wz-1" },
        ]);
      const { service, update } = build(find);

      const result = await service.fixMismatchedWartezeitenIds([
        { parkId: "p1", correctWzId: "wz-1" },
      ]);

      expect(update).not.toHaveBeenCalled();
      expect(result.fixedWzMismatches).toBe(0);
      expect(result.errors).toEqual([
        {
          parkId: "p1",
          error: 'WZ-ID wz-1 is already used by park "Two" (p2)',
        },
      ]);
    });

    it("does nothing for an empty list", async () => {
      const { service, update } = build();

      const result = await service.fixMismatchedWartezeitenIds([]);

      expect(update).not.toHaveBeenCalled();
      expect(result.fixedWzMismatches).toBe(0);
    });
  });

  describe("addMissingQueueTimesIds", () => {
    const findWith = (conflicting: unknown[], targets: unknown[]) =>
      jest
        .fn()
        .mockResolvedValueOnce(conflicting)
        .mockResolvedValueOnce(targets);

    it("adds the id to a park that has none", async () => {
      const { service, update } = build(
        findWith([], [{ id: "p1", queueTimesEntityId: null }]),
      );

      const result = await service.addMissingQueueTimesIds([
        { parkId: "p1", qtId: "qt-1" },
      ]);

      expect(update).toHaveBeenCalledWith("p1", { queueTimesEntityId: "qt-1" });
      expect(invalidate).toHaveBeenCalledWith(redis, "p1");
      expect(result.addedQtIds).toBe(1);
      expect(result.errors).toEqual([]);
    });

    it("refuses an id any park already uses", async () => {
      const { service, update } = build(
        findWith(
          [{ id: "p2", name: "Two", queueTimesEntityId: "qt-1" }],
          [{ id: "p1", queueTimesEntityId: null }],
        ),
      );

      const result = await service.addMissingQueueTimesIds([
        { parkId: "p1", qtId: "qt-1" },
      ]);

      expect(update).not.toHaveBeenCalled();
      expect(result.addedQtIds).toBe(0);
      expect(result.errors[0].error).toBe(
        'QT-ID qt-1 is already used by park "Two" (p2)',
      );
    });

    it("refuses an unknown park", async () => {
      const { service, update } = build(findWith([], []));

      const result = await service.addMissingQueueTimesIds([
        { parkId: "ghost", qtId: "qt-1" },
      ]);

      expect(update).not.toHaveBeenCalled();
      expect(result.errors).toEqual([
        { parkId: "ghost", error: "Park ghost not found" },
      ]);
    });

    it("refuses a park that already has an id", async () => {
      const { service, update } = build(
        findWith([], [{ id: "p1", queueTimesEntityId: "qt-old" }]),
      );

      const result = await service.addMissingQueueTimesIds([
        { parkId: "p1", qtId: "qt-1" },
      ]);

      expect(update).not.toHaveBeenCalled();
      expect(result.errors).toEqual([
        { parkId: "p1", error: "Park p1 already has QT-ID: qt-old" },
      ]);
    });
  });

  describe("addMissingWartezeitenIds", () => {
    const findWith = (conflicting: unknown[], targets: unknown[]) =>
      jest
        .fn()
        .mockResolvedValueOnce(conflicting)
        .mockResolvedValueOnce(targets);

    it("adds the id to a park that has none", async () => {
      const { service, update } = build(
        findWith([], [{ id: "p1", wartezeitenEntityId: null }]),
      );

      const result = await service.addMissingWartezeitenIds([
        { parkId: "p1", wzId: "wz-1" },
      ]);

      expect(update).toHaveBeenCalledWith("p1", {
        wartezeitenEntityId: "wz-1",
      });
      expect(invalidate).toHaveBeenCalledWith(redis, "p1");
      expect(result.addedWzIds).toBe(1);
    });

    it("refuses an id any park already uses", async () => {
      const { service, update } = build(
        findWith(
          [{ id: "p2", name: "Two", wartezeitenEntityId: "wz-1" }],
          [{ id: "p1", wartezeitenEntityId: null }],
        ),
      );

      const result = await service.addMissingWartezeitenIds([
        { parkId: "p1", wzId: "wz-1" },
      ]);

      expect(update).not.toHaveBeenCalled();
      expect(result.errors[0].error).toBe(
        'WZ-ID wz-1 is already used by park "Two" (p2)',
      );
    });

    it("refuses an unknown park", async () => {
      const { service, update } = build(findWith([], []));

      const result = await service.addMissingWartezeitenIds([
        { parkId: "ghost", wzId: "wz-1" },
      ]);

      expect(update).not.toHaveBeenCalled();
      expect(result.errors).toEqual([
        { parkId: "ghost", error: "Park ghost not found" },
      ]);
    });

    it("refuses a park that already has an id", async () => {
      const { service, update } = build(
        findWith([], [{ id: "p1", wartezeitenEntityId: "wz-old" }]),
      );

      const result = await service.addMissingWartezeitenIds([
        { parkId: "p1", wzId: "wz-1" },
      ]);

      expect(update).not.toHaveBeenCalled();
      expect(result.errors).toEqual([
        { parkId: "p1", error: "Park p1 already has WZ-ID: wz-old" },
      ]);
    });

    it("does nothing for an empty list", async () => {
      const { service, update } = build(findWith([], []));

      const result = await service.addMissingWartezeitenIds([]);

      expect(update).not.toHaveBeenCalled();
      expect(result.addedWzIds).toBe(0);
      expect(result.errors).toEqual([]);
    });
  });
});
