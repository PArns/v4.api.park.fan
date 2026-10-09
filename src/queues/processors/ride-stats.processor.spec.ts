import { Job } from "bull";
import { Logger } from "@nestjs/common";
import { RideStatsProcessor } from "./ride-stats.processor";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";

jest.mock("../../common/cache/park-cache-invalidation", () => ({
  invalidateParkCaches: jest.fn(),
}));

/**
 * The processor wraps the Wikidata import: log a failure before Bull swallows
 * it, and publish in the order our caches, the frontend, the frontend again
 * after the CDN window.
 */
describe("RideStatsProcessor", () => {
  let processor: RideStatsProcessor;

  const mockRideStats = { import: jest.fn() };
  const mockRevalidation = {
    revalidateTags: jest.fn().mockResolvedValue(undefined),
  };
  const mockRedis = {};
  const mockQueue = { add: jest.fn().mockResolvedValue(undefined) };

  const job = (data?: { limit?: number }) => ({ data }) as Job<never>;

  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(Logger.prototype, "log").mockImplementation();
    jest.spyOn(Logger.prototype, "warn").mockImplementation();
    jest.spyOn(Logger.prototype, "error").mockImplementation();
    processor = new RideStatsProcessor(
      mockRideStats as never,
      mockRevalidation as never,
      mockRedis as never,
      mockQueue as never,
    );
  });

  afterEach(() => jest.restoreAllMocks());

  describe("handleImportStats", () => {
    it("passes the trial-run limit on to the import", async () => {
      mockRideStats.import.mockResolvedValue({
        written: 0,
        withoutData: 0,
        touchedParks: [],
      });

      await processor.handleImportStats(job({ limit: 5 }));

      expect(mockRideStats.import).toHaveBeenCalledWith(5);
    });

    it("imports without a limit when the job carries no payload", async () => {
      mockRideStats.import.mockResolvedValue({
        written: 0,
        withoutData: 0,
        touchedParks: [],
      });

      await processor.handleImportStats(job(undefined));

      expect(mockRideStats.import).toHaveBeenCalledWith(undefined);
    });

    it("clears nothing when nothing was written", async () => {
      mockRideStats.import.mockResolvedValue({
        written: 0,
        withoutData: 4,
        touchedParks: [],
      });

      await processor.handleImportStats(job({}));

      expect(invalidateParkCaches).not.toHaveBeenCalled();
      expect(mockRevalidation.revalidateTags).not.toHaveBeenCalled();
      expect(mockQueue.add).not.toHaveBeenCalled();
    });

    it("invalidates each touched park, then revalidates, then schedules the second pass", async () => {
      mockRideStats.import.mockResolvedValue({
        written: 3,
        withoutData: 0,
        touchedParks: [
          { parkId: "park-a", attractionIds: ["a1", "a2"] },
          { parkId: "park-b", attractionIds: ["b1"] },
        ],
      });
      const order: string[] = [];
      (invalidateParkCaches as jest.Mock).mockImplementation(
        async (_redis, parkId: string) => {
          order.push(`invalidate:${parkId}`);
        },
      );
      mockRevalidation.revalidateTags.mockImplementation(async () => {
        order.push("revalidate");
      });
      mockQueue.add.mockImplementation(async () => {
        order.push("schedule");
      });

      await processor.handleImportStats(job({}));

      expect(invalidateParkCaches).toHaveBeenCalledWith(mockRedis, "park-a", [
        "a1",
        "a2",
      ]);
      expect(invalidateParkCaches).toHaveBeenCalledWith(mockRedis, "park-b", [
        "b1",
      ]);
      expect(mockRevalidation.revalidateTags).toHaveBeenCalledWith([
        "parks",
        "attractions",
      ]);
      expect(mockQueue.add).toHaveBeenCalledWith(
        "revalidate-parks",
        {},
        expect.objectContaining({
          delay: 16 * 60 * 1000,
          jobId: "ride-stats-revalidate-after-cdn",
        }),
      );
      expect(order).toEqual([
        "invalidate:park-a",
        "invalidate:park-b",
        "revalidate",
        "schedule",
      ]);
    });

    it("keeps going when one park's cache invalidation fails", async () => {
      mockRideStats.import.mockResolvedValue({
        written: 2,
        withoutData: 0,
        touchedParks: [
          { parkId: "park-a", attractionIds: ["a1"] },
          { parkId: "park-b", attractionIds: ["b1"] },
        ],
      });
      (invalidateParkCaches as jest.Mock)
        .mockRejectedValueOnce(new Error("redis down"))
        .mockResolvedValueOnce(undefined);

      await processor.handleImportStats(job({}));

      expect(invalidateParkCaches).toHaveBeenCalledTimes(2);
      expect(mockRevalidation.revalidateTags).toHaveBeenCalledTimes(1);
      expect(mockQueue.add).toHaveBeenCalledTimes(1);
    });

    it("logs an import failure and rethrows it for Bull to count", async () => {
      const failure = new Error("wikidata down");
      mockRideStats.import.mockRejectedValue(failure);

      await expect(processor.handleImportStats(job({}))).rejects.toBe(failure);

      expect(Logger.prototype.error).toHaveBeenCalledWith(
        expect.stringContaining("wikidata down"),
        failure.stack,
      );
      expect(mockRevalidation.revalidateTags).not.toHaveBeenCalled();
    });

    it("rethrows a non-Error rejection without a stack", async () => {
      mockRideStats.import.mockRejectedValue("boom");

      await expect(processor.handleImportStats(job({}))).rejects.toBe("boom");

      expect(Logger.prototype.error).toHaveBeenCalledWith(
        expect.stringContaining("boom"),
        undefined,
      );
    });
  });

  describe("handleRevalidateParks", () => {
    it("revalidates the park and attraction tags", async () => {
      await processor.handleRevalidateParks({} as Job);

      expect(mockRevalidation.revalidateTags).toHaveBeenCalledWith([
        "parks",
        "attractions",
      ]);
    });
  });
});
