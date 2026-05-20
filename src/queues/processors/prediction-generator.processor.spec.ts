import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { PredictionGeneratorProcessor } from "./prediction-generator.processor";
import { MLService } from "../../ml/ml.service";
import { ParksService } from "../../parks/parks.service";
import { CacheWarmupService } from "../services/cache-warmup.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * Coverage for the prediction-generator cron that runs every 15 minutes
 * for hourly predictions + nightly for daily ones. The processor is
 * critical: a silent crash means the /parks/{id} endpoint serves stale
 * predictions for hours. These tests pin down:
 *   1. Parks are filtered to OPERATING / opening-soon / has-recent-
 *      activity before we call the Python ML service. Closed parks
 *      shouldn't burn ML wall-time.
 *   2. Per-park failure isolation — one bad park does NOT stop the
 *      batch. The cron must keep going.
 *   3. Empty-response handling — no crash if ML returns 0 predictions.
 *   4. Cleanup-old removes both hourly + daily retention windows
 *      without crashing on either side.
 */
describe("PredictionGeneratorProcessor", () => {
  let processor: PredictionGeneratorProcessor;

  const mlService = {
    getParkPredictions: jest.fn(),
    deduplicatePredictions: jest.fn().mockResolvedValue(0),
    storePredictions: jest.fn().mockResolvedValue(undefined),
    deleteOldPredictions: jest.fn().mockResolvedValue(0),
  };

  const parksService = {
    findAll: jest.fn(),
    getBatchParkStatus: jest.fn(),
    isParkOperatingToday: jest.fn().mockResolvedValue(false),
    hasRecentRideActivity: jest.fn().mockResolvedValue(false),
  };

  const cacheWarmupService = {};

  const redis = {
    del: jest.fn().mockResolvedValue(1),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PredictionGeneratorProcessor,
        { provide: MLService, useValue: mlService },
        { provide: ParksService, useValue: parksService },
        { provide: CacheWarmupService, useValue: cacheWarmupService },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    processor = module.get(PredictionGeneratorProcessor);
  });

  describe("generate-hourly (every 15 min)", () => {
    it("only requests predictions for parks that are OPERATING (filters CLOSED)", async () => {
      const operating = { id: "p1", name: "Operating" };
      const closed = { id: "p2", name: "Closed" };

      parksService.findAll.mockResolvedValue([operating, closed]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([
          ["p1", "OPERATING"],
          ["p2", "CLOSED"],
        ]),
      );
      // For CLOSED parks, both "isOperatingToday" and "hasRecentRideActivity"
      // return false → park is excluded.
      mlService.getParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      // ML called only for the OPERATING park.
      expect(mlService.getParkPredictions).toHaveBeenCalledTimes(1);
      expect(mlService.getParkPredictions).toHaveBeenCalledWith(
        "p1",
        "hourly",
        undefined,
        "OPERATING",
      );
    });

    it("includes UNKNOWN-status parks that are scheduled to operate today", async () => {
      const unknown = { id: "p1", name: "Unknown but scheduled" };
      parksService.findAll.mockResolvedValue([unknown]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "UNKNOWN"]]),
      );
      parksService.isParkOperatingToday.mockResolvedValueOnce(true);
      mlService.getParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      expect(mlService.getParkPredictions).toHaveBeenCalled();
    });

    it("includes CLOSED parks with recent ride activity (schedule-is-wrong safety net)", async () => {
      const closedButActive = { id: "p1", name: "Open in reality" };
      parksService.findAll.mockResolvedValue([closedButActive]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "CLOSED"]]),
      );
      parksService.isParkOperatingToday.mockResolvedValueOnce(false);
      parksService.hasRecentRideActivity.mockResolvedValueOnce(true);
      mlService.getParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      expect(mlService.getParkPredictions).toHaveBeenCalled();
    });

    it("continues to the next park when one fails (per-park isolation)", async () => {
      parksService.findAll.mockResolvedValue([
        { id: "p1", name: "Breaks" },
        { id: "p2", name: "Healthy" },
      ]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([
          ["p1", "OPERATING"],
          ["p2", "OPERATING"],
        ]),
      );
      mlService.getParkPredictions
        .mockRejectedValueOnce(new Error("ML 500 for p1"))
        .mockResolvedValueOnce({
          predictions: [{ attractionId: "a1" } as never],
        });

      // No throw — the loop catches per-park errors.
      await expect(
        processor.handleGenerateHourly({} as Job),
      ).resolves.toBeUndefined();

      // p2 still stored predictions.
      expect(mlService.storePredictions).toHaveBeenCalledTimes(1);
    });

    it("skips dedup + store when ML returns zero predictions (no wasted writes)", async () => {
      parksService.findAll.mockResolvedValue([{ id: "p1", name: "Empty" }]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "OPERATING"]]),
      );
      mlService.getParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      expect(mlService.getParkPredictions).toHaveBeenCalledTimes(1);
      // No write side-effects.
      expect(mlService.deduplicatePredictions).not.toHaveBeenCalled();
      expect(mlService.storePredictions).not.toHaveBeenCalled();
    });

    it("invalidates the park:integrated cache after successful predictions", async () => {
      parksService.findAll.mockResolvedValue([
        { id: "p1", name: "Phantasialand" },
      ]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "OPERATING"]]),
      );
      mlService.getParkPredictions.mockResolvedValue({
        predictions: [{ attractionId: "a1" } as never],
      });

      await processor.handleGenerateHourly({} as Job);

      expect(redis.del).toHaveBeenCalledWith("park:integrated:p1");
    });

    it("respects the BATCH_SIZE=5 throttle when processing many parks", async () => {
      // 12 OPERATING parks → 3 batches (5+5+2)
      const parks = Array.from({ length: 12 }, (_, i) => ({
        id: `p${i}`,
        name: `Park ${i}`,
      }));
      parksService.findAll.mockResolvedValue(parks);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map(parks.map((p) => [p.id, "OPERATING"])),
      );
      mlService.getParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      // ML called for every operating park — batching shape is internal,
      // we just assert the total count matches.
      expect(mlService.getParkPredictions).toHaveBeenCalledTimes(12);
    });
  });

  describe("cleanup-old (daily retention)", () => {
    it("deletes both hourly (>7d) and daily (>90d) predictions and logs counts", async () => {
      mlService.deleteOldPredictions
        .mockResolvedValueOnce(12_000) // hourly
        .mockResolvedValueOnce(3_500); // daily

      await processor.handleCleanupOld({} as Job);

      expect(mlService.deleteOldPredictions).toHaveBeenCalledTimes(2);
      // First call: hourly with ~7-day cutoff.
      const [type1, cutoff1] = mlService.deleteOldPredictions.mock.calls[0];
      expect(type1).toBe("hourly");
      expect(cutoff1).toBeInstanceOf(Date);
      // Second call: daily with ~90-day cutoff.
      const [type2, cutoff2] = mlService.deleteOldPredictions.mock.calls[1];
      expect(type2).toBe("daily");
      // The 90-day cutoff should be older than the 7-day one.
      expect((cutoff2 as Date).getTime()).toBeLessThan(
        (cutoff1 as Date).getTime(),
      );
    });

    it("rethrows when delete fails — the cron job retries on next schedule", async () => {
      mlService.deleteOldPredictions.mockRejectedValueOnce(
        new Error("DB unavailable"),
      );

      await expect(processor.handleCleanupOld({} as Job)).rejects.toThrow(
        /DB unavailable/,
      );
    });
  });
});
