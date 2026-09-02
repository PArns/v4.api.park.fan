import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { PredictionGeneratorProcessor } from "./prediction-generator.processor";
import { MLService } from "../../ml/ml.service";
import { PredictionLeadSnapshotService } from "../../ml/services/prediction-lead-snapshot.service";
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
    purgeHourlyPredictionsBefore: jest
      .fn()
      .mockResolvedValue({ deleted: 0, windows: 0, done: true }),
  };

  // Rides along with the daily run to record what was predicted at each lead
  // distance. Its failures are caught at the call site so the log names the
  // snapshot rather than the prediction run — the per-park try/catch below is
  // what actually keeps one park from taking down the others.
  const leadSnapshotService = {
    snapshotPark: jest.fn().mockResolvedValue(0),
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
        {
          provide: PredictionLeadSnapshotService,
          useValue: leadSnapshotService,
        },
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

  describe("generate-daily (nightly)", () => {
    const openPark = { id: "p1", name: "Open", timezone: "Europe/Berlin" };

    beforeEach(() => {
      parksService.findAll.mockResolvedValue([openPark]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "OPERATING"]]),
      );
    });

    it("snapshots the lead buckets after storing, with one instant for the run", async () => {
      const predictions = [{ attractionId: "a1", predictionType: "daily" }];
      mlService.getParkPredictions.mockResolvedValue({ predictions });

      await processor.handleGenerateDaily({} as Job);

      expect(leadSnapshotService.snapshotPark).toHaveBeenCalledTimes(1);
      const [park, passed, now] =
        leadSnapshotService.snapshotPark.mock.calls[0];
      expect(park).toBe(openPark);
      expect(passed).toBe(predictions);
      // A Date, not undefined: the lead distance is computed against it, and
      // reading the clock per park would label a batch crossing midnight with
      // two different distances for the same night's work.
      expect(now).toBeInstanceOf(Date);
    });

    it("keeps going for other parks when one park's snapshot throws", async () => {
      // Note what this does and does not prove. The per-park try/catch around
      // the whole body already stops one park from taking down the run, so the
      // catch on the snapshot call is NOT what makes this pass — removing it
      // leaves this test green. What that catch buys is diagnosis: the park
      // still counts as a success and the log names the snapshot rather than
      // reporting "failed to generate daily predictions", which would send the
      // next person looking at the model instead of at this table.
      parksService.findAll.mockResolvedValue([
        openPark,
        { id: "p2", name: "Second", timezone: "Europe/Berlin" },
      ]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([
          ["p1", "OPERATING"],
          ["p2", "OPERATING"],
        ]),
      );
      mlService.getParkPredictions.mockResolvedValue({
        predictions: [{ attractionId: "a1", predictionType: "daily" }],
      });
      leadSnapshotService.snapshotPark.mockRejectedValueOnce(
        new Error("snapshot table missing"),
      );

      await expect(
        processor.handleGenerateDaily({} as Job),
      ).resolves.toBeUndefined();

      // Both parks got their predictions stored — the failure did not take the
      // second one with it, and did not roll back the first one's rows.
      expect(mlService.storePredictions).toHaveBeenCalledTimes(2);
      expect(leadSnapshotService.snapshotPark).toHaveBeenCalledTimes(2);
    });

    it("does not snapshot when there is nothing to store", async () => {
      mlService.getParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateDaily({} as Job);

      expect(mlService.storePredictions).not.toHaveBeenCalled();
      expect(leadSnapshotService.snapshotPark).not.toHaveBeenCalled();
    });
  });

  describe("cleanup-old (daily retention)", () => {
    it("purges hourly by createdAt in windows and daily by predictedTime", async () => {
      mlService.purgeHourlyPredictionsBefore.mockResolvedValueOnce({
        deleted: 12_000,
        windows: 3,
        done: true,
      });
      mlService.deleteOldPredictions.mockResolvedValueOnce(3_500); // daily

      await processor.handleCleanupOld({} as Job);

      // Hourly goes through the windowed, partition-key-aligned purge...
      expect(mlService.purgeHourlyPredictionsBefore).toHaveBeenCalledTimes(1);
      const [hourlyCutoff] =
        mlService.purgeHourlyPredictionsBefore.mock.calls[0];
      expect(hourlyCutoff).toBeInstanceOf(Date);

      // ...daily keeps the predictedTime path (lead time reaches ~1 year).
      expect(mlService.deleteOldPredictions).toHaveBeenCalledTimes(1);
      const [type, dailyCutoff] = mlService.deleteOldPredictions.mock.calls[0];
      expect(type).toBe("daily");
      expect((dailyCutoff as Date).getTime()).toBeLessThan(
        (hourlyCutoff as Date).getTime(),
      );
    });

    it("gives the hourly cutoff a day of slack over the 7-day target window", async () => {
      await processor.handleCleanupOld({} as Job);

      const [hourlyCutoff] =
        mlService.purgeHourlyPredictionsBefore.mock.calls[0];
      const ageDays =
        (Date.now() - (hourlyCutoff as Date).getTime()) / 86_400_000;
      // 8 days: an hourly row's target can sit up to 24h after its createdAt,
      // so cutting at 7 days on createdAt would drop still-wanted targets.
      expect(ageDays).toBeGreaterThan(7.9);
      expect(ageDays).toBeLessThan(8.1);
    });

    it("does not fail when a backlog is left over for the next run", async () => {
      mlService.purgeHourlyPredictionsBefore.mockResolvedValueOnce({
        deleted: 5_000_000,
        windows: 20,
        done: false, // budget exhausted mid-backlog
      });

      await expect(
        processor.handleCleanupOld({} as Job),
      ).resolves.toBeUndefined();
    });

    it("rethrows when delete fails — the cron job retries on next schedule", async () => {
      mlService.purgeHourlyPredictionsBefore.mockRejectedValueOnce(
        new Error("DB unavailable"),
      );

      await expect(processor.handleCleanupOld({} as Job)).rejects.toThrow(
        /DB unavailable/,
      );
    });
  });
});
