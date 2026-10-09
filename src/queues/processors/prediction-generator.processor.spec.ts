import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { PredictionGeneratorProcessor } from "./prediction-generator.processor";
import { MLService } from "../../ml/ml.service";
import { PredictionLeadSnapshotService } from "../../ml/services/prediction-lead-snapshot.service";
import { ForecastAccuracyService } from "../../ml/services/forecast-accuracy.service";
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
    getRawParkPredictions: jest.fn(),
    // The served (PCN-overridden) read. The cron must never call it — see the
    // "stores raw CatBoost" spec below.
    getParkPredictions: jest.fn(),
    deduplicatePredictions: jest.fn().mockResolvedValue(0),
    storePredictions: jest.fn().mockResolvedValue(undefined),
    deleteOldPredictions: jest.fn().mockResolvedValue(0),
    purgeHourlyPredictionsBefore: jest
      .fn()
      .mockResolvedValue({ deleted: 0, windows: 0, done: true }),
    dropExpiredPredictionChunks: jest.fn().mockResolvedValue({
      due: 0,
      dropped: 0,
      lockTimedOut: false,
      attempts: 0,
      overdue: 0,
    }),
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
    getTodaySchedule: jest.fn().mockResolvedValue([]),
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
          provide: ForecastAccuracyService,
          useValue: { rebuild: jest.fn().mockResolvedValue(0) },
        },
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
      mlService.getRawParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      // ML called only for the OPERATING park.
      expect(mlService.getRawParkPredictions).toHaveBeenCalledTimes(1);
      expect(mlService.getRawParkPredictions).toHaveBeenCalledWith(
        "p1",
        "hourly",
        undefined,
        "OPERATING",
      );
    });

    it("stores raw CatBoost, never the served PCN curve (PAR-817)", async () => {
      // wait_time_predictions + prediction_accuracy are the CatBoost side of the
      // PCN-vs-CatBoost board. Feeding them the served read would let the board
      // compare PCN against itself.
      parksService.findAll.mockResolvedValue([{ id: "p1", name: "Open" }]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "OPERATING"]]),
      );
      const catboost = [
        {
          attractionId: "a1",
          predictedTime: "2026-10-09T12:15:00+00:00",
          predictedWaitTime: 30,
          predictionType: "hourly",
          modelVersion: "v1",
        },
      ];
      mlService.getRawParkPredictions.mockResolvedValue({
        predictions: catboost,
        count: 1,
        modelVersion: "v1",
      });

      await processor.handleGenerateHourly({} as Job);

      expect(mlService.getParkPredictions).not.toHaveBeenCalled();
      expect(mlService.storePredictions).toHaveBeenCalledWith(catboost);
    });

    it("includes UNKNOWN-status parks that are scheduled to operate today", async () => {
      const unknown = { id: "p1", name: "Unknown but scheduled" };
      parksService.findAll.mockResolvedValue([unknown]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "UNKNOWN"]]),
      );
      parksService.isParkOperatingToday.mockResolvedValueOnce(true);
      mlService.getRawParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      expect(mlService.getRawParkPredictions).toHaveBeenCalled();
    });

    it("includes CLOSED parks with recent ride activity (schedule-is-wrong safety net)", async () => {
      const closedButActive = { id: "p1", name: "Open in reality" };
      parksService.findAll.mockResolvedValue([closedButActive]);
      parksService.getBatchParkStatus.mockResolvedValue(
        new Map([["p1", "CLOSED"]]),
      );
      parksService.isParkOperatingToday.mockResolvedValueOnce(false);
      parksService.hasRecentRideActivity.mockResolvedValueOnce(true);
      mlService.getRawParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      expect(mlService.getRawParkPredictions).toHaveBeenCalled();
    });

    describe("a closed park with published hours today", () => {
      const HOUR = 60 * 60 * 1000;
      const park = { id: "p1", name: "Scheduled", timezone: "Europe/Berlin" };
      const session = (opensInH: number, closesInH: number) => ({
        scheduleType: "OPERATING",
        openingTime: new Date(Date.now() + opensInH * HOUR),
        closingTime: new Date(Date.now() + closesInH * HOUR),
      });

      beforeEach(() => {
        parksService.findAll.mockResolvedValue([park]);
        parksService.getBatchParkStatus.mockResolvedValue(
          new Map([["p1", "CLOSED"]]),
        );
        mlService.getRawParkPredictions.mockResolvedValue({ predictions: [] });
      });

      it("is predicted from three hours before opening", async () => {
        parksService.getTodaySchedule.mockResolvedValueOnce([session(2.5, 10)]);

        await processor.handleGenerateHourly({} as Job);

        expect(parksService.getTodaySchedule).toHaveBeenCalledWith(
          "p1",
          "Europe/Berlin",
        );
        expect(mlService.getRawParkPredictions).toHaveBeenCalled();
        // Published hours decide; the any-time-today fallback is not asked.
        expect(parksService.isParkOperatingToday).not.toHaveBeenCalled();
      });

      it("is skipped earlier than that", async () => {
        parksService.getTodaySchedule.mockResolvedValueOnce([session(5, 13)]);

        await processor.handleGenerateHourly({} as Job);

        expect(mlService.getRawParkPredictions).not.toHaveBeenCalled();
      });

      it("is skipped after closing, though it operated today", async () => {
        parksService.getTodaySchedule.mockResolvedValueOnce([session(-10, -1)]);
        parksService.isParkOperatingToday.mockResolvedValue(true);

        await processor.handleGenerateHourly({} as Job);

        expect(mlService.getRawParkPredictions).not.toHaveBeenCalled();
        parksService.isParkOperatingToday.mockResolvedValue(false);
      });

      it("is predicted in the gap between two sessions", async () => {
        parksService.getTodaySchedule.mockResolvedValueOnce([
          session(-6, -2),
          session(1, 5),
        ]);

        await processor.handleGenerateHourly({} as Job);

        expect(mlService.getRawParkPredictions).toHaveBeenCalled();
      });

      it("still falls back to recent ride activity after closing", async () => {
        parksService.getTodaySchedule.mockResolvedValueOnce([session(-10, -1)]);
        parksService.hasRecentRideActivity.mockResolvedValueOnce(true);

        await processor.handleGenerateHourly({} as Job);

        expect(mlService.getRawParkPredictions).toHaveBeenCalled();
      });

      it("uses the any-time-today rule when today has no usable hours", async () => {
        parksService.getTodaySchedule.mockResolvedValueOnce([
          { scheduleType: "UNKNOWN", openingTime: null, closingTime: null },
        ]);
        parksService.isParkOperatingToday.mockResolvedValueOnce(true);

        await processor.handleGenerateHourly({} as Job);

        expect(parksService.isParkOperatingToday).toHaveBeenCalledWith("p1");
        expect(mlService.getRawParkPredictions).toHaveBeenCalled();
      });
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
      mlService.getRawParkPredictions
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
      mlService.getRawParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      expect(mlService.getRawParkPredictions).toHaveBeenCalledTimes(1);
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
      mlService.getRawParkPredictions.mockResolvedValue({
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
      mlService.getRawParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateHourly({} as Job);

      // ML called for every operating park — batching shape is internal,
      // we just assert the total count matches.
      expect(mlService.getRawParkPredictions).toHaveBeenCalledTimes(12);
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
      mlService.getRawParkPredictions.mockResolvedValue({ predictions });

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
      mlService.getRawParkPredictions.mockResolvedValue({
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
      mlService.getRawParkPredictions.mockResolvedValue({ predictions: [] });

      await processor.handleGenerateDaily({} as Job);

      expect(mlService.storePredictions).not.toHaveBeenCalled();
      expect(leadSnapshotService.snapshotPark).not.toHaveBeenCalled();
    });
  });

  describe("cleanup-old (daily retention)", () => {
    it("drops expired chunks at the 90-day backstop before the row cleanup", async () => {
      await processor.handleCleanupOld({} as Job);

      expect(mlService.dropExpiredPredictionChunks).toHaveBeenCalledWith(90, {
        overdueAfterDays: 14,
      });
      expect(
        mlService.dropExpiredPredictionChunks.mock.invocationCallOrder[0],
      ).toBeLessThan(
        mlService.purgeHourlyPredictionsBefore.mock.invocationCallOrder[0],
      );
    });

    describe("visibility of missed drops", () => {
      const logger = () =>
        (
          processor as unknown as {
            logger: { warn: jest.Mock; error: jest.Mock };
          }
        ).logger;
      let warn: jest.SpyInstance;
      let error: jest.SpyInstance;

      beforeEach(() => {
        warn = jest.spyOn(logger(), "warn").mockImplementation(() => undefined);
        error = jest
          .spyOn(logger(), "error")
          .mockImplementation(() => undefined);
      });
      afterEach(() => {
        warn.mockRestore();
        error.mockRestore();
      });

      it("warns, but does not raise an error, for a recent miss", async () => {
        mlService.dropExpiredPredictionChunks.mockResolvedValueOnce({
          due: 1,
          dropped: 0,
          lockTimedOut: true,
          attempts: 4,
          overdue: 0,
        });

        await processor.handleCleanupOld({} as Job);

        expect(warn).toHaveBeenCalledWith(
          expect.stringContaining("not dropped"),
        );
        expect(error).not.toHaveBeenCalled();
      });

      it("logs at error level once a chunk is past 90 + 14 days", async () => {
        mlService.dropExpiredPredictionChunks.mockResolvedValueOnce({
          due: 3,
          dropped: 0,
          lockTimedOut: true,
          attempts: 4,
          overdue: 2,
        });

        await processor.handleCleanupOld({} as Job);

        expect(error).toHaveBeenCalledTimes(1);
        expect(error.mock.calls[0][0]).toContain(
          "2 prediction chunk(s) are more than 104 days old",
        );
      });
    });

    it("still runs the row cleanup when the chunk drop fails", async () => {
      mlService.dropExpiredPredictionChunks.mockRejectedValueOnce(
        new Error("boom"),
      );

      await expect(
        processor.handleCleanupOld({} as Job),
      ).resolves.toBeUndefined();
      expect(mlService.purgeHourlyPredictionsBefore).toHaveBeenCalledTimes(1);
      expect(mlService.deleteOldPredictions).toHaveBeenCalledTimes(1);
    });

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

    it("gives the hourly cutoff two days of slack over the 7-day target window", async () => {
      await processor.handleCleanupOld({} as Job);

      const [hourlyCutoff] =
        mlService.purgeHourlyPredictionsBefore.mock.calls[0];
      const ageDays =
        (Date.now() - (hourlyCutoff as Date).getTime()) / 86_400_000;
      // 9 days: an hourly row's target can sit up to HOURLY_PREDICTIONS (48h)
      // after its createdAt, so cutting at 7 or 8 days on createdAt would drop
      // still-wanted targets.
      expect(ageDays).toBeGreaterThan(8.9);
      expect(ageDays).toBeLessThan(9.1);
    });

    it("keeps every target inside the 7-day retention, at the worst-case lead", async () => {
      // The purge deletes on createdAt; retention is stated in targetTime. This
      // is the arithmetic that connects them, and the reason the slack is two
      // days rather than one: the NEWEST row the purge drops is one created an
      // instant before the cutoff, and it was predicting for up to 48h later.
      // That latest dropped target must still be at or before the retention
      // edge — with one day of slack it lands a full day inside it, which is a
      // silent delete of targets nobody asked to lose.
      const RETENTION_DAYS = 7;
      const HORIZON_HOURS = 48; // ml-service/config.py HOURLY_PREDICTIONS

      await processor.handleCleanupOld({} as Job);

      const [hourlyCutoff] =
        mlService.purgeHourlyPredictionsBefore.mock.calls[0];
      const latestDroppedTarget =
        (hourlyCutoff as Date).getTime() + HORIZON_HOURS * 3_600_000;
      const retentionEdge = Date.now() - RETENTION_DAYS * 86_400_000;
      // setDate() moves whole calendar days, which are 23h or 25h long across a
      // DST switch, so the comparison gets an hour of room. It is nowhere near
      // enough to hide the bug: at one day of slack the latest dropped target
      // overshoots the edge by a full 24 hours.
      const DST_SLACK_MS = 3_600_000;

      expect(latestDroppedTarget).toBeLessThanOrEqual(
        retentionEdge + DST_SLACK_MS,
      );
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
