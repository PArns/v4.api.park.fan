import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { AttractionHourlyHistoryProcessor } from "./attraction-hourly-history.processor";
import { AnalyticsService } from "../../analytics/analytics.service";
import { ParksService } from "../../parks/parks.service";
import { AttractionsService } from "../../attractions/attractions.service";

/**
 * Coverage for the nightly hourly-history rollup processor. The job is
 * idempotent and runs against every park, so the tests focus on:
 *   1. The yesterday-cron actually computes for yesterday (per park
 *      timezone), not the current UTC day.
 *   2. Per-park failure isolation — one bad park doesn't bring the cron
 *      down.
 *   3. The backfill entry point enumerates the requested date range and
 *      can be scoped to a single park.
 *   4. Each row written carries the joined slots + downCount, with a
 *      stable empty-array shape for attractions that produced no data.
 */
describe("AttractionHourlyHistoryProcessor", () => {
  let processor: AttractionHourlyHistoryProcessor;

  const mockAnalyticsService = {
    computeParkHourlyHistoryForDate: jest.fn(),
    computeParkDownCountForDate: jest.fn(),
    saveAttractionHourlyHistoryBatch: jest.fn().mockResolvedValue(undefined),
  };

  const mockParksService = {
    findAll: jest.fn(),
    findById: jest.fn(),
  };

  const mockAttractionsService = {
    findByParkId: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AttractionHourlyHistoryProcessor,
        { provide: AnalyticsService, useValue: mockAnalyticsService },
        { provide: ParksService, useValue: mockParksService },
        { provide: AttractionsService, useValue: mockAttractionsService },
      ],
    }).compile();

    processor = module.get(AttractionHourlyHistoryProcessor);
  });

  describe("calculate-yesterday-hourly-history (the daily cron)", () => {
    it("rolls up yesterday's data for every park and joins slots + down counts", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Phantasialand", timezone: "Europe/Berlin" },
      ]);
      mockAttractionsService.findByParkId.mockResolvedValue({
        data: [{ id: "a1" }, { id: "a2" }],
        total: 2,
      });
      mockAnalyticsService.computeParkHourlyHistoryForDate.mockResolvedValue(
        new Map([
          [
            "a1",
            [{ time_slot: "10:00", p90: 30, avgWait: 20, sampleCount: 6 }],
          ],
        ]),
      );
      mockAnalyticsService.computeParkDownCountForDate.mockResolvedValue(
        new Map([["a2", 2]]),
      );

      await processor.handleYesterdayHourlyHistory({} as Job);

      expect(
        mockAnalyticsService.saveAttractionHourlyHistoryBatch,
      ).toHaveBeenCalledTimes(1);
      const [rows] =
        mockAnalyticsService.saveAttractionHourlyHistoryBatch.mock.calls[0];
      expect(rows).toHaveLength(2);
      // Both attractions get a row — empty slot arrays are preserved so
      // the read path can tell "processed, no data" from "not processed".
      const a1Row = rows.find(
        (r: { attractionId: string }) => r.attractionId === "a1",
      );
      const a2Row = rows.find(
        (r: { attractionId: string }) => r.attractionId === "a2",
      );
      expect(a1Row.slots).toHaveLength(1);
      expect(a1Row.downCount).toBe(0);
      expect(a2Row.slots).toEqual([]);
      expect(a2Row.downCount).toBe(2);
    });

    it("computes the date in each park's local timezone, not UTC", async () => {
      // Park in Honolulu (UTC-10): at any UTC time, the local 'yesterday'
      // differs from the UTC 'yesterday' by up to a day. We just assert
      // that compute* is called with a YYYY-MM-DD string, the timezone
      // is forwarded, and both compute calls share the same date.
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Aulani", timezone: "Pacific/Honolulu" },
      ]);
      mockAttractionsService.findByParkId.mockResolvedValue({
        data: [{ id: "a1" }],
        total: 1,
      });
      mockAnalyticsService.computeParkHourlyHistoryForDate.mockResolvedValue(
        new Map(),
      );
      mockAnalyticsService.computeParkDownCountForDate.mockResolvedValue(
        new Map(),
      );

      await processor.handleYesterdayHourlyHistory({} as Job);

      const [hourlyCall] =
        mockAnalyticsService.computeParkHourlyHistoryForDate.mock.calls;
      const [downCall] =
        mockAnalyticsService.computeParkDownCountForDate.mock.calls;
      expect(hourlyCall[0]).toBe("p1");
      expect(hourlyCall[1]).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(hourlyCall[2]).toBe("Pacific/Honolulu");
      expect(downCall[1]).toBe(hourlyCall[1]); // same date in both calls
    });

    it("continues to the next park when one fails (per-park isolation)", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "First", timezone: "UTC" },
        { id: "p2", name: "Second", timezone: "UTC" },
      ]);
      mockAttractionsService.findByParkId
        .mockRejectedValueOnce(new Error("p1 blew up"))
        .mockResolvedValueOnce({ data: [{ id: "a1" }], total: 1 });
      mockAnalyticsService.computeParkHourlyHistoryForDate.mockResolvedValue(
        new Map(),
      );
      mockAnalyticsService.computeParkDownCountForDate.mockResolvedValue(
        new Map(),
      );

      await processor.handleYesterdayHourlyHistory({} as Job);

      // p2 still produces a save — failure in p1 didn't abort the cron.
      expect(
        mockAnalyticsService.saveAttractionHourlyHistoryBatch,
      ).toHaveBeenCalledTimes(1);
      const [rows] =
        mockAnalyticsService.saveAttractionHourlyHistoryBatch.mock.calls[0];
      expect(rows[0].attractionId).toBe("a1");
    });

    it("skips the save call entirely when a park has no attractions", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Empty", timezone: "UTC" },
      ]);
      mockAttractionsService.findByParkId.mockResolvedValue({
        data: [],
        total: 0,
      });

      await processor.handleYesterdayHourlyHistory({} as Job);

      // No attractions → don't even compute, don't write empty saves.
      expect(
        mockAnalyticsService.computeParkHourlyHistoryForDate,
      ).not.toHaveBeenCalled();
      expect(
        mockAnalyticsService.saveAttractionHourlyHistoryBatch,
      ).not.toHaveBeenCalled();
    });
  });

  describe("backfill-attraction-hourly-history", () => {
    it("enumerates every date in the inclusive range and runs the rollup per park", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Park 1", timezone: "UTC" },
      ]);
      mockAttractionsService.findByParkId.mockResolvedValue({
        data: [{ id: "a1" }],
        total: 1,
      });
      mockAnalyticsService.computeParkHourlyHistoryForDate.mockResolvedValue(
        new Map(),
      );
      mockAnalyticsService.computeParkDownCountForDate.mockResolvedValue(
        new Map(),
      );

      const job = {
        data: { fromDate: "2026-05-15", toDate: "2026-05-17" },
      } as Job<{ fromDate: string; toDate: string }>;

      await processor.handleBackfill(job);

      // 3 days × 1 park = 3 compute pairs + 3 saves.
      expect(
        mockAnalyticsService.computeParkHourlyHistoryForDate,
      ).toHaveBeenCalledTimes(3);
      expect(
        mockAnalyticsService.saveAttractionHourlyHistoryBatch,
      ).toHaveBeenCalledTimes(3);

      const dates =
        mockAnalyticsService.computeParkHourlyHistoryForDate.mock.calls.map(
          (c) => c[1],
        );
      expect(dates).toEqual(["2026-05-15", "2026-05-16", "2026-05-17"]);
    });

    it("scopes the backfill to a single park when parkId is provided", async () => {
      mockParksService.findById.mockResolvedValue({
        id: "p2",
        timezone: "UTC",
      });
      mockAttractionsService.findByParkId.mockResolvedValue({
        data: [{ id: "a1" }],
        total: 1,
      });
      mockAnalyticsService.computeParkHourlyHistoryForDate.mockResolvedValue(
        new Map(),
      );
      mockAnalyticsService.computeParkDownCountForDate.mockResolvedValue(
        new Map(),
      );

      await processor.handleBackfill({
        data: {
          parkId: "p2",
          fromDate: "2026-05-18",
          toDate: "2026-05-18",
        },
      } as Job<{ parkId: string; fromDate: string; toDate: string }>);

      expect(mockParksService.findAll).not.toHaveBeenCalled();
      expect(mockParksService.findById).toHaveBeenCalledWith("p2");
      // 1 day × 1 park = exactly one compute pair
      expect(
        mockAnalyticsService.computeParkHourlyHistoryForDate,
      ).toHaveBeenCalledTimes(1);
    });

    it("silently drops a park that resolves to null (deleted between scheduling and running)", async () => {
      mockParksService.findById.mockResolvedValue(null);

      await processor.handleBackfill({
        data: {
          parkId: "deleted-park",
          fromDate: "2026-05-18",
          toDate: "2026-05-19",
        },
      } as Job<{ parkId: string; fromDate: string; toDate: string }>);

      expect(
        mockAnalyticsService.computeParkHourlyHistoryForDate,
      ).not.toHaveBeenCalled();
    });
  });
});
