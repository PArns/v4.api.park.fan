import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { P50BaselineProcessor } from "./p50-baseline.processor";
import { AnalyticsService } from "../../analytics/analytics.service";
import { ParksService } from "../../parks/parks.service";
import { AttractionsService } from "../../attractions/attractions.service";

/**
 * Coverage for the daily P50/P90 baseline cron. The processor used to
 * fire ~10k per-attraction PERCENTILE_CONT scans every night; PR #46
 * collapsed that to ~50 batched per-park scans via
 * `calculateAttractionP50P90ForPark` + `saveAttractionP50P90BaselinesBatch`.
 *
 * These tests pin the new batch contract down — one compute call per
 * park, one save call per park, and per-park failure isolation so a
 * single bad park doesn't tank the cron.
 */
describe("P50BaselineProcessor", () => {
  let processor: P50BaselineProcessor;

  const mockAnalyticsService = {
    parkHasQueueDataInWindow: jest.fn(),
    identifyHeadliners: jest.fn(),
    calculateP50Baseline: jest.fn(),
    saveP50Baselines: jest.fn(),
    calculateAttractionP50P90ForPark: jest.fn(),
    saveAttractionP50P90BaselinesBatch: jest.fn(),
  };

  const mockParksService = {
    findAll: jest.fn(),
  };

  const mockAttractionsService = {
    findByParkId: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        P50BaselineProcessor,
        { provide: AnalyticsService, useValue: mockAnalyticsService },
        { provide: ParksService, useValue: mockParksService },
        { provide: AttractionsService, useValue: mockAttractionsService },
      ],
    }).compile();

    processor = module.get(P50BaselineProcessor);
  });

  describe("handleCalculateAttractionBaselines (daily cron)", () => {
    it("runs ONE compute call per park (not per attraction)", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Park 1", timezone: "Europe/Berlin" },
        { id: "p2", name: "Park 2", timezone: "America/New_York" },
      ]);
      mockAnalyticsService.calculateAttractionP50P90ForPark.mockResolvedValue(
        new Map([
          [
            "a1",
            {
              p50: 20,
              p90: 50,
              sampleCount: 100,
              distinctDays: 90,
              confidence: "high",
              isHeadliner: true,
            },
          ],
        ]),
      );
      mockAnalyticsService.saveAttractionP50P90BaselinesBatch.mockResolvedValue(
        { p50Saved: 1, p90Saved: 1 },
      );

      await processor.handleCalculateAttractionBaselines({} as Job);

      // 2 parks → exactly 2 compute calls, 2 save calls. Not 2× N.
      expect(
        mockAnalyticsService.calculateAttractionP50P90ForPark,
      ).toHaveBeenCalledTimes(2);
      expect(
        mockAnalyticsService.saveAttractionP50P90BaselinesBatch,
      ).toHaveBeenCalledTimes(2);
      // Each park's timezone is forwarded, not a global default.
      expect(
        mockAnalyticsService.calculateAttractionP50P90ForPark,
      ).toHaveBeenCalledWith("p1", "Europe/Berlin");
      expect(
        mockAnalyticsService.calculateAttractionP50P90ForPark,
      ).toHaveBeenCalledWith("p2", "America/New_York");
    });

    it("skips the save call when a park has no qualifying attractions", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Closed", timezone: "UTC" },
      ]);
      mockAnalyticsService.calculateAttractionP50P90ForPark.mockResolvedValue(
        new Map(), // empty → nothing to save
      );

      await processor.handleCalculateAttractionBaselines({} as Job);

      expect(
        mockAnalyticsService.calculateAttractionP50P90ForPark,
      ).toHaveBeenCalledTimes(1);
      // No empty save round-trip.
      expect(
        mockAnalyticsService.saveAttractionP50P90BaselinesBatch,
      ).not.toHaveBeenCalled();
    });

    it("continues to the next park when one fails (per-park isolation)", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Broken", timezone: "UTC" },
        { id: "p2", name: "Healthy", timezone: "UTC" },
      ]);
      mockAnalyticsService.calculateAttractionP50P90ForPark
        .mockRejectedValueOnce(new Error("DB exploded for p1"))
        .mockResolvedValueOnce(
          new Map([
            [
              "a1",
              {
                p50: 20,
                p90: 50,
                sampleCount: 100,
                distinctDays: 90,
                confidence: "high",
                isHeadliner: true,
              },
            ],
          ]),
        );
      mockAnalyticsService.saveAttractionP50P90BaselinesBatch.mockResolvedValue(
        { p50Saved: 1, p90Saved: 1 },
      );

      await processor.handleCalculateAttractionBaselines({} as Job);

      // p2 still got its save through.
      expect(
        mockAnalyticsService.saveAttractionP50P90BaselinesBatch,
      ).toHaveBeenCalledTimes(1);
      const [savedParkId] =
        mockAnalyticsService.saveAttractionP50P90BaselinesBatch.mock.calls[0];
      expect(savedParkId).toBe("p2");
    });

    it("passes park.timezone='UTC' as a safe default when the entity lacks one", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "No tz", timezone: null },
      ]);
      mockAnalyticsService.calculateAttractionP50P90ForPark.mockResolvedValue(
        new Map(),
      );

      await processor.handleCalculateAttractionBaselines({} as Job);

      const [parkId, tz] =
        mockAnalyticsService.calculateAttractionP50P90ForPark.mock.calls[0];
      expect(parkId).toBe("p1");
      expect(tz).toBe("UTC");
    });
  });

  describe("handleCalculateParkBaselines (daily cron)", () => {
    // The park-level cron still iterates parks sequentially since each
    // park's headliner+baseline calculation is small and independent.
    // We just assert the skip-on-no-data + per-park isolation contracts.
    it("skips parks with no queue data in the 548-day window", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Empty", timezone: "UTC" },
      ]);
      mockAnalyticsService.parkHasQueueDataInWindow.mockResolvedValue(false);

      await processor.handleCalculateParkBaselines({} as Job);

      expect(mockAnalyticsService.identifyHeadliners).not.toHaveBeenCalled();
      expect(mockAnalyticsService.calculateP50Baseline).not.toHaveBeenCalled();
      expect(mockAnalyticsService.saveP50Baselines).not.toHaveBeenCalled();
    });

    it("skips parks with no headliner attractions identified", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Small", timezone: "UTC" },
      ]);
      mockAnalyticsService.parkHasQueueDataInWindow.mockResolvedValue(true);
      mockAnalyticsService.identifyHeadliners.mockResolvedValue([]);

      await processor.handleCalculateParkBaselines({} as Job);

      expect(mockAnalyticsService.saveP50Baselines).not.toHaveBeenCalled();
    });

    it("persists the baseline + headliners when both compute steps return data", async () => {
      mockParksService.findAll.mockResolvedValue([
        { id: "p1", name: "Phantasialand", timezone: "Europe/Berlin" },
      ]);
      mockAnalyticsService.parkHasQueueDataInWindow.mockResolvedValue(true);
      const headliners = [
        { attractionId: "h1", parkId: "p1", tier: "tier1" } as any,
      ];
      mockAnalyticsService.identifyHeadliners.mockResolvedValue(headliners);
      mockAnalyticsService.calculateP50Baseline.mockResolvedValue({
        p50: 25,
        p90: 60,
        sampleCount: 5000,
        distinctDays: 200,
        confidence: "high",
        tier: "tier1",
      });

      await processor.handleCalculateParkBaselines({} as Job);

      expect(mockAnalyticsService.saveP50Baselines).toHaveBeenCalledWith(
        "p1",
        expect.objectContaining({ p50: 25, p90: 60, tier: "tier1" }),
        headliners,
      );
    });
  });
});
