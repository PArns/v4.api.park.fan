import { Test, TestingModule } from "@nestjs/testing";
import { ParkIntegrationService } from "./park-integration.service";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { PredictionDeviationService } from "../../ml/services/prediction-deviation.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { ParkEnrichmentService } from "./park-enrichment.service";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { PopularityService } from "../../popularity/popularity.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import type { CrowdLevel } from "../../common/types/crowd-level.type";

/**
 * Focused coverage for the public ML-aggregation helper. ParkIntegrationService
 * is a 1500-line god-object whose hot path (`buildIntegratedResponse`)
 * touches ~15 dependencies; this file targets `aggregateDailyPredictions`
 * — a public, hot, pure-ish path used by yearly-predictions and the
 * calendar route. The recent peak-vs-peak refactor flipped its crowd-
 * level math from "median(predictions) / P50 baseline" to
 * "P90(predictions) / P90 baseline (P50 fallback)" — these tests pin
 * the new contract down.
 */
describe("ParkIntegrationService › aggregateDailyPredictions", () => {
  let service: ParkIntegrationService;

  // determineCrowdLevel inline matches the shared utility; we keep the
  // mock in sync so the test maps wait-time → crowdLevel exactly.
  const determineCrowdLevel = (occupancy: number): CrowdLevel => {
    if (occupancy <= 60) return "very_low";
    if (occupancy <= 89) return "low";
    if (occupancy <= 110) return "moderate";
    if (occupancy <= 150) return "high";
    if (occupancy <= 200) return "very_high";
    return "extreme";
  };

  const analyticsService = {
    getHeadlinerAttractionIds: jest.fn().mockResolvedValue(new Set<string>()),
    getP90BaselineFromCache: jest.fn().mockResolvedValue(0),
    getP50BaselineFromCache: jest.fn().mockResolvedValue(0),
    determineCrowdLevel: jest.fn(determineCrowdLevel),
  };

  const noopRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkIntegrationService,
        // Constructor takes 15 deps — most are unused on this path.
        { provide: ParksService, useValue: {} },
        { provide: WeatherService, useValue: {} },
        { provide: AttractionsService, useValue: {} },
        { provide: ShowsService, useValue: {} },
        { provide: RestaurantsService, useValue: {} },
        { provide: QueueDataService, useValue: {} },
        { provide: AnalyticsService, useValue: analyticsService },
        { provide: MLService, useValue: {} },
        { provide: PredictionAccuracyService, useValue: {} },
        { provide: PredictionDeviationService, useValue: {} },
        { provide: HolidaysService, useValue: {} },
        { provide: ParkEnrichmentService, useValue: {} },
        { provide: ThemeParksClient, useValue: {} },
        { provide: QueueTimesClient, useValue: {} },
        { provide: WartezeitenClient, useValue: {} },
        { provide: PopularityService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: noopRedis },
      ],
    }).compile();

    service = module.get(ParkIntegrationService);
  });

  /**
   * Helper to keep test bodies short. Each entry produces one
   * PredictionDto with the test's chosen attractionId + predicted wait.
   */
  const buildPredictions = (
    rows: Array<{
      attractionId: string;
      date: string;
      hour: number;
      wait: number;
      confidence?: number;
    }>,
  ) =>
    rows.map((r) => ({
      attractionId: r.attractionId,
      predictedTime: `${r.date}T${String(r.hour).padStart(2, "0")}:00:00Z`,
      predictedWaitTime: r.wait,
      confidence: r.confidence ?? 0.9,
      crowdLevel: "moderate" as const,
      baseline: 30,
      modelVersion: "v1",
      predictionType: "daily" as const,
    }));

  describe("crowd level math (peak-vs-peak)", () => {
    it("uses the P90 baseline as the crowd-level denominator when present", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      // P90 baseline = 50; predicted P90 of [30, 50, 70] (sorted) at
      // index floor(3*0.9)=2 → 70. 70/50 = 140% → "high".
      analyticsService.getP90BaselineFromCache.mockResolvedValueOnce(50);
      analyticsService.getP50BaselineFromCache.mockResolvedValueOnce(20);

      const predictions = buildPredictions([
        { attractionId: "h1", date: "2026-06-13", hour: 12, wait: 30 },
        { attractionId: "h1", date: "2026-06-13", hour: 13, wait: 50 },
        { attractionId: "h1", date: "2026-06-13", hour: 14, wait: 70 },
      ]);

      const [day] = await service.aggregateDailyPredictions(predictions, "p1");

      expect(day.crowdLevel).toBe("high");
      // P50 cache call is irrelevant when P90 exists — but it's still
      // read in parallel, no extra round-trip wasted.
      expect(day.date).toBe("2026-06-13");
    });

    it("falls back to the P50 baseline when no P90 row exists yet", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      // P90 = 0 (missing). P50 = 30 → fallback. Predicted P90 = 30 →
      // 30/30 = 100% → "moderate".
      analyticsService.getP90BaselineFromCache.mockResolvedValueOnce(0);
      analyticsService.getP50BaselineFromCache.mockResolvedValueOnce(30);

      const predictions = buildPredictions([
        { attractionId: "h1", date: "2026-06-13", hour: 12, wait: 30 },
      ]);

      const [day] = await service.aggregateDailyPredictions(predictions, "p1");

      expect(day.crowdLevel).toBe("moderate");
    });

    it("defaults to 'moderate' (100%) when no baseline at all is available", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      analyticsService.getP90BaselineFromCache.mockResolvedValueOnce(0);
      analyticsService.getP50BaselineFromCache.mockResolvedValueOnce(0);

      const predictions = buildPredictions([
        { attractionId: "h1", date: "2026-06-13", hour: 12, wait: 99 },
      ]);

      const [day] = await service.aggregateDailyPredictions(predictions, "p1");

      // No baseline → pct hard-coded to 100 → "moderate". This avoids
      // showing a misleading rating for brand-new parks.
      expect(day.crowdLevel).toBe("moderate");
    });
  });

  describe("headliner filtering", () => {
    it("aggregates only headliner predictions when the set is non-empty", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["headliner"]),
      );
      analyticsService.getP90BaselineFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        // Headliner predicts a 70-min wait — high.
        { attractionId: "headliner", date: "2026-06-13", hour: 12, wait: 70 },
        // Filler predicts walk-on — would drag the day's P90 down if
        // included. Excluded by the headliner filter.
        { attractionId: "filler", date: "2026-06-13", hour: 12, wait: 5 },
      ]);

      const [day] = await service.aggregateDailyPredictions(predictions, "p1");

      // 70/50 = 140% → "high". If filler had leaked in, P90 of [5, 70]
      // = 70 at idx floor(2*0.9)=1 → 70 too. But avgWaitTime would
      // be 37 instead of 70 — that's the regression bait.
      expect(day.crowdLevel).toBe("high");
      expect(day.avgWaitTime).toBe(70);
    });

    it("falls back to every prediction when no headliners are defined", async () => {
      // Empty headliner set → "all attractions" mode.
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set<string>(),
      );
      analyticsService.getP90BaselineFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        { attractionId: "a1", date: "2026-06-13", hour: 12, wait: 30 },
        { attractionId: "a2", date: "2026-06-13", hour: 12, wait: 70 },
      ]);

      const [day] = await service.aggregateDailyPredictions(predictions, "p1");

      expect(day.crowdLevel).toBe("high"); // 70/50 = 140%
    });
  });

  describe("recommendation score derivation", () => {
    // The recommendation map covers a hidden user-facing surface — a
    // slide here changes the chip on the calendar from "go!" to "skip".
    it.each([
      ["very_low", "highly_recommended"],
      ["low", "highly_recommended"],
      ["moderate", "recommended"],
      ["high", "neutral"],
      ["very_high", "avoid"],
      ["extreme", "strongly_avoid"],
    ])("crowdLevel '%s' → recommendation '%s'", async (level, expected) => {
      // Drive crowd level by choosing baseline + wait to hit the target
      // bucket. baseline = 50, multipliers: very_low ≤ 60% (wait=30),
      // low 61-89% (45), moderate 90-110% (50), high 111-150% (70),
      // very_high 151-200% (90), extreme > 200% (110).
      // baseline = 50. Pick a wait that lands cleanly in each bucket:
      // very_low ≤60% (wait=30 → 60%), low 61-89% (44 → 88%),
      // moderate 90-110% (50 → 100%), high 111-150% (70 → 140%),
      // very_high 151-200% (95 → 190%), extreme >200% (110 → 220%).
      const waitForBucket: Record<string, number> = {
        very_low: 30,
        low: 44,
        moderate: 50,
        high: 70,
        very_high: 95,
        extreme: 110,
      };
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      analyticsService.getP90BaselineFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        {
          attractionId: "h1",
          date: "2026-06-13",
          hour: 12,
          wait: waitForBucket[level],
        },
      ]);
      const [day] = await service.aggregateDailyPredictions(predictions, "p1");

      expect(day.crowdLevel).toBe(level);
      expect(day.recommendation).toBe(expected);
    });
  });

  describe("output shape & ordering", () => {
    it("returns one entry per distinct date, sorted ascending", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      analyticsService.getP90BaselineFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        // Intentionally out-of-order on input.
        { attractionId: "h1", date: "2026-06-15", hour: 12, wait: 50 },
        { attractionId: "h1", date: "2026-06-13", hour: 12, wait: 50 },
        { attractionId: "h1", date: "2026-06-14", hour: 12, wait: 50 },
      ]);

      const days = await service.aggregateDailyPredictions(predictions, "p1");

      expect(days.map((d) => d.date)).toEqual([
        "2026-06-13",
        "2026-06-14",
        "2026-06-15",
      ]);
    });

    it("averages confidence across the day", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      analyticsService.getP90BaselineFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        {
          attractionId: "h1",
          date: "2026-06-13",
          hour: 12,
          wait: 50,
          confidence: 0.8,
        },
        {
          attractionId: "h1",
          date: "2026-06-13",
          hour: 13,
          wait: 50,
          confidence: 1.0,
        },
      ]);

      const [day] = await service.aggregateDailyPredictions(predictions, "p1");

      // (0.8 + 1.0) / 2 = 0.9
      expect(day.confidencePercentage).toBeCloseTo(0.9);
    });

    it("returns an empty array when no predictions are provided", async () => {
      const days = await service.aggregateDailyPredictions([], "p1");
      expect(days).toEqual([]);
    });
  });
});
