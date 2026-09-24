import { Test, TestingModule } from "@nestjs/testing";
import { ParkIntegrationService } from "./park-integration.service";
import { ParksService } from "../parks.service";
import { ScheduleType } from "../entities/schedule-entry.entity";
import { WeatherService } from "../weather.service";
import { WeatherWarningsService } from "../weather-warnings.service";
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
import { RideProfileService } from "../../attractions/services/ride-profile.service";
import { AttractionOutageService } from "../../attractions/services/attraction-outage.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import type { CrowdLevel } from "../../common/types/crowd-level.type";

/**
 * Focused coverage for the public ML-aggregation helper. ParkIntegrationService
 * is a 1500-line god-object whose hot path (`buildIntegratedResponse`)
 * touches ~15 dependencies; this file targets `aggregateDailyPredictions`
 * — a public, hot, pure-ish path used by yearly-predictions. Its crowd-
 * level math mirrors the monthly calendar's future path (typical-day-peak
 * regime): AVG of predicted headliner waits ÷ the typical-day-peak
 * baseline, so the yearly and monthly calendars agree for the same date.
 * A missing typical-day-peak → 'moderate' (no P50/P90 fallback). These
 * tests pin that contract down.
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
    getTypicalDayPeakFromCache: jest.fn().mockResolvedValue(0),
    determineCrowdLevel: jest.fn(determineCrowdLevel),
  };

  const TZ = "Europe/Copenhagen";

  // No schedule by default: every day stays open, as before PAR-410.
  const parksService = {
    getSchedule: jest.fn().mockResolvedValue([]),
    getOperatingDateRange: jest
      .fn()
      .mockResolvedValue({ minDate: null, maxDate: null }),
    isParkSeasonal: jest.fn().mockResolvedValue(false),
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
        { provide: ParksService, useValue: parksService },
        { provide: WeatherService, useValue: {} },
        {
          provide: WeatherWarningsService,
          useValue: { getActiveWarnings: jest.fn().mockResolvedValue([]) },
        },
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
        { provide: RideProfileService, useValue: {} },
        {
          provide: AttractionOutageService,
          useValue: {
            getCurrentOutages: jest.fn().mockResolvedValue(new Map()),
          },
        },
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

  describe("crowd level math (typical-day-peak)", () => {
    it("uses the typical-day-peak baseline as the crowd-level denominator", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      // typical-day-peak = 50. AVG of [60, 80] = 70 → 70/50 = 140% → "high"
      // (predicted day materially above a typical day's peak).
      analyticsService.getTypicalDayPeakFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        { attractionId: "h1", date: "2026-06-13", hour: 12, wait: 60 },
        { attractionId: "h1", date: "2026-06-13", hour: 13, wait: 80 },
      ]);

      const [day] = await service.aggregateDailyPredictions(
        predictions,
        "p1",
        TZ,
      );

      expect(day.crowdLevel).toBe("high");
      expect(day.avgWaitTime).toBe(70);
      expect(day.date).toBe("2026-06-13");
    });

    it("reads 'unknown' when no typical-day-peak exists (park not ratable, no P50/P90 fallback)", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      // Thin/brand-new park: typical-day-peak = 0 → not ratable → "unknown"
      // ("keine Prognose"), rather than a made-up "moderate". There is
      // deliberately no fallback to a P50/P90 baseline.
      analyticsService.getTypicalDayPeakFromCache.mockResolvedValueOnce(0);

      const predictions = buildPredictions([
        { attractionId: "h1", date: "2026-06-13", hour: 12, wait: 99 },
      ]);

      const [day] = await service.aggregateDailyPredictions(
        predictions,
        "p1",
        TZ,
      );

      expect(day.crowdLevel).toBe("unknown");
      // Not ratable → no recommendation either (PAR-410).
      expect(day.recommendation).toBeUndefined();
    });
  });

  describe("headliner filtering", () => {
    it("aggregates only headliner predictions when the set is non-empty", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["headliner"]),
      );
      analyticsService.getTypicalDayPeakFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        // Headliner predicts a 70-min wait — high.
        { attractionId: "headliner", date: "2026-06-13", hour: 12, wait: 70 },
        // Filler predicts walk-on — would drag the day's AVG down if
        // included. Excluded by the headliner filter.
        { attractionId: "filler", date: "2026-06-13", hour: 12, wait: 5 },
      ]);

      const [day] = await service.aggregateDailyPredictions(
        predictions,
        "p1",
        TZ,
      );

      // Only the headliner counts: AVG = 70 → 70/50 = 140% → "high",
      // avgWaitTime = 70. If filler had leaked in, AVG of [5, 70] = 37.5
      // → 75% → "low" and avgWaitTime = 38 — that's the regression bait.
      expect(day.crowdLevel).toBe("high");
      expect(day.avgWaitTime).toBe(70);
    });

    it("falls back to every prediction when no headliners are defined", async () => {
      // Empty headliner set → "all attractions" mode.
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set<string>(),
      );
      analyticsService.getTypicalDayPeakFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        { attractionId: "a1", date: "2026-06-13", hour: 12, wait: 60 },
        { attractionId: "a2", date: "2026-06-13", hour: 12, wait: 80 },
      ]);

      const [day] = await service.aggregateDailyPredictions(
        predictions,
        "p1",
        TZ,
      );

      expect(day.crowdLevel).toBe("high"); // AVG 70 / 50 = 140%
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
      // Drive crowd level by choosing typical-day-peak + wait to hit the
      // target bucket. A single headliner means AVG = wait. With
      // typical-day-peak = 50, pick a wait that lands cleanly in each bucket:
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
      analyticsService.getTypicalDayPeakFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        {
          attractionId: "h1",
          date: "2026-06-13",
          hour: 12,
          wait: waitForBucket[level],
        },
      ]);
      const [day] = await service.aggregateDailyPredictions(
        predictions,
        "p1",
        TZ,
      );

      expect(day.crowdLevel).toBe(level);
      expect(day.recommendation).toBe(expected);
    });
  });

  describe("schedule: closed days (PAR-410, calendar parity)", () => {
    // Every day predicts a quiet headliner (30 / 50 = 60% → very_low →
    // highly_recommended), which is exactly what a closed February looked
    // like before the schedule was consulted.
    const quietDays = (dates: string[]) =>
      buildPredictions(
        dates.map((date) => ({ attractionId: "h1", date, hour: 12, wait: 30 })),
      );

    beforeEach(() => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      analyticsService.getTypicalDayPeakFromCache.mockResolvedValueOnce(50);
    });

    it("reads a CLOSED schedule entry as closed / closed, without a wait", async () => {
      parksService.getSchedule.mockResolvedValueOnce([
        { date: "2027-02-01", scheduleType: ScheduleType.CLOSED },
        { date: "2027-02-02", scheduleType: ScheduleType.OPERATING },
      ]);

      const days = await service.aggregateDailyPredictions(
        quietDays(["2027-02-01", "2027-02-02"]),
        "p1",
        TZ,
      );

      expect(days[0]).toMatchObject({
        date: "2027-02-01",
        crowdLevel: "closed",
        recommendation: "closed",
      });
      expect(days[0].avgWaitTime).toBeUndefined();
      expect(days[1]).toMatchObject({
        date: "2027-02-02",
        crowdLevel: "very_low",
        recommendation: "highly_recommended",
      });
    });

    it("closes an unscheduled day inside the operating range (gap), keeps one outside for a year-round park", async () => {
      parksService.getOperatingDateRange.mockResolvedValueOnce({
        minDate: "2026-10-01",
        maxDate: "2027-01-31",
      });
      parksService.isParkSeasonal.mockResolvedValueOnce(false);

      const days = await service.aggregateDailyPredictions(
        quietDays(["2026-12-24", "2027-02-10"]),
        "p1",
        TZ,
      );

      expect(days.map((d) => [d.date, d.crowdLevel, d.recommendation])).toEqual(
        [
          ["2026-12-24", "closed", "closed"],
          // After the last OPERATING date of a year-round park: hours not
          // published yet, so the forecast stands.
          ["2027-02-10", "very_low", "highly_recommended"],
        ],
      );
    });

    it("closes an unscheduled day outside the operating range for a seasonal park", async () => {
      parksService.getOperatingDateRange.mockResolvedValueOnce({
        minDate: "2026-10-01",
        maxDate: "2027-01-31",
      });
      parksService.isParkSeasonal.mockResolvedValueOnce(true);
      parksService.getSchedule.mockResolvedValueOnce([
        { date: "2027-02-11", scheduleType: ScheduleType.UNKNOWN },
      ]);

      const days = await service.aggregateDailyPredictions(
        quietDays(["2027-02-10", "2027-02-11"]),
        "p1",
        TZ,
      );

      expect(days.every((d) => d.crowdLevel === "closed")).toBe(true);
      expect(days.every((d) => d.recommendation === "closed")).toBe(true);
    });

    it("an OPERATING entry wins over the gap rule", async () => {
      parksService.getOperatingDateRange.mockResolvedValueOnce({
        minDate: "2026-10-01",
        maxDate: "2027-01-31",
      });
      parksService.getSchedule.mockResolvedValueOnce([
        { date: "2026-12-24", scheduleType: ScheduleType.OPERATING },
      ]);

      const [day] = await service.aggregateDailyPredictions(
        quietDays(["2026-12-24"]),
        "p1",
        TZ,
      );

      expect(day.crowdLevel).toBe("very_low");
    });

    it("serves the forecast unchanged when the schedule cannot be read", async () => {
      parksService.getSchedule.mockRejectedValueOnce(new Error("db down"));

      const [day] = await service.aggregateDailyPredictions(
        quietDays(["2027-02-01"]),
        "p1",
        TZ,
      );

      expect(day.crowdLevel).toBe("very_low");
      expect(day.recommendation).toBe("highly_recommended");
    });

    it("queries the schedule around the predicted window, one day of slack each side", async () => {
      await service.aggregateDailyPredictions(
        quietDays(["2027-02-03", "2027-02-01"]),
        "p1",
        TZ,
      );

      const [, from, to] = parksService.getSchedule.mock.calls[0];
      expect((from as Date).toISOString().slice(0, 10)).toBe("2027-01-31");
      expect((to as Date).toISOString().slice(0, 10)).toBe("2027-02-04");
    });
  });

  describe("output shape & ordering", () => {
    it("returns one entry per distinct date, sorted ascending", async () => {
      analyticsService.getHeadlinerAttractionIds.mockResolvedValueOnce(
        new Set(["h1"]),
      );
      analyticsService.getTypicalDayPeakFromCache.mockResolvedValueOnce(50);

      const predictions = buildPredictions([
        // Intentionally out-of-order on input.
        { attractionId: "h1", date: "2026-06-15", hour: 12, wait: 50 },
        { attractionId: "h1", date: "2026-06-13", hour: 12, wait: 50 },
        { attractionId: "h1", date: "2026-06-14", hour: 12, wait: 50 },
      ]);

      const days = await service.aggregateDailyPredictions(
        predictions,
        "p1",
        TZ,
      );

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
      analyticsService.getTypicalDayPeakFromCache.mockResolvedValueOnce(50);

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

      const [day] = await service.aggregateDailyPredictions(
        predictions,
        "p1",
        TZ,
      );

      // (0.8 + 1.0) / 2 = 0.9
      expect(day.confidencePercentage).toBeCloseTo(0.9);
    });

    it("returns an empty array when no predictions are provided", async () => {
      const days = await service.aggregateDailyPredictions([], "p1", TZ);
      expect(days).toEqual([]);
    });
  });
});
