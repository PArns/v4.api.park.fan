import { Test, TestingModule } from "@nestjs/testing";
import { getQueueToken } from "@nestjs/bull";
import { CalendarService } from "./calendar.service";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { MLService } from "../../ml/ml.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { CrowdLevel } from "../../common/types/crowd-level.type";

/**
 * Targeted coverage for the private `buildPredictedCrowdLevels` helper.
 * This is the path that fed the ML-predicted-day crowd / peakLoad pair
 * in the calendar response, and PR #46 fixed an apples-to-oranges bug
 * in it (peakLoad used to divide predicted-P90 by the P50 baseline,
 * systematically inflating peakLoad). The test pins the corrected
 * peak-vs-peak shape down: crowdLevel and peakLoad must produce the
 * same rating for the same set of predictions when given a single
 * baseline.
 */
describe("CalendarService › buildPredictedCrowdLevels (private)", () => {
  let service: CalendarService;

  const noopRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
    mget: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CalendarService,
        // Real method we exercise is purely in-memory — supply skeleton
        // mocks for every constructor arg.
        {
          provide: ParksService,
          useValue: { getBatchParkStatus: jest.fn() },
        },
        {
          provide: WeatherService,
          useValue: { getCurrentWeather: jest.fn() },
        },
        {
          provide: MLService,
          useValue: { getParkPredictions: jest.fn() },
        },
        {
          provide: AnalyticsService,
          useValue: {
            getLoadRating: (current: number, baseline: number) => {
              if (baseline === 0 || current === 0)
                return { rating: "moderate" as CrowdLevel, baseline };
              const occupancy = (current / baseline) * 100;
              let rating: CrowdLevel = "very_low";
              if (occupancy > 200) rating = "extreme";
              else if (occupancy > 150) rating = "very_high";
              else if (occupancy > 110) rating = "high";
              else if (occupancy > 89) rating = "moderate";
              else if (occupancy > 60) rating = "low";
              return { rating, baseline };
            },
          },
        },
        { provide: HolidaysService, useValue: {} },
        { provide: AttractionsService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: noopRedis },
        {
          provide: getQueueToken("park-metadata"),
          useValue: { add: jest.fn() },
        },
      ],
    }).compile();

    service = module.get(CalendarService);
  });

  const callBuild = (
    predictions: Array<{
      attractionId: string;
      predictedTime: string;
      predictedWaitTime: number;
    }>,
    headlinerIds: Set<string>,
    baseline: number,
  ): Map<string, { crowdLevel: CrowdLevel; peakLoad: CrowdLevel }> =>
    (service as any).buildPredictedCrowdLevels(
      predictions,
      headlinerIds,
      baseline,
    );

  it("emits crowdLevel === peakLoad for a single day (both peak-vs-peak now)", async () => {
    // Six predicted headliner waits for one date — P90 ≈ second-highest
    // value in a 6-element array (floor(6*0.9) = 5 → index 5 of sorted).
    const predictions = [10, 15, 20, 25, 30, 60].map((w, i) => ({
      attractionId: `h${i}`,
      predictedTime: "2026-06-13T10:00:00Z",
      predictedWaitTime: w,
    }));
    const headlinerIds = new Set(predictions.map((p) => p.attractionId));

    const result = callBuild(predictions, headlinerIds, 30);

    const day = result.get("2026-06-13")!;
    // Both fields populated and identical — peakLoad is no longer a
    // separate mixed-percentile rating.
    expect(day.crowdLevel).toBe(day.peakLoad);
  });

  it("filters predictions to the headliner set when one is supplied", async () => {
    const predictions = [
      // Headliner: 60 min predicted
      {
        attractionId: "headliner",
        predictedTime: "2026-06-13T10:00:00Z",
        predictedWaitTime: 60,
      },
      // Filler: 5 min predicted — would pull the P90 down if included.
      {
        attractionId: "filler",
        predictedTime: "2026-06-13T10:00:00Z",
        predictedWaitTime: 5,
      },
    ];

    const result = callBuild(predictions, new Set(["headliner"]), 30);

    const day = result.get("2026-06-13")!;
    // P90 of [60] = 60, baseline 30 → 200% → "very_high".
    expect(day.crowdLevel).toBe("very_high");
  });

  it("falls back to all predictions when the headliner set is empty", async () => {
    const predictions = [
      {
        attractionId: "x",
        predictedTime: "2026-06-13T10:00:00Z",
        predictedWaitTime: 60,
      },
    ];

    const result = callBuild(predictions, new Set<string>(), 30);
    expect(result.get("2026-06-13")?.crowdLevel).toBe("very_high");
  });

  it("groups predictions by date and emits one entry per date", async () => {
    const predictions = [
      {
        attractionId: "h1",
        predictedTime: "2026-06-13T10:00:00Z",
        predictedWaitTime: 30,
      },
      {
        attractionId: "h1",
        predictedTime: "2026-06-14T10:00:00Z",
        predictedWaitTime: 60,
      },
      {
        attractionId: "h1",
        predictedTime: "2026-06-15T10:00:00Z",
        predictedWaitTime: 90,
      },
    ];

    const result = callBuild(predictions, new Set(["h1"]), 30);

    expect(result.size).toBe(3);
    // Different days get different ratings — they're not aggregated.
    expect(result.get("2026-06-13")?.crowdLevel).toBe("moderate"); // 30/30 = 100%
    expect(result.get("2026-06-14")?.crowdLevel).toBe("very_high"); // 60/30 = 200%
    expect(result.get("2026-06-15")?.crowdLevel).toBe("extreme"); // 90/30 = 300%
  });

  it("skips dates that have no headliner predictions (silent drop)", async () => {
    const predictions = [
      {
        attractionId: "filler",
        predictedTime: "2026-06-13T10:00:00Z",
        predictedWaitTime: 30,
      },
    ];

    const result = callBuild(predictions, new Set(["headliner"]), 30);
    expect(result.has("2026-06-13")).toBe(false);
  });
});
