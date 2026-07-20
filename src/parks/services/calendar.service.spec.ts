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

/**
 * Coverage for the calendar "what does this crowd level mean" data: the
 * per-day headliner forecast (avg + top rides) and the priority-ranked
 * neighbouring-region holidays. All three helpers are pure/in-memory.
 */
describe("CalendarService › headliner forecast & neighbour holidays (private)", () => {
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
        { provide: ParksService, useValue: {} },
        { provide: WeatherService, useValue: {} },
        { provide: MLService, useValue: {} },
        { provide: AnalyticsService, useValue: {} },
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

  describe("buildHeadlinerForecasts", () => {
    const call = (
      predictions: Array<{
        attractionId: string;
        predictedTime: string;
        predictedWaitTime: number;
      }>,
      headlinerIds: Set<string>,
      names: Map<string, string>,
    ) =>
      (service as any).buildHeadlinerForecasts(
        predictions,
        headlinerIds,
        names,
      );

    it("lists the top-5 rides by expected wait and the avg across all headliners", () => {
      const waits = [10, 60, 20, 50, 30, 40]; // 6 headliners
      const predictions = waits.map((w, i) => ({
        attractionId: `h${i}`,
        predictedTime: "2026-06-13T10:00:00Z",
        predictedWaitTime: w,
      }));
      const ids = new Set(predictions.map((p) => p.attractionId));
      const names = new Map(
        predictions.map((p) => [p.attractionId, `Ride ${p.attractionId}`]),
      );

      const forecast = call(predictions, ids, names).get("2026-06-13");

      // avg = mean(10,60,20,50,30,40) = 35 (rounded)
      expect(forecast.avgWait).toBe(35);
      // top 5 by wait desc: 60,50,40,30,20 (the 10 is dropped)
      expect(forecast.rides).toHaveLength(5);
      expect(
        forecast.rides.map((r: { waitTime: number }) => r.waitTime),
      ).toEqual([60, 50, 40, 30, 20]);
      expect(forecast.rides[0].name).toBe("Ride h1");
    });

    it("excludes unnamed rides from the list but keeps them in the average", () => {
      const predictions = [
        {
          attractionId: "named",
          predictedTime: "2026-06-13T10:00:00Z",
          predictedWaitTime: 20,
        },
        {
          attractionId: "unnamed",
          predictedTime: "2026-06-13T10:00:00Z",
          predictedWaitTime: 40,
        },
      ];
      const ids = new Set(["named", "unnamed"]);
      const names = new Map([["named", "Taron"]]);

      const forecast = call(predictions, ids, names).get("2026-06-13");

      expect(forecast.avgWait).toBe(30); // (20+40)/2 — unnamed still counts
      expect(forecast.rides).toHaveLength(1);
      expect(forecast.rides[0].name).toBe("Taron");
    });

    it("returns an empty map when there are no headliners", () => {
      const result = call([], new Set<string>(), new Map());
      expect(result.size).toBe(0);
    });
  });

  describe("neighborHolidayPriority", () => {
    const park = {
      influencingRegions: [
        { countryCode: "DE", regionCode: "DE-BY" }, // rank 1
        { countryCode: "DE", regionCode: "DE-RP" }, // rank 2
        { countryCode: "FR", regionCode: null }, // rank 3 (country-level)
      ],
    };
    const rank = (h: { country: string; region: string | null }) =>
      (service as any).neighborHolidayPriority(park, h);

    it("ranks a region-specific match by its position (1-based)", () => {
      expect(rank({ country: "DE", region: "DE-BY" })).toBe(1);
      expect(rank({ country: "DE", region: "DE-RP" })).toBe(2);
    });

    it("normalizes region codes on both sides before matching", () => {
      expect(rank({ country: "DE", region: "BY" })).toBe(1);
    });

    it("treats a country-level config entry (null region) as matching any region", () => {
      expect(rank({ country: "FR", region: "FR-GE" })).toBe(3);
    });

    it("returns Infinity for a region that is not a configured influence", () => {
      expect(rank({ country: "DE", region: "DE-HE" })).toBe(Infinity);
      expect(rank({ country: "NL", region: null })).toBe(Infinity);
    });
  });

  describe("rankNeighborHolidays", () => {
    const call = (raw: unknown[]) => (service as any).rankNeighborHolidays(raw);

    it("dedupes by region+type keeping the strongest priority, sorts by priority", () => {
      const raw = [
        {
          name: "B",
          source: { countryCode: "DE", regionCode: "RP" },
          holidayType: "school",
          priority: 2,
        },
        {
          name: "A",
          source: { countryCode: "DE", regionCode: "BY" },
          holidayType: "school",
          priority: 1,
        },
        // duplicate region+type, weaker priority — must be dropped
        {
          name: "B2",
          source: { countryCode: "DE", regionCode: "RP" },
          holidayType: "school",
          priority: 3,
        },
      ];

      const ranked = call(raw);

      expect(ranked).toHaveLength(2);
      expect(
        ranked.map(
          (n: { source: { regionCode: string } }) => n.source.regionCode,
        ),
      ).toEqual(["BY", "RP"]);
      expect(ranked[1].priority).toBe(2); // strongest kept, not the 3
    });

    it("caps the list at 6 entries", () => {
      const raw = Array.from({ length: 10 }, (_, i) => ({
        name: `H${i}`,
        source: { countryCode: "DE", regionCode: `R${i}` },
        holidayType: "school",
        priority: 1,
      }));
      expect(call(raw)).toHaveLength(6);
    });
  });
});
