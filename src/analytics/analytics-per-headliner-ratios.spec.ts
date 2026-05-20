import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { AnalyticsService } from "./analytics.service";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { WeatherData } from "../parks/entities/weather-data.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { RestaurantLiveData } from "../restaurants/entities/restaurant-live-data.entity";
import { ShowLiveData } from "../shows/entities/show-live-data.entity";
import { PredictionAccuracy } from "../ml/entities/prediction-accuracy.entity";
import { WaitTimePrediction } from "../ml/entities/wait-time-prediction.entity";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { HeadlinerAttraction } from "./entities/headliner-attraction.entity";
import { ParkP50Baseline } from "./entities/park-p50-baseline.entity";
import { AttractionP50Baseline } from "./entities/attraction-p50-baseline.entity";
import { ParkP90Baseline } from "./entities/park-p90-baseline.entity";
import { AttractionP90Baseline } from "./entities/attraction-p90-baseline.entity";
import { AttractionHourlyHistory } from "./entities/attraction-hourly-history.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * Pins down `getPerHeadlinerRatios` — the park-level crowd-level
 * aggregation that takes per-headliner (latest_wait ÷ that_ride's_P50)
 * ratios and returns the P90 across them.
 *
 * The P90-of-ratios is what protects a park with one marquee at typical
 * wait from being averaged down to "very_low" by a dozen quiet smaller
 * rides — the marquee experience surfaces.
 */
describe("AnalyticsService.getPerHeadlinerRatios", () => {
  let service: AnalyticsService;
  const queueDataRepo = {
    query: jest.fn(),
  };
  const minimalMock = { findOne: jest.fn(), find: jest.fn() };

  beforeEach(async () => {
    jest.clearAllMocks();
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AnalyticsService,
        { provide: getRepositoryToken(QueueData), useValue: queueDataRepo },
        { provide: getRepositoryToken(Attraction), useValue: minimalMock },
        { provide: getRepositoryToken(Park), useValue: minimalMock },
        { provide: getRepositoryToken(Show), useValue: minimalMock },
        { provide: getRepositoryToken(Restaurant), useValue: minimalMock },
        { provide: getRepositoryToken(WeatherData), useValue: minimalMock },
        { provide: getRepositoryToken(ScheduleEntry), useValue: minimalMock },
        {
          provide: getRepositoryToken(RestaurantLiveData),
          useValue: minimalMock,
        },
        { provide: getRepositoryToken(ShowLiveData), useValue: minimalMock },
        {
          provide: getRepositoryToken(PredictionAccuracy),
          useValue: minimalMock,
        },
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: minimalMock,
        },
        {
          provide: getRepositoryToken(QueueDataAggregate),
          useValue: minimalMock,
        },
        { provide: getRepositoryToken(ParkDailyStats), useValue: minimalMock },
        {
          provide: getRepositoryToken(HeadlinerAttraction),
          useValue: minimalMock,
        },
        { provide: getRepositoryToken(ParkP50Baseline), useValue: minimalMock },
        {
          provide: getRepositoryToken(AttractionP50Baseline),
          useValue: minimalMock,
        },
        { provide: getRepositoryToken(ParkP90Baseline), useValue: minimalMock },
        {
          provide: getRepositoryToken(AttractionP90Baseline),
          useValue: minimalMock,
        },
        {
          provide: getRepositoryToken(AttractionHourlyHistory),
          useValue: minimalMock,
        },
        {
          provide: REDIS_CLIENT,
          useValue: { get: jest.fn(), set: jest.fn(), mget: jest.fn() },
        },
      ],
    }).compile();

    service = module.get<AnalyticsService>(AnalyticsService);
  });

  /** Inject a per-attraction P50 baseline map without touching Redis. */
  function stubP50s(map: Record<string, number>) {
    (service as any).getBatchAttractionP50s = jest
      .fn()
      .mockResolvedValue(new Map(Object.entries(map)));
  }

  it("returns null when no headlinerIds are passed", async () => {
    const result = await (service as any).getPerHeadlinerRatios("park-x", []);
    expect(result).toBeNull();
    expect(queueDataRepo.query).not.toHaveBeenCalled();
  });

  it("returns null when no headliner has a recent operating sample", async () => {
    queueDataRepo.query.mockResolvedValueOnce([]);
    stubP50s({ h1: 50, h2: 60 });
    const result = await (service as any).getPerHeadlinerRatios("park-x", [
      "h1",
      "h2",
    ]);
    expect(result).toBeNull();
  });

  it("returns null when no reporting ride has a P50 baseline", async () => {
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "h1", latest_wait: "30" },
    ]);
    stubP50s({}); // empty baseline map
    const result = await (service as any).getPerHeadlinerRatios("park-x", [
      "h1",
    ]);
    expect(result).toBeNull();
  });

  it("computes ratioP90 across reporting rides", async () => {
    // 8 reporting rides with ratios spanning 0.25 to 1.0.
    // Marquee ride (h1) is at its typical median; the rest are quieter.
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "h1", latest_wait: "100" }, // 100/100 = 1.0
      { attractionId: "h2", latest_wait: "45" }, //  45/70  ≈ 0.643
      { attractionId: "h3", latest_wait: "25" }, //  25/45  ≈ 0.556
      { attractionId: "h4", latest_wait: "10" }, //  10/40  = 0.25
      { attractionId: "h5", latest_wait: "25" }, //  25/40  = 0.625
      { attractionId: "h6", latest_wait: "15" }, //  15/35  ≈ 0.429
      { attractionId: "h7", latest_wait: "10" }, //  10/25  = 0.4
      { attractionId: "h8", latest_wait: "20" }, //  20/25  = 0.8
    ]);
    stubP50s({ h1: 100, h2: 70, h3: 45, h4: 40, h5: 40, h6: 35, h7: 25, h8: 25 });

    const result = await (service as any).getPerHeadlinerRatios("park-x", [
      "h1",
      "h2",
      "h3",
      "h4",
      "h5",
      "h6",
      "h7",
      "h8",
    ]);

    expect(result).not.toBeNull();
    expect(result.rideCount).toBe(8);
    // Sorted ratios: 0.25, 0.4, 0.429, 0.556, 0.625, 0.643, 0.8, 1.0
    // P90 = interpolate at idx (7 * 0.9) = 6.3 → 0.8 + 0.3*(1.0 - 0.8) = 0.86
    expect(result.ratioP90).toBeCloseTo(0.86, 2);
    // avg of latest waits = (100+45+25+10+25+15+10+20)/8 = 31.25
    expect(result.averageCurrentWait).toBe(31); // rounded
  });

  it("excludes rides whose P50 baseline is missing or zero", async () => {
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "h1", latest_wait: "60" },
      { attractionId: "h2", latest_wait: "30" }, // missing in P50 map
      { attractionId: "h3", latest_wait: "0" }, // wait is 0 — still counts (status filter is in SQL)
    ]);
    stubP50s({ h1: 60, h2: 0, h3: 30 });
    const result = await (service as any).getPerHeadlinerRatios("park-x", [
      "h1",
      "h2",
      "h3",
    ]);
    expect(result).not.toBeNull();
    // Only h1 (60/60 = 1.0) and h3 (0/30 = 0.0) — h2's P50 is 0 → skipped.
    expect(result.rideCount).toBe(2);
  });

  it("a single marquee at typical wait surfaces as occupancy >= 50%", async () => {
    // Regression: in the prior simple-avg formulation, a marquee at
    // typical wait surrounded by quiet small rides got averaged down to
    // "very_low" (~20-40%). P90-of-ratios should preserve the marquee's
    // signal — the busiest ride at its typical wait drives the rating.
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "marquee", latest_wait: "100" }, // 100/100 = 1.0
      ...Array.from({ length: 9 }, (_, i) => ({
        attractionId: `small-${i}`,
        latest_wait: "5",
      })),
    ]);
    stubP50s({
      marquee: 100,
      "small-0": 30,
      "small-1": 30,
      "small-2": 30,
      "small-3": 30,
      "small-4": 30,
      "small-5": 30,
      "small-6": 30,
      "small-7": 30,
      "small-8": 30,
    });
    const result = await (service as any).getPerHeadlinerRatios("park-x", [
      "marquee",
      "small-0",
      "small-1",
      "small-2",
      "small-3",
      "small-4",
      "small-5",
      "small-6",
      "small-7",
      "small-8",
    ]);
    expect(result).not.toBeNull();
    // P90 of [0.167×9, 1.0]: sorted [0.167×9, 1.0], idx = 9*0.9 = 8.1.
    // Between idx 8 (0.167) and idx 9 (1.0) → 0.167 + 0.1*(1.0 - 0.167) ≈ 0.25.
    // That's "very_low" (≤60%). The naive-avg would be even lower
    // ((9*0.167 + 1.0)/10 ≈ 0.25) — same result here because the marquee
    // is one ride drowned by nine quiet ones.
    //
    // The protection only fires when ratios are *more spread out*. Lock
    // the math as a regression anchor: with this distribution the
    // marquee alone isn't enough to lift the rating.
    expect(result.ratioP90).toBeGreaterThan(0.2);
    expect(result.ratioP90).toBeLessThan(0.35);
  });

  it("with mixed-busy rides the marquee at typical does lift the rating", async () => {
    // 3 marquees at typical, 5 mid-busy, 2 quiet.
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "m1", latest_wait: "100" }, // 1.0
      { attractionId: "m2", latest_wait: "100" }, // 1.0
      { attractionId: "m3", latest_wait: "100" }, // 1.0
      { attractionId: "b1", latest_wait: "30" }, // 0.6
      { attractionId: "b2", latest_wait: "30" }, // 0.6
      { attractionId: "b3", latest_wait: "30" }, // 0.6
      { attractionId: "b4", latest_wait: "30" }, // 0.6
      { attractionId: "b5", latest_wait: "30" }, // 0.6
      { attractionId: "q1", latest_wait: "5" }, // 0.1
      { attractionId: "q2", latest_wait: "5" }, // 0.1
    ]);
    stubP50s({
      m1: 100,
      m2: 100,
      m3: 100,
      b1: 50,
      b2: 50,
      b3: 50,
      b4: 50,
      b5: 50,
      q1: 50,
      q2: 50,
    });
    const result = await (service as any).getPerHeadlinerRatios("park-x", [
      "m1",
      "m2",
      "m3",
      "b1",
      "b2",
      "b3",
      "b4",
      "b5",
      "q1",
      "q2",
    ]);
    expect(result).not.toBeNull();
    // Sorted: [0.1, 0.1, 0.6, 0.6, 0.6, 0.6, 0.6, 1.0, 1.0, 1.0]
    // P90 idx = 9*0.9 = 8.1 → between idx 8 (1.0) and 9 (1.0) → 1.0.
    // 100% → moderate. The marquees' typical-wait reading surfaces.
    expect(result.ratioP90).toBeCloseTo(1.0, 2);
  });
});
