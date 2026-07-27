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
import { AttractionRopeDrop } from "./entities/attraction-rope-drop.entity";
import { AttractionTypicalWaits } from "./entities/attraction-typical-waits.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * Pins down `getHeadlinerLoad` — the park-level crowd-level aggregation:
 * Σ(current headliner waits) ÷ Σ(those rides' P50 baselines).
 *
 * A baseline-weighted mean, deliberately not a percentile over per-ride
 * ratios. The percentile version was an extreme-value estimator: with the
 * headliner set capped at 10 its P90 index landed on the second-busiest
 * ride, so one mid-size ride above its own median rated the whole park
 * "high" while the marquees sat at half their typical wait.
 */
describe("AnalyticsService.getHeadlinerLoad", () => {
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
          provide: getRepositoryToken(AttractionTypicalWaits),
          useValue: {
            find: jest.fn().mockResolvedValue([]),
            count: jest.fn().mockResolvedValue(0),
            upsert: jest.fn(),
            delete: jest.fn(),
          },
        },
        {
          provide: getRepositoryToken(AttractionRopeDrop),
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
    const result = await (service as any).getHeadlinerLoad("park-x", []);
    expect(result).toBeNull();
    expect(queueDataRepo.query).not.toHaveBeenCalled();
  });

  it("returns null when no headliner has a recent operating sample", async () => {
    queueDataRepo.query.mockResolvedValueOnce([]);
    stubP50s({ h1: 50, h2: 60 });
    const result = await (service as any).getHeadlinerLoad("park-x", [
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
    const result = await (service as any).getHeadlinerLoad("park-x", ["h1"]);
    expect(result).toBeNull();
  });

  it("computes Σ waits ÷ Σ baselines across reporting rides", async () => {
    // 8 reporting rides. Marquee (h1) is at its typical median; the rest
    // are quieter, so the park as a whole is below typical.
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "h1", latest_wait: "100" },
      { attractionId: "h2", latest_wait: "45" },
      { attractionId: "h3", latest_wait: "25" },
      { attractionId: "h4", latest_wait: "10" },
      { attractionId: "h5", latest_wait: "25" },
      { attractionId: "h6", latest_wait: "15" },
      { attractionId: "h7", latest_wait: "10" },
      { attractionId: "h8", latest_wait: "20" },
    ]);
    stubP50s({
      h1: 100,
      h2: 70,
      h3: 45,
      h4: 40,
      h5: 40,
      h6: 35,
      h7: 25,
      h8: 25,
    });

    const result = await (service as any).getHeadlinerLoad("park-x", [
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
    // Σ waits = 250, Σ P50 = 380 → 0.658 (≈ 66 % → "low").
    expect(result.loadRatio).toBeCloseTo(250 / 380, 4);
    // avg of latest waits = 250/8 = 31.25
    expect(result.averageCurrentWait).toBe(31); // rounded
    // The matched baseline: 380/8 = 47.5. currentAvg/typicalAvg must
    // reproduce loadRatio so the API's breakdown can't contradict it.
    expect(result.averageTypicalWait).toBeCloseTo(47.5, 4);
    expect(250 / 8 / result.averageTypicalWait).toBeCloseTo(
      result.loadRatio,
      6,
    );
  });

  it("excludes rides whose P50 baseline is missing or zero", async () => {
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "h1", latest_wait: "60" },
      { attractionId: "h2", latest_wait: "30" }, // missing in P50 map
      { attractionId: "h3", latest_wait: "0" }, // wait is 0 — still counts (status filter is in SQL)
    ]);
    stubP50s({ h1: 60, h2: 0, h3: 30 });
    const result = await (service as any).getHeadlinerLoad("park-x", [
      "h1",
      "h2",
      "h3",
    ]);
    expect(result).not.toBeNull();
    // Only h1 (60/60 = 1.0) and h3 (0/30 = 0.0) — h2's P50 is 0 → skipped.
    expect(result.rideCount).toBe(2);
  });

  it("weights the marquee by its baseline instead of averaging ratios", async () => {
    // One marquee at its typical wait plus nine small rides at a sixth of
    // theirs. A plain mean of per-ride ratios reads 0.25 — the marquee is
    // one vote among ten. Weighting by baseline gives it the 100 minutes
    // of queue it actually represents: (100 + 9*5) / (100 + 9*30) = 0.39.
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "marquee", latest_wait: "100" },
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
    const result = await (service as any).getHeadlinerLoad("park-x", [
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
    expect(result.loadRatio).toBeCloseTo(145 / 370, 4);
    // Still "very_low" — nine rides at a sixth of typical IS a quiet park —
    // but measurably above the unweighted 0.25.
    expect(result.loadRatio).toBeGreaterThan(0.25);
  });

  it("does not let one ride above its own median rate the whole park high", async () => {
    // The Phantasialand case. Nine headliners: the two marquees sit at
    // under half their typical wait, while two mid-size rides run above
    // their own (much smaller) medians. The old P90-across-ratios landed
    // on the second-highest ratio (1.33 → "high"); the weighted mean sees
    // a park at 83 % of typical → "low".
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "taron", latest_wait: "20" }, // 20/45 = 0.44
      { attractionId: "fly", latest_wait: "20" }, // 20/40 = 0.50
      { attractionId: "mamba", latest_wait: "15" }, // 15/30 = 0.50
      { attractionId: "riverquest", latest_wait: "15" }, // 15/30 = 0.50
      { attractionId: "colorado", latest_wait: "15" }, // 15/25 = 0.60
      { attractionId: "winja-fear", latest_wait: "35" }, // 35/30 = 1.17
      { attractionId: "winja-force", latest_wait: "35" }, // 35/30 = 1.17
      { attractionId: "wakobato", latest_wait: "40" }, // 40/30 = 1.33
      { attractionId: "crazybats", latest_wait: "45" }, // 45/30 = 1.50
    ]);
    stubP50s({
      taron: 45,
      fly: 40,
      mamba: 30,
      riverquest: 30,
      colorado: 25,
      "winja-fear": 30,
      "winja-force": 30,
      wakobato: 30,
      crazybats: 30,
    });
    const result = await (service as any).getHeadlinerLoad("park-x", [
      "taron",
      "fly",
      "mamba",
      "riverquest",
      "colorado",
      "winja-fear",
      "winja-force",
      "wakobato",
      "crazybats",
    ]);
    expect(result).not.toBeNull();
    // Σ waits = 240, Σ P50 = 290 → 0.828.
    expect(result.loadRatio).toBeCloseTo(240 / 290, 4);
    expect(service.determineCrowdLevel(result.loadRatio * 100)).toBe("low");
  });

  it("keeps a genuinely busy park high", async () => {
    // Same park, a real peak day: every headliner above its median.
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "taron", latest_wait: "90" },
      { attractionId: "fly", latest_wait: "80" },
      { attractionId: "mamba", latest_wait: "45" },
      { attractionId: "crazybats", latest_wait: "40" },
    ]);
    stubP50s({ taron: 45, fly: 40, mamba: 30, crazybats: 30 });
    const result = await (service as any).getHeadlinerLoad("park-x", [
      "taron",
      "fly",
      "mamba",
      "crazybats",
    ]);
    expect(result).not.toBeNull();
    // Σ waits = 255, Σ P50 = 145 → 1.76 → "very_high".
    expect(result.loadRatio).toBeCloseTo(255 / 145, 4);
    expect(service.determineCrowdLevel(result.loadRatio * 100)).toBe(
      "very_high",
    );
  });

  it("counts quiet headliners instead of filtering them out", async () => {
    // The 10-minute MIN_WAIT_TIME_THRESHOLD must not apply here: dropping
    // sub-10-minute rides would delete exactly the queues that make a day
    // quiet and bias the park upward. The SQL carries no wait floor, and a
    // 5-minute reading pulls the ratio down as it should.
    queueDataRepo.query.mockResolvedValueOnce([
      { attractionId: "h1", latest_wait: "5" },
      { attractionId: "h2", latest_wait: "5" },
    ]);
    stubP50s({ h1: 40, h2: 30 });
    const result = await (service as any).getHeadlinerLoad("park-x", [
      "h1",
      "h2",
    ]);
    expect(result).not.toBeNull();
    expect(result.rideCount).toBe(2);
    expect(result.loadRatio).toBeCloseTo(10 / 70, 4);
    // The query must not carry a minimum-wait parameter.
    const [, params] = queueDataRepo.query.mock.calls[0];
    expect(params).toHaveLength(2);
  });
});
