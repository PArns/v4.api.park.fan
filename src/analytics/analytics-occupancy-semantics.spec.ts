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
 * Regression coverage for the peak-vs-peak crowd reading.
 *
 * These tests pin the math down so a future refactor can't silently
 * slide back to P50-vs-P50 or — worse — a mixed-percentile reading like
 * the calendar `peakLoad` bug (P90 numerator ÷ P50 denominator).
 *
 *   - Park occupancy: currentPeak ÷ P90 baseline.
 *   - P90 missing → P50 fallback chain (still apples-to-apples).
 *   - getAttractionCrowdLevel is baseline-agnostic; it just computes
 *     (wait ÷ baseline) × 100 → CrowdLevel via thresholds.
 *   - Thresholds: 100% = moderate (not "high" — sanity check on the
 *     ±10% moderate band).
 */
describe("AnalyticsService — peak-vs-median occupancy semantics", () => {
  let service: AnalyticsService;

  const redisStore = new Map<string, string>();
  const redis = {
    get: jest.fn((k: string) => Promise.resolve(redisStore.get(k) ?? null)),
    set: jest.fn((k: string, v: string) => {
      redisStore.set(k, v);
      return Promise.resolve("OK");
    }),
    mget: jest.fn((...keys: string[]) =>
      Promise.resolve(keys.map((k) => redisStore.get(k) ?? null)),
    ),
  };

  const queueDataRepo = {
    query: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      select: jest.fn().mockReturnThis(),
      addSelect: jest.fn().mockReturnThis(),
      innerJoin: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      groupBy: jest.fn().mockReturnThis(),
      getRawOne: jest.fn().mockResolvedValue({ count: "5" }),
      getRawMany: jest.fn().mockResolvedValue([]),
    })),
  };

  const headlinerRepo = {
    find: jest.fn(),
  };

  const parkP50Repo = { findOne: jest.fn() };
  const parkP90Repo = { findOne: jest.fn() };
  const attractionRepo = { findOne: jest.fn(), find: jest.fn() };
  const minimalMock = { findOne: jest.fn(), find: jest.fn() };

  beforeEach(async () => {
    redisStore.clear();
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AnalyticsService,
        { provide: getRepositoryToken(QueueData), useValue: queueDataRepo },
        { provide: getRepositoryToken(Attraction), useValue: attractionRepo },
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
          useValue: headlinerRepo,
        },
        { provide: getRepositoryToken(ParkP50Baseline), useValue: parkP50Repo },
        {
          provide: getRepositoryToken(AttractionP50Baseline),
          useValue: minimalMock,
        },
        { provide: getRepositoryToken(ParkP90Baseline), useValue: parkP90Repo },
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
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get<AnalyticsService>(AnalyticsService);
    // Force the legacy park-wide path. The headliner-load path requires
    // attraction-level P50 baselines and is exercised by dedicated tests.
    (service as any).getHeadlinerLoad = jest.fn().mockResolvedValue(null);
  });

  /**
   * Helper: queue up the four query() responses calculateParkOccupancy
   * makes in order — current peak, trend bucket 1, trend bucket 2,
   * active-attractions count.
   */
  function stubOccupancyQueries(opts: {
    currentPeak: number | null;
    activeCount?: number;
  }) {
    queueDataRepo.query.mockReset();
    if (opts.currentPeak == null) {
      // No data at any threshold/window expansion — the peak fn recurses
      // through minWaitTime fallback and window expansion (20 → 60 → 240)
      // so feed it empty arrays for every retry.
      queueDataRepo.query
        .mockResolvedValueOnce([]) // window 20, minWait=10
        .mockResolvedValueOnce([]) // window 20, minWait=0
        .mockResolvedValueOnce([]) // window 60, minWait=0
        .mockResolvedValueOnce([]); // window 240, minWait=0
    } else {
      queueDataRepo.query.mockResolvedValueOnce([
        { attractionId: "h1", latest_wait: String(opts.currentPeak) },
      ]);
    }
    queueDataRepo.query.mockResolvedValueOnce([]); // trend bucket 1
    queueDataRepo.query.mockResolvedValueOnce([]); // trend bucket 2
    queueDataRepo.query.mockResolvedValueOnce([
      { operating_count: String(opts.activeCount ?? 0) },
    ]);
  }

  describe("calculateParkOccupancy", () => {
    it("uses the P50 baseline when one exists (peak-vs-median)", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      // P50 = 50. Current park peak = 50. Should read as 100% — typical day.
      redisStore.set(
        "park:p50:p1",
        JSON.stringify({ p50: 50, confidence: "high" }),
      );
      parkP50Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p50Baseline: 50,
        confidence: "high",
      });
      stubOccupancyQueries({ currentPeak: 50, activeCount: 5 });

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(100);
      expect(result.baseline90thPercentile).toBe(50);
      expect(result.confidence).toBe("high");
      expect(result.comparedToTypical).toBe(0);
    });

    it("returns a low-confidence default when no P50 baseline exists", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      // No P50 cache entry and DB returns null → no baseline, low confidence.
      // (P50 and P90 are written atomically by the daily cron; a missing
      // row means the park is brand-new and both percentiles are absent.)
      parkP50Repo.findOne.mockResolvedValue(null);
      stubOccupancyQueries({ currentPeak: 40, activeCount: 5 });

      const result = await service.calculateParkOccupancy("p1");

      // Spec: degrade to 50% / low confidence rather than running a
      // 548-day PERCENTILE_CONT on the hot path.
      expect(result.current).toBe(50);
      expect(result.confidence).toBe("low");
      expect(result.baseline90thPercentile).toBe(0);
    });

    it("returns zero occupancy when no live peak data is available yet", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      stubOccupancyQueries({ currentPeak: null });

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(0);
      expect(result.trend).toBe("stable");
      // No live sample at all is the definition of "nothing to rate", so the
      // branch must carry the rating itself. Leaving crowdLevel undefined let
      // getParkStatistics fall through to determineCrowdLevel(0) and publish
      // a fabricated "very_low" for a park we had heard nothing from.
      expect(result.crowdLevel).toBe("unknown");
      // No baseline lookup is attempted on the no-data branch.
      expect(parkP50Repo.findOne).not.toHaveBeenCalled();
    });

    it("reads 'high' (130%+) when today's peak materially outruns the typical wait", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      parkP50Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p50Baseline: 50,
        confidence: "high",
      });
      redisStore.set(
        "park:p50:p1",
        JSON.stringify({ p50: 50, confidence: "high" }),
      );
      // Current peak = 70, baseline = 50 → 140% → "high" band per
      // determineCrowdLevel (111-150%).
      stubOccupancyQueries({ currentPeak: 70, activeCount: 5 });

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(140);
      expect(result.comparisonStatus).toBe("higher");
      // Calling determineCrowdLevel on the same percentage maps to "high".
      expect(service.determineCrowdLevel(result.current)).toBe("high");
    });

    it("keeps comparedToTypical on the same basis as current in the headliner-load path", async () => {
      // Regression: `current` and `comparedToTypical` used to be computed
      // from different denominators, producing contradictory pairs like
      // current 204 % / comparedToTypical 42 %. They must now always
      // satisfy comparedToTypical === current − 100.
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      parkP50Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p50Baseline: 50,
        confidence: "high",
      });
      redisStore.set(
        "park:p50:p1",
        JSON.stringify({ p50: 50, confidence: "high" }),
      );
      // Force the headliner-load path: loadRatio 2.04 → current 204 %.
      (service as any).getHeadlinerLoad = jest.fn().mockResolvedValue({
        loadRatio: 2.04,
        averageCurrentWait: 102,
        averageTypicalWait: 50,
        rideCount: 3,
      });
      // Trend buckets + active-attractions count (headliner path makes one
      // trend query, then getActiveAttractionsCount via the query builder).
      queueDataRepo.query.mockReset();
      queueDataRepo.query.mockResolvedValue([]);

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(204);
      expect(result.comparedToTypical).toBe(result.current - 100);
      expect(result.comparedToTypical).toBe(104);
      expect(result.comparisonStatus).toBe("higher");
    });

    it("returns a breakdown that divides out to `current`", async () => {
      // The park page renders currentAvgWait next to typicalAvgWait and the
      // percentage side by side; they must be the same measurement. Before,
      // typicalAvgWait was the park-wide P50 while `current` came from the
      // per-ride aggregate, so the page could show "25 min now / 30 min
      // typical" beside "+23 % busier than typical".
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      parkP50Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p50Baseline: 50,
        confidence: "high",
      });
      redisStore.set(
        "park:p50:p1",
        JSON.stringify({ p50: 50, confidence: "high" }),
      );
      // 240 min of queue across 9 rides whose baselines total 290.
      (service as any).getHeadlinerLoad = jest.fn().mockResolvedValue({
        loadRatio: 240 / 290,
        averageCurrentWait: Math.round(240 / 9),
        averageTypicalWait: 290 / 9,
        rideCount: 9,
      });
      queueDataRepo.query.mockReset();
      queueDataRepo.query.mockResolvedValue([]);

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(83);
      expect(service.determineCrowdLevel(result.current)).toBe("low");
      // Both rounded to the nearest 5: 26.7 → 25, 32.2 → 30. The displayed
      // pair points the same way as the rating (below typical), unlike the
      // old park-wide denominator.
      expect(result.breakdown?.currentAvgWait).toBe(25);
      expect(result.breakdown?.typicalAvgWait).toBe(30);
      expect(result.comparisonStatus).toBe("lower");
    });

    it("reads 'very_low' (<=60%) when the current peak is well below typical", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      parkP50Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p50Baseline: 50,
        confidence: "high",
      });
      redisStore.set(
        "park:p50:p1",
        JSON.stringify({ p50: 50, confidence: "high" }),
      );
      stubOccupancyQueries({ currentPeak: 30, activeCount: 5 });

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(60);
      expect(result.comparisonStatus).toBe("lower");
      expect(service.determineCrowdLevel(result.current)).toBe("very_low");
    });
  });

  describe("averageTodayAcrossRides (today's avg/peak pair)", () => {
    const call = (rows: unknown[]) =>
      (service as any).averageTodayAcrossRides(rows);

    it("averages both figures over the same rides", () => {
      // Per-ride averages 30/20/10 → 20; per-ride maxima 60/40/20 → 40.
      const result = call([
        { avg_wait: "30", max_wait: "60" },
        { avg_wait: "20", max_wait: "40" },
        { avg_wait: "10", max_wait: "20" },
      ]);
      expect(result).toEqual({ avgWaitToday: 20, peakWaitToday: 40 });
    });

    it("never reports an average above the peak", () => {
      // Regression: avgWaitToday was a pooled P90 over all attractions and
      // peakWaitToday the mean of per-headliner maxima, so the park page
      // could render "Ø 45 min" next to "Peak 40 min". Per ride AVG ≤ MAX,
      // so the pair is now ordered by construction.
      const rows = [
        { avg_wait: "44", max_wait: "45" },
        { avg_wait: "5", max_wait: "90" },
        { avg_wait: "38", max_wait: "38" },
      ];
      const result = call(rows)!;
      expect(result.avgWaitToday).toBeLessThanOrEqual(result.peakWaitToday);
    });

    it("returns null when no ride reported, leaving the caller's fallback", () => {
      expect(call([])).toBeNull();
    });
  });

  describe("getLoadRating (no invented ratings)", () => {
    it("returns 'unknown' when there is no baseline to rate against", () => {
      // Previously "moderate" — indistinguishable, to a reader, from a
      // measured typical day. Same rule rateOrUnknown applies elsewhere.
      expect(service.getLoadRating(40, 0).rating).toBe("unknown");
      expect(service.getLoadRating(40, -1).rating).toBe("unknown");
      expect(service.getLoadRating(40, NaN).rating).toBe("unknown");
    });

    it("rates a walk-on as very_low rather than moderate", () => {
      // A 0-minute wait against a real baseline is data, not missing data.
      expect(service.getLoadRating(0, 50).rating).toBe("very_low");
    });

    it("maps the ratio onto the shared threshold ladder", () => {
      expect(service.getLoadRating(50, 50).rating).toBe("moderate");
      expect(service.getLoadRating(70, 50).rating).toBe("high");
      expect(service.getLoadRating(40, 50).rating).toBe("low");
    });
  });

  describe("getAttractionCrowdLevel (baseline-agnostic mapping)", () => {
    it("returns null for missing wait time (no current data)", () => {
      expect(service.getAttractionCrowdLevel(undefined, 60)).toBeNull();
      expect(
        service.getAttractionCrowdLevel(null as unknown as number, 60),
      ).toBeNull();
    });

    it("rates a walk-on against a real baseline instead of calling it unknown", () => {
      // 0 min with a REAL baseline is a measurement, not missing data — the
      // same rule getLoadRating follows. Treating it as "no data" made the
      // attraction payload contradict itself: crowdLevel "unknown" beside a
      // comparison of "much_lower" derived from the very same pair.
      expect(service.getAttractionCrowdLevel(0, 60)).toBe("very_low");
    });

    it("returns null when no baseline is available — caller decides the fallback", () => {
      expect(service.getAttractionCrowdLevel(40, 0)).toBeNull();
      expect(service.getAttractionCrowdLevel(40, undefined)).toBeNull();
      expect(service.getAttractionCrowdLevel(0, 0)).toBeNull();
    });

    it("maps the peak-vs-peak ratio to the right CrowdLevel bucket", () => {
      // Baseline = 50 (typical peak)
      expect(service.getAttractionCrowdLevel(50, 50)).toBe("moderate");
      expect(service.getAttractionCrowdLevel(30, 50)).toBe("very_low"); // 60%
      expect(service.getAttractionCrowdLevel(40, 50)).toBe("low"); // 80%
      expect(service.getAttractionCrowdLevel(70, 50)).toBe("high"); // 140%
      expect(service.getAttractionCrowdLevel(85, 50)).toBe("very_high"); // 170%
      expect(service.getAttractionCrowdLevel(120, 50)).toBe("extreme"); // 240%
    });
  });

  describe("determineCrowdLevel (threshold ladder)", () => {
    it("locks the threshold boundaries — a slide here is a breaking API change", () => {
      // Edges first
      expect(service.determineCrowdLevel(60)).toBe("very_low");
      expect(service.determineCrowdLevel(61)).toBe("low");
      expect(service.determineCrowdLevel(89)).toBe("low");
      expect(service.determineCrowdLevel(90)).toBe("moderate");
      expect(service.determineCrowdLevel(110)).toBe("moderate");
      expect(service.determineCrowdLevel(111)).toBe("high");
      expect(service.determineCrowdLevel(150)).toBe("high");
      expect(service.determineCrowdLevel(151)).toBe("very_high");
      expect(service.determineCrowdLevel(200)).toBe("very_high");
      expect(service.determineCrowdLevel(201)).toBe("extreme");
    });

    it("100% reads as 'moderate' — the ±10% band around typical", () => {
      // This is the entire point of peak-vs-peak — 100% must read
      // "typical day's peak", not "high" or "low".
      expect(service.determineCrowdLevel(100)).toBe("moderate");
    });
  });
});
