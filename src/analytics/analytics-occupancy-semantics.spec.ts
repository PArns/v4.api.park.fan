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
describe("AnalyticsService — peak-vs-peak occupancy semantics", () => {
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
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get<AnalyticsService>(AnalyticsService);
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
    queueDataRepo.query.mockResolvedValueOnce(
      opts.currentPeak == null
        ? []
        : [{ attractionId: "h1", peak_wait: String(opts.currentPeak) }],
    );
    queueDataRepo.query.mockResolvedValueOnce([]); // trend bucket 1
    queueDataRepo.query.mockResolvedValueOnce([]); // trend bucket 2
    queueDataRepo.query.mockResolvedValueOnce([
      { operating_count: String(opts.activeCount ?? 0) },
    ]);
  }

  describe("calculateParkOccupancy", () => {
    it("uses the P90 baseline when one exists (peak-vs-peak)", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      // P90 = 50. Current park peak = 50. Should read as 100% — typical day.
      redisStore.set(
        "park:p90:p1",
        JSON.stringify({ p90: 50, confidence: "high" }),
      );
      parkP90Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p90Baseline: 50,
        confidence: "high",
      });
      stubOccupancyQueries({ currentPeak: 50, activeCount: 5 });

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(100);
      expect(result.baseline90thPercentile).toBe(50);
      expect(result.confidence).toBe("high");
      expect(result.comparedToTypical).toBe(0);
    });

    it("falls back to P50 when P90 baseline row is missing", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      // P90 absent — fallback to P50 = 30. Current peak = 30 → 100%.
      // No `park:p90:p1` cache entry, and the DB returns null.
      parkP90Repo.findOne.mockResolvedValue(null);
      redisStore.set(
        "park:p50:p1",
        JSON.stringify({ p50: 30, confidence: "medium" }),
      );
      parkP50Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p50Baseline: 30,
        confidence: "medium",
      });
      stubOccupancyQueries({ currentPeak: 30, activeCount: 5 });

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(100);
      expect(result.baseline90thPercentile).toBe(30);
      // Fell back to the P50 confidence, not "low" default.
      expect(result.confidence).toBe("medium");
    });

    it("returns a low-confidence default when neither P90 nor P50 baseline exists", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      parkP90Repo.findOne.mockResolvedValue(null);
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
      // No baseline lookup is attempted on the no-data branch.
      expect(parkP90Repo.findOne).not.toHaveBeenCalled();
    });

    it("reads 'high' (130%+) when today's peak materially outruns the typical peak", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      parkP90Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p90Baseline: 50,
        confidence: "high",
      });
      // Current peak = 70, baseline = 50 → 140% → "high" band per
      // determineCrowdLevel (111-150%).
      stubOccupancyQueries({ currentPeak: 70, activeCount: 5 });

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(140);
      expect(result.comparisonStatus).toBe("higher");
      // Calling determineCrowdLevel on the same percentage maps to "high".
      expect(service.determineCrowdLevel(result.current)).toBe("high");
    });

    it("reads 'low' (<90%) when the current peak is well below typical", async () => {
      headlinerRepo.find.mockResolvedValue([{ attractionId: "h1" }]);
      parkP90Repo.findOne.mockResolvedValue({
        parkId: "p1",
        p90Baseline: 50,
        confidence: "high",
      });
      stubOccupancyQueries({ currentPeak: 30, activeCount: 5 });

      const result = await service.calculateParkOccupancy("p1");

      expect(result.current).toBe(60);
      expect(result.comparisonStatus).toBe("lower");
      // 60% → "very_low" by the threshold table (very_low: ≤ 60%).
      expect(service.determineCrowdLevel(result.current)).toBe("very_low");
    });
  });

  describe("getAttractionCrowdLevel (baseline-agnostic mapping)", () => {
    it("returns null for missing wait time (no current data)", () => {
      expect(service.getAttractionCrowdLevel(undefined, 60)).toBeNull();
      expect(service.getAttractionCrowdLevel(0, 60)).toBeNull();
    });

    it("returns null when no baseline is available — caller decides the fallback", () => {
      expect(service.getAttractionCrowdLevel(40, 0)).toBeNull();
      expect(service.getAttractionCrowdLevel(40, undefined)).toBeNull();
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
