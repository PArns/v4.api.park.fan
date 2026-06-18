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
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * Coverage for the typical-day-peak baseline (the calendar's crowd-level
 * reference: a day's peak ÷ a typical day's peak). See
 * docs/analytics/crowd-level-typical-day-peak.md.
 */
describe("AnalyticsService — typical-day-peak baseline", () => {
  let service: AnalyticsService;

  const redisStore = new Map<string, string>();
  const redis = {
    get: jest.fn((key: string) => Promise.resolve(redisStore.get(key) ?? null)),
    set: jest.fn((key: string, val: string) => {
      redisStore.set(key, val);
      return Promise.resolve("OK");
    }),
    mget: jest.fn((...keys: string[]) =>
      Promise.resolve(keys.map((k) => redisStore.get(k) ?? null)),
    ),
  };

  const queueDataRepo = { query: jest.fn() };
  const parkRepo = { findOne: jest.fn() };
  const parkP50Repo = { findOne: jest.fn() };
  const minimalMock = { findOne: jest.fn(), find: jest.fn(), save: jest.fn() };

  beforeEach(async () => {
    redisStore.clear();
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AnalyticsService,
        { provide: getRepositoryToken(QueueData), useValue: queueDataRepo },
        { provide: getRepositoryToken(Attraction), useValue: minimalMock },
        { provide: getRepositoryToken(Park), useValue: parkRepo },
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
        { provide: getRepositoryToken(ParkP50Baseline), useValue: parkP50Repo },
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
          provide: getRepositoryToken(AttractionRopeDrop),
          useValue: minimalMock,
        },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get<AnalyticsService>(AnalyticsService);
  });

  describe("calculateTypicalDayPeak", () => {
    it("returns 0 (and runs no query) when there are no headliners", async () => {
      const result = await service.calculateTypicalDayPeak("p1", []);
      expect(result).toEqual({ typicalDayPeak: 0, operatingDays: 0 });
      expect(queueDataRepo.query).not.toHaveBeenCalled();
    });

    it("returns the median of daily peaks + operating-day count from the SQL, parsed as numbers", async () => {
      parkRepo.findOne.mockResolvedValueOnce({ timezone: "Europe/Berlin" });
      queueDataRepo.query.mockResolvedValueOnce([
        { typical_day_peak: "40.30", operating_days: 92 },
      ]);

      const result = await service.calculateTypicalDayPeak("p1", ["a1", "a2"]);

      expect(result).toEqual({ typicalDayPeak: 40.3, operatingDays: 92 });
      expect(queueDataRepo.query).toHaveBeenCalledTimes(1);
      // headliner ids + park timezone are threaded into the query params.
      const params = queueDataRepo.query.mock.calls[0][1];
      expect(params[0]).toEqual(["a1", "a2"]);
      expect(params[1]).toBe("Europe/Berlin");
    });

    it("returns 0 when the park has no qualifying daily data", async () => {
      parkRepo.findOne.mockResolvedValueOnce({ timezone: "UTC" });
      queueDataRepo.query.mockResolvedValueOnce([
        { typical_day_peak: null, operating_days: 0 },
      ]);

      const result = await service.calculateTypicalDayPeak("p1", ["a1"]);
      expect(result).toEqual({ typicalDayPeak: 0, operatingDays: 0 });
    });
  });

  describe("getTypicalDayPeakFromCache", () => {
    it("returns the Redis value when warm — no DB hit", async () => {
      redisStore.set("park:typicalpeak:p1", "40.3");

      const value = await service.getTypicalDayPeakFromCache("p1");

      expect(value).toBe(40.3);
      expect(parkP50Repo.findOne).not.toHaveBeenCalled();
    });

    it("falls back to the park_p50_baselines column and warms the cache", async () => {
      parkP50Repo.findOne.mockResolvedValueOnce({ typicalDayPeak: "38.5" });

      const value = await service.getTypicalDayPeakFromCache("p1");

      expect(value).toBe(38.5);
      expect(redisStore.get("park:typicalpeak:p1")).toBe("38.5");
    });

    it("returns 0 when neither Redis nor the column has a value", async () => {
      parkP50Repo.findOne.mockResolvedValueOnce(null);
      expect(await service.getTypicalDayPeakFromCache("missing")).toBe(0);
    });

    it("returns 0 when the column is NULL (brand-new park, pre-cron)", async () => {
      parkP50Repo.findOne.mockResolvedValueOnce({ typicalDayPeak: null });
      const value = await service.getTypicalDayPeakFromCache("p1");
      expect(value).toBe(0);
      // A NULL column must not be written back to Redis as a poisoned value.
      expect(redisStore.has("park:typicalpeak:p1")).toBe(false);
    });
  });

  describe("calculateP50Baseline — thin-data gate (≥ 30 operating days)", () => {
    const headliners = [
      {
        attractionId: "a1",
        p50Wait548d: 30,
        p90Wait548d: 50,
        sampleCount: 100,
        operatingDays: 40,
        tier: "tier1",
      },
    ] as unknown as Parameters<typeof service.calculateP50Baseline>[1];

    it("forces typicalDayPeak to 0 (→ NULL column) when < 30 operating days feed the median", async () => {
      parkRepo.findOne.mockResolvedValue({ timezone: "UTC" });
      // Real median exists, but only over 12 operating days → noise → gated.
      queueDataRepo.query.mockResolvedValueOnce([
        { typical_day_peak: "45.00", operating_days: 12 },
      ]);

      const result = await service.calculateP50Baseline("p1", headliners);

      expect(result.typicalDayPeak).toBe(0);
      // P50/P90 stay computed — they still drive the live ratio + ML feature.
      expect(result.p50).toBe(30);
      expect(result.p90).toBe(50);
    });

    it("keeps the typicalDayPeak when ≥ 30 operating days support the median", async () => {
      parkRepo.findOne.mockResolvedValue({ timezone: "UTC" });
      queueDataRepo.query.mockResolvedValueOnce([
        { typical_day_peak: "45.00", operating_days: 60 },
      ]);

      const result = await service.calculateP50Baseline("p1", headliners);

      expect(result.typicalDayPeak).toBe(45);
    });
  });

  describe("isParkRatable", () => {
    it("is true when a typical-day-peak baseline exists (≥ 30 operating days)", async () => {
      redisStore.set("park:typicalpeak:p1", "40.3");
      expect(await service.isParkRatable("p1")).toBe(true);
    });

    it("is false when there is no baseline (thin/brand-new park)", async () => {
      parkP50Repo.findOne.mockResolvedValueOnce(null);
      expect(await service.isParkRatable("p1")).toBe(false);
    });
  });
});
