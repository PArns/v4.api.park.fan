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
 * Focused coverage for the P90 baseline + hourly-history infrastructure
 * introduced in PR #46. The existing analytics.service.spec covers the
 * crowd-level math; this file targets the read/write helpers and the
 * fallback chain so regressions in either direction surface quickly.
 */
describe("AnalyticsService — P90 + hourly history", () => {
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
    del: jest.fn((key: string) => {
      redisStore.delete(key);
      return Promise.resolve(1);
    }),
    pipeline: jest.fn(() => {
      const ops: Array<[string, string]> = [];
      return {
        set: jest.fn(function (this: { set: jest.Mock }, k: string, v: string) {
          ops.push([k, v]);
          return this;
        }),
        exec: jest.fn(async () => {
          for (const [k, v] of ops) redisStore.set(k, v);
          return [];
        }),
      };
    }),
  };

  const queueDataRepo = {
    query: jest.fn(),
    createQueryBuilder: jest.fn(),
  };

  const parkP90Repo = {
    findOne: jest.fn(),
    upsert: jest.fn().mockResolvedValue({ identifiers: [] }),
  };
  const attractionP50Repo = {
    findOne: jest.fn(),
    save: jest.fn(),
    upsert: jest.fn().mockResolvedValue({ identifiers: [] }),
  };
  const attractionP90Repo = {
    findOne: jest.fn(),
    find: jest.fn().mockResolvedValue([]),
    save: jest.fn(),
    upsert: jest.fn().mockResolvedValue({ identifiers: [] }),
  };
  const headlinerRepo = {
    find: jest.fn().mockResolvedValue([]),
  };
  const hourlyRepo = {
    upsert: jest.fn().mockResolvedValue({ identifiers: [] }),
    createQueryBuilder: jest.fn(),
  };

  const minimalMock = { findOne: jest.fn(), find: jest.fn(), save: jest.fn() };

  beforeEach(async () => {
    redisStore.clear();
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
          useValue: headlinerRepo,
        },
        {
          provide: getRepositoryToken(ParkP50Baseline),
          useValue: minimalMock,
        },
        {
          provide: getRepositoryToken(AttractionP50Baseline),
          useValue: attractionP50Repo,
        },
        { provide: getRepositoryToken(ParkP90Baseline), useValue: parkP90Repo },
        {
          provide: getRepositoryToken(AttractionP90Baseline),
          useValue: attractionP90Repo,
        },
        {
          provide: getRepositoryToken(AttractionHourlyHistory),
          useValue: hourlyRepo,
        },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get<AnalyticsService>(AnalyticsService);
  });

  describe("getP90BaselineFromCache", () => {
    it("returns the P90 value from Redis when present (JSON shape)", async () => {
      redisStore.set(
        "park:p90:p1",
        JSON.stringify({ p90: 50, confidence: "high" }),
      );

      const value = await service.getP90BaselineFromCache("p1");

      expect(value).toBe(50);
      expect(parkP90Repo.findOne).not.toHaveBeenCalled();
    });

    it("falls back to the DB and warms the cache when Redis is empty", async () => {
      parkP90Repo.findOne.mockResolvedValueOnce({
        parkId: "p1",
        p90Baseline: 42,
        confidence: "medium",
      });

      const value = await service.getP90BaselineFromCache("p1");

      expect(value).toBe(42);
      expect(parkP90Repo.findOne).toHaveBeenCalledWith({
        where: { parkId: "p1" },
      });
      expect(redis.set).toHaveBeenCalled();
      expect(redisStore.get("park:p90:p1")).toContain("42");
    });

    it("returns 0 when no row exists — caller must use the P50 fallback", async () => {
      parkP90Repo.findOne.mockResolvedValueOnce(null);

      const value = await service.getP90BaselineFromCache("missing");

      expect(value).toBe(0);
    });
  });

  describe("getAttractionP90BaselineFromCache", () => {
    it("returns Redis value when warm", async () => {
      redisStore.set("attraction:p90:a1", "75");

      const v = await service.getAttractionP90BaselineFromCache("a1");

      expect(v).toBe(75);
      expect(attractionP90Repo.findOne).not.toHaveBeenCalled();
    });

    it("hydrates the cache from the DB when cold", async () => {
      attractionP90Repo.findOne.mockResolvedValueOnce({
        attractionId: "a1",
        p90Baseline: 60,
        confidence: "high",
      });

      const v = await service.getAttractionP90BaselineFromCache("a1");

      expect(v).toBe(60);
      expect(redisStore.get("attraction:p90:a1")).toBe("60");
    });

    it("returns 0 when neither cache nor DB has the row", async () => {
      attractionP90Repo.findOne.mockResolvedValueOnce(null);
      const v = await service.getAttractionP90BaselineFromCache("unknown");
      expect(v).toBe(0);
    });
  });

  describe("getBatchAttractionP90Baselines", () => {
    it("returns empty map for empty input without hitting Redis or DB", async () => {
      const result = await service.getBatchAttractionP90Baselines([]);
      expect(result.size).toBe(0);
      expect(redis.mget).not.toHaveBeenCalled();
      expect(attractionP90Repo.find).not.toHaveBeenCalled();
    });

    it("serves all ids from Redis when fully warm — no DB hit", async () => {
      redisStore.set("attraction:p90:a1", "30");
      redisStore.set("attraction:p90:a2", "60");

      const result = await service.getBatchAttractionP90Baselines(["a1", "a2"]);

      expect(result.get("a1")).toBe(30);
      expect(result.get("a2")).toBe(60);
      expect(attractionP90Repo.find).not.toHaveBeenCalled();
    });

    it("hydrates missing ids from the DB and pipelines them back into Redis", async () => {
      redisStore.set("attraction:p90:a1", "30");
      attractionP90Repo.find.mockResolvedValueOnce([
        { attractionId: "a2", p90Baseline: 60 },
        { attractionId: "a3", p90Baseline: 0 }, // sentinel — should be skipped
      ]);

      const result = await service.getBatchAttractionP90Baselines([
        "a1",
        "a2",
        "a3",
      ]);

      expect(result.get("a1")).toBe(30);
      expect(result.get("a2")).toBe(60);
      // a3 has a 0 baseline — caller treats absence as "no peak baseline yet"
      expect(result.has("a3")).toBe(false);
      // a2 should be in Redis now (write-back)
      expect(redisStore.get("attraction:p90:a2")).toBe("60");
      // a3 (no usable baseline) is negatively cached with the "-1" sentinel so
      // it stops re-hitting Postgres every request. The read path skips "-1",
      // so it does NOT poison results — consumers still fall back to P50/0.
      expect(redisStore.get("attraction:p90:a3")).toBe("-1");
    });

    it("serves a negatively-cached (-1) id from Redis without a DB hit", async () => {
      redisStore.set("attraction:p90:a1", "-1");

      const result = await service.getBatchAttractionP90Baselines(["a1"]);

      expect(result.has("a1")).toBe(false); // sentinel → treated as absent
      expect(attractionP90Repo.find).not.toHaveBeenCalled(); // no DB re-query
    });
  });

  describe("getAttractionHourlyHistory", () => {
    it("returns a date-keyed map for rows in the range", async () => {
      const qb = {
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue([
          {
            attractionId: "a1",
            date: "2026-05-18",
            slots: [
              { time_slot: "10:00", p90: 30, avgWait: 20, sampleCount: 6 },
            ],
            downCount: 0,
          },
          {
            attractionId: "a1",
            date: "2026-05-19",
            slots: [],
            downCount: 2,
          },
        ]),
      };
      hourlyRepo.createQueryBuilder.mockReturnValueOnce(qb);

      const result = await service.getAttractionHourlyHistory(
        "a1",
        "2026-05-18",
        "2026-05-20",
      );

      expect(result.size).toBe(2);
      expect(result.get("2026-05-18")?.slots[0].p90).toBe(30);
      expect(result.get("2026-05-19")?.downCount).toBe(2);
      expect(qb.andWhere).toHaveBeenCalledWith(
        "h.date BETWEEN :fromDate AND :toDate",
        { fromDate: "2026-05-18", toDate: "2026-05-20" },
      );
    });

    it("normalises Date-typed `date` columns to YYYY-MM-DD strings", async () => {
      const qb = {
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue([
          {
            attractionId: "a1",
            date: new Date("2026-05-18T00:00:00Z"),
            slots: [],
            downCount: 0,
          },
        ]),
      };
      hourlyRepo.createQueryBuilder.mockReturnValueOnce(qb);

      const result = await service.getAttractionHourlyHistory(
        "a1",
        "2026-05-18",
        "2026-05-18",
      );

      expect(result.has("2026-05-18")).toBe(true);
    });
  });

  describe("computeParkHourlyHistoryForDate", () => {
    it("groups raw rows into per-attraction slot arrays with rounded values", async () => {
      queueDataRepo.query.mockResolvedValueOnce([
        {
          attraction_id: "a1",
          time_slot: "10:00",
          p90: "27.3",
          avg_wait: "18.6",
          sample_count: "5",
        },
        {
          attraction_id: "a1",
          time_slot: "10:15",
          p90: "31.9",
          avg_wait: "22.4",
          sample_count: "6",
        },
        {
          attraction_id: "a2",
          time_slot: "10:00",
          p90: "12.1",
          avg_wait: "8.7",
          sample_count: "3",
        },
      ]);

      const result = await service.computeParkHourlyHistoryForDate(
        "p1",
        "2026-05-18",
        "Europe/Berlin",
      );

      expect(result.size).toBe(2);
      const a1Slots = result.get("a1")!;
      expect(a1Slots).toHaveLength(2);
      expect(a1Slots[0]).toMatchObject({ time_slot: "10:00", sampleCount: 5 });
      // Values rounded to nearest 5 — see roundToNearest5Minutes
      expect(a1Slots[0].p90 % 5).toBe(0);
      expect(a1Slots[0].avgWait % 5).toBe(0);
    });

    it("returns an empty map when the SQL produces no rows", async () => {
      queueDataRepo.query.mockResolvedValueOnce([]);

      const result = await service.computeParkHourlyHistoryForDate(
        "p1",
        "2026-05-18",
        "UTC",
      );

      expect(result.size).toBe(0);
    });
  });

  describe("computeParkDownCountForDate", () => {
    it("returns a Map<attractionId, downCount> when the SQL has rows", async () => {
      queueDataRepo.query.mockResolvedValueOnce([
        { attraction_id: "a1", down_count: "3" },
        { attraction_id: "a2", down_count: "0" },
      ]);

      const result = await service.computeParkDownCountForDate(
        "p1",
        "2026-05-18",
        "UTC",
      );

      expect(result.get("a1")).toBe(3);
      expect(result.get("a2")).toBe(0);
    });
  });

  describe("saveAttractionHourlyHistoryBatch", () => {
    it("no-ops when given an empty array (saves a round-trip on empty parks)", async () => {
      await service.saveAttractionHourlyHistoryBatch([]);
      expect(hourlyRepo.upsert).not.toHaveBeenCalled();
    });

    it("upserts every row in one call with the right conflict path", async () => {
      await service.saveAttractionHourlyHistoryBatch([
        {
          attractionId: "a1",
          parkId: "p1",
          date: "2026-05-18",
          slots: [{ time_slot: "10:00", p90: 30, avgWait: 20, sampleCount: 4 }],
          downCount: 0,
        },
        {
          attractionId: "a2",
          parkId: "p1",
          date: "2026-05-18",
          slots: [],
          downCount: 2,
        },
      ]);

      expect(hourlyRepo.upsert).toHaveBeenCalledTimes(1);
      const [rows, conflictKeys] = hourlyRepo.upsert.mock.calls[0];
      expect(rows).toHaveLength(2);
      expect(conflictKeys).toEqual(["attractionId", "date"]);
      // Empty slot arrays preserved — the read path uses absence vs. empty
      // to distinguish "not processed yet" from "no qualifying samples".
      expect(rows[1].slots).toEqual([]);
    });
  });

  describe("calculateAttractionP50P90ForPark — batched per-park scan", () => {
    it("runs ONE SQL query for the whole park instead of one per attraction", async () => {
      queueDataRepo.query.mockResolvedValueOnce([
        {
          attraction_id: "a1",
          p50: "18.5",
          p90: "42.0",
          sample_count: "1200",
          distinct_days: "120",
        },
        {
          attraction_id: "a2",
          p50: "8.0",
          p90: "15.0",
          sample_count: "30",
          distinct_days: "10",
        },
      ]);
      headlinerRepo.find.mockResolvedValueOnce([{ attractionId: "a1" }]);

      const result = await service.calculateAttractionP50P90ForPark(
        "p1",
        "Europe/Berlin",
      );

      // Crucial perf invariant: a single GROUP BY query, not N.
      expect(queueDataRepo.query).toHaveBeenCalledTimes(1);
      expect(headlinerRepo.find).toHaveBeenCalledTimes(1);

      expect(result.size).toBe(2);
      expect(result.get("a1")).toEqual({
        p50: 18.5,
        p90: 42,
        sampleCount: 1200,
        distinctDays: 120,
        confidence: "high", // ≥ 90 distinct days
        isHeadliner: true,
      });
      expect(result.get("a2")).toEqual({
        p50: 8,
        p90: 15,
        sampleCount: 30,
        distinctDays: 10,
        confidence: "low", // < 30 distinct days
        isHeadliner: false,
      });
    });

    it("derives confidence levels from distinctDays at the right boundaries", async () => {
      queueDataRepo.query.mockResolvedValueOnce([
        {
          attraction_id: "high",
          p50: "10",
          p90: "20",
          sample_count: "1",
          distinct_days: "90",
        },
        {
          attraction_id: "medium",
          p50: "10",
          p90: "20",
          sample_count: "1",
          distinct_days: "30",
        },
        {
          attraction_id: "low",
          p50: "10",
          p90: "20",
          sample_count: "1",
          distinct_days: "29",
        },
      ]);

      const result = await service.calculateAttractionP50P90ForPark(
        "p1",
        "UTC",
      );

      expect(result.get("high")!.confidence).toBe("high");
      expect(result.get("medium")!.confidence).toBe("medium");
      expect(result.get("low")!.confidence).toBe("low");
    });

    it("returns an empty map when the park has no qualifying queue data", async () => {
      queueDataRepo.query.mockResolvedValueOnce([]);

      const result = await service.calculateAttractionP50P90ForPark(
        "empty-park",
        "UTC",
      );

      expect(result.size).toBe(0);
      // headliner lookup still happens (cheap) but its result is unused.
      expect(headlinerRepo.find).toHaveBeenCalled();
    });
  });

  describe("saveAttractionP50P90BaselinesBatch — bulk upsert + pipelined Redis", () => {
    it("upserts P50 and P90 in two calls regardless of row count", async () => {
      const rows = [
        {
          attractionId: "a1",
          p50: 20,
          p90: 50,
          sampleCount: 100,
          distinctDays: 100,
          confidence: "high" as const,
          isHeadliner: true,
        },
        {
          attractionId: "a2",
          p50: 10,
          p90: 25,
          sampleCount: 50,
          distinctDays: 50,
          confidence: "medium" as const,
          isHeadliner: false,
        },
      ];

      const summary = await service.saveAttractionP50P90BaselinesBatch(
        "p1",
        rows,
      );

      expect(summary).toEqual({ p50Saved: 2, p90Saved: 2 });
      // Exactly one upsert per table, not per row.
      expect(attractionP50Repo.upsert).toHaveBeenCalledTimes(1);
      expect(attractionP90Repo.upsert).toHaveBeenCalledTimes(1);
      const [p50Rows, p50Conflict] = attractionP50Repo.upsert.mock.calls[0];
      expect(p50Rows).toHaveLength(2);
      expect(p50Conflict).toEqual(["attractionId"]);
    });

    it("skips P50 / P90 rows with sentinel-0 baselines (insufficient data)", async () => {
      const rows = [
        {
          attractionId: "a1",
          p50: 20,
          p90: 0, // no peak baseline yet
          sampleCount: 50,
          distinctDays: 50,
          confidence: "medium" as const,
          isHeadliner: false,
        },
        {
          attractionId: "a2",
          p50: 0, // no qualifying data at all
          p90: 0,
          sampleCount: 0,
          distinctDays: 0,
          confidence: "low" as const,
          isHeadliner: false,
        },
      ];

      const summary = await service.saveAttractionP50P90BaselinesBatch(
        "p1",
        rows,
      );

      // a1's P50 saved, neither P90 saved, a2 fully skipped.
      expect(summary).toEqual({ p50Saved: 1, p90Saved: 0 });
      expect(attractionP50Repo.upsert).toHaveBeenCalledTimes(1);
      // P90 upsert never fired because no row had p90 > 0.
      expect(attractionP90Repo.upsert).not.toHaveBeenCalled();
    });

    it("warms the Redis cache for each saved baseline in a single pipeline round-trip", async () => {
      await service.saveAttractionP50P90BaselinesBatch("p1", [
        {
          attractionId: "a1",
          p50: 20,
          p90: 50,
          sampleCount: 100,
          distinctDays: 100,
          confidence: "high",
          isHeadliner: true,
        },
      ]);

      // Both keys are primed (the pipeline mock applies them on exec).
      expect(redisStore.get("attraction:p50:a1")).toBe("20");
      expect(redisStore.get("attraction:p90:a1")).toBe("50");
      // pipeline.exec was called exactly once — not per-row.
      expect(redis.pipeline).toHaveBeenCalledTimes(1);
    });

    it("no-ops cleanly when given an empty array (Honolulu-style brand-new parks)", async () => {
      const summary = await service.saveAttractionP50P90BaselinesBatch(
        "p1",
        [],
      );

      expect(summary).toEqual({ p50Saved: 0, p90Saved: 0 });
      expect(attractionP50Repo.upsert).not.toHaveBeenCalled();
      expect(attractionP90Repo.upsert).not.toHaveBeenCalled();
      // No pipeline either — wasted Redis round-trip avoided.
      expect(redis.pipeline).not.toHaveBeenCalled();
    });
  });
});
