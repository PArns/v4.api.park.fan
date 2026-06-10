import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { PredictionAccuracyService } from "./prediction-accuracy.service";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";
import { AttractionAccuracyStats } from "../entities/attraction-accuracy-stats.entity";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * Coverage for PredictionAccuracyService — drives the
 * /attractions/{id} prediction-quality badge. The service is large
 * (1800+ lines) so this file targets the public surfaces that are
 * user-visible: `calculateAccuracyBadge`, `recordPredictions`' upsert
 * contract, and `getAttractionAccuracyWithBadge`'s 3-layer cache
 * fallback (Redis → pre-aggregated table → raw SQL).
 */
describe("PredictionAccuracyService", () => {
  let service: PredictionAccuracyService;

  const accuracyRepo = {
    upsert: jest.fn().mockResolvedValue({ identifiers: [] }),
    findOne: jest.fn(),
    find: jest.fn().mockResolvedValue([]),
    query: jest.fn().mockResolvedValue([]),
    manager: {
      transaction: jest.fn(
        (cb: (em: unknown) => Promise<void>): Promise<void> =>
          cb({
            query: jest.fn().mockResolvedValue(undefined),
            getRepository: () => accuracyRepo,
          }),
      ),
    },
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      select: jest.fn().mockReturnThis(),
      addSelect: jest.fn().mockReturnThis(),
      groupBy: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getRawOne: jest.fn().mockResolvedValue(null),
      getRawMany: jest.fn().mockResolvedValue([]),
    })),
  };
  const statsRepo = { findOne: jest.fn() };
  const predictionRepo = { findOne: jest.fn(), find: jest.fn() };
  const queueDataRepo = {
    findOne: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getOne: jest.fn().mockResolvedValue(null),
    })),
  };
  const redisStore = new Map<string, string>();
  const redis = {
    get: jest.fn((k: string) => Promise.resolve(redisStore.get(k) ?? null)),
    set: jest.fn((k: string, v: string) => {
      redisStore.set(k, v);
      return Promise.resolve("OK");
    }),
    del: jest.fn(),
  };

  beforeEach(async () => {
    redisStore.clear();
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PredictionAccuracyService,
        {
          provide: getRepositoryToken(PredictionAccuracy),
          useValue: accuracyRepo,
        },
        {
          provide: getRepositoryToken(AttractionAccuracyStats),
          useValue: statsRepo,
        },
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: predictionRepo,
        },
        { provide: getRepositoryToken(QueueData), useValue: queueDataRepo },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(PredictionAccuracyService);
  });

  describe("calculateAccuracyBadge — public ladder", () => {
    // The badge thresholds drive the prediction-quality UI on attraction
    // detail pages. A slide here = different chip shown to users.
    it.each([
      [4.9, 100, "excellent"],
      [9.9, 100, "good"],
      [14.9, 100, "fair"],
      [25, 100, "poor"],
      [2, 9, "insufficient_data"], // under 10 comparisons → no badge
      [2, 10, "excellent"], // exactly 10 → badge kicks in
    ])(
      "MAE=%d compared=%d → badge '%s'",
      (mae: number, compared: number, expected: string) => {
        const result = service.calculateAccuracyBadge(mae, compared);
        expect(result.badge).toBe(expected);
      },
    );

    it("emits a sample-count message when below 10 compared predictions", () => {
      const result = service.calculateAccuracyBadge(2, 5);
      expect(result.message).toMatch(/at least 10/i);
      expect(result.message).toContain("5");
    });

    it("rounds the MAE in the 'poor' message for readability", () => {
      const result = service.calculateAccuracyBadge(25.6, 100);
      expect(result.badge).toBe("poor");
      // The message uses Math.round so the displayed value is integer
      expect(result.message).toContain("26");
      expect(result.message).not.toContain(".6");
    });
  });

  describe("recordPredictions — upsert contract", () => {
    it("upserts on (attractionId, targetTime) — never inserts duplicates", async () => {
      const prediction = {
        attractionId: "a-1",
        createdAt: new Date(),
        predictedTime: new Date(),
        predictedWaitTime: 30,
        modelVersion: "v1",
        predictionType: "hourly",
        features: { foo: 1 },
      } as unknown as WaitTimePrediction;

      await service.recordPredictions([prediction]);

      expect(accuracyRepo.upsert).toHaveBeenCalledTimes(1);
      const [, options] = accuracyRepo.upsert.mock.calls[0];
      expect(options).toMatchObject({
        conflictPaths: ["attractionId", "targetTime"],
      });
    });
  });

  describe("getAttractionAccuracyWithBadge — cache layers", () => {
    it("layer 1: returns the Redis-cached payload without DB touches", async () => {
      const cached = {
        badge: "good",
        last30Days: {
          mae: 7.5,
          mape: 0.15,
          rmse: 9,
          comparedPredictions: 200,
          totalPredictions: 250,
        },
      };
      redisStore.set("accuracy:badge:a-1:30d", JSON.stringify(cached));

      const result = await service.getAttractionAccuracyWithBadge("a-1");

      expect(result).toEqual(cached);
      // No repo lookups on cache hit.
      expect(statsRepo.findOne).not.toHaveBeenCalled();
    });

    it("layer 2: serves from the pre-aggregated table when Redis misses", async () => {
      statsRepo.findOne.mockResolvedValueOnce({
        attractionId: "a-1",
        badge: "fair",
        mae: 12,
        comparedPredictions: 50,
        totalPredictions: 60,
        message: "Predictions provide general guidance",
      });

      const result = await service.getAttractionAccuracyWithBadge("a-1");

      expect(result.badge).toBe("fair");
      expect(result.last30Days.mae).toBe(12);
      expect(result.last30Days.comparedPredictions).toBe(50);
      // Cache primed for the next call.
      expect(redisStore.get("accuracy:badge:a-1:30d")).toBeDefined();
    });

    it("layer 3: falls through to raw SQL aggregation when nothing else has data", async () => {
      statsRepo.findOne.mockResolvedValueOnce(null);
      // Layer 3 uses repo.query() (raw SQL), not createQueryBuilder.
      accuracyRepo.query.mockResolvedValueOnce([
        {
          total_predictions: "50",
          compared_predictions: "40",
          mae: "8.5",
          mape: "0.2",
          rmse: "10.5",
        },
      ]);

      const result = await service.getAttractionAccuracyWithBadge("a-1");

      // Badge derived from MAE=8.5 and compared=40 → "good"
      expect(result.badge).toBe("good");
      // Layer 3 also primes the Redis cache so the next hit is L1.
      expect(redisStore.get("accuracy:badge:a-1:30d")).toBeDefined();
    });
  });
});
