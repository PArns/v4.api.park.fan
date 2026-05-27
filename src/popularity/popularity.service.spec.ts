import { Test, TestingModule } from "@nestjs/testing";
import { PopularityService } from "./popularity.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * PopularityService is called on every successful GET request via the
 * PopularityInterceptor. Two non-obvious contracts to pin:
 *   1. ZINCRBY errors are swallowed silently — a Redis blip on every
 *      page load must NOT propagate into the response chain.
 *   2. ZREVRANGE errors return an empty array — callers (cache warmup)
 *      treat that as "no popular entities" rather than crashing.
 */
describe("PopularityService", () => {
  let service: PopularityService;

  const pipeline = {
    zunionstore: jest.fn().mockReturnThis(),
    zremrangebyscore: jest.fn().mockReturnThis(),
    exec: jest.fn().mockResolvedValue([]),
  };
  const redis = {
    zincrby: jest.fn().mockResolvedValue(1),
    zrevrange: jest.fn(),
    del: jest.fn().mockResolvedValue(2),
    pipeline: jest.fn(() => pipeline),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PopularityService,
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(PopularityService);
  });

  describe("recordParkHit", () => {
    it("increments the park's score in the `popularity:parks` sorted set", async () => {
      await service.recordParkHit("p-1");
      expect(redis.zincrby).toHaveBeenCalledWith("popularity:parks", 1, "p-1");
    });

    it("swallows Redis errors silently (must not break every request)", async () => {
      redis.zincrby.mockRejectedValueOnce(new Error("Redis down"));
      // No throw — interceptor would otherwise propagate this to the
      // response handler.
      await expect(service.recordParkHit("p-1")).resolves.toBeUndefined();
    });
  });

  describe("recordAttractionHit", () => {
    it("increments the attraction's score in the `popularity:attractions` set", async () => {
      await service.recordAttractionHit("a-1");
      expect(redis.zincrby).toHaveBeenCalledWith(
        "popularity:attractions",
        1,
        "a-1",
      );
    });

    it("swallows Redis errors silently", async () => {
      redis.zincrby.mockRejectedValueOnce(new Error("Redis down"));
      await expect(service.recordAttractionHit("a-1")).resolves.toBeUndefined();
    });
  });

  describe("getTopParks", () => {
    it("returns the top-N park IDs by score (descending)", async () => {
      redis.zrevrange.mockResolvedValueOnce(["p-1", "p-2", "p-3"]);

      const result = await service.getTopParks(3);

      expect(result).toEqual(["p-1", "p-2", "p-3"]);
      // ZREVRANGE uses 0..limit-1 indices.
      expect(redis.zrevrange).toHaveBeenCalledWith("popularity:parks", 0, 2);
    });

    it("uses the documented default limit of 50", async () => {
      redis.zrevrange.mockResolvedValueOnce([]);
      await service.getTopParks();
      // Default limit=50 → ZREVRANGE 0..49
      expect(redis.zrevrange).toHaveBeenCalledWith("popularity:parks", 0, 49);
    });

    it("returns an empty array on Redis failure (cache warmup degrades gracefully)", async () => {
      redis.zrevrange.mockRejectedValueOnce(new Error("Redis down"));
      const result = await service.getTopParks();
      expect(result).toEqual([]);
    });
  });

  describe("getTopAttractions", () => {
    it("returns the top-N attraction IDs by score", async () => {
      redis.zrevrange.mockResolvedValueOnce(["a-1", "a-2"]);
      const result = await service.getTopAttractions(2);
      expect(result).toEqual(["a-1", "a-2"]);
      expect(redis.zrevrange).toHaveBeenCalledWith(
        "popularity:attractions",
        0,
        1,
      );
    });

    it("uses the documented default limit of 200", async () => {
      redis.zrevrange.mockResolvedValueOnce([]);
      await service.getTopAttractions();
      expect(redis.zrevrange).toHaveBeenCalledWith(
        "popularity:attractions",
        0,
        199,
      );
    });

    it("returns an empty array on Redis failure", async () => {
      redis.zrevrange.mockRejectedValueOnce(new Error("Redis down"));
      const result = await service.getTopAttractions();
      expect(result).toEqual([]);
    });
  });

  describe("applyDecay", () => {
    it("scales each set by the decay factor and prunes faded entries", async () => {
      await service.applyDecay();

      // One decay op per ranking set (parks + attractions).
      expect(pipeline.zunionstore).toHaveBeenCalledWith(
        "popularity:parks",
        1,
        "popularity:parks",
        "WEIGHTS",
        0.9,
      );
      expect(pipeline.zunionstore).toHaveBeenCalledWith(
        "popularity:attractions",
        1,
        "popularity:attractions",
        "WEIGHTS",
        0.9,
      );
      // Entries below half a hit are removed (exclusive lower bound).
      expect(pipeline.zremrangebyscore).toHaveBeenCalledWith(
        "popularity:parks",
        "-inf",
        "(0.5",
      );
      expect(pipeline.exec).toHaveBeenCalledTimes(2);
    });

    it("swallows Redis errors silently", async () => {
      pipeline.exec.mockRejectedValueOnce(new Error("Redis down"));
      await expect(service.applyDecay()).resolves.toBeUndefined();
    });
  });

  describe("resetScores", () => {
    it("deletes both sorted sets in a single Redis call", async () => {
      await service.resetScores();
      expect(redis.del).toHaveBeenCalledTimes(1);
      expect(redis.del).toHaveBeenCalledWith(
        "popularity:parks",
        "popularity:attractions",
      );
    });
  });
});
