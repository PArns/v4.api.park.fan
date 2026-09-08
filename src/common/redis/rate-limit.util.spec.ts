import { Logger } from "@nestjs/common";
import {
  checkRateLimit,
  hashRateLimitKey,
  incrementWithWindow,
} from "./rate-limit.util";
import { RedisMock } from "../../../test/mocks/redis.mock";

describe("rate-limit.util", () => {
  let redis: RedisMock;
  let logger: Logger;

  beforeEach(() => {
    redis = new RedisMock();
    logger = { warn: jest.fn() } as unknown as Logger;
  });

  describe("incrementWithWindow", () => {
    it("counts up from one", async () => {
      expect(await incrementWithWindow(redis as never, "k", 60)).toBe(1);
      expect(await incrementWithWindow(redis as never, "k", 60)).toBe(2);
    });

    it("sets a TTL on the very first increment", async () => {
      await incrementWithWindow(redis as never, "k", 60);
      expect(await redis.ttl("k")).toBeGreaterThan(0);
    });

    it("is self-healing when an earlier call's expire never landed", async () => {
      // Simulates exactly the old bug: an INCR that succeeded with no TTL
      // ever attached, as if the process died (or the connection dropped)
      // between the two calls. The old `if (count === 1)` guard would never
      // revisit this key again — every later call sees `count > 1` and skips
      // the `EXPIRE` outright, so the counter would sit here forever.
      await redis.incr("k"); // count is now 1, no TTL — the crash
      expect(await redis.ttl("k")).toBe(-1); // no TTL, confirmed

      const count = await incrementWithWindow(redis as never, "k", 60);
      expect(count).toBe(2);
      // `NX` on this call still finds no TTL and sets one, even though the
      // count is no longer 1.
      expect(await redis.ttl("k")).toBeGreaterThan(0);
    });

    it("never pushes an already-running window forward (NX, not unconditional)", async () => {
      await incrementWithWindow(redis as never, "k", 60);
      const firstTtl = await redis.ttl("k");
      redis.now = () => Date.now() + 30_000; // 30s later, still mid-window
      await incrementWithWindow(redis as never, "k", 60);
      const secondTtl = await redis.ttl("k");
      // The remaining time has ticked down, not reset to a fresh 60s — a
      // sliding window would let a steady stream of calls hold it open
      // forever.
      expect(secondTtl).toBeLessThan(firstTtl);
    });
  });

  describe("checkRateLimit", () => {
    const args = (overrides: Partial<Parameters<typeof checkRateLimit>[0]>) => ({
      redis: redis as never,
      logger,
      label: "Test limiter",
      key: "bucket",
      max: 3,
      windowSeconds: 60,
      ...overrides,
    });

    it("allows up to the max", async () => {
      for (let i = 0; i < 3; i++) {
        expect((await checkRateLimit(args({}))).allowed).toBe(true);
      }
    });

    it("refuses the call that pushes the count over the max, with a positive retry-after", async () => {
      for (let i = 0; i < 3; i++) await checkRateLimit(args({}));
      const verdict = await checkRateLimit(args({}));
      expect(verdict.allowed).toBe(false);
      expect(verdict.retryAfterSeconds).toBeGreaterThan(0);
    });

    it("fails open and logs when Redis throws", async () => {
      const broken = {
        incr: () => Promise.reject(new Error("connection refused")),
      };
      const verdict = await checkRateLimit(
        args({ redis: broken as never }),
      );
      expect(verdict).toEqual({ allowed: true, retryAfterSeconds: 0 });
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining("Test limiter"),
      );
    });
  });

  describe("hashRateLimitKey", () => {
    it("is deterministic and never returns the input back", () => {
      const hashed = hashRateLimitKey("198.51.100.7");
      expect(hashed).toBe(hashRateLimitKey("198.51.100.7"));
      expect(hashed).not.toContain("198.51.100.7");
    });

    it("tells different inputs apart", () => {
      expect(hashRateLimitKey("198.51.100.7")).not.toBe(
        hashRateLimitKey("198.51.100.8"),
      );
    });
  });
});
