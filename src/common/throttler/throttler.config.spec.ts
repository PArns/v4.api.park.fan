import type { ThrottlerOptions } from "@nestjs/throttler";
import {
  getThrottleBypassHeader,
  getThrottleBypassKeys,
  getThrottlerOptions,
  isThrottlingEnabled,
} from "./throttler.config";

// Every value is read lazily from process.env, so each test sets the env it
// needs. Restore the original env afterwards.
describe("throttler.config", () => {
  const ORIGINAL_ENV = process.env;
  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV };
  });
  afterAll(() => {
    process.env = ORIGINAL_ENV;
  });

  describe("getThrottlerOptions / isThrottlingEnabled", () => {
    it("defaults to 300 requests / 60s (ttl expressed in ms for v6)", () => {
      delete process.env.THROTTLE_LIMIT;
      delete process.env.THROTTLE_TTL;
      delete process.env.THROTTLE_ANON_LIMIT;
      delete process.env.THROTTLE_ANON_HOURLY_LIMIT;
      expect(getThrottlerOptions()).toEqual([
        { ttl: 60000, limit: 300 },
        { name: "anonymous-minute", ttl: 60000, limit: 60 },
        { name: "anonymous-hour", ttl: 3600000, limit: 1000 },
      ]);
      expect(isThrottlingEnabled()).toBe(true);
    });

    it("honours THROTTLE_TTL / THROTTLE_LIMIT overrides", () => {
      process.env.THROTTLE_TTL = "30";
      process.env.THROTTLE_LIMIT = "100";
      expect((getThrottlerOptions() as ThrottlerOptions[])[0]).toEqual({
        ttl: 30000,
        limit: 100,
      });
    });

    it("tunes or turns off each anonymous limit", () => {
      process.env.THROTTLE_ANON_LIMIT = "30";
      process.env.THROTTLE_ANON_HOURLY_LIMIT = "0";
      const names = (getThrottlerOptions() as ThrottlerOptions[]).map(
        (options) => ("name" in options ? options.name : "default"),
      );
      expect(names).toEqual(["default", "anonymous-minute"]);
      expect((getThrottlerOptions() as ThrottlerOptions[])[1]).toMatchObject({
        limit: 30,
      });
      delete process.env.THROTTLE_ANON_LIMIT;
      delete process.env.THROTTLE_ANON_HOURLY_LIMIT;
    });

    it("THROTTLE_LIMIT=0 disables throttling", () => {
      process.env.THROTTLE_LIMIT = "0";
      expect(isThrottlingEnabled()).toBe(false);
    });
  });

  describe("getThrottleBypassHeader", () => {
    it("defaults to x-auth-key", () => {
      delete process.env.THROTTLE_BYPASS_HEADER;
      expect(getThrottleBypassHeader()).toBe("x-auth-key");
    });

    it("lowercases a custom header (req.headers keys are lowercased)", () => {
      process.env.THROTTLE_BYPASS_HEADER = "X-Frontend-Token";
      expect(getThrottleBypassHeader()).toBe("x-frontend-token");
    });
  });

  describe("getThrottleBypassKeys", () => {
    it("is empty when unset → bypass disabled", () => {
      delete process.env.THROTTLE_BYPASS_KEYS;
      expect(getThrottleBypassKeys()).toEqual([]);
    });

    it("splits on comma, trims, and drops empty entries", () => {
      process.env.THROTTLE_BYPASS_KEYS = " a , b ,, c ";
      expect(getThrottleBypassKeys()).toEqual(["a", "b", "c"]);
    });
  });
});
