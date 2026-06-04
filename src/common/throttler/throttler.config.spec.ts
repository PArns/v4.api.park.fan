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
      expect(getThrottlerOptions()).toEqual([{ ttl: 60000, limit: 300 }]);
      expect(isThrottlingEnabled()).toBe(true);
    });

    it("honours THROTTLE_TTL / THROTTLE_LIMIT overrides", () => {
      process.env.THROTTLE_TTL = "30";
      process.env.THROTTLE_LIMIT = "100";
      expect(getThrottlerOptions()).toEqual([{ ttl: 30000, limit: 100 }]);
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
