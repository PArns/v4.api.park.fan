import { ExecutionContext } from "@nestjs/common";
import { ThrottlerGuard } from "@nestjs/throttler";
import { CfThrottlerGuard } from "./cf-throttler.guard";

// Instantiate without the DI constructor — we only exercise the two
// overridden methods, which don't touch injected deps (getTracker is pure;
// the bypass branch of shouldSkip returns before delegating to super).
const guard = Object.create(CfThrottlerGuard.prototype) as CfThrottlerGuard;
const callGetTracker = (req: unknown) =>
  (guard as any).getTracker(req) as Promise<string>;
const callShouldSkip = (ctx: ExecutionContext) =>
  (guard as any).shouldSkip(ctx) as Promise<boolean>;

const ctxWithHeaders = (headers: Record<string, unknown>): ExecutionContext =>
  ({
    switchToHttp: () => ({ getRequest: () => ({ headers }) }),
    getHandler: () => undefined,
    getClass: () => undefined,
  }) as unknown as ExecutionContext;

describe("CfThrottlerGuard", () => {
  const ORIGINAL_ENV = process.env;
  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV };
  });
  afterAll(() => {
    process.env = ORIGINAL_ENV;
  });

  describe("getTracker — real client IP", () => {
    it("prefers CF-Connecting-IP", async () => {
      const key = await callGetTracker({
        headers: {
          "cf-connecting-ip": "203.0.113.7",
          "x-forwarded-for": "10.0.0.1",
        },
        ip: "172.16.0.1",
      });
      expect(key).toBe("203.0.113.7");
    });

    it("falls back to the first X-Forwarded-For hop", async () => {
      const key = await callGetTracker({
        headers: { "x-forwarded-for": "198.51.100.9, 10.0.0.1" },
        ip: "172.16.0.1",
      });
      expect(key).toBe("198.51.100.9");
    });

    it("falls back to req.ip when no proxy headers are present", async () => {
      const key = await callGetTracker({ headers: {}, ip: "172.16.0.1" });
      expect(key).toBe("172.16.0.1");
    });
  });

  describe("shouldSkip — bypass allow-list", () => {
    it("skips when the bypass header carries a configured key", async () => {
      process.env.THROTTLE_BYPASS_KEYS = "secret-a, secret-b";
      const superSpy = jest
        .spyOn(ThrottlerGuard.prototype as any, "shouldSkip")
        .mockResolvedValue(false);

      const skipped = await callShouldSkip(
        ctxWithHeaders({ "x-auth-key": "secret-b" }),
      );

      expect(skipped).toBe(true);
      expect(superSpy).not.toHaveBeenCalled(); // short-circuits before super
      superSpy.mockRestore();
    });

    it("honours a custom bypass header name", async () => {
      process.env.THROTTLE_BYPASS_KEYS = "topsecret";
      process.env.THROTTLE_BYPASS_HEADER = "x-frontend-token";

      const skipped = await callShouldSkip(
        ctxWithHeaders({ "x-frontend-token": "topsecret" }),
      );

      expect(skipped).toBe(true);
    });

    it("delegates to the default @SkipThrottle handling on a wrong key", async () => {
      process.env.THROTTLE_BYPASS_KEYS = "secret-a";
      const superSpy = jest
        .spyOn(ThrottlerGuard.prototype as any, "shouldSkip")
        .mockResolvedValue(false);

      const skipped = await callShouldSkip(
        ctxWithHeaders({ "x-auth-key": "wrong" }),
      );

      expect(skipped).toBe(false);
      expect(superSpy).toHaveBeenCalled();
      superSpy.mockRestore();
    });

    it("delegates to super when no bypass keys are configured", async () => {
      delete process.env.THROTTLE_BYPASS_KEYS;
      const superSpy = jest
        .spyOn(ThrottlerGuard.prototype as any, "shouldSkip")
        .mockResolvedValue(false);

      const skipped = await callShouldSkip(
        ctxWithHeaders({ "x-auth-key": "anything" }),
      );

      expect(skipped).toBe(false);
      expect(superSpy).toHaveBeenCalled();
      superSpy.mockRestore();
    });
  });
});
