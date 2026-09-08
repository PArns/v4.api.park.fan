import { HttpException } from "@nestjs/common";
import { PushFollowAccessGuard } from "./push-follow-access.guard";
import { PushService } from "./push.service";
import { PushFollowWriteRateLimitService } from "./push-follow-write-rate-limit.service";

/**
 * The one rule `RideAlertsController` and `ShowFollowsController` both
 * delegate here: never let a handler run against an endpoint with no
 * subscription, and never let a write past the rate limiter.
 */
describe("PushFollowAccessGuard", () => {
  const ENDPOINT = "https://fcm.googleapis.com/fcm/send/e1";

  let guard: PushFollowAccessGuard;
  let findByEndpoint: jest.Mock;
  let check: jest.Mock;

  beforeEach(() => {
    findByEndpoint = jest
      .fn()
      .mockResolvedValue({ id: "sub-1", endpoint: ENDPOINT });
    check = jest
      .fn()
      .mockResolvedValue({ allowed: true, retryAfterSeconds: 0 });
    guard = new PushFollowAccessGuard(
      { findByEndpoint } as unknown as PushService,
      { check } as unknown as PushFollowWriteRateLimitService,
    );
  });

  const req = () => ({ headers: {}, socket: {} }) as never;

  describe("subscriptionOrThrow", () => {
    it("returns the subscription for a known endpoint", async () => {
      const subscription = await guard.subscriptionOrThrow(ENDPOINT);
      expect(subscription).toEqual({ id: "sub-1", endpoint: ENDPOINT });
    });

    it("refuses a missing endpoint before ever querying the repository", async () => {
      await expect(
        guard.subscriptionOrThrow(undefined as unknown as string),
      ).rejects.toMatchObject({ status: 400 });
      expect(findByEndpoint).not.toHaveBeenCalled();
    });

    it("refuses a blank endpoint the same way", async () => {
      await expect(guard.subscriptionOrThrow("   ")).rejects.toMatchObject({
        status: 400,
      });
      expect(findByEndpoint).not.toHaveBeenCalled();
    });

    it("404s an endpoint with no stored subscription", async () => {
      findByEndpoint.mockResolvedValueOnce(null);
      await expect(guard.subscriptionOrThrow(ENDPOINT)).rejects.toMatchObject({
        status: 404,
      });
    });
  });

  describe("writeGuard", () => {
    it("lets an allowed write through", async () => {
      await expect(
        guard.writeGuard(req(), "ride-alert"),
      ).resolves.toBeUndefined();
      expect(check).toHaveBeenCalledWith(null, "ride-alert");
    });

    it("checks the bucket its caller names, not a fixed one", async () => {
      await guard.writeGuard(req(), "show-follow");
      expect(check).toHaveBeenCalledWith(null, "show-follow");
    });

    it("429s with the limiter's own retry-after once it says no", async () => {
      check.mockResolvedValueOnce({ allowed: false, retryAfterSeconds: 42 });
      const error = await guard
        .writeGuard(req(), "ride-alert")
        .catch((e: unknown) => e);
      expect(error).toBeInstanceOf(HttpException);
      expect((error as HttpException).getStatus()).toBe(429);
      expect((error as HttpException).getResponse()).toMatchObject({
        retryAfterSeconds: 42,
        message: expect.stringContaining("ride-alert"),
      });
    });
  });
});
