import { AdminTurnstileService } from "./admin-turnstile.service";
import { isAdminLoginTurnstileEnforced } from "../../config/admin-auth.config";

/**
 * Two things are under test and the second is the one that could take the admin
 * down: whether a token is judged correctly, and whether the check is switched
 * on at all. Enforcement with no way to recognise our own frontend would refuse
 * every login there is, so `isAdminLoginTurnstileEnforced` has to stay off
 * until both halves are configured.
 */
describe("AdminTurnstileService", () => {
  const ORIGINAL_ENV = { ...process.env };
  let service: AdminTurnstileService;
  let fetchMock: jest.Mock;

  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV };
    delete process.env.ADMIN_TURNSTILE_SECRET_KEY;
    delete process.env.TURNSTILE_SECRET_KEY;
    delete process.env.THROTTLE_BYPASS_KEYS;
    delete process.env.ADMIN_LOGIN_TURNSTILE;

    service = new AdminTurnstileService();
    fetchMock = jest.fn();
    global.fetch = fetchMock as unknown as typeof fetch;
    jest.spyOn(console, "error").mockImplementation(() => undefined);
  });

  afterEach(() => {
    process.env = { ...ORIGINAL_ENV };
    jest.restoreAllMocks();
  });

  const respondWith = (payload: unknown) =>
    fetchMock.mockResolvedValue({ json: async () => payload });

  describe("verify", () => {
    it("accepts a token Cloudflare confirms", async () => {
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
      respondWith({ success: true });

      await expect(service.verify("tok", "198.51.100.7")).resolves.toEqual({
        success: true,
      });
    });

    it("sends the secret, the token and the caller's address", async () => {
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
      respondWith({ success: true });

      await service.verify("tok", "198.51.100.7");

      const [url, init] = fetchMock.mock.calls[0];
      expect(url).toContain("challenges.cloudflare.com");
      const sent = init.body as URLSearchParams;
      expect(sent.get("secret")).toBe("s3cret");
      expect(sent.get("response")).toBe("tok");
      expect(sent.get("remoteip")).toBe("198.51.100.7");
    });

    it("omits remoteip when the address is unknown", async () => {
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
      respondWith({ success: true });

      await service.verify("tok", null);

      expect(
        (fetchMock.mock.calls[0][1].body as URLSearchParams).has("remoteip"),
      ).toBe(false);
    });

    it("refuses a token Cloudflare rejects, and keeps the reason", async () => {
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
      respondWith({ success: false, "error-codes": ["timeout-or-duplicate"] });

      await expect(service.verify("spent", null)).resolves.toEqual({
        success: false,
        reason: "timeout-or-duplicate",
      });
    });

    it("refuses an empty token without asking Cloudflare", async () => {
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";

      await expect(service.verify("", null)).resolves.toEqual({
        success: false,
        reason: "missing-token",
      });
      expect(fetchMock).not.toHaveBeenCalled();
    });

    it("refuses rather than passing when siteverify cannot be reached", async () => {
      // The alternative would make the check removable by anyone able to
      // disturb one connection.
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
      fetchMock.mockRejectedValue(new Error("ETIMEDOUT"));

      await expect(service.verify("tok", null)).resolves.toEqual({
        success: false,
        reason: "network-error",
      });
    });

    it("refuses when no secret is configured", async () => {
      await expect(service.verify("tok", null)).resolves.toEqual({
        success: false,
        reason: "not-configured",
      });
      expect(fetchMock).not.toHaveBeenCalled();
    });

    it("falls back to the frontend's TURNSTILE_SECRET_KEY name", async () => {
      process.env.TURNSTILE_SECRET_KEY = "shared";
      respondWith({ success: true });

      await service.verify("tok", null);

      expect(
        (fetchMock.mock.calls[0][1].body as URLSearchParams).get("secret"),
      ).toBe("shared");
    });
  });

  describe("isAdminLoginTurnstileEnforced", () => {
    it("is off when nothing is configured", () => {
      expect(isAdminLoginTurnstileEnforced()).toBe(false);
    });

    it("is off with a secret but no way to recognise our own frontend", () => {
      // The lockout case: park.fan's proxy verifies on its own side and sends
      // no token, so with no bypass keys it would be refused like a stranger.
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
      expect(isAdminLoginTurnstileEnforced()).toBe(false);
    });

    it("is off with bypass keys but no secret to check tokens against", () => {
      process.env.THROTTLE_BYPASS_KEYS = "front-end-key";
      expect(isAdminLoginTurnstileEnforced()).toBe(false);
    });

    it("is on once both halves are set", () => {
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
      process.env.THROTTLE_BYPASS_KEYS = "front-end-key";
      expect(isAdminLoginTurnstileEnforced()).toBe(true);
    });

    it("can be switched off explicitly while both remain set", () => {
      process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
      process.env.THROTTLE_BYPASS_KEYS = "front-end-key";
      process.env.ADMIN_LOGIN_TURNSTILE = "false";
      expect(isAdminLoginTurnstileEnforced()).toBe(false);
    });
  });
});
