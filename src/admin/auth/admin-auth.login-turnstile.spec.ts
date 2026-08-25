import { ForbiddenException } from "@nestjs/common";
import { AdminAuthController } from "./admin-auth.controller";
import { AdminTurnstileService } from "./admin-turnstile.service";

/**
 * Who has to solve the challenge, and who must not be asked.
 *
 * The split is the whole design. park.fan's admin proxy solves Turnstile in the
 * browser and verifies it in its own route before it forwards anything here, so
 * by the time a login reaches this controller from the frontend the token has
 * already been redeemed — and a Turnstile token may be redeemed once. Asking
 * for it again would fail every login the admin makes. Anyone reaching
 * `/v1/admin/auth/login` directly has met no challenge at all, and is exactly
 * who this exists for.
 *
 * `THROTTLE_BYPASS_KEYS` is what tells the two apart, because it already is:
 * the same header decides whether the global rate limiter applies.
 */

const FRONTEND_KEY = "front-end-key";

function build(verdict: { success: boolean; reason?: string }) {
  const turnstile = {
    verify: jest.fn(async () => verdict),
  } as unknown as AdminTurnstileService;

  const auth = {
    login: jest.fn(async () => ({
      outcome: "ok" as const,
      token: "session-token",
      session: {
        userId: "user-1",
        email: "you@park.fan",
        displayName: "You",
        role: "owner" as const,
        expiresAt: Date.now() + 60_000,
        mustChangePassword: false,
        mustEnrolTotp: false,
      },
    })),
  };
  const audit = { record: jest.fn(async () => undefined) };

  const controller = new AdminAuthController(
    auth as never,
    {} as never,
    audit as never,
    turnstile,
  );

  return { controller, turnstile, auth };
}

const request = (headers: Record<string, string>) =>
  ({ headers, ip: "198.51.100.7" }) as never;

const credentials = (extra: Record<string, unknown> = {}) =>
  ({
    email: "you@park.fan",
    password: "correct horse battery",
    ...extra,
  }) as never;

describe("AdminAuthController — Turnstile on the login", () => {
  const ORIGINAL_ENV = { ...process.env };

  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV };
    delete process.env.ADMIN_LOGIN_TURNSTILE;
    process.env.ADMIN_TURNSTILE_SECRET_KEY = "s3cret";
    process.env.THROTTLE_BYPASS_KEYS = FRONTEND_KEY;
  });

  afterEach(() => {
    process.env = { ...ORIGINAL_ENV };
  });

  it("never asks our own frontend, whose token is already spent", async () => {
    const { controller, turnstile, auth } = build({ success: true });

    await controller.login(
      credentials(),
      request({ "x-auth-key": FRONTEND_KEY }),
    );

    expect(turnstile.verify).not.toHaveBeenCalled();
    expect(auth.login).toHaveBeenCalled();
  });

  it("asks a direct caller, and lets a solved one through", async () => {
    const { controller, turnstile, auth } = build({ success: true });

    await controller.login(credentials({ turnstileToken: "tok" }), request({}));

    expect(turnstile.verify).toHaveBeenCalledWith("tok", "198.51.100.7");
    expect(auth.login).toHaveBeenCalled();
  });

  it("refuses a direct caller with no token, before the password is looked at", async () => {
    // Before, not after: the lockout and the limiter only count attempts that
    // were already made, and a spray that never reaches them costs nothing.
    const { controller, auth } = build({
      success: false,
      reason: "missing-token",
    });

    await expect(controller.login(credentials(), request({}))).rejects.toThrow(
      ForbiddenException,
    );
    expect(auth.login).not.toHaveBeenCalled();
  });

  it("refuses a direct caller whose token Cloudflare rejects", async () => {
    const { controller, auth } = build({
      success: false,
      reason: "timeout-or-duplicate",
    });

    await expect(
      controller.login(credentials({ turnstileToken: "spent" }), request({})),
    ).rejects.toThrow(ForbiddenException);
    expect(auth.login).not.toHaveBeenCalled();
  });

  it("refuses a wrong key like any other stranger", async () => {
    const { controller, turnstile } = build({ success: false });

    await expect(
      controller.login(credentials(), request({ "x-auth-key": "guessed" })),
    ).rejects.toThrow(ForbiddenException);
    expect(turnstile.verify).toHaveBeenCalled();
  });

  it("asks nobody while no secret is configured", async () => {
    // The state every deployment is in until the env var is set. It must behave
    // exactly as it did before this existed.
    delete process.env.ADMIN_TURNSTILE_SECRET_KEY;
    const { controller, turnstile, auth } = build({ success: false });

    await controller.login(credentials(), request({}));

    expect(turnstile.verify).not.toHaveBeenCalled();
    expect(auth.login).toHaveBeenCalled();
  });

  it("asks nobody while no bypass key can identify the frontend", async () => {
    // Enforcing here would refuse the admin proxy along with everyone else.
    delete process.env.THROTTLE_BYPASS_KEYS;
    const { controller, turnstile, auth } = build({ success: false });

    await controller.login(credentials(), request({}));

    expect(turnstile.verify).not.toHaveBeenCalled();
    expect(auth.login).toHaveBeenCalled();
  });
});
