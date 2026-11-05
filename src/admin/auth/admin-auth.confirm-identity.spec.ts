import { HttpException, UnauthorizedException } from "@nestjs/common";
import { AdminAuthService } from "./admin-auth.service";
import { AdminSessionStore } from "./admin-session.store";
import { AdminLoginRateLimitService } from "./admin-login-rate-limit.service";
import { RedisMock } from "../../../test/mocks/redis.mock";
import { hashPassword } from "./password.util";
import { throughFilter } from "../../../test/helpers/through-filter";
import { generateTotpSecret, totpCodeForStep, totpStep } from "./totp.util";

/**
 * Creating a personal access token asks the account for its password again, and
 * for a code when it has two-factor on. The answers are the contract the
 * account page's dialog is written against (`invalid-password`,
 * `totp-required`, `invalid-totp`, and a 429 from the action limiter), so each
 * is pinned here with the thing that must not happen beside it.
 */

const PASSWORD = "a-long-enough-password";
const ACTION = "token-create";

async function build(overrides: Record<string, unknown> = {}) {
  const user = {
    id: "user-1",
    passwordHash: await hashPassword(PASSWORD),
    totpEnabled: false,
    totpSecret: null as string | null,
    totpLastStep: null as string | null,
    ...overrides,
  };
  const users = {
    findOne: jest.fn(async (): Promise<unknown> => user),
    update: jest.fn(async (): Promise<unknown> => ({ affected: 1 })),
  };
  const limiter = new AdminLoginRateLimitService(new RedisMock() as never);
  const service = new AdminAuthService(
    users as never,
    new AdminSessionStore(new RedisMock() as never),
    limiter,
    { revokeAllForUser: jest.fn(async () => 0) } as never,
  );
  return { service, users, limiter, user };
}

async function refusal(promise: Promise<unknown>): Promise<HttpException> {
  try {
    await promise;
  } catch (error) {
    return error as HttpException;
  }
  throw new Error("expected a refusal");
}

describe("confirmIdentity", () => {
  // Every password check runs scrypt (~100 ms, several times that on a busy
  // runner), and the limiter's tests make ten to twenty of them.
  jest.setTimeout(30_000);

  it("lets a right password through when the account has no second factor", async () => {
    const { service } = await build();
    await expect(
      service.confirmIdentity("user-1", PASSWORD, undefined, ACTION),
    ).resolves.toBeUndefined();
  });

  it("refuses a wrong password as invalid-password and counts it", async () => {
    const { service, limiter } = await build();
    const failure = jest.spyOn(limiter, "recordActionFailure");
    const error = await refusal(
      service.confirmIdentity("user-1", "not-the-password", undefined, ACTION),
    );
    expect(error).toBeInstanceOf(UnauthorizedException);
    expect(error.getResponse()).toMatchObject({ error: "invalid-password" });
    expect(failure).toHaveBeenCalledWith(ACTION, "user-1");
  });

  it("refuses an empty password the same way", async () => {
    const { service } = await build();
    const error = await refusal(
      service.confirmIdentity("user-1", "", undefined, ACTION),
    );
    expect(error.getResponse()).toMatchObject({ error: "invalid-password" });
  });

  describe("with two-factor on", () => {
    const secret = generateTotpSecret();
    const twoFactor = { totpEnabled: true, totpSecret: secret };

    it("asks for a code once the password is right, without counting it as a failure", async () => {
      const { service, limiter } = await build(twoFactor);
      const failure = jest.spyOn(limiter, "recordActionFailure");
      const error = await refusal(
        service.confirmIdentity("user-1", PASSWORD, undefined, ACTION),
      );
      expect(error).toBeInstanceOf(UnauthorizedException);
      expect(error.getResponse()).toMatchObject({ error: "totp-required" });
      expect(failure).not.toHaveBeenCalled();
    });

    it("does not reveal the second factor to a wrong password", async () => {
      const { service } = await build(twoFactor);
      const error = await refusal(
        service.confirmIdentity("user-1", "wrong", undefined, ACTION),
      );
      expect(error.getResponse()).toMatchObject({ error: "invalid-password" });
    });

    it("accepts a current code and claims its step", async () => {
      const { service, users } = await build(twoFactor);
      const step = totpStep();
      await service.confirmIdentity(
        "user-1",
        PASSWORD,
        totpCodeForStep(secret, step),
        ACTION,
      );
      const [where, patch] = users.update.mock.calls[0] as unknown as [
        { id: string },
        { totpLastStep: string },
      ];
      expect(where.id).toBe("user-1");
      expect(Number(patch.totpLastStep)).toBeGreaterThanOrEqual(step - 1);
    });

    it("refuses a code that does not match as invalid-totp and counts it", async () => {
      const { service, limiter, users } = await build(twoFactor);
      const failure = jest.spyOn(limiter, "recordActionFailure");
      const wrong = totpCodeForStep(secret, totpStep() + 50);
      const error = await refusal(
        service.confirmIdentity("user-1", PASSWORD, wrong, ACTION),
      );
      expect(error.getResponse()).toMatchObject({ error: "invalid-totp" });
      expect(failure).toHaveBeenCalledWith(ACTION, "user-1");
      expect(users.update).not.toHaveBeenCalled();
    });

    it("refuses a step the account already used, such as the one it signed in with", async () => {
      const step = totpStep();
      const { service } = await build({
        ...twoFactor,
        totpLastStep: String(step + 1),
      });
      const error = await refusal(
        service.confirmIdentity(
          "user-1",
          PASSWORD,
          totpCodeForStep(secret, step),
          ACTION,
        ),
      );
      expect(error.getResponse()).toMatchObject({ error: "invalid-totp" });
    });

    it("lets exactly one of two parallel requests spend a code", async () => {
      const { service, users } = await build(twoFactor);
      users.update
        .mockResolvedValueOnce({ affected: 1 })
        .mockResolvedValueOnce({ affected: 0 });
      const code = totpCodeForStep(secret, totpStep());
      const results = await Promise.allSettled([
        service.confirmIdentity("user-1", PASSWORD, code, ACTION),
        service.confirmIdentity("user-1", PASSWORD, code, ACTION),
      ]);
      expect(results.map((r) => r.status).sort()).toEqual([
        "fulfilled",
        "rejected",
      ]);
    });
  });

  describe("the action limiter", () => {
    it("answers 429 with the wait after ten wrong attempts, before any password is checked", async () => {
      const { service, users } = await build();
      for (let attempt = 0; attempt < 10; attempt++) {
        await refusal(service.confirmIdentity("user-1", "wrong", "", ACTION));
      }
      users.findOne.mockClear();

      const error = await refusal(
        service.confirmIdentity("user-1", PASSWORD, undefined, ACTION),
      );
      expect(error.getStatus()).toBe(429);
      expect(error.getResponse()).toMatchObject({
        error: "rate-limited",
        retryAfterSeconds: expect.any(Number),
      });
      // The right password does not get past it, and costs no scrypt.
      expect(users.findOne).not.toHaveBeenCalled();
    });

    it("reaches the client as a 429 with the error, the wait and a Retry-After header", async () => {
      const { service } = await build();
      for (let attempt = 0; attempt < 10; attempt++) {
        await refusal(service.confirmIdentity("user-1", "wrong", "", ACTION));
      }
      const { body, headers } = await throughFilter(
        () => service.confirmIdentity("user-1", PASSWORD, undefined, ACTION),
        { url: "/v1/admin/auth/tokens" },
      );
      expect(body).toMatchObject({
        statusCode: 429,
        error: "rate-limited",
        retryAfterSeconds: expect.any(Number),
      });
      expect(Number(headers["Retry-After"])).toBeGreaterThan(0);
    });

    it("reaches the client as the three 401 errors a form acts on", async () => {
      const { service } = await build({
        totpEnabled: true,
        totpSecret: generateTotpSecret(),
      });
      const wrong = await throughFilter(() =>
        service.confirmIdentity("user-1", "nope", undefined, ACTION),
      );
      expect(wrong.body).toMatchObject({
        statusCode: 401,
        error: "invalid-password",
      });
      const missing = await throughFilter(() =>
        service.confirmIdentity("user-1", PASSWORD, undefined, ACTION),
      );
      expect(missing.body).toMatchObject({
        statusCode: 401,
        error: "totp-required",
      });
      const bad = await throughFilter(() =>
        service.confirmIdentity("user-1", PASSWORD, "000000", ACTION),
      );
      expect(bad.body).toMatchObject({
        statusCode: 401,
        error: "invalid-totp",
      });
    });

    it("forgets the failures once the account proves itself", async () => {
      const { service } = await build();
      for (let attempt = 0; attempt < 9; attempt++) {
        await refusal(service.confirmIdentity("user-1", "wrong", "", ACTION));
      }
      await service.confirmIdentity("user-1", PASSWORD, undefined, ACTION);
      // Nine more would have been refused with the old count.
      for (let attempt = 0; attempt < 9; attempt++) {
        const error = await refusal(
          service.confirmIdentity("user-1", "wrong", "", ACTION),
        );
        expect(error.getStatus()).toBe(401);
      }
    });

    it("counts per account and per action", async () => {
      const { service } = await build();
      for (let attempt = 0; attempt < 10; attempt++) {
        await refusal(service.confirmIdentity("user-1", "wrong", "", ACTION));
      }
      const other = await refusal(
        service.confirmIdentity("user-1", "wrong", "", "another-action"),
      );
      expect(other.getStatus()).toBe(401);
    });
  });
});
