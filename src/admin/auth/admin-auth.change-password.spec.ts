import { UnauthorizedException } from "@nestjs/common";
import { AdminAuthService } from "./admin-auth.service";
import { AdminSessionStore } from "./admin-session.store";
import { AdminLoginRateLimitService } from "./admin-login-rate-limit.service";
import { RedisMock } from "../../../test/mocks/redis.mock";
import { hashPassword } from "./password.util";

/**
 * What a password change does to the session it hands back.
 *
 * It is the one endpoint a half-authorised session may reach — the guard lets
 * `mustChangePassword` and `mustEnrolTotp` sessions through to it and to
 * nothing else — so what it writes onto the replacement session is a gate, not
 * bookkeeping. It re-issued one without the enrolment debt, which read as
 * "already enrolled": under `ADMIN_REQUIRE_TOTP` a password change was a way
 * around the second factor, and nothing in the suite touched this service.
 */

const OLD_PASSWORD = "the-old-one-is-long";
const NEW_PASSWORD = "and-so-is-the-new-one";

async function build(overrides: Record<string, unknown> = {}) {
  const user = {
    id: "user-1",
    email: "you@park.fan",
    displayName: "You",
    role: "owner" as const,
    passwordHash: await hashPassword(OLD_PASSWORD),
    totpEnabled: false,
    ...overrides,
  };

  const users = {
    findOne: jest.fn(async (): Promise<unknown> => user),
    update: jest.fn(async (): Promise<unknown> => ({ affected: 1 })),
    count: jest.fn(async (): Promise<number> => 1),
  };

  const sessions = new AdminSessionStore(new RedisMock() as never);
  const limiter = new AdminLoginRateLimitService(new RedisMock() as never);
  const service = new AdminAuthService(
    users as never,
    sessions,
    limiter as never,
  );

  // The session the change is made from — the one that survives.
  const { token } = await sessions.create({
    userId: user.id,
    email: user.email,
    displayName: user.displayName,
    role: user.role,
    ip: null,
    userAgent: null,
    mustChangePassword: true,
    mustEnrolTotp: true,
  });

  return { service, sessions, users, token };
}

describe("changeOwnPassword", () => {
  const previous = process.env.ADMIN_REQUIRE_TOTP;

  afterEach(() => {
    if (previous === undefined) delete process.env.ADMIN_REQUIRE_TOTP;
    else process.env.ADMIN_REQUIRE_TOTP = previous;
  });

  it("keeps the enrolment debt on the re-issued session", async () => {
    process.env.ADMIN_REQUIRE_TOTP = "true";
    const { service, sessions, token } = await build({ totpEnabled: false });

    const { reissuedToken } = await service.changeOwnPassword(
      "user-1",
      OLD_PASSWORD,
      NEW_PASSWORD,
      token,
    );
    expect(reissuedToken).toBeTruthy();

    const session = await sessions.resolve(reissuedToken!);
    expect(session?.mustEnrolTotp).toBe(true);
    // The debt it DID clear is the right one.
    expect(session?.mustChangePassword).toBe(false);
  });

  it("does not invent a debt for an account that has enrolled", async () => {
    process.env.ADMIN_REQUIRE_TOTP = "true";
    const { service, sessions, token } = await build({ totpEnabled: true });

    const { reissuedToken } = await service.changeOwnPassword(
      "user-1",
      OLD_PASSWORD,
      NEW_PASSWORD,
      token,
    );
    const session = await sessions.resolve(reissuedToken!);
    expect(session?.mustEnrolTotp).toBe(false);
  });

  it("owes nothing where the deployment does not require a second factor", async () => {
    delete process.env.ADMIN_REQUIRE_TOTP;
    const { service, sessions, token } = await build({ totpEnabled: false });

    const { reissuedToken } = await service.changeOwnPassword(
      "user-1",
      OLD_PASSWORD,
      NEW_PASSWORD,
      token,
    );
    const session = await sessions.resolve(reissuedToken!);
    expect(session?.mustEnrolTotp).toBe(false);
  });

  it("ends every other session of the account", async () => {
    const { service, sessions, token } = await build();
    const other = await sessions.create({
      userId: "user-1",
      email: "you@park.fan",
      displayName: "You",
      role: "owner",
      ip: null,
      userAgent: null,
      mustChangePassword: false,
      mustEnrolTotp: false,
    });

    await service.changeOwnPassword(
      "user-1",
      OLD_PASSWORD,
      NEW_PASSWORD,
      token,
    );

    expect(await sessions.resolve(other.token)).toBeNull();
    // Including the one it was made from: the replacement is a new token.
    expect(await sessions.resolve(token)).toBeNull();
  });

  it("reports the new session's own expiry, not the idle window", async () => {
    // The caller writes a cookie from this. Twelve hours was the backend's
    // sliding idle window, which does not slide in a Max-Age — an admin who
    // changed their password was signed out that evening with six days left.
    const { service } = await build();
    const { expiresAt } = await service.changeOwnPassword(
      "user-1",
      OLD_PASSWORD,
      NEW_PASSWORD,
      "no-such-token-so-nothing-is-kept",
    );
    // No surviving session to re-issue, so no date either.
    expect(expiresAt).toBeNull();
  });

  it("refuses a wrong current password", async () => {
    const { service, users } = await build();
    await expect(
      service.changeOwnPassword("user-1", "not-it-at-all", NEW_PASSWORD, null),
    ).rejects.toBeInstanceOf(UnauthorizedException);
    expect(users.update).not.toHaveBeenCalled();
  });
});
