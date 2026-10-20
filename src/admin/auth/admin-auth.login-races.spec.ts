import { UnauthorizedException } from "@nestjs/common";
import { FindOperator } from "typeorm";
import { AdminAuthService } from "./admin-auth.service";
import { AdminSessionStore } from "./admin-session.store";
import { AdminLoginRateLimitService } from "./admin-login-rate-limit.service";
import { RedisMock } from "../../../test/mocks/redis.mock";
import { hashPassword } from "./password.util";
import { generateTotpSecret, totpCodeForStep, totpStep } from "./totp.util";

/**
 * Logins that overlap. Each of these read a row, spent ~100 ms in scrypt, and
 * wrote back what it had read — so parallel requests never saw each other.
 */

const PASSWORD = "a-long-enough-password";

async function build(overrides: Record<string, unknown> = {}) {
  const row: Record<string, unknown> = {
    id: "user-1",
    email: "you@park.fan",
    displayName: "You",
    role: "editor",
    isActive: true,
    mustChangePassword: false,
    failedLoginCount: 0,
    lockedUntil: null,
    totpEnabled: false,
    totpSecret: null,
    totpLastStep: null,
    passwordHash: await hashPassword(PASSWORD),
    ...overrides,
  };

  const users = {
    // A copy per read, as a real query returns.
    findOne: jest.fn(async (): Promise<unknown> => ({ ...row })),
    update: jest.fn(
      async (criteria: Record<string, unknown>, fields: object) => {
        // The TOTP claim is conditional; evaluate it the way Postgres would.
        if (criteria.totpLastStep instanceof FindOperator) {
          const stored = row.totpLastStep as string | null;
          const next = (fields as { totpLastStep: string }).totpLastStep;
          if (stored !== null && Number(stored) >= Number(next)) {
            return { affected: 0 };
          }
        }
        Object.assign(row, fields);
        return { affected: 1 };
      },
    ),
    // The atomic failure counter, done the way the SQL does it.
    query: jest.fn(async (_sql: string, params: unknown[]) => {
      const [, threshold, minutes] = params as [string, number, number];
      row.failedLoginCount = (row.failedLoginCount as number) + 1;
      if ((row.failedLoginCount as number) >= threshold) {
        row.lockedUntil = new Date(Date.now() + minutes * 60_000);
      }
      return [
        [
          {
            failed_login_count: row.failedLoginCount,
            locked_until: row.lockedUntil,
          },
        ],
        1,
      ];
    }),
    count: jest.fn(async (): Promise<number> => 1),
  };

  const sessions = new AdminSessionStore(new RedisMock() as never);
  const limiter = new AdminLoginRateLimitService(new RedisMock() as never);
  const service = new AdminAuthService(users as never, sessions, limiter);
  return { service, sessions, users, row };
}

describe("AdminAuthService login races", () => {
  it("locks the account when the wrong passwords arrive in parallel", async () => {
    const { service, row } = await build();

    const results = await Promise.allSettled(
      Array.from({ length: 8 }, (_, i) =>
        service.login({
          email: "you@park.fan",
          password: `wrong-${i}`,
          ip: `198.51.100.${i}`,
        }),
      ),
    );

    expect(results.every((r) => r.status === "rejected")).toBe(true);
    expect(row.failedLoginCount).toBe(8);
    expect((row.lockedUntil as Date).getTime()).toBeGreaterThan(Date.now());
  });

  it("accepts a TOTP code once, even when it is replayed in parallel", async () => {
    const secret = generateTotpSecret();
    const { service } = await build({ totpEnabled: true, totpSecret: secret });
    const code = totpCodeForStep(secret, totpStep());

    const results = await Promise.allSettled([
      service.login({
        email: "you@park.fan",
        password: PASSWORD,
        totpCode: code,
      }),
      service.login({
        email: "you@park.fan",
        password: PASSWORD,
        totpCode: code,
      }),
    ]);

    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    const rejected = results.find((r) => r.status === "rejected");
    expect((rejected as PromiseRejectedResult).reason).toBeInstanceOf(
      UnauthorizedException,
    );
  });

  it("drops the session when the account was deactivated mid-login", async () => {
    const { service, sessions, users, row } = await build();
    // The first read sees an active account; by the time the session exists,
    // an owner has deactivated it.
    users.findOne.mockImplementationOnce(async () => {
      const snapshot = { ...row };
      row.isActive = false;
      return snapshot;
    });
    const create = jest.spyOn(sessions, "create");

    await expect(
      service.login({ email: "you@park.fan", password: PASSWORD }),
    ).rejects.toThrow(UnauthorizedException);
    const { token } = await create.mock.results[0].value;
    expect(await sessions.resolve(token)).toBeNull();
  });

  it("issues the role the account has now, not the one it had at the start", async () => {
    const { service, users, row } = await build({ role: "owner" });
    users.findOne.mockImplementationOnce(async () => {
      const snapshot = { ...row };
      row.role = "viewer";
      return snapshot;
    });

    const result = await service.login({
      email: "you@park.fan",
      password: PASSWORD,
    });
    expect(result.outcome === "ok" && result.session.role).toBe("viewer");
  });
});
