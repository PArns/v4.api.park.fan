import { ExecutionContext, ForbiddenException, UnauthorizedException } from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { AdminAuthGuard } from "./admin-auth.guard";
import { AdminSessionStore } from "./admin-session.store";
import { RedisMock } from "../../../test/mocks/redis.mock";
import {
  ADMIN_ALLOW_PENDING_PASSWORD_KEY,
  ADMIN_MIN_ROLE_KEY,
  ADMIN_PUBLIC_KEY,
} from "./admin-auth.decorators";
import type { AdminRole } from "./entities/admin-user.entity";
import type { RequestWithAdmin } from "./admin-principal";

/** A context whose metadata answers come from a plain object. */
function contextFor(
  request: Partial<RequestWithAdmin>,
): ExecutionContext {
  const full = {
    headers: {},
    query: {},
    method: "POST",
    originalUrl: "/v1/admin/flush-cache",
    ...request,
  } as RequestWithAdmin;
  return {
    switchToHttp: () => ({ getRequest: () => full }),
    getHandler: () => function handler() {},
    getClass: () => class Controller {},
  } as unknown as ExecutionContext;
}

function reflectorReturning(
  metadata: Partial<Record<string, unknown>>,
): Reflector {
  return {
    getAllAndOverride: (key: string) => metadata[key],
  } as unknown as Reflector;
}

describe("AdminAuthGuard", () => {
  let sessions: AdminSessionStore;

  const LEGACY = "a-long-shared-secret-value";

  beforeEach(() => {
    sessions = new AdminSessionStore(new RedisMock() as never);
    delete process.env.ADMIN_LEGACY_PASS;
    delete process.env.ADMIN_LEGACY_PASS_ENABLED;
    delete process.env.ADMIN_LEGACY_PASS_ROLE;
  });

  async function tokenFor(
    role: AdminRole,
    mustChangePassword = false,
  ): Promise<string> {
    const { token } = await sessions.create({
      userId: "user-1",
      email: "you@park.fan",
      displayName: "You",
      role,
      ip: null,
      userAgent: null,
      mustChangePassword,
    });
    return token;
  }

  it("refuses a request with no credential at all", async () => {
    const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
    await expect(guard.canActivate(contextFor({}))).rejects.toBeInstanceOf(
      UnauthorizedException,
    );
  });

  it("lets a public endpoint through without one", async () => {
    const guard = new AdminAuthGuard(
      reflectorReturning({ [ADMIN_PUBLIC_KEY]: true }),
      sessions,
    );
    await expect(guard.canActivate(contextFor({}))).resolves.toBe(true);
  });

  it("accepts a bearer session and attaches the principal", async () => {
    const token = await tokenFor("editor");
    const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
    const request: Partial<RequestWithAdmin> = {
      headers: { authorization: `Bearer ${token}` },
    };
    const context = contextFor(request);

    await expect(guard.canActivate(context)).resolves.toBe(true);
    const attached = (
      context.switchToHttp().getRequest() as RequestWithAdmin
    ).admin;
    expect(attached).toMatchObject({
      userId: "user-1",
      role: "editor",
      legacy: false,
    });
  });

  it("refuses a bearer token that no longer resolves", async () => {
    const token = await tokenFor("editor");
    await sessions.destroy(token);
    const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
    await expect(
      guard.canActivate(contextFor({ headers: { authorization: `Bearer ${token}` } })),
    ).rejects.toBeInstanceOf(UnauthorizedException);
  });

  describe("roles", () => {
    it("enforces the declared minimum", async () => {
      const token = await tokenFor("author");
      const guard = new AdminAuthGuard(
        reflectorReturning({ [ADMIN_MIN_ROLE_KEY]: "owner" }),
        sessions,
      );
      await expect(
        guard.canActivate(
          contextFor({ headers: { authorization: `Bearer ${token}` } }),
        ),
      ).rejects.toBeInstanceOf(ForbiddenException);
    });

    it("treats the minimum as a floor, not an equality", async () => {
      // An owner must pass an endpoint that asks for "editor or above";
      // enumerating roles instead of ranking them is how that breaks.
      const token = await tokenFor("owner");
      const guard = new AdminAuthGuard(
        reflectorReturning({ [ADMIN_MIN_ROLE_KEY]: "editor" }),
        sessions,
      );
      await expect(
        guard.canActivate(
          contextFor({ headers: { authorization: `Bearer ${token}` } }),
        ),
      ).resolves.toBe(true);
    });

    it("lets any session read an endpoint that declares nothing", async () => {
      const token = await tokenFor("viewer");
      const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
      await expect(
        guard.canActivate(
          contextFor({ headers: { authorization: `Bearer ${token}` } }),
        ),
      ).resolves.toBe(true);
    });
  });

  describe("a session that owes a password change", () => {
    it("is refused everywhere by default", async () => {
      const token = await tokenFor("owner", true);
      const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
      await expect(
        guard.canActivate(
          contextFor({ headers: { authorization: `Bearer ${token}` } }),
        ),
      ).rejects.toBeInstanceOf(ForbiddenException);
    });

    it("reaches the endpoints that opt in", async () => {
      const token = await tokenFor("owner", true);
      const guard = new AdminAuthGuard(
        reflectorReturning({ [ADMIN_ALLOW_PENDING_PASSWORD_KEY]: true }),
        sessions,
      );
      await expect(
        guard.canActivate(
          contextFor({ headers: { authorization: `Bearer ${token}` } }),
        ),
      ).resolves.toBe(true);
    });
  });

  describe("the deprecated shared pass", () => {
    it("authenticates from the query string when configured", async () => {
      process.env.ADMIN_LEGACY_PASS = LEGACY;
      const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
      const context = contextFor({ query: { pass: LEGACY } });
      await expect(guard.canActivate(context)).resolves.toBe(true);
      expect(
        (context.switchToHttp().getRequest() as RequestWithAdmin).admin,
      ).toMatchObject({ legacy: true, userId: null, role: "owner" });
    });

    it("authenticates from the header too", async () => {
      process.env.ADMIN_LEGACY_PASS = LEGACY;
      const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
      await expect(
        guard.canActivate(contextFor({ headers: { "x-admin-pass": LEGACY } })),
      ).resolves.toBe(true);
    });

    it("refuses a wrong value, and one that is merely a prefix", async () => {
      process.env.ADMIN_LEGACY_PASS = LEGACY;
      const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
      for (const pass of ["wrong", LEGACY.slice(0, -1), LEGACY + "x", ""]) {
        await expect(
          guard.canActivate(contextFor({ query: { pass } })),
        ).rejects.toBeInstanceOf(UnauthorizedException);
      }
    });

    it("is unavailable when no secret is configured", async () => {
      const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
      await expect(
        guard.canActivate(contextFor({ query: { pass: "" } })),
      ).rejects.toBeInstanceOf(UnauthorizedException);
    });

    it("can be switched off while the secret is still set", async () => {
      process.env.ADMIN_LEGACY_PASS = LEGACY;
      process.env.ADMIN_LEGACY_PASS_ENABLED = "false";
      const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
      await expect(
        guard.canActivate(contextFor({ query: { pass: LEGACY } })),
      ).rejects.toBeInstanceOf(UnauthorizedException);
    });

    it("can be narrowed to a lesser role", async () => {
      process.env.ADMIN_LEGACY_PASS = LEGACY;
      process.env.ADMIN_LEGACY_PASS_ROLE = "viewer";
      const guard = new AdminAuthGuard(
        reflectorReturning({ [ADMIN_MIN_ROLE_KEY]: "owner" }),
        sessions,
      );
      await expect(
        guard.canActivate(contextFor({ query: { pass: LEGACY } })),
      ).rejects.toBeInstanceOf(ForbiddenException);
    });

    it("never wins over a valid session", async () => {
      process.env.ADMIN_LEGACY_PASS = LEGACY;
      const token = await tokenFor("viewer");
      const guard = new AdminAuthGuard(reflectorReturning({}), sessions);
      const context = contextFor({
        headers: { authorization: `Bearer ${token}` },
        query: { pass: LEGACY },
      });
      await expect(guard.canActivate(context)).resolves.toBe(true);
      expect(
        (context.switchToHttp().getRequest() as RequestWithAdmin).admin,
      ).toMatchObject({ legacy: false, role: "viewer" });
    });
  });
});
