import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Patch,
  Post,
  Req,
  UnauthorizedException,
  UseGuards,
} from "@nestjs/common";
import { ApiOperation, ApiResponse, ApiTags } from "@nestjs/swagger";
import { Throttle } from "@nestjs/throttler";
import { AdminAuthService, toPublicUser } from "./admin-auth.service";
import { AdminSessionStore } from "./admin-session.store";
import { AdminAuditService } from "./admin-audit.service";
import { AdminAuthGuard } from "./admin-auth.guard";
import {
  AdminAllowPendingPassword,
  AdminMinRole,
  AdminPublic,
  CurrentAdmin,
} from "./admin-auth.decorators";
import type { AdminPrincipal, RequestWithAdmin } from "./admin-principal";
import {
  AdminChangePasswordDto,
  AdminCreateUserDto,
  AdminLoginDto,
  AdminResetPasswordDto,
  AdminTotpConfirmDto,
  AdminTotpDisableDto,
  AdminUpdateUserDto,
} from "./dto/auth.dto";

/**
 * Sessions and accounts for the admin surface.
 *
 * The session token is returned in the response body rather than set as a
 * cookie here, because this API never talks to the admin's browser: the
 * frontend proxies `/api/admin/*` server-side and holds the token in an
 * httpOnly cookie of its own. Two consequences worth stating, since they are
 * why this controller looks the way it does — the token never reaches
 * JavaScript in the browser, and CSRF is the frontend's problem to solve on
 * its own origin, not something a `SameSite` attribute set by api.park.fan
 * could help with.
 */
@ApiTags("admin")
@Controller("admin/auth")
@UseGuards(AdminAuthGuard)
export class AdminAuthController {
  constructor(
    private readonly auth: AdminAuthService,
    private readonly sessions: AdminSessionStore,
    private readonly audit: AdminAuditService,
  ) {}

  @Post("login")
  @AdminPublic()
  @HttpCode(HttpStatus.OK)
  // Ten attempts a minute per address, on top of the per-account lockout. The
  // two catch different things: this one a broad spray across many accounts
  // from one place, the lockout a focused guess at one account from many.
  @Throttle({ default: { limit: 10, ttl: 60_000 } })
  @ApiOperation({
    summary: "Sign in",
    description:
      "Returns an opaque session token. Send it as `Authorization: Bearer <token>` " +
      "on every other admin request. Answers `{ status: 'totp-required' }` when the " +
      "account has two-factor enabled and no code was supplied.",
  })
  @ApiResponse({
    status: 200,
    description: "Signed in, or a second factor is needed",
  })
  @ApiResponse({ status: 401, description: "Invalid credentials" })
  @ApiResponse({
    status: 429,
    description: "Too many attempts, or the account is locked",
  })
  async login(@Body() body: AdminLoginDto, @Req() request: RequestWithAdmin) {
    const userAgent = request.headers?.["user-agent"];
    const result = await this.auth.login({
      email: body.email,
      password: body.password,
      totpCode: body.totpCode,
      ip: clientIp(request),
      userAgent: typeof userAgent === "string" ? userAgent : null,
    });

    if (result.outcome === "totp-required") {
      return { status: "totp-required" as const };
    }
    if (result.outcome === "locked") {
      return {
        status: "locked" as const,
        retryAfterSeconds: result.retryAfterSeconds,
      };
    }
    if (result.outcome === "rate-limited") {
      return {
        status: "rate-limited" as const,
        retryAfterSeconds: result.retryAfterSeconds,
      };
    }

    await this.audit.record({
      actor: {
        userId: result.session.userId,
        email: result.session.email,
        displayName: result.session.displayName,
        role: result.session.role,
        sessionToken: null,
        legacy: false,
        ip: clientIp(request),
        mustChangePassword: result.session.mustChangePassword,
      },
      action: "auth.login",
      entityType: "admin_user",
      entityId: result.session.userId,
      entityLabel: result.session.email,
    });

    return {
      status: "ok" as const,
      token: result.token,
      expiresAt: new Date(result.session.expiresAt).toISOString(),
      user: {
        id: result.session.userId,
        email: result.session.email,
        displayName: result.session.displayName,
        role: result.session.role,
        mustChangePassword: result.session.mustChangePassword,
      },
    };
  }

  @Post("logout")
  @AdminAllowPendingPassword()
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Sign out of this session" })
  async logout(@CurrentAdmin() admin: AdminPrincipal): Promise<void> {
    if (admin.sessionToken) await this.auth.logout(admin.sessionToken);
  }

  @Get("me")
  @AdminAllowPendingPassword()
  @ApiOperation({
    summary: "Who this session belongs to",
    description:
      "The frontend calls this on every load to decide whether the stored " +
      "cookie is still worth anything, and what the signed-in account may do.",
  })
  async me(@CurrentAdmin() admin: AdminPrincipal) {
    if (admin.legacy) {
      return {
        id: null,
        email: admin.email,
        displayName: admin.displayName,
        role: admin.role,
        legacy: true,
        mustChangePassword: false,
        totpEnabled: false,
      };
    }
    const user = admin.userId ? await this.auth.findById(admin.userId) : null;
    if (!user) throw new UnauthorizedException("Session no longer valid");
    return { ...toPublicUser(user), legacy: false };
  }

  @Post("change-password")
  @AdminAllowPendingPassword()
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 5, ttl: 60_000 } })
  @ApiOperation({
    summary: "Change your own password",
    description:
      "Drops every other session of the account and re-issues this one; the " +
      "new token is in the response and replaces the old one.",
  })
  async changePassword(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: AdminChangePasswordDto,
  ) {
    if (!admin.userId) {
      throw new UnauthorizedException(
        "The shared legacy pass has no password to change",
      );
    }
    const { reissuedToken } = await this.auth.changeOwnPassword(
      admin.userId,
      body.currentPassword,
      body.newPassword,
      admin.sessionToken,
    );
    await this.audit.record({
      actor: admin,
      action: "auth.password.change",
      entityType: "admin_user",
      entityId: admin.userId,
      entityLabel: admin.email,
    });
    return { status: "ok" as const, token: reissuedToken };
  }

  // ── sessions ──────────────────────────────────────────────────────────────

  @Get("sessions")
  @ApiOperation({ summary: "Where this account is currently signed in" })
  async listSessions(@CurrentAdmin() admin: AdminPrincipal) {
    if (!admin.userId) return { sessions: [] };
    const sessions = await this.sessions.listForUser(
      admin.userId,
      admin.sessionToken ?? undefined,
    );
    return {
      sessions: sessions.map((s) => ({
        id: s.id,
        current: s.current,
        ip: s.ip,
        userAgent: s.userAgent,
        createdAt: new Date(s.createdAt).toISOString(),
        lastSeenAt: new Date(s.lastSeenAt).toISOString(),
        expiresAt: new Date(s.expiresAt).toISOString(),
      })),
    };
  }

  @Delete("sessions/:id")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Revoke one of your sessions" })
  async revokeSession(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    if (!admin.userId) return;
    await this.sessions.destroyByShortId(admin.userId, id);
  }

  @Post("sessions/revoke-all")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: "Sign out everywhere, including here" })
  async revokeAll(@CurrentAdmin() admin: AdminPrincipal) {
    if (!admin.userId) return { revoked: 0 };
    const revoked = await this.sessions.destroyAllForUser(admin.userId);
    await this.audit.record({
      actor: admin,
      action: "auth.sessions.revoke-all",
      entityType: "admin_user",
      entityId: admin.userId,
      entityLabel: admin.email,
    });
    return { revoked };
  }

  // ── two-factor ────────────────────────────────────────────────────────────

  @Post("totp/begin")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Start two-factor enrolment",
    description:
      "Returns the secret and its otpauth:// URI. Two-factor stays OFF until " +
      "a code from the app confirms the secret arrived intact.",
  })
  async beginTotp(@CurrentAdmin() admin: AdminPrincipal) {
    if (!admin.userId)
      throw new UnauthorizedException("No account behind this session");
    return this.auth.beginTotpEnrolment(admin.userId);
  }

  @Post("totp/confirm")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Confirm and enable two-factor" })
  async confirmTotp(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: AdminTotpConfirmDto,
  ): Promise<void> {
    if (!admin.userId)
      throw new UnauthorizedException("No account behind this session");
    await this.auth.confirmTotpEnrolment(admin.userId, body.code);
    await this.audit.record({
      actor: admin,
      action: "auth.totp.enable",
      entityType: "admin_user",
      entityId: admin.userId,
      entityLabel: admin.email,
    });
  }

  @Post("totp/disable")
  @HttpCode(HttpStatus.NO_CONTENT)
  @Throttle({ default: { limit: 5, ttl: 60_000 } })
  @ApiOperation({
    summary: "Turn two-factor off",
    description: "Needs the account password AND a current code.",
  })
  async disableTotp(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: AdminTotpDisableDto,
  ): Promise<void> {
    if (!admin.userId)
      throw new UnauthorizedException("No account behind this session");
    await this.auth.disableTotp(admin.userId, body.password, body.code);
    await this.audit.record({
      actor: admin,
      action: "auth.totp.disable",
      entityType: "admin_user",
      entityId: admin.userId,
      entityLabel: admin.email,
    });
  }

  // ── accounts (owner only) ─────────────────────────────────────────────────

  @Get("users")
  @AdminMinRole("owner")
  @ApiOperation({ summary: "All admin accounts" })
  async listUsers() {
    return { users: await this.auth.list() };
  }

  @Post("users")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: "Create an admin account",
    description:
      "The password is temporary: the account is created owing a change and " +
      "can reach nothing but the change-password endpoint until it makes one.",
  })
  async createUser(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: AdminCreateUserDto,
  ) {
    const user = await this.auth.create(body);
    await this.audit.record({
      actor: admin,
      action: "user.create",
      entityType: "admin_user",
      entityId: user.id,
      entityLabel: user.email,
      after: { email: user.email, role: user.role },
    });
    return user;
  }

  @Patch("users/:id")
  @AdminMinRole("owner")
  @ApiOperation({ summary: "Change an account's name, role or activation" })
  async updateUser(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
    @Body() body: AdminUpdateUserDto,
  ) {
    const before = await this.auth.findById(id);
    const user = await this.auth.update(id, body);
    await this.audit.record({
      actor: admin,
      action: "user.update",
      entityType: "admin_user",
      entityId: id,
      entityLabel: user.email,
      before: before
        ? {
            displayName: before.displayName,
            role: before.role,
            isActive: before.isActive,
          }
        : null,
      after: {
        displayName: user.displayName,
        role: user.role,
        isActive: user.isActive,
      },
    });
    return user;
  }

  @Post("users/:id/reset-password")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Reset somebody else's password",
    description:
      "Drops all of that account's sessions and requires it to choose a new " +
      "password at the next login.",
  })
  async resetPassword(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
    @Body() body: AdminResetPasswordDto,
  ) {
    const result = await this.auth.resetPassword(id, body.newPassword);
    const user = await this.auth.findById(id);
    await this.audit.record({
      actor: admin,
      action: "user.reset-password",
      entityType: "admin_user",
      entityId: id,
      entityLabel: user?.email ?? id,
    });
    return result;
  }
}

function clientIp(request: RequestWithAdmin): string | null {
  const cf = request.headers?.["cf-connecting-ip"];
  if (typeof cf === "string" && cf) return cf;
  const forwarded = request.headers?.["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded) {
    return forwarded.split(",")[0].trim();
  }
  return request.ip ?? null;
}
