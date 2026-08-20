import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  Logger,
  UnauthorizedException,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { timingSafeEqual } from "crypto";
import { isIP } from "net";
import { AdminSessionStore } from "./admin-session.store";
import { roleAtLeast, type AdminRole } from "./entities/admin-user.entity";
import type { AdminPrincipal, RequestWithAdmin } from "./admin-principal";
import {
  ADMIN_ALLOW_PENDING_PASSWORD_KEY,
  ADMIN_MIN_ROLE_KEY,
  ADMIN_PUBLIC_KEY,
} from "./admin-auth.decorators";
import {
  getLegacyAdminPass,
  getLegacyAdminPassRole,
  isLegacyAdminPassEnabled,
} from "../../config/admin-auth.config";

/**
 * The guard on every administrative endpoint.
 *
 * Before it, there was none. `/v1/admin/*` was documented as "protected in
 * production via Cloudflare", which is true of traffic that arrives through
 * Cloudflare and says nothing at all about traffic that does not — this
 * application never checked the `pass` parameter it advertised, in any
 * environment. Anything able to reach the origin could merge parks, retire
 * attractions or reset every cache.
 *
 * Two ways in, in this order:
 *
 *  1. `Authorization: Bearer <session token>` — a real session belonging to a
 *     real account. Resolved against Redis, so revocation is immediate.
 *  2. `?pass=<shared secret>` or `x-admin-pass` — the deprecated shared secret,
 *     kept alive for the maintenance scripts and runbooks that still send it.
 *     Compared in constant time, logged on every use, granted a configurable
 *     role, and switchable off with one environment variable.
 *
 * Cloudflare's rule stays where it is. This is the second lock, on the inside
 * of the same door.
 */
@Injectable()
export class AdminAuthGuard implements CanActivate {
  private readonly logger = new Logger(AdminAuthGuard.name);
  private legacyUseWarnedAt = 0;

  constructor(
    private readonly reflector: Reflector,
    private readonly sessions: AdminSessionStore,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const targets = [context.getHandler(), context.getClass()];

    if (this.reflector.getAllAndOverride<boolean>(ADMIN_PUBLIC_KEY, targets)) {
      return true;
    }

    const request = context.switchToHttp().getRequest<RequestWithAdmin>();
    const principal =
      (await this.fromSession(request)) ?? this.fromLegacyPass(request);

    if (!principal) {
      throw new UnauthorizedException(
        "Admin authentication required — sign in at /v1/admin/auth/login",
      );
    }

    const minRole =
      this.reflector.getAllAndOverride<AdminRole>(
        ADMIN_MIN_ROLE_KEY,
        targets,
      ) ?? "viewer";
    if (!roleAtLeast(principal.role, minRole)) {
      throw new ForbiddenException(
        `This action needs the "${minRole}" role or above; this account is "${principal.role}"`,
      );
    }

    if (principal.mustChangePassword || principal.mustEnrolTotp) {
      const allowed = this.reflector.getAllAndOverride<boolean>(
        ADMIN_ALLOW_PENDING_PASSWORD_KEY,
        targets,
      );
      if (!allowed) {
        throw new ForbiddenException(
          principal.mustChangePassword
            ? "This account must choose a new password before it can do anything else"
            : "This deployment requires two-factor: enrol before doing anything else",
        );
      }
    }

    request.admin = principal;
    return true;
  }

  private async fromSession(
    request: RequestWithAdmin,
  ): Promise<AdminPrincipal | null> {
    const header = request.headers?.authorization;
    const raw = Array.isArray(header) ? header[0] : header;
    if (!raw || !raw.toLowerCase().startsWith("bearer ")) return null;

    const token = raw.slice(7).trim();
    if (!token) return null;

    const session = await this.sessions.resolve(token);
    if (!session) return null;

    return {
      userId: session.userId,
      email: session.email,
      displayName: session.displayName,
      role: session.role,
      sessionToken: token,
      legacy: false,
      ip: clientIp(request),
      mustChangePassword: session.mustChangePassword,
      mustEnrolTotp: session.mustEnrolTotp === true,
    };
  }

  private fromLegacyPass(request: RequestWithAdmin): AdminPrincipal | null {
    if (!isLegacyAdminPassEnabled()) return null;

    const expected = getLegacyAdminPass();
    const headerPass = request.headers?.["x-admin-pass"];
    const provided =
      (typeof headerPass === "string" ? headerPass : undefined) ??
      (typeof request.query?.pass === "string"
        ? request.query.pass
        : undefined);

    if (!provided || !constantTimeEquals(provided, expected)) return null;

    // Once a minute at most: this path is hit by scheduled scripts, and a line
    // per request would bury everything else in the log.
    const now = Date.now();
    if (now - this.legacyUseWarnedAt > 60_000) {
      this.legacyUseWarnedAt = now;
      // The path WITHOUT its query string. `originalUrl` carries `?pass=…`,
      // and this line was writing the shared admin secret into the application
      // log in cleartext on every scripted call — readable by everyone with
      // access to the container logs, which is a far wider group than the
      // people who hold the secret.
      const path = (request.originalUrl ?? request.url ?? "?").split("?")[0];
      this.logger.warn(
        `⚠️  Deprecated shared admin pass used for ${request.method} ${path} — migrate the caller to a session token`,
      );
    }

    return {
      userId: null,
      email: "legacy-pass",
      displayName: "Legacy shared pass",
      role: getLegacyAdminPassRole() as AdminRole,
      sessionToken: null,
      legacy: true,
      ip: clientIp(request),
      mustChangePassword: false,
      mustEnrolTotp: false,
    };
  }
}

/** Length-safe constant-time comparison. */
function constantTimeEquals(a: string, b: string): boolean {
  if (!a || !b) return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  // timingSafeEqual throws on a length mismatch, which would itself leak the
  // length. Compare a fixed-size digest-shaped pair instead: pad both to the
  // longer length so the comparison always runs.
  const length = Math.max(bufA.length, bufB.length);
  const paddedA = Buffer.alloc(length);
  const paddedB = Buffer.alloc(length);
  bufA.copy(paddedA);
  bufB.copy(paddedB);
  return timingSafeEqual(paddedA, paddedB) && bufA.length === bufB.length;
}

/**
 * The requesting address, preferring Cloudflare's header — same order as
 * CfThrottlerGuard, and validated the same way for the same reason.
 *
 * Both headers are forgeable by anything reaching the origin directly, so an
 * unvalidated value lets a caller send a fresh garbage "address" per request:
 * every login failure then lands in its own rate-limit bucket, the per-address
 * limiter never fires, and each attempt leaves a Redis key alive for fifteen
 * minutes. Requiring a syntactically valid IP closes both — a forged but valid
 * address is still one of ~4 billion, not unbounded.
 */
export function clientIp(request: RequestWithAdmin): string | null {
  const cf = request.headers?.["cf-connecting-ip"];
  if (typeof cf === "string" && isIP(cf)) return cf;

  const forwarded = request.headers?.["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.length > 0) {
    const first = forwarded.split(",")[0].trim();
    if (isIP(first)) return first;
  }

  const direct = request.ip;
  return typeof direct === "string" && isIP(direct) ? direct : null;
}
