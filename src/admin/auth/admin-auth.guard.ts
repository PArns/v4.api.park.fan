import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  Logger,
  UnauthorizedException,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { AdminSessionStore } from "./admin-session.store";
import { constantTimeEquals } from "./token-hash.util";
import {
  getRateLimitIp,
  type RequestLike,
} from "../../common/utils/request.util";
import { roleAtLeast, type AdminRole } from "./entities/admin-user.entity";
import type { AdminPrincipal, RequestWithAdmin } from "./admin-principal";
import {
  ADMIN_ALLOW_PENDING_PASSWORD_KEY,
  ADMIN_LEGACY_ALLOWED_KEY,
  ADMIN_MIN_ROLE_KEY,
  ADMIN_PUBLIC_KEY,
  ADMIN_TOKEN_ONLY_KEY,
} from "./admin-auth.decorators";
import { AdminApiTokenStore } from "./admin-api-token.store";
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
 * An endpoint marked `@AdminTokenOnly()` (the MCP server) has a third and
 * accepts nothing else: `Authorization: Bearer pfa_…|pfo_…`, a personal access
 * token or an OAuth access token, looked up in Postgres with its account and
 * capped at the token's role.
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
    private readonly apiTokens: AdminApiTokenStore,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const targets = [context.getHandler(), context.getClass()];

    if (this.reflector.getAllAndOverride<boolean>(ADMIN_PUBLIC_KEY, targets)) {
      return true;
    }

    const request = context.switchToHttp().getRequest<RequestWithAdmin>();
    const tokenOnly =
      this.reflector.getAllAndOverride<boolean>(
        ADMIN_TOKEN_ONLY_KEY,
        targets,
      ) === true;
    const principal = tokenOnly
      ? await this.fromApiToken(request)
      : ((await this.fromSession(request)) ?? this.fromLegacyPass(request));

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

    // A surface can refuse the shared pass whatever role it was given. See
    // `AdminLegacyAccess`: the pass defaults to owner, and owner is what
    // creating an admin account takes.
    if (
      principal.legacy &&
      this.reflector.getAllAndOverride<boolean>(
        ADMIN_LEGACY_ALLOWED_KEY,
        targets,
      ) === false
    ) {
      throw new ForbiddenException(
        "The deprecated shared pass cannot manage admin accounts — sign in with an owner account",
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

  private async fromApiToken(
    request: RequestWithAdmin,
  ): Promise<AdminPrincipal | null> {
    const token = bearerToken(request);
    if (!token) return null;
    return this.apiTokens.resolvePrincipal(token, clientIp(request));
  }

  private async fromSession(
    request: RequestWithAdmin,
  ): Promise<AdminPrincipal | null> {
    const token = bearerToken(request);
    if (!token) return null;

    const session = await this.sessions.resolve(token);
    if (!session) return null;

    return {
      userId: session.userId,
      email: session.email,
      displayName: session.displayName,
      role: session.role,
      sessionToken: token,
      sessionUserAgent: session.userAgent,
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
      role: getLegacyAdminPassRole(),
      sessionToken: null,
      legacy: true,
      ip: clientIp(request),
      mustChangePassword: false,
      mustEnrolTotp: false,
    };
  }
}

/** The value of `Authorization: Bearer …`, or null. */
export function bearerToken(request: RequestWithAdmin): string | null {
  const header = request.headers?.authorization;
  const raw = Array.isArray(header) ? header[0] : header;
  if (!raw || !raw.toLowerCase().startsWith("bearer ")) return null;
  const token = raw.slice(7).trim();
  return token || null;
}

/**
 * The requesting address: `getRateLimitIp`, the same reading the throttler and
 * every other limiter use (docs/deployment/edge-and-throttling.md).
 *
 * A request carrying a throttle-bypass key comes from our own frontend, so the
 * administrator's address is the first `x-forwarded-for` hop; its
 * `cf-connecting-ip` is the Vercel function, one address for every
 * administrator, which once put every login failure into one bucket. Anyone
 * else is `cf-connecting-ip` only when `CF_ORIGIN_SECRET` vouches for it, else
 * the socket. Every value is `isIP()`-checked, so a caller cannot open a fresh
 * rate-limit bucket per request with garbage.
 */
export function clientIp(request: RequestWithAdmin): string | null {
  return getRateLimitIp(request as unknown as RequestLike);
}
