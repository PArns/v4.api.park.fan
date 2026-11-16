import { UnauthorizedException } from "@nestjs/common";
import type { AdminRole } from "./entities/admin-user.entity";
import type { AdminApiTokenKind } from "./admin-api-token.constants";
import type { AdminSession } from "./admin-session.store";

/**
 * Who is making this request, as every admin endpoint sees it.
 *
 * One shape for every authentication path — a real session, an API token and
 * the legacy shared pass — so that no endpoint has to know which one it got. The legacy
 * path is the reason `userId` is nullable: a shared secret has no person
 * behind it, and an audit row that pretends otherwise is worse than one that
 * says `legacy-pass` out loud.
 */
export interface AdminPrincipal {
  userId: string | null;
  email: string;
  displayName: string;
  role: AdminRole;
  /** The opaque session token, when this request came in on one. */
  sessionToken: string | null;
  /**
   * The User-Agent the session signed in with. Requests reach the API through
   * the admin proxy, whose own User-Agent says nothing about the browser.
   */
  sessionUserAgent?: string | null;
  /** True when authenticated by the deprecated shared `pass=` secret. */
  legacy: boolean;
  ip: string | null;
  mustChangePassword: boolean;
  /** True while `ADMIN_REQUIRE_TOTP` is on and this account has not enrolled. */
  mustEnrolTotp: boolean;
  /**
   * Set when the request came in on a personal access token or an OAuth access
   * token rather than a session. `role` is then already capped by the token.
   */
  apiToken?: {
    id: string;
    kind: AdminApiTokenKind;
    clientName: string | null;
    scope: string;
  };
}

/** Express's Request, once the guard has attached the principal. */
export interface RequestWithAdmin {
  admin?: AdminPrincipal;
  headers: Record<string, string | string[] | undefined>;
  query: Record<string, unknown>;
  ip?: string;
  method: string;
  originalUrl?: string;
  url?: string;
}

/** The account behind a session; the guard has already refused the shared pass. */
export function requireAccount(admin: AdminPrincipal): string {
  if (!admin.userId) {
    throw new UnauthorizedException("No account behind this session");
  }
  return admin.userId;
}

/**
 * The principal a session stands for: the guard's for a request carrying the session's token, and
 * the audit actor of a sign-in or registration, which carries none yet (`token: null`).
 */
export function principalFromSession(
  session: AdminSession,
  ip: string | null,
  token: string | null,
): AdminPrincipal {
  return {
    userId: session.userId,
    email: session.email,
    displayName: session.displayName,
    role: session.role,
    sessionToken: token,
    ...(token && { sessionUserAgent: session.userAgent }),
    legacy: false,
    ip,
    mustChangePassword: session.mustChangePassword,
    mustEnrolTotp: session.mustEnrolTotp === true,
  };
}

/** The login's answer for a new session: the token, when it ends, and who it is. */
export function sessionAnswer(token: string, session: AdminSession) {
  return {
    status: "ok" as const,
    token,
    expiresAt: new Date(session.expiresAt).toISOString(),
    user: {
      id: session.userId,
      email: session.email,
      displayName: session.displayName,
      role: session.role,
      mustChangePassword: session.mustChangePassword,
      mustEnrolTotp: session.mustEnrolTotp === true,
    },
  };
}
