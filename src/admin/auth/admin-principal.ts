import type { AdminRole } from "./entities/admin-user.entity";
import type { AdminApiTokenKind } from "./admin-api-token.constants";

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
