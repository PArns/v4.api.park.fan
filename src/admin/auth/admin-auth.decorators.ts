import {
  SetMetadata,
  createParamDecorator,
  type ExecutionContext,
} from "@nestjs/common";
import type { AdminRole } from "./entities/admin-user.entity";
import type { AdminPrincipal, RequestWithAdmin } from "./admin-principal";

export const ADMIN_PUBLIC_KEY = "adminPublic";
export const ADMIN_MIN_ROLE_KEY = "adminMinRole";
export const ADMIN_ALLOW_PENDING_PASSWORD_KEY = "adminAllowPendingPassword";
export const ADMIN_LEGACY_ALLOWED_KEY = "adminLegacyAllowed";

/**
 * Marks an endpoint as reachable without a session — the login form, and
 * nothing else worth adding lightly. The guard is registered on the whole
 * admin surface, so an endpoint is protected unless it says otherwise: a
 * forgotten decorator locks something down rather than opening it up.
 */
export const AdminPublic = (): MethodDecorator & ClassDecorator =>
  SetMetadata(ADMIN_PUBLIC_KEY, true);

/**
 * The least privileged role that may call this endpoint.
 *
 * Stated as a minimum rather than a list, so adding a role above an existing
 * one does not silently exclude it from every endpoint written before it
 * existed. Undecorated endpoints require `viewer`, i.e. any valid session.
 */
export const AdminMinRole = (
  role: AdminRole,
): MethodDecorator & ClassDecorator => SetMetadata(ADMIN_MIN_ROLE_KEY, role);

/**
 * Lets an endpoint be reached by a session that still owes a password change.
 *
 * Exactly three do: the one that changes it, the one that reports who you are,
 * and logout. Everything else refuses, because a temporary password handed
 * over in a chat message should not be able to retire an attraction.
 */
export const AdminAllowPendingPassword = (): MethodDecorator =>
  SetMetadata(ADMIN_ALLOW_PENDING_PASSWORD_KEY, true);

/**
 * Whether the deprecated shared pass may reach this endpoint.
 *
 * Three states, and the third is the point: unset means "no opinion", which is
 * how every job and merge endpoint the runbooks call keeps working. `false` on
 * a controller class refuses the shared pass for everything on it, and `true`
 * on one handler brings that handler back — `getAllAndOverride` reads the
 * handler before the class.
 *
 * It exists because the shared pass runs as `owner` by default, and owner is
 * what `POST auth/users` asks for. A secret that lives in a runbook and a
 * Cloudflare rule could therefore mint a real, permanent, password-holding
 * admin account, and the audit row would say `legacy-pass` — visibly nobody.
 * Account management is the one surface where "whoever has the secret" is not
 * an acceptable answer, so the auth controller says `false` and names its two
 * exceptions rather than waiting to be told about a new endpoint.
 */
export const AdminLegacyAccess = (
  allowed: boolean,
): MethodDecorator & ClassDecorator =>
  SetMetadata(ADMIN_LEGACY_ALLOWED_KEY, allowed);

/** Injects the authenticated principal into a handler parameter. */
export const CurrentAdmin = createParamDecorator(
  (_data: unknown, context: ExecutionContext): AdminPrincipal => {
    const request = context.switchToHttp().getRequest<RequestWithAdmin>();
    if (!request.admin) {
      // Unreachable behind the guard; thrown rather than returning a fake
      // principal so a future @AdminPublic endpoint that asks for one fails
      // loudly instead of writing audit rows attributed to nobody.
      throw new Error(
        "CurrentAdmin used on an endpoint without AdminAuthGuard",
      );
    }
    return request.admin;
  },
);
