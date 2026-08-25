import { Logger } from "@nestjs/common";
import {
  ADMIN_ROLES,
  type AdminRole,
} from "../admin/auth/entities/admin-user.entity";
import { getThrottleBypassKeys } from "../common/throttler/throttler.config";

/**
 * Admin authentication config.
 *
 * Functions rather than constants so tests can set the env vars after import,
 * matching revalidation.config.ts and ml-services.config.ts.
 *
 * Background for the legacy switch below: before named accounts existed, every
 * `/v1/admin/*` endpoint was gated on a single `pass=` query secret that this
 * application never checked — a Cloudflare rule did, in production only. Any
 * request that reached the origin directly could merge parks, retire
 * attractions or flush every cache with no credential whatsoever, and nothing
 * that happened was attributable to anyone. Named sessions replace that, but
 * the shared secret cannot simply vanish: it is what the maintenance scripts
 * and the curation runbooks in todo.md still send. So it stays, demoted to a
 * deprecated path that logs every use, and can be switched off the day nothing
 * depends on it.
 */

/** The deprecated shared secret. Empty ⇒ the legacy path is unavailable. */
export function getLegacyAdminPass(): string {
  return process.env.ADMIN_LEGACY_PASS || "";
}

/**
 * Whether the deprecated `?pass=` path may still authenticate.
 *
 * Defaults to enabled when a secret is configured, so an existing deployment
 * that sets nothing new keeps working. Set `ADMIN_LEGACY_PASS_ENABLED=false`
 * to close it.
 */
export function isLegacyAdminPassEnabled(): boolean {
  if (process.env.ADMIN_LEGACY_PASS_ENABLED === "false") return false;
  return getLegacyAdminPass().length > 0;
}

/**
 * The role the legacy shared secret is granted.
 *
 * `owner` by default because that is what it effectively had — every endpoint,
 * no exceptions — and quietly narrowing it would break the runbooks it exists
 * to keep working. Narrow it deliberately with `ADMIN_LEGACY_PASS_ROLE=editor`
 * once the destructive scripts have accounts of their own.
 *
 * A value that is not one of the four roles falls back to the default and says
 * so. Passed through unchecked it reached `roleAtLeast` as an unknown key, so
 * `ADMIN_ROLE_RANK[role] >= 0` compared `undefined` and every runbook call was
 * refused with "this account is \"admin\"" — on endpoints with no role
 * requirement at all, and with nothing at boot to point at the typo.
 */
export function getLegacyAdminPassRole(): AdminRole {
  const configured = process.env.ADMIN_LEGACY_PASS_ROLE?.trim();
  if (!configured) return "owner";
  if ((ADMIN_ROLES as readonly string[]).includes(configured)) {
    return configured as AdminRole;
  }
  if (!warnedAboutLegacyRole) {
    warnedAboutLegacyRole = true;
    new Logger("AdminAuthConfig").error(
      `ADMIN_LEGACY_PASS_ROLE="${configured}" is not one of ${ADMIN_ROLES.join(
        ", ",
      )} — falling back to "owner"`,
    );
  }
  return "owner";
}

let warnedAboutLegacyRole = false;

/**
 * The first account, created on boot when the table is empty.
 *
 * Without it a fresh deployment has no way in: the endpoints that create
 * accounts require an owner session, and there is no owner. Both variables must
 * be set; the password is used once and should be changed at the first login
 * (the account is created owing a password change, so the API enforces that
 * rather than trusting the operator to remember).
 */
export function getBootstrapAdmin(): {
  email: string;
  password: string;
} | null {
  const email = process.env.ADMIN_BOOTSTRAP_EMAIL?.trim().toLowerCase();
  const password = process.env.ADMIN_BOOTSTRAP_PASSWORD;
  if (!email || !password) return null;
  return { email, password };
}

/**
 * How many consecutive failed logins lock an account, and for how long.
 *
 * Per account rather than per IP, because the frontend proxies every admin
 * call server-side: from this API's vantage point a password-spraying browser
 * and a legitimate one share an address, so an IP counter would either never
 * fire or lock out the only person using it.
 */
export function getLoginLockoutThreshold(): number {
  const raw = Number.parseInt(
    process.env.ADMIN_LOGIN_LOCKOUT_THRESHOLD ?? "",
    10,
  );
  return Number.isFinite(raw) && raw > 0 ? raw : 8;
}

export function getLoginLockoutMinutes(): number {
  const raw = Number.parseInt(
    process.env.ADMIN_LOGIN_LOCKOUT_MINUTES ?? "",
    10,
  );
  return Number.isFinite(raw) && raw > 0 ? raw : 15;
}

/** Whether new accounts must enrol in TOTP before they can do anything. */
export function isTotpRequired(): boolean {
  return process.env.ADMIN_REQUIRE_TOTP === "true";
}

/**
 * The Turnstile secret the login verifies tokens against.
 *
 * `ADMIN_TURNSTILE_SECRET_KEY` first so this can be pointed at its own widget;
 * `TURNSTILE_SECRET_KEY` after it, because the frontend already calls it that
 * and a deployment sharing one widget between the upload form and the login
 * should not have to set the same value twice under two names.
 */
export function getAdminTurnstileSecret(): string {
  return (
    process.env.ADMIN_TURNSTILE_SECRET_KEY?.trim() ||
    process.env.TURNSTILE_SECRET_KEY?.trim() ||
    ""
  );
}

/** How long siteverify gets before the login gives up on it. */
export function getAdminTurnstileTimeoutMs(): number {
  const raw = Number.parseInt(process.env.ADMIN_TURNSTILE_TIMEOUT_MS ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : 5000;
}

/**
 * Whether the login demands a solved challenge from callers who are not our
 * frontend.
 *
 * Two conditions, and the second is the one that matters. Enforcement needs a
 * secret to check tokens against — and it needs a way to tell our own frontend
 * apart from a stranger, which is `THROTTLE_BYPASS_KEYS`. With no bypass keys
 * configured every caller looks like a stranger, including park.fan's admin
 * proxy, which verifies the challenge on its own side and therefore sends no
 * token here. Enforcing in that state would refuse every login there is.
 *
 * So it stays off until both are set, and says so once at boot rather than
 * failing quietly in either direction. Turning it on is two env vars and no
 * code change on either side.
 */
export function isAdminLoginTurnstileEnforced(): boolean {
  if (process.env.ADMIN_LOGIN_TURNSTILE === "false") return false;
  if (!getAdminTurnstileSecret()) return false;
  return getThrottleBypassKeys().length > 0;
}
