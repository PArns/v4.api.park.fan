import type { AdminRole } from "./entities/admin-user.entity";

/**
 * Personal access tokens and OAuth grants: the credentials an MCP client holds
 * instead of an admin session. One place for their formats and lifetimes.
 */

/** A personal access token, created on the account page. */
export const PAT_PREFIX = "pfa_";
/** An OAuth access token, issued by `/v1/oauth/token`. */
export const OAUTH_ACCESS_PREFIX = "pfo_";
/** An OAuth refresh token. Never accepted as a bearer. */
export const OAUTH_REFRESH_PREFIX = "pfr_";

/** Characters of a token shown in the token list: the prefix plus four. */
export const DISPLAY_PREFIX_LENGTH = 8;

export const ACCESS_TOKEN_TTL_SECONDS = 60 * 60;
export const REFRESH_TOKEN_TTL_SECONDS = 30 * 24 * 60 * 60;

/** The lifetimes the token form offers; `null` is a token that never expires. */
export const PAT_EXPIRY_DAYS = [30, 90, 365] as const;

/**
 * Every scope a token can carry, and the most its holder may do with it.
 * v1 has one, read-only; the effective role is the lesser of this and the
 * account's own role, so a token never outranks the person who made it.
 */
export const ADMIN_TOKEN_SCOPES = {
  "admin:read": "viewer",
} as const satisfies Record<string, AdminRole>;

export type AdminTokenScope = keyof typeof ADMIN_TOKEN_SCOPES;
export const DEFAULT_ADMIN_TOKEN_SCOPE: AdminTokenScope = "admin:read";

export const ADMIN_API_TOKEN_KINDS = ["pat", "oauth"] as const;
export type AdminApiTokenKind = (typeof ADMIN_API_TOKEN_KINDS)[number];

/** What a stored secret is: a PAT, or one half of an OAuth grant. */
export type AdminApiTokenSecretKind = "pat" | "access" | "refresh";

/** Whether a bearer value has the shape of a token this API issued. */
export function looksLikeApiToken(value: string): boolean {
  return value.startsWith(PAT_PREFIX) || value.startsWith(OAUTH_ACCESS_PREFIX);
}
