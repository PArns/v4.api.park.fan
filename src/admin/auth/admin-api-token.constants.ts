import { roleAtLeast, type AdminRole } from "./entities/admin-user.entity";

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
 * Every scope a token can carry, and the most its holder may do with it,
 * narrowest first. The effective role is the lesser of this and the
 * account's own role, so a token never outranks the person who made it.
 * `admin:write` reads too and stops at `editor`: curation and the editor's
 * actions. `admin:owner` reaches the owner-only actions too (merges,
 * retirement, maintenance, API keys) and is granted to an owner only.
 */
export const ADMIN_TOKEN_SCOPES = {
  "admin:read": "viewer",
  "admin:write": "editor",
  "admin:owner": "owner",
} as const satisfies Record<string, AdminRole>;

export type AdminTokenScope = keyof typeof ADMIN_TOKEN_SCOPES;
export const DEFAULT_ADMIN_TOKEN_SCOPE: AdminTokenScope = "admin:read";
export const WRITE_ADMIN_TOKEN_SCOPE: AdminTokenScope = "admin:write";
export const OWNER_ADMIN_TOKEN_SCOPE: AdminTokenScope = "admin:owner";

/** Every scope, narrowest first, for the discovery documents and the DTOs. */
export const ADMIN_TOKEN_SCOPE_NAMES = Object.keys(
  ADMIN_TOKEN_SCOPES,
) as AdminTokenScope[];

/** The narrowest scope whose cap reaches `role`: the one a tool of that role needs. */
export function scopeForRole(role: AdminRole): AdminTokenScope {
  return (
    ADMIN_TOKEN_SCOPE_NAMES.find((scope) =>
      roleAtLeast(ADMIN_TOKEN_SCOPES[scope], role),
    ) ?? OWNER_ADMIN_TOKEN_SCOPE
  );
}

/**
 * The scope an OAuth request asks for: the widest of the known scopes its
 * space-separated `scope` names, else `admin:read`. Anything else asked for
 * is not granted, which RFC 6749 §3.3 allows; the token response names what
 * was.
 */
export function requestedScope(raw: string | undefined): AdminTokenScope {
  const asked = (raw ?? "").split(/\s+/);
  return (
    [...ADMIN_TOKEN_SCOPE_NAMES]
      .reverse()
      .find((scope) => asked.includes(scope)) ?? DEFAULT_ADMIN_TOKEN_SCOPE
  );
}

/**
 * The scope an account may hand out: the one asked for, or the widest
 * narrower one whose cap the account reaches itself. An editor asking for
 * `admin:owner` gets `admin:write`, a viewer or author asking for either
 * gets `admin:read`, so a token's stored scope never claims what its account
 * could not do.
 */
export function grantableScope(
  scope: AdminTokenScope,
  role: AdminRole,
): AdminTokenScope {
  const names = ADMIN_TOKEN_SCOPE_NAMES;
  return (
    names
      .slice(0, names.indexOf(scope) + 1)
      .reverse()
      .find((name) => roleAtLeast(role, ADMIN_TOKEN_SCOPES[name])) ??
    DEFAULT_ADMIN_TOKEN_SCOPE
  );
}

export const ADMIN_API_TOKEN_KINDS = ["pat", "oauth"] as const;
export type AdminApiTokenKind = (typeof ADMIN_API_TOKEN_KINDS)[number];

/** What a stored secret is: a PAT, or one half of an OAuth grant. */
export type AdminApiTokenSecretKind = "pat" | "access" | "refresh";

/** Whether a bearer value has the shape of a token this API issued. */
export function looksLikeApiToken(value: string): boolean {
  return value.startsWith(PAT_PREFIX) || value.startsWith(OAUTH_ACCESS_PREFIX);
}
