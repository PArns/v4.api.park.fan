/**
 * Which redirect URIs a client may register and use. An authorization code is
 * sent to this address, so it is the one value in the flow that decides who
 * receives the grant.
 *
 * - `https://<host><path>` where the host is on `OAUTH_REDIRECT_HOSTS` (claude.ai
 *   and claude.com by default) and the path on `OAUTH_REDIRECT_PATHS`
 *   (`/api/mcp/auth_callback` by default) with no query; `*` lifts either list;
 * - `http://127.0.0.1|localhost|[::1][:port]/…`, a loopback listener such as
 *   Claude Code's (RFC 8252 §7.3);
 * - nothing else: no other scheme, no user info, no fragment.
 */

export const MAX_REDIRECT_URI_LENGTH = 2000;

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]"]);

export type RedirectUriVerdict = { ok: true } | { ok: false; reason: string };

/** Where an https redirect may point; see `getOAuthRedirectPolicy`. */
export interface RedirectPolicy {
  hosts: string[] | "*";
  paths: string[] | "*";
}

function parse(uri: string): URL | null {
  try {
    return new URL(uri);
  } catch {
    return null;
  }
}

export function isLoopbackRedirect(url: URL): boolean {
  return url.protocol === "http:" && LOOPBACK_HOSTS.has(url.hostname);
}

/** Whether `uri` may be registered as a redirect URI. */
export function checkRedirectUri(
  uri: unknown,
  policy: RedirectPolicy,
): RedirectUriVerdict {
  if (typeof uri !== "string" || uri.length === 0) {
    return { ok: false, reason: "a redirect URI must be a string" };
  }
  if (uri.length > MAX_REDIRECT_URI_LENGTH) {
    return { ok: false, reason: "the redirect URI is too long" };
  }
  if (uri.includes("#")) {
    return { ok: false, reason: "a redirect URI must not have a fragment" };
  }
  const url = parse(uri);
  if (!url) return { ok: false, reason: "the redirect URI is not a URL" };
  if (url.username || url.password) {
    return { ok: false, reason: "a redirect URI must not carry user info" };
  }
  if (isLoopbackRedirect(url)) return { ok: true };
  if (url.protocol !== "https:") {
    return {
      ok: false,
      reason: "a redirect URI must be https, or http on a loopback address",
    };
  }
  if (policy.hosts !== "*" && !policy.hosts.includes(url.hostname)) {
    return {
      ok: false,
      reason: `redirects to ${url.hostname} are not allowed on this server`,
    };
  }
  if (
    policy.paths !== "*" &&
    (!policy.paths.includes(url.pathname) ||
      url.search !== "" ||
      url.port !== "")
  ) {
    return {
      ok: false,
      reason: `only ${policy.paths.join(", ")} may receive codes on ${url.hostname}`,
    };
  }
  return { ok: true };
}

/**
 * Whether a requested redirect URI is one the client registered. Exact string
 * equality, except that a loopback URI may name another port (RFC 8252 §7.3:
 * a native client takes whichever port is free when it starts listening).
 */
export function isRegisteredRedirect(
  requested: string,
  registered: readonly string[],
): boolean {
  if (registered.includes(requested)) return true;

  const url = parse(requested);
  if (!url || !isLoopbackRedirect(url) || requested.includes("#")) return false;
  if (url.username || url.password) return false;

  return registered.some((candidate) => {
    const known = parse(candidate);
    return (
      known !== null &&
      isLoopbackRedirect(known) &&
      known.hostname === url.hostname &&
      known.pathname === url.pathname &&
      known.search === url.search
    );
  });
}
