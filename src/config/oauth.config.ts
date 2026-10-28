import { getRevalidateUrl } from "./revalidation.config";

/**
 * Where the admin MCP's OAuth surface lives and whom it sends people to.
 *
 * Functions, like every config file here, so a test can set the variables
 * after import and so `.env` is read after `ConfigModule` has loaded it.
 */

const DEFAULT_API_PUBLIC_URL = "https://api.park.fan";

/** Hosts an `https` redirect URI may name when `OAUTH_REDIRECT_HOSTS` is unset. */
export const DEFAULT_OAUTH_REDIRECT_HOSTS = ["claude.ai", "claude.com"];

function withoutTrailingSlash(url: string): string {
  return url.replace(/\/+$/, "");
}

/**
 * This API's public origin, which is the OAuth issuer and the base of the MCP
 * resource URL. Both are compared byte for byte by clients, so a deployment
 * behind another host name has to say so.
 */
export function getApiPublicUrl(): string {
  return withoutTrailingSlash(
    process.env.API_PUBLIC_URL?.trim() || DEFAULT_API_PUBLIC_URL,
  );
}

/**
 * The frontend's origin, where the consent page is. Falls back to the origin
 * of `REVALIDATE_URL`, the one frontend address this API already knows.
 */
export function getFrontendUrl(): string {
  const configured = process.env.FRONTEND_URL?.trim();
  if (configured) return withoutTrailingSlash(configured);
  return new URL(getRevalidateUrl()).origin;
}

/** The protected resource: the MCP endpoint. Tokens are issued for it alone. */
export function getMcpResourceUrl(): string {
  return `${getApiPublicUrl()}/v1/mcp`;
}

/** Where the resource's metadata is, as the 401 challenge names it. */
export function getMcpResourceMetadataUrl(): string {
  return `${getApiPublicUrl()}/.well-known/oauth-protected-resource/v1/mcp`;
}

/**
 * Host names an `https` redirect URI may point at, lowercased. `*` allows any
 * host. Loopback redirects (Claude Code, the MCP Inspector) are always allowed
 * and are not listed here.
 *
 * A narrow default because the consent page is the only thing between a
 * phishing link and a grant: with any host allowed, a stranger can register a
 * client named "Claude" that redirects to their own server and send an admin
 * the consent link. Limited to claude.ai, the code lands in the admin's own
 * claude.ai session, which checks `state` against it.
 */
export function getOAuthRedirectHosts(): string[] | "*" {
  const configured = (process.env.OAUTH_REDIRECT_HOSTS ?? "")
    .split(",")
    .map((host) => host.trim().toLowerCase())
    .filter((host) => host.length > 0);
  if (configured.includes("*")) return "*";
  return configured.length > 0 ? configured : DEFAULT_OAUTH_REDIRECT_HOSTS;
}
