import { McpToolError } from "../mcp-tool";
import {
  getRevalidateOrigin,
  getRevalidateSecret,
  isRevalidationEnabled,
} from "../../config/revalidation.config";

/**
 * park.fan's own server routes that admin tools call, such as the app's news
 * export check. They take the revalidation secret as the bearer, the one
 * credential the site and this API already share, so they are called at the
 * origin of `REVALIDATE_URL` and nowhere else: `FRONTEND_URL` may name another
 * host, and that host would be handed the secret.
 */

/** The news-export check reads twelve lists from the bucket before it answers. */
export const SITE_ROUTE_TIMEOUT_MS = 15_000;

/** A report is a few kilobytes; a body past this is not one. */
const SITE_ROUTE_MAX_CHARS = 1_000_000;

/** Named, so park.fan's logs can tell these calls from a browser's. */
const SITE_ROUTE_USER_AGENT = "api.park.fan-mcp";

/** Why a site route cannot be called from this deployment. */
export const SITE_ROUTE_NOT_CONFIGURED =
  "This API is not set up to call park.fan: REVALIDATE_SECRET is not set, or REVALIDATE_URL " +
  "is not a URL (a development or CI deployment).";

/** Where and with which secret this API calls park.fan, or null when it does not. */
export function siteRouteTarget(): { origin: string; secret: string } | null {
  if (!isRevalidationEnabled()) return null;
  try {
    return { origin: getRevalidateOrigin(), secret: getRevalidateSecret() };
  } catch {
    return null;
  }
}

/** A site route's answer: its status and its JSON, null where the body is not JSON. */
export interface SiteRouteAnswer {
  status: number;
  body: unknown;
}

/**
 * Calls one of park.fan's routes with the shared secret and reads its JSON.
 * Any status comes back for the tool to judge; what never reached an answer
 * (not set up, no answer in time, no connection) throws for the model. A
 * redirect is not followed, so the secret goes to this origin only.
 */
export async function callSiteRoute(
  method: "GET" | "POST",
  path: string,
): Promise<SiteRouteAnswer> {
  const target = siteRouteTarget();
  if (!target) throw new McpToolError(SITE_ROUTE_NOT_CONFIGURED);
  let response: Response;
  try {
    response = await fetch(`${target.origin}${path}`, {
      method,
      redirect: "manual",
      signal: AbortSignal.timeout(SITE_ROUTE_TIMEOUT_MS),
      headers: {
        Authorization: `Bearer ${target.secret}`,
        Accept: "application/json",
        "User-Agent": SITE_ROUTE_USER_AGENT,
      },
    });
  } catch (error) {
    const timedOut =
      error instanceof Error &&
      (error.name === "TimeoutError" || error.name === "AbortError");
    throw new McpToolError(
      timedOut
        ? `park.fan did not answer ${path} within ${SITE_ROUTE_TIMEOUT_MS / 1000} s`
        : `park.fan could not be reached for ${path}`,
    );
  }
  const declared = Number(response.headers.get("content-length"));
  const text =
    declared > SITE_ROUTE_MAX_CHARS
      ? null
      : await response.text().catch(() => "");
  if (text === null || text.length > SITE_ROUTE_MAX_CHARS) {
    throw new McpToolError(`park.fan answered ${path} with an oversized body`);
  }
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }
  return { status: response.status, body };
}
