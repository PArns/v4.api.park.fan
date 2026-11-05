import type { Request } from "express";
import {
  DEFAULT_PRODUCTION_CORS_ORIGINS,
  isAllowedCorsOrigin,
} from "../common/cors/cors.config";

/**
 * Whether a beacon comes from the site itself. In production that is a
 * request whose `Origin` (or, should a browser leave it out, whose `Referer`)
 * is park.fan: a preview, a local build or a script without one is not
 * counted. Every browser sends `Origin` on a cross-origin POST, `sendBeacon`
 * included. Used by the statistic's collect endpoint and by the most-read
 * counter's deprecated one.
 */
export function isFromSite(request: Request, isProduction: boolean): boolean {
  if (!isProduction) return true;
  const origin = request.get("origin");
  if (origin)
    return isAllowedCorsOrigin(origin, DEFAULT_PRODUCTION_CORS_ORIGINS);
  const referer = request.get("referer") ?? "";
  return DEFAULT_PRODUCTION_CORS_ORIGINS.some((site) =>
    referer.startsWith(`${site}/`),
  );
}

/**
 * The `Cross-Origin-Resource-Policy` of a beacon's answer. Helmet sends
 * `same-origin` everywhere, and `sendBeacon` is a no-cors request from
 * park.fan, so the browser blocks the answer and every beacon shows as an
 * error in the developer tools, though the hit counted. The answer is an
 * empty 204, so letting any origin read it gives nothing away.
 */
export const BEACON_RESOURCE_POLICY = "cross-origin";
