import type { CorsOptions } from "@nestjs/common/interfaces/external/cors-options.interface";

/**
 * Browser origins allowed in production when `CORS_ORIGINS` is unset: the
 * frontend itself, so it can call the API without its own server-side relay.
 * See docs/deployment/cors.md.
 */
export const DEFAULT_PRODUCTION_CORS_ORIGINS = [
  "https://park.fan",
  "https://www.park.fan",
];

// No wildcard: this list also applies when NODE_ENV is accidentally unset in
// a deployed environment, so localhost is the widest default.
const DEFAULT_DEVELOPMENT_CORS_ORIGINS = [
  /^https?:\/\/localhost(:\d+)?$/,
  /^https?:\/\/127\.0\.0\.1(:\d+)?$/,
];

/**
 * `CORS_ORIGINS`, comma-separated. Replaces the default list rather than
 * extending it, so production must name park.fan again when it is set.
 */
export const getConfiguredCorsOrigins = (): string[] =>
  (process.env.CORS_ORIGINS ?? "")
    .split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0);

/**
 * An entry is an exact origin, or one with `*` standing for letters, digits
 * and hyphens inside a host label (`https://park-*-arns.vercel.app` for the
 * Vercel previews). A `*` never matches a dot, so it cannot reach another
 * domain.
 */
export function corsOriginMatcher(entry: string): string | RegExp {
  if (!entry.includes("*")) return entry;
  const escaped = entry
    .split("*")
    .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  return new RegExp(`^${escaped.join("[a-z0-9-]+")}$`);
}

export function isAllowedCorsOrigin(
  origin: string | undefined,
  allowed: (string | RegExp)[],
): boolean {
  if (!origin) return false;
  return allowed.some((entry) =>
    typeof entry === "string" ? entry === origin : entry.test(origin),
  );
}

/**
 * Options for `app.enableCors`. A request from an origin that is not allowed,
 * or with no `Origin` header at all, is answered normally without CORS
 * headers: an error from the origin callback becomes a 500 for every
 * server-side caller, and the frontend's own server renders through this API.
 */
export function buildCorsOptions(isProduction: boolean): CorsOptions {
  const configured = getConfiguredCorsOrigins().map(corsOriginMatcher);
  const allowed =
    configured.length > 0
      ? configured
      : isProduction
        ? DEFAULT_PRODUCTION_CORS_ORIGINS
        : DEFAULT_DEVELOPMENT_CORS_ORIGINS;

  return {
    origin: (origin, callback) =>
      callback(null, isAllowedCorsOrigin(origin, allowed)),
    credentials: isProduction,
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "X-Admin-API-Key"],
    // A JSON write is preflighted; two hours is Chrome's cap, so the trip and
    // push writes of one visit share a single OPTIONS.
    maxAge: 7200,
    // Retry-After is not a CORS-safelisted response header: without this a
    // cross-origin browser client gets the header on a 429 but reads null
    // (PAR-242).
    exposedHeaders: ["Retry-After"],
  };
}
