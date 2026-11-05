import type {
  CorsOptions,
  CorsOptionsDelegate,
} from "@nestjs/common/interfaces/external/cors-options.interface";

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

/**
 * The admin MCP's public surface: the MCP endpoint, the OAuth endpoints a
 * client calls itself, and the discovery documents. A browser-based client
 * (the MCP Inspector) calls them from whatever origin it runs on, and none of
 * them reads a cookie — the credential is a bearer token or a PKCE verifier
 * in the request — so every origin may. `/v1/oauth/authorize` is a page the
 * browser navigates to, and the consent endpoints go through park.fan's
 * proxy; neither needs CORS.
 */
const PUBLIC_CORS_PATH =
  /^(?:\/v1\/mcp|\/v1\/oauth\/(?:register|token|revoke)|\/\.well-known\/oauth-(?:protected-resource|authorization-server)(?:\/.*)?)$/;

export function isPublicCorsPath(url: string | undefined): boolean {
  return PUBLIC_CORS_PATH.test((url ?? "").split("?")[0]);
}

export const PUBLIC_CORS_OPTIONS: CorsOptions = {
  origin: "*",
  credentials: false,
  methods: ["GET", "POST", "DELETE", "OPTIONS"],
  allowedHeaders: [
    "Authorization",
    "Content-Type",
    "Accept",
    "Mcp-Protocol-Version",
    "Mcp-Session-Id",
    "Last-Event-ID",
  ],
  exposedHeaders: [
    "WWW-Authenticate",
    "Mcp-Protocol-Version",
    "Mcp-Session-Id",
    "Retry-After",
  ],
  maxAge: 7200,
};

/**
 * What `src/main.ts` passes to `app.enableCors`: the public options above for
 * the MCP surface, `buildCorsOptions` for everything else.
 */
export function buildCorsDelegate(
  isProduction: boolean,
): CorsOptionsDelegate<{ url?: string }> {
  const site = buildCorsOptions(isProduction);
  return (request, callback) =>
    callback(null, isPublicCorsPath(request.url) ? PUBLIC_CORS_OPTIONS : site);
}
