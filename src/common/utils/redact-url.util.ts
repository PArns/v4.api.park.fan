/**
 * Take the credentials out of a URL before anything writes it down.
 *
 * The deprecated shared admin pass travels as `?pass=<secret>`, and it is a
 * full-privilege, non-expiring, non-revocable credential. Everyone who can read
 * the container logs is a much wider group than the people who hold it, so a
 * single log line with the secret in it hands out owner rights that nobody can
 * take back without rotating the pass everywhere.
 *
 * This lives here, and not next to one of its callers, because there are two
 * and they log at opposite ends of a request: `LoggingInterceptor` writes the
 * line for a request that returned, `HttpExceptionFilter` writes the line for
 * one that threw. Redacting in only one of them covers only half the traffic,
 * which is what happened when this was a function inside the interceptor: the
 * `tap()` it logs from is a next-only observer, so every 4xx and 5xx went to
 * the filter instead and wrote the secret out in full. That includes the cases
 * this codebase creates on purpose, like `cache/reset` refusing a call without
 * `?confirm=true`, and the guard rejecting a valid pass on an endpoint the pass
 * is not allowed to reach: verified, refused, logged.
 */

/**
 * Query parameters whose value must never reach the log.
 *
 * `endpoint` is `GET /v1/push/ride-alerts|show-follows`'s own capability:
 * `push-notification.processor.ts` hashes it before it goes into a Redis key
 * for exactly the same reason a credential is redacted here — anyone holding
 * it can send that browser a notification. It travels in the query string
 * rather than a body because these are `GET`s, not because it is any less
 * sensitive than the trip/subscribe endpoints' bodies, which never reach
 * this function at all.
 */
const REDACTED_PARAMS = new Set([
  "pass",
  "token",
  "password",
  "secret",
  "endpoint",
  // OAuth (docs/admin/mcp.md). None belongs in a URL this API is sent, but
  // a confused client puts them there, and an error line is where it shows.
  "code",
  "code_verifier",
  "access_token",
  "refresh_token",
  "client_secret",
  "state",
  // `?ip=` on the location and favourites routes names the address to look up
  // instead of the caller's own. It is still a visitor's address, and the
  // privacy policy says no log line carries one.
  "ip",
  // Where a visitor stands, on `/v1/discovery/nearby` and `/v1/favorites`. Not an address, but a
  // position: park.fan's app tells its users the server keeps none (PAR-1430).
  "lat",
  "lng",
  "lon",
  "long",
  "latitude",
  "longitude",
]);

/**
 * Path segments that are capabilities: the id of a pending OAuth request, which
 * any admin session could answer while it lives (docs/admin/mcp.md).
 */
const REDACTED_PATH = /(\/admin\/oauth\/requests\/)[^/?#]+/i;

export function redactUrl(rawUrl: string): string {
  const url = rawUrl.replace(REDACTED_PATH, "$1***");
  const cut = url.indexOf("?");
  if (cut === -1) return url;

  const path = url.slice(0, cut);
  const params = new URLSearchParams(url.slice(cut + 1));
  let touched = false;
  for (const key of [...params.keys()]) {
    // `lat[]=` and `lat[0]=` are `lat` to Express's query parser, so they are here too.
    if (REDACTED_PARAMS.has(key.toLowerCase().replace(/\[.*$/, ""))) {
      params.set(key, "***");
      touched = true;
    }
  }
  if (!touched) return url;
  const rest = params.toString();
  return rest ? `${path}?${rest}` : path;
}
