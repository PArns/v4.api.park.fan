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

/** Query parameters whose value must never reach the log. */
const REDACTED_PARAMS = new Set(["pass", "token", "password", "secret"]);

export function redactUrl(url: string): string {
  const cut = url.indexOf("?");
  if (cut === -1) return url;

  const path = url.slice(0, cut);
  const params = new URLSearchParams(url.slice(cut + 1));
  let touched = false;
  for (const key of [...params.keys()]) {
    if (REDACTED_PARAMS.has(key.toLowerCase())) {
      params.set(key, "***");
      touched = true;
    }
  }
  if (!touched) return url;
  const rest = params.toString();
  return rest ? `${path}?${rest}` : path;
}
