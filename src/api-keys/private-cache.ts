import type { Response } from "express";

/**
 * The `Cache-Control` a keyed response leaves with: what was chosen for the
 * browser, but never a shared cache. Cloudflare caches by URL and does not
 * look at `X-API-Key`, so a forecast cached there once would reach every later
 * caller, key or no key.
 */
export function privateCacheControl(current: string | undefined): string {
  const maxAge = /(?:^|,)\s*max-age=(\d+)/i.exec(current ?? "")?.[1];
  return maxAge ? `private, max-age=${maxAge}` : "private, no-store";
}

/**
 * Makes every `Cache-Control` this response will carry private, whoever sets
 * it and in whatever order: the route's own `HttpCacheInterceptor`, a handler
 * calling `res.setHeader`, or the global policy. Wrapping `setHeader` rather
 * than rewriting in a `tap()` is what keeps that independent of interceptor
 * order, which decorators decide and nobody reads.
 */
export function keepResponsePrivate(response: Response): void {
  const setHeader = response.setHeader.bind(response);
  response.setHeader = ((name: string, value: number | string | string[]) => {
    const header = name.toLowerCase();
    if (header === "cdn-cache-control") return response;
    if (header === "cache-control") {
      return setHeader(name, privateCacheControl(String(value)));
    }
    return setHeader(name, value);
  }) as Response["setHeader"];
  if (!response.headersSent) setHeader("Cache-Control", "private, no-store");
}
