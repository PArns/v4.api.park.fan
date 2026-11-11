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

/** The longest a shared cache may keep a keyed answer, in seconds. */
export const MAX_SHARED_KEYED_SECONDS = 300;

/**
 * A public `Cache-Control` with its shared lifetime capped and its stale
 * windows dropped. A keyed copy at Cloudflare outlives a revocation or a
 * narrowed park list for as long as it is kept, and nothing here can purge it,
 * so it is kept five minutes at most, the time the key lookup is cached anyway.
 */
export function cappedSharedCacheControl(current: string): string {
  if (!/\bpublic\b/i.test(current)) return current;
  const maxAge = Number(/(?:^|,)\s*max-age=(\d+)/i.exec(current)?.[1] ?? 0);
  const shared = Number(
    /(?:^|,)\s*s-maxage=(\d+)/i.exec(current)?.[1] ?? maxAge,
  );
  const cap = (seconds: number) => Math.min(seconds, MAX_SHARED_KEYED_SECONDS);
  return `public, max-age=${cap(maxAge)}, s-maxage=${cap(shared)}`;
}

/**
 * `current` plus the given header names, each once, keeping the order and
 * spelling of what was there.
 */
export function mergeVary(current: string | undefined, add: string[]): string {
  const names = (current ?? "")
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean);
  for (const name of add) {
    if (
      !names.some((present) => present.toLowerCase() === name.toLowerCase())
    ) {
      names.push(name);
    }
  }
  return names.join(", ");
}

/**
 * The shared-cache mode: the response keeps the public `Cache-Control` its
 * route chose, capped by `cappedSharedCacheControl`, and every `Vary` it gets
 * carries the key headers, so a cache
 * that honours them stores one copy per key. Cloudflare honours `Vary` only for
 * headers a Cache Rule names (Vary setting, `passthrough`); without that rule
 * this mode would hand one key's answer to every caller, which is why it is off
 * unless `FORECAST_SHARED_CACHE=vary` says the rule is in place.
 *
 * Starts `private, no-store`, so a refusal or an error stays out of every
 * cache; only a success gets the route's public policy.
 */
export function varyResponseByKey(
  response: Response,
  keyHeaders: string[],
): void {
  const setHeader = response.setHeader.bind(response);
  response.setHeader = ((name: string, value: number | string | string[]) => {
    const header = name.toLowerCase();
    if (header === "vary") {
      return setHeader(name, mergeVary(String(value), keyHeaders));
    }
    if (header === "cdn-cache-control") return response;
    if (header === "cache-control") {
      return setHeader(name, cappedSharedCacheControl(String(value)));
    }
    return setHeader(name, value);
  }) as Response["setHeader"];
  if (response.headersSent) return;
  setHeader("Cache-Control", "private, no-store");
  const current = response.getHeader?.("Vary");
  setHeader(
    "Vary",
    mergeVary(typeof current === "string" ? current : undefined, keyHeaders),
  );
}

/** Whether the Cloudflare Vary rule is in place and keyed answers may be shared. */
export function isForecastSharedCacheEnabled(): boolean {
  return process.env.FORECAST_SHARED_CACHE === "vary";
}
