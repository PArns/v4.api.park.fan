import { SITE_LOCALES } from "../site-analytics/site-analytics.constants";
import { MAX_PATH_LENGTH, MAX_QUERY_LENGTH } from "./search-console.constants";

/**
 * A page URL as the site statistic knows the page: its path, lower case, no
 * origin, query, fragment or trailing slash, so `https://www.park.fan/de/Parks/`
 * is `/de/parks` and joins `site_pageviews.path` and `.entry`. Null for a page
 * on a host the property does not count (a subdomain of a `sc-domain:`
 * property), for text that is no URL, and for a path no real page has.
 */
export function pagePath(url: string, hosts: readonly string[]): string | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }
  if (!hosts.includes(parsed.host.toLowerCase())) return null;
  const path = parsed.pathname.toLowerCase().replace(/\/+$/, "") || "/";
  return path.length <= MAX_PATH_LENGTH ? path : null;
}

/** The language a path is in by its first segment, `''` for a path outside the six. */
export function localeOfPath(path: string): string {
  const first = path.split("/")[1] ?? "";
  return (SITE_LOCALES as readonly string[]).includes(first) ? first : "";
}

/** A query as stored: trimmed and lower case, cut at {@link MAX_QUERY_LENGTH}. */
export function normalizeQuery(query: string): string {
  return query.trim().toLowerCase().slice(0, MAX_QUERY_LENGTH);
}
