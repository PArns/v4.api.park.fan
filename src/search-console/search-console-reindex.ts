/**
 * The pure half of the reindex queue: reading a sitemap's `<lastmod>`s and
 * deciding from a page's change day and Google's last crawl whether Google has
 * seen the change. docs/frontend/search-console.md.
 */

/** Whether Google has crawled the page since it changed. */
export type ReindexState =
  /** Google does not know the page or never crawled it. */
  | "unknown"
  /** Crawled before the change, for certain. */
  | "stale"
  /** Crawled within the change's window: maybe before it, maybe after. */
  | "uncertain"
  /** Crawled after the change. */
  | "fresh";

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * The frontend's content-change crawl runs at 05:30 UTC and stamps the day it
 * ran, so a `<lastmod>` of D means a change between D−1 05:30 and D 05:30. A
 * crawl past D 06:00 has seen it; one before D−1 00:00 has not.
 */
const OBSERVED_BY_MS = 6 * 60 * 60 * 1000;

/** Where a page stands, from its change day (`YYYY-MM-DD`) and Google's last crawl. */
export function reindexState(
  changedOn: string,
  lastCrawlAt: Date | null,
): ReindexState {
  if (!lastCrawlAt) return "unknown";
  const day = Date.parse(`${changedOn}T00:00:00Z`);
  const crawl = lastCrawlAt.getTime();
  if (crawl < day - DAY_MS) return "stale";
  if (crawl < day + OBSERVED_BY_MS) return "uncertain";
  return "fresh";
}

/** One `<url>` or `<sitemap>` of a sitemap file. */
export interface SitemapEntry {
  loc: string;
  lastmod: string | null;
}

const decode = (text: string): string =>
  text
    .trim()
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");

/**
 * The entries of a sitemap or sitemap index, with `<lastmod>` cut to its day.
 * A regex, not an XML parser: the files are park.fan's own and flat, and the
 * `xhtml:link` alternates inside a `<url>` carry no `<loc>`.
 */
export function parseSitemap(xml: string): {
  index: boolean;
  entries: SitemapEntry[];
} {
  const index = /<sitemapindex[\s>]/.test(xml);
  const tag = index ? "sitemap" : "url";
  const entries: SitemapEntry[] = [];
  for (const [, body] of xml.matchAll(
    new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, "g"),
  )) {
    const loc = /<loc>([\s\S]*?)<\/loc>/.exec(body)?.[1];
    if (!loc) continue;
    const lastmod = /<lastmod>\s*(\d{4}-\d{2}-\d{2})/.exec(body)?.[1] ?? null;
    entries.push({ loc: decode(loc), lastmod });
  }
  return { index, entries };
}
