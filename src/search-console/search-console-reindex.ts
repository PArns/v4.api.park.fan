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

/** A change older than this is out of the queue: Google has recrawled or will not soon. */
export const REINDEX_RECENT_DAYS = 30;

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

/**
 * What a "request indexing" gains on a page: its impressions over 28 days
 * times the days Google had not crawled it when inspected, at most
 * {@link REINDEX_RECENT_DAYS} so a page Google has long forgotten does not
 * outrank pages it shows far more often, and half that for an `uncertain`
 * page whose change Google may have seen. Google's own gap is the guess at how
 * long it takes without a request: a page it crawled two days ago comes round
 * again anyway. A page never crawled counts the full days.
 */
export function reindexPriority(
  impressions: number,
  state: Exclude<ReindexState, "fresh">,
  lastCrawlAt: Date | null,
  inspectedAt: Date,
): number {
  const days = lastCrawlAt
    ? (inspectedAt.getTime() - lastCrawlAt.getTime()) / DAY_MS
    : REINDEX_RECENT_DAYS;
  const counted = Math.min(REINDEX_RECENT_DAYS, Math.max(0, days));
  return impressions * counted * (state === "uncertain" ? 0.5 : 1);
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
