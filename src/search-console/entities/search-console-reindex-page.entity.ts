import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * A page whose sitemap `<lastmod>` is recent, with what URL Inspection last
 * said about it: the candidates of the reindex queue. The daily scan rewrites
 * the set from the sitemaps (a page whose change ages out is deleted) and
 * inspects the ones Google may not have seen since; the admin lists those whose
 * last crawl predates the change. docs/frontend/search-console.md.
 */
@Entity("search_console_reindex_pages")
export class SearchConsoleReindexPage {
  /** The page's path, as `search_console_rows.page` stores it (`/de/blog/…`). */
  @PrimaryColumn({ type: "varchar", length: 820 })
  path: string;

  /** The sitemap's `<lastmod>` day: when park.fan saw the content change. */
  @Column({ name: "changed_on", type: "date" })
  changedOn: string;

  /** Search impressions over the last 28 days with data, for the order. */
  @Column({ type: "integer", default: 0 })
  impressions: number;

  /** Search clicks over the same 28 days. */
  @Column({ type: "integer", default: 0 })
  clicks: number;

  /** Impression-weighted average position over the same days; null without impressions. */
  @Column({ type: "double precision", nullable: true })
  position: number | null;

  @Column({ name: "inspected_at", type: "timestamptz", nullable: true })
  inspectedAt: Date | null;

  /** Google's last crawl at the last inspection; null when it never crawled. */
  @Column({ name: "last_crawl_at", type: "timestamptz", nullable: true })
  lastCrawlAt: Date | null;

  /** Google's coverage state at the last inspection ("Submitted and indexed", …). */
  @Column({ name: "coverage_state", type: "text", nullable: true })
  coverageState: string | null;
}
