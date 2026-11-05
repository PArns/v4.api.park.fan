import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * Page views and visits of one UTC hour, as one flush window wrote them. A
 * count, not a log: no row carries an address, a User-Agent or a visit id,
 * and two hits with the same dimensions in one window are one row. Three
 * kinds of row:
 *
 * - a page view's: its page dimensions, `views` and `seconds` (foreground
 *   time reported for it); `entry`, `referrer` and `channel` empty, so no
 *   page in the middle of a visit is tied to where its reader came from;
 * - a visit's start: the first page view's dimensions, `entry` (that page's
 *   path), `referrer`, `channel`, `visits` = visits started,
 *   `engaged` = those that reached a second page view (added in the hour of
 *   the second one) and `visit_seconds`, the visit's foreground time (added
 *   as it is reported), so the bounce rate of anything a filter selects is
 *   `1 − engaged / visits` and its average visit `visit_seconds / visits`;
 * - a visit's exit: the last page view's dimensions with the same visit
 *   dimensions, `exits` = visits that ended there and `visit_span_seconds`,
 *   from their first hit to their last, in the hour of the last hit.
 *
 * Insert-only: the flusher writes each window once and nothing updates a row.
 * A TimescaleDB hypertable (`HYPERTABLES`), compressed after a week, its
 * chunks dropped after `RETENTION_DAYS`; the bucket index is the one
 * `create_hypertable` makes, declared so that `synchronize` keeps it.
 * docs/frontend/site-analytics.md.
 */
@Entity("site_pageviews")
@Index("site_pageviews_bucket_idx", ["bucket"])
export class SitePageviewRow {
  @PrimaryGeneratedColumn({ type: "bigint" })
  id: string;

  /** The UTC hour. */
  @PrimaryColumn({ type: "timestamptz" })
  bucket: Date;

  @Column({ type: "varchar", length: 2 })
  locale: string;

  @Column({ type: "varchar", length: 820 })
  path: string;

  @Column({ name: "page_type", type: "varchar", length: 32 })
  pageType: string;

  /** `mobile`, `tablet` or `desktop`, from the User-Agent (`SITE_DEVICES`). */
  @Column({ type: "varchar", length: 8 })
  device: string;

  @Column({ type: "varchar", length: 10 })
  browser: string;

  @Column({ type: "varchar", length: 10 })
  os: string;

  /** ISO 3166-1 alpha-2, lowercase; `zz` where GeoIP cannot tell. */
  @Column({ type: "varchar", length: 2 })
  country: string;

  /**
   * `datacenter`, `other` or `unknown` (`SITE_NETWORKS`), from the address's
   * autonomous system; `unknown` on rows from before the column.
   */
  @Column({ type: "varchar", length: 10, default: "unknown" })
  network: string;

  /** The first `Accept-Language` tag's primary subtag; `zz` without one. */
  @Column({
    name: "browser_language",
    type: "varchar",
    length: 2,
    default: "zz",
  })
  browserLanguage: string;

  /** A visit's row: its first page view's path; `''` on a page view's row. */
  @Column({ type: "varchar", length: 820, default: "" })
  entry: string;

  /** A visit's referrer site (`google`, or the external host); `''` direct, and on a page view's row. */
  @Column({ type: "varchar", length: 100, default: "" })
  referrer: string;

  @Column({ type: "varchar", length: 10, default: "" })
  channel: string;

  @Column({ type: "integer", default: 0 })
  views: number;

  /** Foreground seconds reported for the row's page views. */
  @Column({ type: "integer", default: 0 })
  seconds: number;

  @Column({ type: "integer", default: 0 })
  visits: number;

  @Column({ type: "integer", default: 0 })
  engaged: number;

  /** Foreground seconds of the row's visits, over all their pages. */
  @Column({ name: "visit_seconds", type: "integer", default: 0 })
  visitSeconds: number;

  /** Visits that ended on the row's page. */
  @Column({ type: "integer", default: 0 })
  exits: number;

  /** Seconds from the first hit of the row's ended visits to their last. */
  @Column({ name: "visit_span_seconds", type: "integer", default: 0 })
  visitSpanSeconds: number;
}
