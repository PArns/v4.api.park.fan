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
 * and two hits with the same dimensions in one window are one row. Two kinds
 * of row:
 *
 * - a page view's: its page dimensions and `views`; `entry`, `referrer`,
 *   `channel` and `campaign` empty, so no page past a visit's first is tied
 *   to where its reader came from;
 * - a visit's: the first page view's dimensions, `entry` (that page's path),
 *   `referrer`, `channel`, `campaign`, `visits` = visits started and
 *   `engaged` = those that reached a second page view (added in the hour of
 *   the second one), so the bounce rate of anything a filter selects is
 *   `1 − engaged / visits`.
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

  @Column({ type: "varchar", length: 8 })
  viewport: string;

  @Column({ type: "varchar", length: 10 })
  browser: string;

  @Column({ type: "varchar", length: 10 })
  os: string;

  /** ISO 3166-1 alpha-2, lowercase; `zz` where GeoIP cannot tell. */
  @Column({ type: "varchar", length: 2 })
  country: string;

  /** A visit's row: its first page view's path; `''` on a page view's row. */
  @Column({ type: "varchar", length: 820, default: "" })
  entry: string;

  /** A visit's referrer site (`google`, a host, a `utm_source`); `''` direct, and on a page view's row. */
  @Column({ type: "varchar", length: 100, default: "" })
  referrer: string;

  @Column({ type: "varchar", length: 10, default: "" })
  channel: string;

  /** A visit's `utm_campaign`; `''` without one, and on a page view's row. */
  @Column({ type: "varchar", length: 100, default: "" })
  campaign: string;

  @Column({ type: "integer", default: 0 })
  views: number;

  @Column({ type: "integer", default: 0 })
  visits: number;

  @Column({ type: "integer", default: 0 })
  engaged: number;
}
