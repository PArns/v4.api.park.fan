import { Column, Entity, PrimaryColumn, PrimaryGeneratedColumn } from "typeorm";

/**
 * Page views of one flush window (five minutes, UTC) with all their
 * dimensions together, so any dimension can filter any other and every
 * breakdown is a GROUP BY. A count, not a log: no row carries an address, a
 * User-Agent or a visit id, and two page views with the same dimensions in one
 * window are one row.
 *
 * `entry`, `referrer`, `channel` and `campaign` are the visit's, copied onto
 * each of its rows. `visits` is 1 on the row of a visit's first page view;
 * `engaged` is added to that same row's dimensions (in the window of the
 * second page view) when the visit sees a second one, so the bounce rate of
 * anything a filter selects is `1 − engaged / visits`.
 *
 * Insert-only: the flusher writes each window once and nothing updates a row.
 * A TimescaleDB hypertable (`HYPERTABLES`), compressed after a week, its
 * chunks dropped after `RETENTION_DAYS`. docs/frontend/site-analytics.md.
 */
@Entity("site_pageviews")
export class SitePageviewRow {
  @PrimaryGeneratedColumn({ type: "bigint" })
  id: string;

  /** The flush window's start. */
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

  /** The visit's first path; `''` where the visit is not known. */
  @Column({ type: "varchar", length: 820, default: "" })
  entry: string;

  /** The visit's referrer site (`google`, a host, a `utm_source`); `''` direct. */
  @Column({ type: "varchar", length: 100, default: "" })
  referrer: string;

  @Column({ type: "varchar", length: 10, default: "" })
  channel: string;

  /** The visit's `utm_campaign`; `''` without one. */
  @Column({ type: "varchar", length: 100, default: "" })
  campaign: string;

  @Column({ type: "integer", default: 0 })
  views: number;

  @Column({ type: "integer", default: 0 })
  visits: number;

  @Column({ type: "integer", default: 0 })
  engaged: number;
}
