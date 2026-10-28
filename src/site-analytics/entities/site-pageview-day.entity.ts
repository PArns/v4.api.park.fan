import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * `site_pageviews` summed to the UTC day, per cuboid: the same columns, but a
 * row carries only the dimensions its cuboid names (and `locale`, which every
 * cuboid keeps); the others hold `''`. A cuboid is one grouping of the day's
 * hourly rows, so `breakdown?dim=country` reads the few hundred rows of the
 * `country` cuboid instead of the day's thirty thousand. Written once per day
 * and cuboid by the nightly `rollup-days` job, never updated; a hypertable on
 * `bucket` (the day's UTC midnight), compressed by cuboid, its chunks dropped
 * after `RETENTION_DAYS` like the hourly table's. The sums are `bigint`: a day
 * adds up more than a window of five minutes did. See
 * `site-analytics.rollup.ts` and docs/frontend/site-analytics.md.
 */
@Entity("site_pageviews_daily")
@Index("site_pageviews_daily_bucket_idx", ["bucket"])
@Index("idx_site_pageviews_daily_cuboid_bucket", ["cuboid", "bucket"])
export class SitePageviewDay {
  @PrimaryGeneratedColumn({ type: "bigint" })
  id: string;

  /** The UTC day's midnight. */
  @PrimaryColumn({ type: "timestamptz" })
  bucket: Date;

  /** Which grouping this row belongs to (`PAGEVIEW_CUBOIDS`). */
  @Column({ type: "smallint" })
  cuboid: number;

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

  @Column({ type: "varchar", length: 2 })
  country: string;

  @Column({ name: "browser_language", type: "varchar", length: 2 })
  browserLanguage: string;

  @Column({ type: "varchar", length: 820 })
  entry: string;

  @Column({ type: "varchar", length: 100 })
  referrer: string;

  @Column({ type: "varchar", length: 10 })
  channel: string;

  @Column({ type: "bigint" })
  views: string;

  @Column({ type: "bigint" })
  seconds: string;

  @Column({ type: "bigint" })
  visits: string;

  @Column({ type: "bigint" })
  engaged: string;

  @Column({ name: "visit_seconds", type: "bigint" })
  visitSeconds: string;

  @Column({ type: "bigint" })
  exits: string;

  @Column({ name: "visit_span_seconds", type: "bigint" })
  visitSpanSeconds: string;
}
