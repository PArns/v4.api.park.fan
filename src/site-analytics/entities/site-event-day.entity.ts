import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * `site_events` summed to the UTC day, per cuboid, as `site_pageviews_daily`
 * does for page views: a row carries only the dimensions its cuboid names and
 * `locale`; the others hold `''`, the properties `{}` and the entity NULL.
 * `props` belongs to `props_key` and is kept with it. Insert-only, a
 * hypertable written by the nightly `rollup-days` job. See
 * `site-analytics.rollup.ts` and docs/frontend/site-analytics.md.
 */
@Entity("site_events_daily")
@Index("site_events_daily_bucket_idx", ["bucket"])
@Index("idx_site_events_daily_cuboid_bucket", ["cuboid", "bucket"])
export class SiteEventDay {
  @PrimaryGeneratedColumn({ type: "bigint" })
  id: string;

  /** The UTC day's midnight. */
  @PrimaryColumn({ type: "timestamptz" })
  bucket: Date;

  /** Which grouping this row belongs to (`EVENT_CUBOIDS`). */
  @Column({ type: "smallint" })
  cuboid: number;

  @Column({ type: "varchar", length: 48 })
  name: string;

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

  @Column({ type: "varchar", length: 2 })
  country: string;

  @Column({ name: "props_key", type: "text" })
  propsKey: string;

  // A plain object, as in `site_events`: an SQL expression default made
  // `synchronize` alter the column at every start.
  @Column({ type: "jsonb", default: {} })
  props: Record<string, string>;

  @Column({ type: "uuid", nullable: true })
  entity: string | null;

  @Column({ type: "bigint" })
  count: string;
}
