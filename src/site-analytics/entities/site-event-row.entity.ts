import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * Events of one flush window (five minutes, UTC): name, properties and entity
 * with the same page and visit dimensions as `site_pageviews`. The properties
 * sit twice, as a canonical key (`k=v` in key order, `&`-joined) that names
 * the combination, and as `jsonb` to group by one property. Insert-only, a
 * hypertable like `site_pageviews`. docs/frontend/site-analytics.md.
 */
@Entity("site_events")
@Index("idx_site_events_name_bucket", ["name", "bucket"])
export class SiteEventRow {
  @PrimaryGeneratedColumn({ type: "bigint" })
  id: string;

  /** The flush window's start. */
  @PrimaryColumn({ type: "timestamptz" })
  bucket: Date;

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

  @Column({ type: "varchar", length: 820, default: "" })
  entry: string;

  @Column({ type: "varchar", length: 100, default: "" })
  referrer: string;

  @Column({ type: "varchar", length: 10, default: "" })
  channel: string;

  @Column({ type: "varchar", length: 100, default: "" })
  campaign: string;

  @Column({ name: "props_key", type: "text", default: "" })
  propsKey: string;

  @Column({ type: "jsonb", default: () => "'{}'::jsonb" })
  props: Record<string, string>;

  /** A park's, ride's, show's or restaurant's id. */
  @Column({ type: "uuid", nullable: true })
  entity: string | null;

  @Column({ type: "integer", default: 0 })
  count: number;
}
