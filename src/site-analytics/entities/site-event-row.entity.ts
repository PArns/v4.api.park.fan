import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * Events of one UTC hour, as one flush window wrote them: name, properties
 * and entity with the page dimensions of `site_pageviews`, never the visit's.
 * The properties sit twice, as a canonical key (`k=v` in key order,
 * `&`-joined) that names the combination, and as `jsonb` to group by one
 * property. Insert-only, a hypertable like `site_pageviews`; the bucket index
 * is the one `create_hypertable` makes, declared so that `synchronize` keeps
 * it. docs/frontend/site-analytics.md.
 */
@Entity("site_events")
@Index("idx_site_events_name_bucket", ["name", "bucket"])
@Index("site_events_bucket_idx", ["bucket"])
export class SiteEventRow {
  @PrimaryGeneratedColumn({ type: "bigint" })
  id: string;

  /** The UTC hour. */
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

  /** `mobile`, `tablet` or `desktop`, from the User-Agent (`SITE_DEVICES`). */
  @Column({ type: "varchar", length: 8 })
  device: string;

  @Column({ type: "varchar", length: 10 })
  browser: string;

  @Column({ type: "varchar", length: 10 })
  os: string;

  @Column({ type: "varchar", length: 2 })
  country: string;

  @Column({ name: "props_key", type: "text", default: "" })
  propsKey: string;

  // A plain object rather than a SQL expression: TypeORM compares it with the
  // column's default as Postgres reports it and finds no change, where the
  // expression made `synchronize` alter the column on every start.
  @Column({ type: "jsonb", default: {} })
  props: Record<string, string>;

  /** A park's, ride's, show's or restaurant's id. */
  @Column({ type: "uuid", nullable: true })
  entity: string | null;

  @Column({ type: "integer", default: 0 })
  count: number;
}
