import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * One-off imports into the statistic that ran, so a second start does not
 * run them again: `page_view_days`, the most-read counter's table before it
 * moved onto this pipeline (PAR-1151).
 */
@Entity("site_analytics_imports")
export class SiteAnalyticsImport {
  @PrimaryColumn({ type: "varchar", length: 64 })
  name: string;

  @Column({ name: "imported_at", type: "timestamptz", default: () => "now()" })
  importedAt: Date;

  @Column({ type: "integer", default: 0 })
  rows: number;
}
