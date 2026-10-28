import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * One written flush window. The flusher inserts this row in the same
 * transaction as the window's counts, so a retry after a crash finds it and
 * writes nothing twice. A hypertable, so its old chunks go with the others'.
 */
@Entity("site_analytics_flushes")
export class SiteAnalyticsFlush {
  @PrimaryColumn({ type: "timestamptz" })
  bucket: Date;

  @Column({ name: "flushed_at", type: "timestamptz", default: () => "now()" })
  flushedAt: Date;

  @Column({ name: "pageview_rows", type: "integer", default: 0 })
  pageviewRows: number;

  @Column({ name: "event_rows", type: "integer", default: 0 })
  eventRows: number;
}
