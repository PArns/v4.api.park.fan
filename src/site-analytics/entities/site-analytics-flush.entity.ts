import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * One written claim of a flush window. The flusher inserts this row in the
 * same transaction as the claim's counts, so a retry after a crash finds it
 * and writes nothing twice; counts that reached the window after its claim
 * are a second claim of the same window. A hypertable, so its old chunks go
 * with the others'.
 */
@Entity("site_analytics_flushes")
export class SiteAnalyticsFlush {
  /** The five-minute window's start. */
  @PrimaryColumn({ type: "timestamptz" })
  bucket: Date;

  @PrimaryColumn({ type: "uuid" })
  claim: string;

  @Column({ name: "flushed_at", type: "timestamptz", default: () => "now()" })
  flushedAt: Date;

  @Column({ name: "pageview_rows", type: "integer", default: 0 })
  pageviewRows: number;

  @Column({ name: "event_rows", type: "integer", default: 0 })
  eventRows: number;
}
