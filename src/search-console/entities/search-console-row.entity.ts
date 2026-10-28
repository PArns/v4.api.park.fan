import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * Search Console's numbers for one day, as one import batch wrote them: a row
 * per page, query, country and device, or, with `query` null, per page,
 * country and device. The second kind is the property's own totals: Google
 * leaves queries it anonymises out of the query rows, but counts them here, so
 * pages and series read these and the queries report reads the others.
 *
 * Insert-only. A day imported again is a new batch (`search_console_batches`
 * names the current one per day) and the reports read only that batch; the
 * rows of the older ones stay until their chunk is dropped, so a re-import
 * leaves no dead tuple. `position_sum` is position times impressions, so a sum
 * over rows divided by the impressions is the impression-weighted position;
 * the click-through rate is computed when read. A TimescaleDB hypertable
 * (`HYPERTABLES`) on `day`, compressed after 35 days, its chunks dropped past
 * `RETENTION_DAYS` by the nightly job. docs/frontend/search-console.md.
 */
@Entity("search_console_rows")
@Index("search_console_rows_day_idx", ["day"])
export class SearchConsoleRow {
  @PrimaryGeneratedColumn({ type: "bigint" })
  id: string;

  /** The day, Search Console's own (Pacific time). */
  @PrimaryColumn({ type: "date" })
  day: string;

  @Column({ name: "batch_id", type: "integer" })
  batchId: number;

  /** The page's path on the site: lower case, no origin, query or trailing slash. */
  @Column({ type: "varchar", length: 820 })
  page: string;

  /** The search query; null on a row that has none (the property's totals). */
  @Column({ type: "text", nullable: true })
  query: string | null;

  /** The page's language by its path's first segment; `''` when it has none. */
  @Column({ type: "varchar", length: 2, default: "" })
  locale: string;

  /** ISO 3166-1 alpha-3, lower case, as Search Console reports it (`deu`). */
  @Column({ type: "varchar", length: 3 })
  country: string;

  @Column({ type: "varchar", length: 8 })
  device: string;

  @Column({ type: "integer", default: 0 })
  clicks: number;

  @Column({ type: "integer", default: 0 })
  impressions: number;

  @Column({ name: "position_sum", type: "double precision", default: 0 })
  positionSum: number;
}
