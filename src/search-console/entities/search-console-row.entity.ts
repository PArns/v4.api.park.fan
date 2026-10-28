import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * Search Console's numbers for one day, as one import batch wrote them, at
 * four levels: with a `query`, a row per page, query, country and device;
 * without one, complete rows per page (`country` and `device` empty), per
 * country (`page` and `device` empty) or per device (`page` and `country`
 * empty). Google drops the queries it anonymises as soon as a page is combined
 * with a country or device, so only the single-dimension requests are
 * complete: the page rows answer pages and series, the country and device rows
 * the totals under that filter, the query rows the queries.
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

  /** The page's path on the site: lower case, no origin, query or trailing slash; `''` on a country or device row. */
  @Column({ type: "varchar", length: 820 })
  page: string;

  /** The search query; null on a complete page, country or device row. */
  @Column({ type: "text", nullable: true })
  query: string | null;

  /** The page's language by its path's first segment; `''` when it has none. */
  @Column({ type: "varchar", length: 2, default: "" })
  locale: string;

  /** ISO 3166-1 alpha-3, lower case, as Search Console reports it (`deu`); `''` on a page or device row. */
  @Column({ type: "varchar", length: 3 })
  country: string;

  /** `''` on a page or country row. */
  @Column({ type: "varchar", length: 8 })
  device: string;

  @Column({ type: "integer", default: 0 })
  clicks: number;

  @Column({ type: "integer", default: 0 })
  impressions: number;

  @Column({ name: "position_sum", type: "double precision", default: 0 })
  positionSum: number;
}
