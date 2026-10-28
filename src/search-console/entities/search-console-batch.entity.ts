import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * One import of one day. The newest batch of a day is the day's data; its
 * `rows`, `clicks`, `impressions` and `position_sum` are what the next run
 * compares a fresh download with, so a day that has not changed writes
 * nothing. A day Search Console had nothing for gets a batch with no rows,
 * which says it was asked. Small and read on every report, so a plain table:
 * a batch is inserted once and only the retention job deletes one.
 */
@Entity("search_console_batches")
@Index("search_console_batches_day_idx", ["day", "id"])
export class SearchConsoleBatch {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: "date" })
  day: string;

  @CreateDateColumn({ name: "imported_at", type: "timestamptz" })
  importedAt: Date;

  /** Rows written, both kinds. */
  @Column({ type: "integer", default: 0 })
  rows: number;

  /**
   * The layout of the rows (`BATCH_FORMAT`); a lower one is re-imported. A
   * default, so adding the column rewrites nothing.
   */
  @Column({ type: "integer", default: 1 })
  format: number;

  /** The property's clicks that day, from the complete page rows. */
  @Column({ type: "integer", default: 0 })
  clicks: number;

  @Column({ type: "integer", default: 0 })
  impressions: number;

  @Column({ name: "position_sum", type: "double precision", default: 0 })
  positionSum: number;
}
