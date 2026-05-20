import {
  Entity,
  Column,
  PrimaryColumn,
  Index,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  JoinColumn,
} from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * Per-day per-attraction hourly history rollup.
 *
 * Pre-aggregates the 15-min-slot P90 / avg / sample-count breakdown that
 * the attraction history endpoint used to compute live on every cache
 * miss. Each row holds a single immutable day's slots as JSONB, so the
 * history endpoint can read a date range with one SELECT instead of N
 * PERCENTILE_CONTs over raw queue_data.
 *
 * Lifecycle:
 * - Populated by `attraction-hourly-history` cron at 4:30 AM for the
 *   previous day across every attraction (one query per park, grouped
 *   by attraction).
 * - Today's slots are NOT stored here; the history endpoint still runs
 *   a single-day live query for the in-progress day.
 * - Rows are immutable once written. Cleanup (retention) is handled by
 *   the existing data-retention policy; defaulting to keep ~2 years.
 */
@Entity("attraction_hourly_history")
@Index("idx_attraction_hourly_history_park", ["parkId"])
@Index("idx_attraction_hourly_history_date", ["date"])
export class AttractionHourlyHistory {
  @PrimaryColumn("uuid")
  attractionId: string;

  @PrimaryColumn({ type: "date" })
  date: string;

  @Column("uuid")
  parkId: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  /**
   * Array of 15-min slot rollups for this attraction on this date, sorted
   * by `time_slot`. Empty array means we have schedule/data for the day
   * but no qualifying samples (waitTime >= 5, status = OPERATING, queueType
   * = STANDBY) — distinct from the row not existing at all.
   */
  @Column({ type: "jsonb" })
  slots: Array<{
    time_slot: string; // "HH:MM" at 15-min grid (00, 15, 30, 45)
    p90: number;
    avgWait: number;
    sampleCount: number;
  }>;

  @Column({ type: "int", default: 0 })
  downCount: number;

  @Column({ type: "timestamptz" })
  calculatedAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
