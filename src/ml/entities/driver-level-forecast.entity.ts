import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryColumn,
} from "typeorm";

/**
 * The latest nightly driver-level SHADOW (PAR-1058): ml-service's
 * `driver_level.py`, a port of ml-bench's `driver_level` (PAR-830), writes one
 * row per ride and target date for origin+1 … origin+91 and replaces the whole
 * table every night (~2 600 rides × 91 days). Never served: the forward archive
 * copies the level into `forecast_archive_curves.driver_day_peak` at capture,
 * which is what keeps its history, so this table needs no retention.
 *
 * Plain table, no FK (written by ml-service, outside this entity graph).
 */
@Entity("driver_level_forecasts")
@Index("idx_dlf_park", ["parkId"])
export class DriverLevelForecast {
  @PrimaryColumn({ name: "attraction_id", type: "uuid" })
  attractionId: string;

  @PrimaryColumn({ name: "target_date", type: "date" })
  targetDate: string;

  @Column({ name: "park_id", type: "uuid" })
  parkId: string;

  /** The park-local date the run counted its leads from. */
  @Column({ name: "origin_date", type: "date" })
  originDate: string;

  @Column({ name: "lead_days", type: "smallint" })
  leadDays: number;

  /** Daily P90 level in minutes: naive × exp(ŷ). */
  @Column({ type: "real" })
  level: number;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
