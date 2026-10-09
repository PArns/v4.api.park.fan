import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryColumn,
} from "typeorm";

/**
 * The park-level half of the forward archive (PAR-831): what the planner and
 * the calendar said about one park-day, as served at the 06:00 origin.
 *
 * The ride curves live in `forecast_archive_curves`; this row carries what is
 * not per ride — the crowd bucket the "Prognose heute" hero and the trip
 * assistant use (`predictedCrowdLevel`), the planner's tier and accuracy block,
 * and whether the day had rides at all (`ridesUnavailable`), which is the
 * coverage half of decision metric D9. A day with no curves is exactly the row
 * the curve table cannot hold.
 *
 * ~200 parks × 8 leads a day, a few hundred bytes each — kept a year.
 */
@Entity("forecast_archive_park_days")
@Index("idx_fapd_target", ["targetDate"])
export class ForecastArchiveParkDay {
  @PrimaryColumn({ name: "origin_at", type: "timestamptz" })
  originAt: Date;

  @PrimaryColumn({ name: "park_id", type: "uuid" })
  parkId: string;

  @PrimaryColumn({ name: "target_date", type: "date" })
  targetDate: string;

  @Column({ name: "lead_days", type: "smallint" })
  leadDays: number;

  /** plan_day `tier` (measured / composed / climatology / long_range …). */
  @Column({ type: "varchar", length: 16, nullable: true })
  tier: string | null;

  /** plan_day `context.status` (OPERATING / CLOSED / UNKNOWN). */
  @Column({ type: "varchar", length: 16, nullable: true })
  status: string | null;

  /** plan_day `context.crowdLevel` — the calendar cell the planner shows. */
  @Column({ name: "crowd_level", type: "varchar", length: 16, nullable: true })
  crowdLevel: string | null;

  /**
   * The calendar's `predictedCrowdLevel` — the forecast bucket the "Prognose
   * heute" hero reads, never live-overridden (decision metric D6).
   */
  @Column({
    name: "predicted_crowd_level",
    type: "varchar",
    length: 16,
    nullable: true,
  })
  predictedCrowdLevel: string | null;

  @Column({ name: "open_hour", type: "smallint", nullable: true })
  openHour: number | null;

  @Column({ name: "close_hour", type: "smallint", nullable: true })
  closeHour: number | null;

  @Column({ name: "hours_source", type: "varchar", length: 16, nullable: true })
  hoursSource: string | null;

  /** Rides the plan carried a curve for. */
  @Column({ name: "rides_offered", type: "smallint", default: 0 })
  ridesOffered: number;

  /** plan_day `ridesUnavailable.reason` when `rides` was empty. */
  @Column({
    name: "rides_unavailable",
    type: "varchar",
    length: 32,
    nullable: true,
  })
  ridesUnavailable: string | null;

  /** plan_day `accuracy.basis` ("measured" | "unmeasured"). */
  @Column({
    name: "accuracy_basis",
    type: "varchar",
    length: 16,
    nullable: true,
  })
  accuracyBasis: string | null;

  /** plan_day `accuracy.typicalError`. */
  @Column({ name: "typical_error", type: "real", nullable: true })
  typicalError: number | null;

  /** plan_day `leadTimeMae`. */
  @Column({ name: "lead_time_mae", type: "real", nullable: true })
  leadTimeMae: number | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
