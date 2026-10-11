import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
  Unique,
  CreateDateColumn,
} from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * PredictionAccuracy Entity
 *
 * Tracks prediction vs reality for model monitoring and improvement
 * Used for:
 * - Model performance tracking (compareWithActuals job)
 * - Pre-aggregation into attraction_accuracy_stats
 *
 * Note: API now reads from attraction_accuracy_stats for display,
 * this table is only used by background jobs.
 *
 * Two predictions per row since PAR-854: `predictedWaitTime` (raw CatBoost,
 * for the model boards and the ride day curve) and `servedWaitTime` (what the
 * reader saw, for the per-ride badge). Both are scored against the same actual.
 */
@Entity("prediction_accuracy")
@Unique("uq_pa_attraction_target", ["attractionId", "targetTime"])
// Composite index for the hourly aggregate-stats query:
//   WHERE comparison_status = 'COMPLETED' AND target_time >= $1
// Also covers getAttractionAccuracyStats per-attraction aggregate and cleanup-old job.
@Index("idx_pa_status_target", ["comparisonStatus", "targetTime"])
// Removed unused/redundant indexes:
// - @Index("idx_pa_attraction_target", ...) - redundant with unique constraint above
// - @Index(["modelVersion", "createdAt"]) - never queried (0 scans)
// - @Index(["targetTime"]) - covered by composite index above
// - @Index("idx_pa_target_actual", ...) - barely used (40 scans)
export class PredictionAccuracy {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  // No standalone index: attraction_id is the leading column of the
  // uq_pa_attraction_target unique index, which already serves attraction_id
  // lookups. A separate single-column index had 0 scans and only added write
  // cost to every accuracy upsert.
  @Column({ name: "attraction_id" })
  attractionId: string;

  @ManyToOne(() => Attraction)
  @JoinColumn({ name: "attraction_id" })
  attraction: Attraction;

  @Column({ name: "prediction_time", type: "timestamptz" })
  predictionTime: Date;

  @Column({ name: "target_time", type: "timestamptz" })
  targetTime: Date;

  @Column({ name: "predicted_wait_time", type: "int" })
  predictedWaitTime: number;

  @Column({ name: "actual_wait_time", type: "int", nullable: true })
  actualWaitTime: number | null; // Filled when actual data becomes available

  @Column({ name: "absolute_error", type: "int", nullable: true })
  absoluteError: number | null; // |predicted - actual|

  @Column({ name: "percentage_error", type: "float", nullable: true })
  percentageError: number | null; // (|predicted - actual| / actual) * 100

  @Column()
  modelVersion: string;

  /**
   * The wait a reader was SERVED for this slot when the row was written
   * (PAR-854) — `predictedWaitTime` after the PCN champion-swap override and
   * the §7.7 persistence blend, captured at generation time with the lead it
   * had then. `predictedWaitTime` stays the raw CatBoost answer, because the
   * admin boards compare CatBoost against PCN and must not see PCN on both
   * sides.
   *
   * NULL on every row written before PAR-854. Those scored CatBoost while users
   * were shown PCN, so the per-ride badge reads only rows that carry this
   * column. (The ride day-curve band reads `absoluteError` — that curve draws
   * CatBoost's own stored forecast; see `AttractionAccuracyStats.rawMae`.)
   */
  @Column({ name: "served_wait_time", type: "int", nullable: true })
  servedWaitTime: number | null;

  /** Which model produced {@link servedWaitTime}: `pcn` (override + blend) or
   *  `catboost` (flag off, no PCN forecast for the slot, or PCN stale). NULL
   *  exactly when `servedWaitTime` is. */
  @Column({
    name: "served_source",
    type: "varchar",
    length: 16,
    nullable: true,
  })
  servedSource: "pcn" | "catboost" | null;

  /** |servedWaitTime − actualWaitTime|, filled by the comparison job beside
   *  `absoluteError`. An unplanned closure charges the full served wait, as it
   *  does the raw one. */
  @Column({ name: "served_absolute_error", type: "int", nullable: true })
  servedAbsoluteError: number | null;

  @Column({
    type: "enum",
    enum: ["hourly", "daily"],
  })
  predictionType: "hourly" | "daily";

  // Features used (for debugging which features led to errors)
  @Column({ type: "jsonb", nullable: true })
  features: Record<string, unknown>;

  // Track unplanned closures (predicted open, but actually closed)
  @Column({ type: "boolean", default: false })
  wasUnplannedClosure: boolean;

  // No standalone index: comparison_status is the leading column of
  // idx_pa_status_target (comparison_status, target_time), which already serves
  // status-only lookups. A separate single-column index on a low-cardinality
  // enum added write cost for no read benefit.
  @Column({
    name: "comparison_status",
    type: "enum",
    enum: ["PENDING", "COMPLETED", "MISSED"],
    default: "PENDING",
  })
  comparisonStatus: "PENDING" | "COMPLETED" | "MISSED";

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
