import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
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
 */
@Entity("prediction_accuracy")
// Keep only essential indexes for compareWithActuals job
@Index("idx_pa_attraction_target", ["attractionId", "targetTime"])
// Removed unused indexes:
// - @Index(["modelVersion", "createdAt"]) - never queried (0 scans)
// - @Index(["targetTime"]) - covered by composite index (172 scans)
// - @Index("idx_pa_target_actual", ...) - barely used (40 scans)
export class PredictionAccuracy {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ name: "attraction_id" })
  @Index()
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

  @Column({
    name: "comparison_status",
    type: "enum",
    enum: ["PENDING", "COMPLETED", "MISSED"],
    default: "PENDING",
  })
  @Index() // Index for fast lookup of pending records
  comparisonStatus: "PENDING" | "COMPLETED" | "MISSED";

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
