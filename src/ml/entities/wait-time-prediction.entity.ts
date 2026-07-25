import {
  Entity,
  PrimaryColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
  BeforeInsert,
} from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { v4 as uuidv4 } from "uuid";

/**
 * WaitTimePrediction Entity
 *
 * Stores ML predictions for attraction wait times
 * - Hourly: Next 24 hours (for immediate planning)
 * - Daily: Next 14 days (for trip planning)
 */
@Entity("wait_time_predictions")
@Index(["attractionId", "predictedTime"])
@Index(["predictionType", "createdAt"])
@Index(["attractionId", "predictionType", "createdAt"]) // getStoredPredictions: WHERE attractionId=X AND predictionType=Y AND createdAt>=Z
// Removed (2026-07-25) — this is the heaviest-written table in the system
// (~228k rows per prediction run), so every index is paid for on each insert:
// - @Index(["modelVersion"]) — 335 MB across chunks for 4 lifetime scans. The
//   only `modelVersion` filter in the codebase queries ml_feature_stats, a
//   different table, so nothing ever read this one.
// - @Index() on attractionId — 225 MB, fully covered as the leftmost prefix of
//   both ["attractionId", "predictedTime"] and
//   ["attractionId", "predictionType", "createdAt"].
export class WaitTimePrediction {
  // Composite Primary Key (required for TimescaleDB)
  @PrimaryColumn("uuid")
  id: string;

  @PrimaryColumn({ type: "timestamptz" })
  createdAt: Date; // When prediction was made

  @Column()
  attractionId: string;

  @ManyToOne(() => Attraction)
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column({ type: "timestamptz" })
  predictedTime: Date; // Time being predicted for

  @Column({ type: "int" })
  predictedWaitTime: number; // Predicted wait time in minutes

  @Column({
    type: "enum",
    enum: ["hourly", "daily"],
  })
  predictionType: "hourly" | "daily";

  @Column({ type: "float", nullable: true })
  confidence: number; // 0-100 confidence score

  @Column({
    type: "enum",
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
      "closed",
    ],
    nullable: true,
  })
  crowdLevel:
    | "very_low"
    | "low"
    | "moderate"
    | "high"
    | "very_high"
    | "extreme"
    | "closed";

  @Column({ type: "text", nullable: true })
  status: string | null;

  @Column({ type: "float", nullable: true })
  baseline: number; // Baseline wait time for comparison (rolling_avg_7d)

  @Column()
  modelVersion: string; // e.g., "v1.2.0", "catboost-2025-01" - for internal tracking only

  // Feature values used for prediction (for debugging)
  @Column({ type: "jsonb", nullable: true })
  features: Record<string, unknown>;

  @BeforeInsert()
  generateId(): void {
    if (!this.id) {
      this.id = uuidv4();
    }
    if (!this.createdAt) {
      this.createdAt = new Date();
    }
  }
}
