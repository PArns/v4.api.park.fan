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
// ["attractionId", "predictionType", "createdAt"] is NOT declared separately —
// it is the leftmost prefix of the primary key below, which serves
// getStoredPredictions (WHERE attractionId=X AND predictionType=Y AND createdAt>=Z).
// Removed (2026-07-25) — this is the heaviest-written table in the system
// (~228k rows per prediction run), so every index is paid for on each insert:
// - @Index(["modelVersion"]) — 335 MB across chunks for 4 lifetime scans. The
//   only `modelVersion` filter in the codebase queries ml_feature_stats, a
//   different table, so nothing ever read this one.
// - @Index() on attractionId — 225 MB, fully covered as the leftmost prefix of
//   both ["attractionId", "predictedTime"] and
//   ["attractionId", "predictionType", "createdAt"].
export class WaitTimePrediction {
  // NATURAL primary key (2026-07-25). The old surrogate key was (id uuid,
  // createdAt): 822 MB — the largest index on the table — with ZERO scans,
  // because nothing ever looks a prediction up by id. TimescaleDB requires the
  // partition column (createdAt) in any unique index, so the surrogate key
  // could not be narrowed; replacing it with the natural key removes the
  // 822 MB index AND the 16-byte-per-row uuid column, and its leftmost prefix
  // subsumes the former ["attractionId", "predictionType", "createdAt"] index
  // (another 276 MB). Verified unique across all 24.66M rows before the switch.
  @PrimaryColumn()
  attractionId: string;

  @PrimaryColumn({ type: "timestamptz" })
  createdAt: Date; // When prediction was made — also the partition column

  @PrimaryColumn({ type: "timestamptz" })
  predictedTime: Date; // Time being predicted for

  @ManyToOne(() => Attraction)
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column({ type: "int" })
  predictedWaitTime: number; // Predicted wait time in minutes

  @PrimaryColumn({
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
  defaultCreatedAt(): void {
    if (!this.createdAt) {
      this.createdAt = new Date();
    }
  }
}
