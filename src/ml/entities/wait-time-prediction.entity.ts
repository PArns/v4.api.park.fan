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
@Index(["modelVersion"])
export class WaitTimePrediction {
  // Composite Primary Key (required for TimescaleDB)
  @PrimaryColumn("uuid")
  id: string;

  @PrimaryColumn({ type: "timestamp" })
  createdAt: Date; // When prediction was made

  @Column()
  @Index()
  attractionId: string;

  @ManyToOne(() => Attraction)
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column({ type: "timestamp" })
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

  @Column({ type: "varchar", nullable: true })
  status: string | null;

  @Column({ type: "float", nullable: true })
  baseline: number; // Baseline wait time for comparison (rolling_avg_7d)

  @Column()
  modelVersion: string; // e.g., "v1.2.0", "catboost-2025-01"

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
