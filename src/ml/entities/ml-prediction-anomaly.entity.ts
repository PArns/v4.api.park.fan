import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
  ManyToOne,
  JoinColumn,
} from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * ML Prediction Anomaly Entity
 *
 * Tracks anomalous predictions that deviate significantly from expected patterns.
 * Used for identifying model issues, data quality problems, and edge cases.
 */
@Entity("ml_prediction_anomalies")
@Index(["attractionId", "detectedAt"])
@Index(["severity", "detectedAt"])
@Index(["anomalyType", "detectedAt"])
export class MLPredictionAnomaly {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ name: "attraction_id" })
  @Index()
  attractionId: string;

  @ManyToOne(() => Attraction)
  @JoinColumn({ name: "attraction_id" })
  attraction: Attraction;

  @Column({ name: "park_id", type: "varchar", length: 255, nullable: true })
  parkId: string | null;

  @Column({
    type: "enum",
    enum: [
      "extreme_value",
      "large_error",
      "unexpected_closure",
      "feature_drift",
      "confidence_mismatch",
    ],
    name: "anomaly_type",
  })
  @Index()
  anomalyType:
    | "extreme_value"
    | "large_error"
    | "unexpected_closure"
    | "feature_drift"
    | "confidence_mismatch";

  @Column({
    type: "enum",
    enum: ["low", "medium", "high"],
    default: "medium",
  })
  @Index()
  severity: "low" | "medium" | "high";

  // Prediction details
  @Column({ type: "timestamptz", name: "predicted_time" })
  predictedTime: Date;

  @Column({ type: "int", name: "predicted_wait_time" })
  predictedWaitTime: number;

  @Column({ type: "int", nullable: true, name: "actual_wait_time" })
  actualWaitTime: number | null;

  @Column({ type: "int", nullable: true, name: "absolute_error" })
  absoluteError: number | null;

  @Column({ type: "float", nullable: true, name: "confidence" })
  confidence: number | null;

  // Anomaly metrics
  @Column({ type: "float", name: "anomaly_score" })
  anomalyScore: number; // 0-100, higher = more anomalous

  @Column({ type: "text", nullable: true, name: "reason" })
  reason: string | null; // Human-readable explanation

  // Context
  @Column({ type: "jsonb", nullable: true, name: "feature_values" })
  featureValues: Record<string, unknown> | null; // Features at time of prediction

  @Column({ type: "varchar", length: 50, name: "model_version" })
  modelVersion: string;

  @Column({ type: "timestamptz", name: "detected_at" })
  @Index()
  detectedAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
