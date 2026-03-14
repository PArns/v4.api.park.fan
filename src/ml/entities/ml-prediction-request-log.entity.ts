import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from "typeorm";

/**
 * ML Prediction Request Log Entity
 *
 * Logs all prediction requests for analytics and monitoring.
 * Tracks request volume, latency, and usage patterns.
 */
@Entity("ml_prediction_request_log")
@Index(["parkId", "createdAt"]) // Only composite index needed for park-scoped time queries
export class MLPredictionRequestLog {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ type: "varchar", length: 255, nullable: true, name: "park_id" })
  parkId: string | null;

  @Column({ type: "int", name: "attraction_count" })
  attractionCount: number;

  @Column({ type: "int", name: "park_count" })
  parkCount: number;

  @Column({
    type: "enum",
    enum: ["hourly", "daily"],
    name: "prediction_type",
  })
  predictionType: "hourly" | "daily";

  @Column({ type: "varchar", length: 50, name: "model_version" })
  modelVersion: string;

  // Performance metrics
  @Column({ type: "int", name: "duration_ms" })
  durationMs: number; // Request duration in milliseconds

  @Column({ type: "int", nullable: true, name: "prediction_count" })
  predictionCount: number | null; // Number of predictions returned

  // Request metadata
  @Column({ type: "jsonb", nullable: true, name: "request_metadata" })
  requestMetadata: Record<string, unknown> | null; // Additional request info

  @CreateDateColumn({ type: "timestamptz", name: "created_at" })
  createdAt: Date;
}
