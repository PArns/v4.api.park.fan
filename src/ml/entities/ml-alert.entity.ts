import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";

/**
 * ML Alert Entity
 *
 * Tracks alerts for model performance issues, data drift, and anomalies.
 * Provides alert history and acknowledgment tracking.
 */
@Entity("ml_alerts")
@Index(["status", "severity", "createdAt"])
@Index(["alertType", "createdAt"])
export class MLAlert {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({
    type: "enum",
    enum: [
      "accuracy_degradation",
      "feature_drift",
      "low_coverage",
      "anomaly_detected",
      "model_drift",
    ],
    name: "alert_type",
  })
  @Index()
  alertType:
    | "accuracy_degradation"
    | "feature_drift"
    | "low_coverage"
    | "anomaly_detected"
    | "model_drift";

  @Column({
    type: "enum",
    enum: ["low", "medium", "high", "critical"],
    default: "medium",
  })
  @Index()
  severity: "low" | "medium" | "high" | "critical";

  @Column({
    type: "enum",
    enum: ["active", "acknowledged", "resolved"],
    default: "active",
  })
  @Index()
  status: "active" | "acknowledged" | "resolved";

  @Column({ type: "varchar", length: 255 })
  title: string;

  @Column({ type: "text" })
  message: string;

  // Context data (JSON)
  @Column({ type: "jsonb", nullable: true })
  metrics: Record<string, unknown> | null;

  @Column({ type: "jsonb", nullable: true })
  context: Record<string, unknown> | null; // Additional context

  // Acknowledgment
  @Column({
    type: "varchar",
    length: 255,
    nullable: true,
    name: "acknowledged_by",
  })
  acknowledgedBy: string | null;

  @Column({ type: "timestamptz", nullable: true, name: "acknowledged_at" })
  acknowledgedAt: Date | null;

  // Resolution
  @Column({ type: "text", nullable: true, name: "resolution_note" })
  resolutionNote: string | null;

  @Column({ type: "timestamptz", nullable: true, name: "resolved_at" })
  resolvedAt: Date | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
