import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from "typeorm";

/**
 * ML Feature Drift Entity
 *
 * Tracks detected feature drift over time.
 * Records when production feature distributions significantly
 * deviate from training distributions.
 */
@Entity("ml_feature_drift")
@Index(["modelVersion", "featureName", "detectedAt"])
@Index(["status", "detectedAt"])
export class MLFeatureDrift {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ name: "model_version" })
  @Index()
  modelVersion: string;

  @Column({ name: "feature_name" })
  @Index()
  featureName: string;

  // Drift metrics
  @Column({ type: "float", name: "drift_score" })
  driftScore: number; // 0-100, higher = more drift

  @Column({ type: "float", name: "training_mean" })
  trainingMean: number;

  @Column({ type: "float", name: "production_mean" })
  productionMean: number;

  @Column({ type: "float", name: "training_std" })
  trainingStd: number;

  @Column({ type: "float", name: "production_std" })
  productionStd: number;

  // Statistical test results
  @Column({ type: "float", nullable: true, name: "ks_statistic" })
  ksStatistic: number | null; // Kolmogorov-Smirnov test

  @Column({ type: "float", nullable: true, name: "wasserstein_distance" })
  wassersteinDistance: number | null; // Earth mover's distance

  // Status
  @Column({
    type: "enum",
    enum: ["healthy", "warning", "critical"],
    default: "healthy",
  })
  @Index()
  status: "healthy" | "warning" | "critical";

  // Sample counts
  @Column({ type: "int", name: "production_sample_count" })
  productionSampleCount: number;

  @Column({ type: "timestamptz", name: "detected_at" })
  @Index()
  detectedAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
