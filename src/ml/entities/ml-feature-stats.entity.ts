import {
  Entity,
  PrimaryColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";

/**
 * ML Feature Stats Entity
 *
 * Stores feature distribution statistics from model training.
 * Used for data drift detection by comparing production features
 * against training distributions.
 */
@Entity("ml_feature_stats")
@Index(["modelVersion", "featureName"], { unique: true })
export class MLFeatureStats {
  @PrimaryColumn({ name: "model_version" })
  modelVersion: string;

  @PrimaryColumn({ name: "feature_name" })
  @Index()
  featureName: string;

  // Distribution statistics
  @Column({ type: "float" })
  mean: number;

  @Column({ type: "float" })
  std: number;

  @Column({ type: "float" })
  min: number;

  @Column({ type: "float" })
  max: number;

  // Percentiles for robust drift detection
  @Column({ type: "float", name: "percentile_10" })
  percentile10: number;

  @Column({ type: "float", name: "percentile_50" })
  percentile50: number;

  @Column({ type: "float", name: "percentile_90" })
  percentile90: number;

  // Sample count for context
  @Column({ type: "int", name: "sample_count" })
  sampleCount: number;

  // Feature type (categorical vs numeric)
  @Column({ type: "varchar", length: 20, name: "feature_type" })
  featureType: "numeric" | "categorical";

  // For categorical features: most common values
  @Column({ type: "jsonb", nullable: true, name: "top_values" })
  topValues: Record<string, number> | null; // { value: count }

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
