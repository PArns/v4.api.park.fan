import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from "typeorm";

/**
 * MLModel Entity
 *
 * Tracks ML model versions, performance metrics, and metadata
 */
@Entity("ml_models")
@Index(["version"], { unique: true })
@Index(["isActive"])
export class MLModel {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ unique: true })
  version: string; // e.g., "v1.2.0", "catboost-2025-01-26"

  @Column()
  modelType: string; // "catboost", "xgboost", "lightgbm"

  @Column()
  filePath: string; // Path to serialized model file

  // Performance metrics (on validation set)
  @Column({ type: "float", nullable: true })
  mae: number; // Mean Absolute Error

  @Column({ type: "float", nullable: true })
  rmse: number; // Root Mean Squared Error

  @Column({ type: "float", nullable: true })
  mape: number; // Mean Absolute Percentage Error

  @Column({ type: "float", nullable: true })
  r2Score: number; // R² Score

  // Training metadata
  @Column({ type: "timestamptz" })
  trainedAt: Date;

  @Column({ type: "timestamptz" })
  trainDataStartDate: Date;

  @Column({ type: "timestamptz" })
  trainDataEndDate: Date;

  @Column({ type: "int" })
  trainSamples: number;

  @Column({ type: "int", nullable: true })
  validationSamples: number;

  // Features used in this model
  @Column({ type: "jsonb" })
  featuresUsed: string[];

  // Hyperparameters
  @Column({ type: "jsonb", nullable: true })
  hyperparameters: Record<string, unknown>;

  @Column({ default: true })
  isActive: boolean; // Only one model should be active at a time

  @Column({ type: "text", nullable: true })
  notes: string;

  @CreateDateColumn()
  createdAt: Date;
}
