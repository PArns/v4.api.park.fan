import {
  Entity,
  Column,
  PrimaryColumn,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";

/**
 * Model Comparison Entity — TFT-vs-CatBoost forward scoreboard.
 *
 * One row per (targetDate, model). Both models make genuine FORWARD daily-peak
 * forecasts (CatBoost: its 14:00 daily prediction as a peak proxy; TFT: daily P90);
 * once a target date has passed, the score-comparison job scores each model's
 * forecast (made before the target) against the realised actual daily P90 peak.
 *
 * Fair by construction: only forecasts created strictly before the target date
 * are scored — no holdout leakage, no in-sample advantage.
 *
 * Written: daily via the nf-training queue `score-comparison` job (~08:00 UTC).
 */
@Entity("model_comparisons")
@Index("idx_model_comparison_model", ["model"])
export class ModelComparison {
  @PrimaryColumn({ type: "date" })
  targetDate: string; // the day being scored (park-local date)

  @PrimaryColumn({ type: "varchar", length: 16 })
  model: "catboost" | "tft";

  @Column({ type: "int" })
  n: number; // attractions scored for this (targetDate, model)

  @Column({ type: "double precision" })
  mae: number; // mean abs error vs actual daily P90 peak (minutes)

  @Column({ type: "double precision" })
  bias: number; // mean (pred - actual); negative = under-prediction

  @Column({ type: "double precision" })
  meanActual: number; // mean actual daily P90 peak across scored attractions

  @Column({ type: "double precision" })
  meanPred: number; // mean predicted peak across scored attractions

  @Column({ type: "int" })
  avgLeadDays: number; // mean lead time (target_date - forecast made date)

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
