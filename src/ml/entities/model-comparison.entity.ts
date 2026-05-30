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
 * forecasts (CatBoost: per-day MAX over DAILY_PEAK_HOURS ≈ P90 peak; TFT: daily P90);
 * once a target date has passed, the score-comparison job scores each model's
 * forecast (made before the target) against the realised actual daily P90 peak.
 *
 * Fair by construction: (1) only forecasts created strictly before the target date
 * are scored (no holdout leakage); (2) both models are scored on the SAME matched
 * (attraction, target_date) intersection at a comparable lead — each model's forward
 * forecast is snapshotted durably (tft_forecasts / catboost_daily_forecasts), so
 * `n` and `meanActual` are identical across the two rows of a given targetDate and
 * the lead no longer diverges (CatBoost's was inflated by daily prediction dedup).
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

  // Segment of the matched population this row scores. "all" = every matched
  // attraction; "busy" = matched rows whose realised daily P90 >= 40 (where the
  // crowd signal lives); "headliner" = matched rows in headliner_attractions.
  // Overall MAE hides TFT's edge (it lives on the busy/headliner tail), so the
  // board reports all three. PK includes segment → 3 rows per (targetDate, model).
  @PrimaryColumn({ type: "varchar", length: 16, default: "all" })
  segment: "all" | "busy" | "headliner";

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
