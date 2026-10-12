import {
  Entity,
  Column,
  PrimaryColumn,
  ManyToOne,
  JoinColumn,
  UpdateDateColumn,
} from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * AttractionAccuracyStats Entity
 *
 * Pre-aggregated prediction accuracy statistics per attraction.
 * Updated daily by cron job to avoid N+1 queries on park endpoint.
 *
 * Contains:
 * - MAE (Mean Absolute Error) from last 30 days
 * - Badge (excellent/good/fair/poor/insufficient_data)
 * - Prediction counts for context
 */
@Entity("attraction_accuracy_stats")
export class AttractionAccuracyStats {
  @PrimaryColumn({ name: "attraction_id" })
  attractionId: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attraction_id" })
  attraction: Attraction;

  /**
   * Mean absolute error of the SERVED intraday forecast (minutes) — PCN plus
   * the persistence blend where it serves, CatBoost where it does not
   * (PAR-854). Drives `badge`. Counted only over rows that recorded a served
   * value, so it is NULL for a ride with none yet.
   */
  @Column({ type: "real", nullable: true })
  mae: number | null;

  /**
   * Mean absolute error of the RAW CatBoost hourly answer (minutes), over every
   * compared row in the window; NULL below the badge's sample floor.
   *
   * Kept beside `mae` for the one surface that draws CatBoost's own curve: the
   * ride day curve (`/stats/day`) plots `wait_time_predictions`, which is raw
   * CatBoost, so its `forecastError` band must be CatBoost's error — a PCN
   * error drawn around a CatBoost line would describe a different forecast.
   */
  @Column({ name: "raw_mae", type: "real", nullable: true })
  rawMae: number | null;

  @Column({ name: "compared_predictions", type: "int", default: 0 })
  comparedPredictions: number;

  @Column({ name: "total_predictions", type: "int", default: 0 })
  totalPredictions: number;

  @Column({
    type: "varchar",
    length: 20,
    default: "insufficient_data",
  })
  badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";

  @Column({ type: "text", nullable: true })
  message: string | null;

  @UpdateDateColumn({ name: "updated_at", type: "timestamptz" })
  updatedAt: Date;
}
