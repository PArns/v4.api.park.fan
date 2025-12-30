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

  @Column({ type: "real", nullable: true })
  mae: number | null; // Mean Absolute Error (minutes)

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
