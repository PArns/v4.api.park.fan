import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
  CreateDateColumn,
} from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * PredictionAccuracy Entity
 *
 * Tracks prediction vs reality for model monitoring and improvement
 * Used for:
 * - Model performance tracking
 * - Feature engineering feedback
 * - Displaying accuracy metrics to users
 */
@Entity("prediction_accuracy")
@Index(["attractionId", "targetTime"])
@Index(["modelVersion", "createdAt"])
@Index(["targetTime"])
export class PredictionAccuracy {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column()
  @Index()
  attractionId: string;

  @ManyToOne(() => Attraction)
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column({ type: "timestamp" })
  predictionTime: Date; // When the prediction was made

  @Column({ type: "timestamp" })
  targetTime: Date; // The time that was being predicted

  @Column({ type: "int" })
  predictedWaitTime: number;

  @Column({ type: "int", nullable: true })
  actualWaitTime: number | null; // Filled when actual data becomes available

  @Column({ type: "int", nullable: true })
  absoluteError: number | null; // |predicted - actual|

  @Column({ type: "float", nullable: true })
  percentageError: number | null; // (|predicted - actual| / actual) * 100

  @Column()
  modelVersion: string;

  @Column({
    type: "enum",
    enum: ["hourly", "daily"],
  })
  predictionType: "hourly" | "daily";

  // Features used (for debugging which features led to errors)
  @Column({ type: "jsonb", nullable: true })
  features: Record<string, unknown>;

  // Track unplanned closures (predicted open, but actually closed)
  @Column({ type: "boolean", default: false })
  wasUnplannedClosure: boolean;

  @CreateDateColumn()
  createdAt: Date;
}
