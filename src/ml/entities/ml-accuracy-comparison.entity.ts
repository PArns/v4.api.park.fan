import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from "typeorm";

/**
 * ML Accuracy Comparison Entity
 *
 * Stores daily comparisons between predicted and actual wait times
 * for model drift monitoring and accuracy tracking
 */
@Entity("ml_accuracy_comparisons")
@Index(["date"])
@Index(["attractionId"])
export class MLAccuracyComparison {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ type: "date" })
  date: Date;

  @Column()
  parkId: string;

  @Column()
  attractionId: string;

  @Column({ type: "timestamptz" })
  predictedAt: Date;

  @Column({ type: "timestamptz" })
  actualAt: Date;

  @Column({ type: "int" })
  predictedWaitTime: number;

  @Column({ type: "int" })
  actualWaitTime: number;

  @Column({ type: "int" })
  absoluteError: number; // |predicted - actual|

  @Column({ type: "varchar", length: 20 })
  predictionType: string; // 'hourly' | 'daily'

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
