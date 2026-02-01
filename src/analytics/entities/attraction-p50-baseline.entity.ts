import {
  Entity,
  Column,
  PrimaryColumn,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * Attraction P50 Baseline Entity
 *
 * Stores P50 (median) baseline per individual attraction.
 * Used for per-ride crowd level calculations.
 *
 * Updated: Daily via background job (4 AM)
 * Cached: Redis `attraction:p50:{attractionId}` (24h TTL)
 */
@Entity("attraction_p50_baselines")
@Index("idx_attraction_p50_park", ["parkId"])
@Index("idx_attraction_p50_confidence", ["confidence"])
@Index("idx_attraction_p50_headliner", ["isHeadliner"])
export class AttractionP50Baseline {
  @PrimaryColumn("uuid")
  attractionId: string;

  @Column("uuid")
  parkId: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @ManyToOne(() => Park)
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column({ type: "decimal", precision: 10, scale: 2 })
  p50Baseline: number; // P50 (median) wait time in minutes

  @Column({ type: "boolean", default: false })
  isHeadliner: boolean; // Is this attraction a headliner?

  @Column({ type: "int" })
  sampleCount: number; // Total data points

  @Column({ type: "int" })
  distinctDays: number; // Distinct days in calculation

  @Column({ type: "varchar", length: 10 })
  confidence: "high" | "medium" | "low"; // Data quality indicator

  @Column({ type: "timestamptz" })
  calculatedAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
