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

/**
 * Park P50 Baseline Entity
 *
 * Stores pre-calculated P50 (median) baselines per park.
 * Calculated using headliner attractions only over 548-day window.
 *
 * Single source of truth for park-wide crowd level baselines.
 *
 * Updated: Daily via background job (3 AM)
 * Cached: Redis `park:p50:{parkId}` (24h TTL)
 */
@Entity("park_p50_baselines")
@Index("idx_park_p50_confidence", ["confidence"])
@Index("idx_park_p50_calculated", ["calculatedAt"])
export class ParkP50Baseline {
  @PrimaryColumn("uuid")
  parkId: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column({ type: "decimal", precision: 10, scale: 2 })
  p50Baseline: number; // P50 (median) wait time in minutes

  @Column({ type: "int" })
  headlinerCount: number; // Number of headliners used

  @Column({ type: "varchar", length: 10 })
  tier: "tier1" | "tier2" | "tier3"; // Which tier was used

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
