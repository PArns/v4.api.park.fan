import {
  Entity,
  Column,
  PrimaryColumn,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";

/**
 * Park P90 Baseline Entity
 *
 * Stores pre-calculated P90 (peak) baselines per park — populated alongside
 * the P50 baseline (3 AM cron) using the same headliner pool. Decoupled
 * from ParkP50Baseline so callers that only need peak/typical don't pay
 * for the other column and so the two baselines can evolve independently.
 *
 * Cached: Redis `park:p90:{parkId}` (24h TTL)
 */
@Entity("park_p90_baselines")
export class ParkP90Baseline {
  @PrimaryColumn("uuid")
  parkId: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column({ type: "decimal", precision: 10, scale: 2 })
  p90Baseline: number;

  @Column({ type: "int" })
  headlinerCount: number;

  @Column({ type: "varchar", length: 10 })
  tier: "tier1" | "tier2" | "tier3";

  @Column({ type: "int" })
  sampleCount: number;

  @Column({ type: "int" })
  distinctDays: number;

  @Column({ type: "varchar", length: 10 })
  confidence: "high" | "medium" | "low";

  @Column({ type: "timestamptz" })
  calculatedAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
