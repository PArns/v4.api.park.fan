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
 * Attraction P90 Baseline Entity
 *
 * Stores P90 (peak) baseline per individual attraction — populated by the
 * same 4 AM cron as the P50 baseline, sharing the underlying 548-day scan
 * so we don't pay PERCENTILE_CONT twice. Used for peakCrowdLevel
 * comparisons (crowd vs. peak-day reference) on the calendar, attraction
 * detail, and similar surfaces.
 *
 * Cached: Redis `attraction:p90:{attractionId}` (24h TTL)
 */
@Entity("attraction_p90_baselines")
@Index("idx_attraction_p90_park", ["parkId"])
export class AttractionP90Baseline {
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
  p90Baseline: number;

  @Column({ type: "boolean", default: false })
  isHeadliner: boolean;

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
