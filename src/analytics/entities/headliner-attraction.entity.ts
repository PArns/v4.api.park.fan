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
 * Headliner Attraction Entity
 *
 * Stores identified headliner attractions per park with tier classification.
 * Used for P50 baseline calculation (headliners only).
 *
 * Tier Classification:
 * - tier1: Major parks (AVG > 15min, P90 > 25min)
 * - tier2: Medium parks (Top 50%, relative thresholds)
 * - tier3: Small parks (All attractions, fallback)
 *
 * Updated: Daily via background job
 */
@Entity("headliner_attractions")
@Index(["parkId", "tier"])
export class HeadlinerAttraction {
  @PrimaryColumn("uuid")
  parkId: string;

  @PrimaryColumn("uuid")
  attractionId: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column({ type: "varchar", length: 10 })
  tier: "tier1" | "tier2" | "tier3";

  @Column({ type: "decimal", precision: 10, scale: 2 })
  avgWait548d: number; // Average wait time over 548 days

  @Column({ type: "decimal", precision: 10, scale: 2 })
  p50Wait548d: number; // P50 (median) over 548 days

  @Column({ type: "decimal", precision: 10, scale: 2 })
  p90Wait548d: number; // P90 over 548 days

  @Column({ type: "int" })
  operatingDays: number; // Days attraction was operating

  @Column({ type: "int" })
  sampleCount: number; // Total data points

  @Column({ type: "timestamptz" })
  lastCalculatedAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
