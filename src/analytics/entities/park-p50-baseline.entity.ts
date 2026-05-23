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
export class ParkP50Baseline {
  @PrimaryColumn("uuid")
  parkId: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column({ type: "decimal", precision: 10, scale: 2 })
  p50Baseline: number; // P50 (median) wait time in minutes

  // Typical-day-peak baseline: median over operating days of the day value
  // (AVG across headliners of each ride's daily P90). This is the reference
  // the calendar divides a day's peak by, so 100% = a typical day = moderate.
  // Written atomically with p50/p90 by the daily cron. Nullable so existing
  // rows survive the rollout until the next cron run fills it.
  @Column({ type: "decimal", precision: 10, scale: 2, nullable: true })
  typicalDayPeak: number | null;

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
