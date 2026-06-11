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
import { RopeDropDayBucket } from "../../common/types/rope-drop.type";

/**
 * Attraction Rope-Drop Entity
 *
 * Stores the precomputed "is it worth rope-dropping this headliner" recommendation
 * per attraction (tier1/tier2 headliners only, parks with a schedule only).
 *
 * Two-layer model (see plan / docs):
 * - Shape (`rideByMinutesAfterOpen`, `bestSlotMinutesAfterOpen`): opening-relative
 *   offsets pooled over the full available history (season-stable).
 * - Levels (`busyPeak`/`openWait`/`savings` + `byDaytype`): absolute minutes on a
 *   trailing window, recomputed daily so they track the current season.
 *
 * Lifecycle:
 * - Populated by the `rope-drop` cron at 5:15 AM (after attraction-hourly-history
 *   at 4:30 AM), one query per park over tier1/2 headliners.
 * - `worth` flips across the year by design — a ride drops off the list in the
 *   off-season. Cache TTLs are kept short accordingly.
 * - Only worthy/qualifying rows carry a recommendation; non-worthy rides may still
 *   be stored with `worth=false` for completeness.
 */
@Entity("attraction_rope_drop")
@Index("idx_attraction_rope_drop_park", ["parkId"])
export class AttractionRopeDrop {
  @PrimaryColumn("uuid")
  attractionId: string;

  @Column("uuid")
  parkId: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column({ type: "boolean", default: false })
  worth: boolean;

  @Column({ type: "varchar", length: 10, nullable: true })
  strength: "high" | "moderate" | null;

  @Column({ type: "varchar", length: 10 })
  confidence: "high" | "medium" | "low";

  // Headline levels (busier of the weekend/weekday buckets).
  @Column({ type: "decimal", precision: 10, scale: 2 })
  busyPeak: number;

  @Column({ type: "decimal", precision: 10, scale: 2 })
  openWait: number;

  @Column({ type: "decimal", precision: 10, scale: 2 })
  savings: number;

  // Shape (opening-relative offsets, minutes after open).
  @Column({ type: "int" })
  rideByMinutesAfterOpen: number;

  @Column({ type: "int" })
  bestSlotMinutesAfterOpen: number;

  // Absolute wait (minutes) at the trough — the "come back later" payoff.
  @Column({ type: "decimal", precision: 10, scale: 2, default: 0 })
  bestSlotWait: number;

  // End-of-day verdict (evening counterpart to rope drop).
  @Column({ type: "boolean", default: false })
  endOfDayWorth: boolean;

  @Column({ type: "decimal", precision: 10, scale: 2, default: 0 })
  endOfDaySavings: number;

  // Level breakdown by day type.
  @Column({ type: "jsonb" })
  byDaytype: {
    weekend: RopeDropDayBucket;
    weekday: RopeDropDayBucket;
  };

  @Column({ type: "int" })
  windowDays: number; // trailing window length used for levels

  @Column({ type: "int" })
  sampleDays: number; // distinct operating days that contributed (confidence)

  @Column({ type: "timestamptz" })
  calculatedAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
