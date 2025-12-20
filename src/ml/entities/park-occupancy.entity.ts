import {
  Entity,
  PrimaryColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
  BeforeInsert,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { v4 as uuidv4 } from "uuid";

/**
 * ParkOccupancy Entity
 *
 * Stores historical occupancy scores for parks
 * Used for tracking trends and analytics
 */
@Entity("park_occupancy")
@Index(["parkId", "timestamp"])
@Index(["timestamp"])
export class ParkOccupancy {
  // Composite Primary Key (required for TimescaleDB)
  @PrimaryColumn("uuid")
  id: string;

  @PrimaryColumn({ type: "timestamptz" })
  timestamp: Date;

  @Column()
  @Index()
  parkId: string;

  @ManyToOne(() => Park)
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column({ type: "int" })
  occupancyScore: number; // 0-100+ (based on 95th percentile)

  @Column({
    type: "enum",
    enum: ["increasing", "stable", "decreasing"],
  })
  trend: "increasing" | "stable" | "decreasing";

  @Column({ type: "int" })
  comparedToTypical: number; // Percentage difference from baseline

  @Column({ type: "float" })
  baseline95thPercentile: number; // The P95 baseline used

  @Column({ type: "float" })
  currentAvgWait: number;

  @Column({ type: "int" })
  activeAttractions: number;

  @Column({ type: "int" })
  totalAttractions: number;

  @BeforeInsert()
  generateId(): void {
    if (!this.id) {
      this.id = uuidv4();
    }
    if (!this.timestamp) {
      this.timestamp = new Date();
    }
  }
}
