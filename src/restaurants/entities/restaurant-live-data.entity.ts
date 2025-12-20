import {
  Entity,
  PrimaryColumn,
  Column,
  ManyToOne,
  Index,
  JoinColumn,
  BeforeInsert,
} from "typeorm";
import { Restaurant } from "./restaurant.entity";
import { LiveStatus } from "../../external-apis/themeparks/themeparks.types";

/**
 * Restaurant Live Data Entity
 *
 * Stores live dining availability and wait times for restaurants.
 * Supports tracking:
 * - Status changes (OPERATING, CLOSED, DOWN, REFURBISHMENT)
 * - Dining wait times (minutes)
 * - Party size availability
 *
 * TimescaleDB hypertable with composite primary key for time-series data.
 *
 * Delta Strategy:
 * - Save when status changes
 * - Save when wait time changes by > 5 minutes (same threshold as attraction queues)
 * - Save when party size changes
 * - Skip if no significant changes detected
 */
@Entity("restaurant_live_data")
@Index(["restaurantId", "timestamp"]) // For efficient time-series queries
@Index(["status", "timestamp"]) // For filtering by status
export class RestaurantLiveData {
  @PrimaryColumn({ type: "uuid" })
  id: string;

  @ManyToOne(() => Restaurant, (restaurant) => restaurant.restaurantLiveData, {
    onDelete: "CASCADE",
  })
  @JoinColumn({ name: "restaurantId" })
  restaurant: Restaurant;

  @Column()
  restaurantId: string;

  @Column({
    type: "enum",
    enum: LiveStatus,
  })
  status: LiveStatus;

  // Dining availability (not all restaurants provide this)
  @Column({ type: "int", nullable: true })
  partySize: number | null; // e.g., 2, 4, 6

  @Column({ type: "int", nullable: true })
  waitTime: number | null; // minutes (similar to attraction wait times)

  // API timestamp: when ThemeParks.wiki last updated this data
  @Column({ type: "timestamptz", nullable: true })
  lastUpdated: Date | null;

  @Column({ type: "jsonb", nullable: true })
  operatingHours: Array<{
    type: string;
    startTime: string;
    endTime: string;
  }> | null;

  @PrimaryColumn({ type: "timestamptz" })
  timestamp: Date;

  @BeforeInsert()
  generateId() {
    if (!this.id) {
      this.id = crypto.randomUUID();
    }
    if (!this.timestamp) {
      this.timestamp = new Date();
    }
  }
}
