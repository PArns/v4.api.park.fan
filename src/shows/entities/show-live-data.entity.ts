import {
  Entity,
  PrimaryColumn,
  Column,
  ManyToOne,
  Index,
  JoinColumn,
  BeforeInsert,
} from "typeorm";
import { Show } from "./show.entity";
import { LiveStatus } from "../../external-apis/themeparks/themeparks.types";

/**
 * Show Live Data Entity
 *
 * Stores live status and showtimes for shows.
 * Supports tracking:
 * - Status changes (OPERATING, CLOSED, DOWN, REFURBISHMENT)
 * - Showtimes (performances, parades, fireworks, etc.)
 *
 * TimescaleDB hypertable with composite primary key for time-series data.
 *
 * Delta Strategy:
 * - Save when status changes
 * - Save when showtimes array changes (new times, removed times, or modified times)
 * - Skip if no significant changes detected
 */
@Entity("show_live_data")
@Index(["showId", "timestamp"]) // For efficient time-series queries
@Index(["status", "timestamp"]) // For filtering by status
export class ShowLiveData {
  @PrimaryColumn({ type: "uuid" })
  id: string;

  @ManyToOne(() => Show, (show) => show.liveData, { onDelete: "CASCADE" })
  @JoinColumn({ name: "showId" })
  show: Show;

  @Column()
  showId: string;

  @Column({
    type: "enum",
    enum: LiveStatus,
  })
  status: LiveStatus;

  // Showtimes array (JSONB for flexible schema)
  // Supports: performances, parades, fireworks, character meet & greets, etc.
  @Column({ type: "jsonb", nullable: true })
  showtimes: Array<{
    type: string; // e.g., "Performance", "Parade", "Fireworks"
    startTime: string; // ISO timestamp
    endTime?: string; // ISO timestamp (optional)
  }> | null;

  // API timestamp: when ThemeParks.wiki last updated this data
  @Column({ type: "timestamp", nullable: true })
  lastUpdated: Date | null;

  @Column({ type: "jsonb", nullable: true })
  operatingHours: Array<{
    type: string;
    startTime: string;
    endTime: string;
  }> | null;

  @PrimaryColumn({ type: "timestamp" })
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
