import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
  JoinColumn,
} from "typeorm";
import { Park } from "./park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * Schedule Entry Entity
 *
 * Stores park operating schedules from ThemeParks.wiki API.
 * Includes regular operating hours, special events, ticketed events, etc.
 *
 * API Source: GET /entity/{id}/schedule
 */

export enum ScheduleType {
  OPERATING = "OPERATING",
  TICKETED_EVENT = "TICKETED_EVENT",
  PRIVATE_EVENT = "PRIVATE_EVENT",
  EXTRA_HOURS = "EXTRA_HOURS",
  INFO = "INFO",
  MAINTENANCE = "MAINTENANCE",
  CLOSED = "CLOSED",
  UNKNOWN = "UNKNOWN", // For holidays/bridge days without specific hours
}

@Entity("schedule_entries")
@Index(["parkId", "date", "scheduleType"]) // Covers range queries getSchedule(parkId, from, to) via leftmost prefix
@Index(["date"])
@Index(["attractionId", "date"])
export class ScheduleEntry {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column()
  parkId: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE", nullable: true })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column({ nullable: true })
  attractionId: string | null;

  @Column({ type: "date" })
  date: Date;

  @Column({
    type: "enum",
    enum: ScheduleType,
  })
  scheduleType: ScheduleType;

  @Column({ type: "timestamptz", nullable: true })
  openingTime: Date | null;

  @Column({ type: "timestamptz", nullable: true })
  closingTime: Date | null;

  @Column({ type: "text", nullable: true })
  description: string | null;

  @Column({ type: "jsonb", nullable: true })
  purchases: Array<{
    type: string;
    price: {
      amount: number;
      currency: string;
      formatted: string;
    };
  }> | null;

  @Column({ type: "boolean", default: false })
  isHoliday: boolean;

  @Column({ type: "text", nullable: true })
  holidayName: string | null;

  @Column({ type: "boolean", default: false })
  isBridgeDay: boolean;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
