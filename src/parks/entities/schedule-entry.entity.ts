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
}

@Entity("schedule_entries")
@Index(["park", "date"])
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

  @Column({ type: "timestamp", nullable: true })
  openingTime: Date | null;

  @Column({ type: "timestamp", nullable: true })
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

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
