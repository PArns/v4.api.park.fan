import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
  ManyToOne,
  JoinColumn,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";

@Entity("park_daily_stats")
@Index(["parkId", "date"], { unique: true })
export class ParkDailyStats {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column()
  parkId: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column({ type: "date" })
  date: string; // YYYY-MM-DD

  @Column({ type: "int", nullable: true })
  p90WaitTime: number | null;

  @Column({ type: "int", nullable: true })
  maxWaitTime: number | null;

  @Column({ type: "int", nullable: true })
  attendance: number | null;

  @Column({ type: "jsonb", nullable: true })
  metadata: Record<string, any> | null;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
