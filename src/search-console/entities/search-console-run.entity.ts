import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * One run of the import, for the status: when it last succeeded and what the
 * last one failed on. Inserted when a run starts and completed once when it
 * ends, and stamped every minute while it runs (`heartbeatAt`). A run that
 * died with its process stays unfinished; once its stamp is older than the
 * lock's five minutes it no longer counts as running.
 * Pruned after 90 days.
 */
@Entity("search_console_runs")
export class SearchConsoleRun {
  @PrimaryGeneratedColumn()
  id: number;

  /** `cron` or `manual`. */
  @Column({ name: "triggered_by", type: "varchar", length: 8 })
  triggeredBy: string;

  @CreateDateColumn({ name: "started_at", type: "timestamptz" })
  startedAt: Date;

  @Column({ name: "finished_at", type: "timestamptz", nullable: true })
  finishedAt: Date | null;

  /** The last minute the run was alive; null until its first stamp. */
  @Column({ name: "heartbeat_at", type: "timestamptz", nullable: true })
  heartbeatAt: Date | null;

  @Column({ type: "boolean", nullable: true })
  ok: boolean | null;

  @Column({ name: "days_checked", type: "integer", default: 0 })
  daysChecked: number;

  @Column({ name: "batches_written", type: "integer", default: 0 })
  batchesWritten: number;

  @Column({ name: "rows_written", type: "integer", default: 0 })
  rowsWritten: number;

  @Column({ type: "text", nullable: true })
  error: string | null;
}
