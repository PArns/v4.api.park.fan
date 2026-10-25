import { Column, Entity, Index, PrimaryColumn } from "typeorm";

/**
 * The park-level day as published, one row per version (PAR-1061, design PR 2).
 *
 * `schedule_entries` is upserted, deleted on retraction and rewritten by the
 * gap fill, so it only knows its latest state. This log records each change of
 * a park-day's derived state with `known_from` / `known_to`, written by
 * `ParksService.recordScheduleVersions` after the last writer of a sync has
 * finished with a park. Append-only apart from closing `known_to`; nothing reads
 * it yet. See docs/architecture/covariate-source.md §3.2.
 *
 * No FK (a merge re-points `park_id` through `PARK_DEPENDENCIES`), no hypertable.
 */
@Entity("schedule_day_versions")
@Index("uq_schedule_day_versions_open", ["parkId", "day"], {
  unique: true,
  where: `"known_to" IS NULL`,
})
export class ScheduleDayVersion {
  @PrimaryColumn({ type: "uuid", name: "park_id" })
  parkId: string;

  /** Park-local service date. */
  @PrimaryColumn({ type: "date" })
  day: string;

  @PrimaryColumn({ type: "timestamptz", name: "known_from" })
  knownFrom: Date;

  /** Null = the current version. */
  @Column({ type: "timestamptz", name: "known_to", nullable: true })
  knownTo: Date | null;

  /** OPERATING / CLOSED / UNKNOWN, or NONE (no park-level row: never published, or retracted). */
  @Column({ type: "text" })
  state: string;

  @Column({ type: "timestamptz", name: "open_utc", nullable: true })
  openUtc: Date | null;

  @Column({ type: "timestamptz", name: "close_utc", nullable: true })
  closeUtc: Date | null;

  @Column({ type: "smallint", name: "n_windows" })
  nWindows: number;

  @Column({ type: "boolean", name: "ticketed_event" })
  ticketedEvent: boolean;

  @Column({ type: "boolean", name: "private_event" })
  privateEvent: boolean;

  @Column({ type: "boolean", name: "extra_hours" })
  extraHours: boolean;

  @Column({ type: "timestamptz", name: "extra_open_utc", nullable: true })
  extraOpenUtc: Date | null;

  @Column({ type: "timestamptz", name: "extra_close_utc", nullable: true })
  extraCloseUtc: Date | null;

  /** themeparks / wartezeiten / live_fallback / gap_fill / backfill */
  @Column({ type: "text" })
  source: string;

  /** False for a park's first logged versions, whose `known_from` is `max(updatedAt)`. */
  @Column({ type: "boolean", name: "asof_exact" })
  asofExact: boolean;
}
