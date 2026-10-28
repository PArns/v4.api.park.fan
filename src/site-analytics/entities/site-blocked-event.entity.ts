import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * An event name the admin blocked: no longer recorded, still listed (flagged)
 * in the reports.
 */
@Entity("site_blocked_events")
export class SiteBlockedEvent {
  @PrimaryColumn({ type: "varchar", length: 48 })
  name: string;

  @Column({ name: "blocked_at", type: "timestamptz", default: () => "now()" })
  blockedAt: Date;

  /** The admin's e-mail. */
  @Column({ name: "blocked_by", type: "varchar", length: 255 })
  blockedBy: string;
}
