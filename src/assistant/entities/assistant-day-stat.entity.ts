import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * What the pruned chats of one UTC day added up to, so the admin's 30-day statistic outlives the
 * 7-day retention. Counts only: no text, no visitor key, nothing that tells one chat from another.
 * `pruneConversations` adds each batch it deletes; `stats()` sums these with the live rows.
 */
@Entity("assistant_day_stats")
export class AssistantDayStat {
  /** The UTC day the chats (or, for `up`/`down`, the rated answers) were created. */
  @PrimaryColumn({ type: "date" })
  day: string;

  @Column({ type: "integer", default: 0 })
  chats: number;

  @Column({ type: "integer", default: 0 })
  messages: number;

  @Column({ type: "bigint", default: 0 })
  tokens: string;

  @Column({ type: "numeric", precision: 12, scale: 6, default: 0 })
  costUsd: string;

  @Column({ type: "integer", default: 0 })
  up: number;

  @Column({ type: "integer", default: 0 })
  down: number;
}
