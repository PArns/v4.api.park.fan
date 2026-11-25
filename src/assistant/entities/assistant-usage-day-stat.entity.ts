import { Column, Entity, PrimaryColumn } from "typeorm";
import type { AssistantSource } from "../assistant-session.util";

/** `apiKeyId` of the chats no data key opened: a primary key column cannot be null. */
export const NO_API_KEY = "00000000-0000-0000-0000-000000000000";

/**
 * What the pruned chats of one UTC day added up to per source (website or app) and data key, with
 * the tokens by kind, so the token statistic and its sums per key outlive the 7-day retention. Counts only, as
 * `assistant_day_stats`, which stays as it is and holds the same chats without the source: what
 * that table has beyond this one was pruned before the source was stored, and counts as `web`.
 */
@Entity("assistant_usage_day_stats")
export class AssistantUsageDayStat {
  /** The UTC day the chats were created. */
  @PrimaryColumn({ type: "date" })
  day: string;

  @PrimaryColumn({ type: "varchar", length: 8 })
  source: AssistantSource;

  /** The data key whose sessions started the chats, `NO_API_KEY` for a browser's. */
  @PrimaryColumn({ type: "uuid", default: NO_API_KEY })
  apiKeyId: string;

  @Column({ type: "integer", default: 0 })
  chats: number;

  @Column({ type: "integer", default: 0 })
  messages: number;

  /** Fresh input and cache writes, as `assistant_messages.inputTokens`. */
  @Column({ type: "bigint", default: 0 })
  inputTokens: string;

  @Column({ type: "bigint", default: 0 })
  outputTokens: string;

  @Column({ type: "bigint", default: 0 })
  cacheReadTokens: string;

  @Column({ type: "numeric", precision: 12, scale: 6, default: 0 })
  costUsd: string;
}
