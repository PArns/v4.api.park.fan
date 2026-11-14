import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * What the tool calls of the pruned chats of one UTC day added up to, per
 * tool, outcome, region and prompt version, so the tool report outlives the
 * chats' 7-day retention. Counts only: no input, no text, nothing that tells
 * one chat from another. `pruneConversations` adds each batch it deletes;
 * `toolStats()` sums these with the live rows. docs/admin/mcp.md#parkys-tools.
 */
@Entity("assistant_tool_day_stats")
export class AssistantToolDayStat {
  /** The UTC day the answer was written. */
  @PrimaryColumn({ type: "date" })
  day: string;

  /** The answer's `SYSTEM_PROMPT_VERSION`; 0 for an answer without one. */
  @PrimaryColumn({ name: "prompt_version", type: "smallint" })
  promptVersion: number;

  /** One of Parky's tools, or `(unknown)` for a name his list does not have. */
  @PrimaryColumn({ type: "varchar", length: 48 })
  tool: string;

  /** `ok`, an error kind (`ASSISTANT_TOOL_ERROR_KINDS`), or `error` for a failure stored before the kinds. */
  @PrimaryColumn({ type: "varchar", length: 16 })
  outcome: string;

  /** The trip region the input named, when `regions.ts` has it; `''` otherwise. */
  @PrimaryColumn({ type: "varchar", length: 32 })
  region: string;

  @Column({ type: "integer", default: 0 })
  calls: number;
}
