import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  ManyToOne,
  PrimaryGeneratedColumn,
} from "typeorm";
import { AssistantConversation } from "./assistant-conversation.entity";

/**
 * Why a tool call failed, as a closed code: a tool the list does not have, an
 * input its schema refused, a refusal of the tool itself (no such park, the
 * plan route's 400), or a failure of ours. docs/admin/mcp.md#parkys-tools.
 */
export const ASSISTANT_TOOL_ERROR_KINDS = [
  "unknown_tool",
  "invalid_input",
  "refused",
  "failed",
] as const;
export type AssistantToolErrorKind =
  (typeof ASSISTANT_TOOL_ERROR_KINDS)[number];

/** A tool call of one answer, as the admin shows it. */
export interface AssistantToolCall {
  name: string;
  input: unknown;
  /** True when the tool failed or refused the input. */
  error?: boolean;
  /** Why, with `error`; absent on the calls stored before the code. */
  errorKind?: AssistantToolErrorKind;
  /** `invalid_input`: the paths the schema refused (`dates.from`), never their values. */
  issues?: string[];
  /** Keys of the input the tool does not take, which its schema dropped without a word. */
  ignoredKeys?: string[];
}

/** One question or one answer. */
@Entity("assistant_messages")
@Index("idx_assistant_messages_conversation", ["conversationId", "createdAt"])
export class AssistantMessage {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ type: "uuid" })
  conversationId: string;

  @ManyToOne(() => AssistantConversation, (c) => c.messages, {
    onDelete: "CASCADE",
  })
  conversation: AssistantConversation;

  @Column({ type: "varchar", length: 9 })
  role: "user" | "assistant";

  @Column({ type: "text" })
  text: string;

  @Column({ type: "jsonb", nullable: true })
  toolCalls: AssistantToolCall[] | null;

  @Column({ type: "integer", default: 0 })
  inputTokens: number;

  @Column({ type: "integer", default: 0 })
  outputTokens: number;

  @Column({ type: "integer", default: 0 })
  cacheReadTokens: number;

  @Column({ type: "numeric", precision: 10, scale: 6, default: 0 })
  costUsd: string;

  /** Thumbs up (1) or down (−1) from the visitor; null when not rated. */
  @Column({ type: "smallint", nullable: true })
  rating: number | null;

  /** The `SYSTEM_PROMPT_VERSION` an answer was written with; null on questions and older answers. */
  @Column({ type: "smallint", nullable: true })
  promptVersion: number | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
