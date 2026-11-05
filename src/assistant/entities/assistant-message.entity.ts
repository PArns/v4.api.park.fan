import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  ManyToOne,
  PrimaryGeneratedColumn,
} from "typeorm";
import { AssistantConversation } from "./assistant-conversation.entity";

/** A tool call of one answer, as the admin shows it. */
export interface AssistantToolCall {
  name: string;
  input: unknown;
  /** True when the tool failed or refused the input. */
  error?: boolean;
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

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
