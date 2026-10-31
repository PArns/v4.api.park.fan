import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  OneToMany,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from "typeorm";
import { AssistantMessage } from "./assistant-message.entity";

/**
 * One chat with Parky. No address is stored: `visitorKey` is a hash of the
 * address with a salt that changes every day, enough to tell two chats of one
 * visitor apart on the same day and nothing after (docs/rules/no-address-in-logs.md).
 */
@Entity("assistant_conversations")
@Index("idx_assistant_conversations_created", ["createdAt"])
export class AssistantConversation {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ type: "varchar", length: 5 })
  locale: string;

  /** The page the chat was opened on. */
  @Column({ type: "varchar", length: 300 })
  startPath: string;

  @Column({ type: "varchar", length: 32, nullable: true })
  visitorKey: string | null;

  /** Hash of the session token that started the chat; only that session may continue it. */
  @Column({ type: "varchar", length: 32, nullable: true })
  sessionHash: string | null;

  @Column({ type: "integer", default: 0 })
  messageCount: number;

  @Column({ type: "integer", default: 0 })
  totalTokens: number;

  @Column({ type: "numeric", precision: 10, scale: 6, default: 0 })
  costUsd: string;

  /** −1 when any answer got a thumbs down, 1 when only thumbs up, 0 otherwise. */
  @Column({ type: "smallint", default: 0 })
  rating: number;

  @OneToMany(() => AssistantMessage, (message) => message.conversation)
  messages: AssistantMessage[];

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
