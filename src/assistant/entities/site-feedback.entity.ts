import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from "typeorm";

export const FEEDBACK_STATUSES = ["new", "done", "archived"] as const;
export type FeedbackStatus = (typeof FEEDBACK_STATUSES)[number];

/** Where feedback came from: the widget's feedback tab, or Parky passing on what a visitor wrote. */
export const FEEDBACK_SOURCES = ["form", "chat"] as const;
export type FeedbackSource = (typeof FEEDBACK_SOURCES)[number];

/** What Parky took feedback from the chat for. */
export const FEEDBACK_KINDS = [
  "bug",
  "wrong_data",
  "missing_park_or_ride",
  "feature_wish",
  "praise",
  "criticism",
] as const;
export type FeedbackKind = (typeof FEEDBACK_KINDS)[number];

/** Feedback a visitor sent from the widget's feedback tab, or wrote to Parky in the chat. */
@Entity("site_feedback")
@Index("idx_site_feedback_status_created", ["status", "createdAt"])
export class SiteFeedback {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ type: "text" })
  message: string;

  /** Only when the visitor wants an answer. */
  @Column({ type: "varchar", length: 254, nullable: true })
  email: string | null;

  @Column({ type: "varchar", length: 5 })
  locale: string;

  @Column({ type: "varchar", length: 300 })
  pagePath: string;

  /** Text with a default, not a Postgres enum: the rows already there read as the form's. */
  @Column({ type: "varchar", length: 8, default: "form" })
  source: FeedbackSource;

  /** Only for feedback from the chat, whose `message` is then the visitor's words as typed. */
  @Column({ type: "varchar", length: 24, nullable: true })
  kind: FeedbackKind | null;

  /** The chat open in the widget when the feedback was sent. */
  @Column({ type: "uuid", nullable: true })
  conversationId: string | null;

  @Column({ type: "varchar", length: 8, default: "new" })
  status: FeedbackStatus;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
