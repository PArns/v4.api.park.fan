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

/** Feedback a visitor sent from the widget's feedback tab. */
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
