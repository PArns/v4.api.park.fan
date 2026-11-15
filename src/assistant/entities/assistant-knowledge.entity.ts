import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryColumn,
  UpdateDateColumn,
} from "typeorm";

/**
 * One topic of Parky's knowledge base, the wiki the admin and the admin MCP edit: background he
 * reads with read_knowledge only when a question needs it (docs/frontend/assistant.md#parkys-knowledge-base).
 */
@Entity("assistant_knowledge")
export class AssistantKnowledge {
  /** The topic's name as read_knowledge takes it: lowercase words joined by "_". */
  @PrimaryColumn({ type: "varchar", length: 40 })
  topic: string;

  /** What the topic holds and when to read it; the prompt lists these lines. */
  @Column({ type: "text" })
  readWhen: string;

  /** The background itself, in English, written for Parky and never shown to a visitor. */
  @Column({ type: "text" })
  text: string;

  /** Off: kept and editable, but neither listed in the prompt nor readable. */
  @Column({ type: "boolean", default: true })
  enabled: boolean;

  @Column({ type: "varchar", length: 200, nullable: true })
  updatedBy: string | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
