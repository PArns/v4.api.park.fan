import { Column, Entity, PrimaryColumn, UpdateDateColumn } from "typeorm";

/** The one row of Parky's settings; `id` is always `1`. */
@Entity("assistant_settings")
export class AssistantSettings {
  @PrimaryColumn({ type: "smallint", default: 1 })
  id: number;

  @Column({ type: "boolean", default: false })
  enabled: boolean;

  /** Share of browsers that see the widget, 0–100, by a stable bucket per browser. */
  @Column({ type: "smallint", default: 0 })
  rolloutPercent: number;

  /** `sealSecret()` output; null when no key was entered. */
  @Column({ type: "text", nullable: true })
  apiKeySealed: string | null;

  /** What the admin shows instead of the key, e.g. `sk-ant-…abcd`. */
  @Column({ type: "varchar", length: 32, nullable: true })
  apiKeyMasked: string | null;

  /** Tokens all chats may spend per UTC day (about 12,000 a chat, 0.1 cent); 0 means no cap. */
  @Column({ type: "integer", default: 50_000_000 })
  dailyTokenBudget: number;

  @Column({ type: "varchar", length: 200, nullable: true })
  updatedBy: string | null;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
