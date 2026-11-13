import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from "typeorm";
import type { TicketLink, TicketRules } from "../tickets";

/** The research behind a product, kept for the editor. */
export interface TicketSource {
  url: string;
  title?: string;
  accessed?: string;
  note?: string;
}

/**
 * One ticket product of a trip region's catalog, edited in the admin
 * (docs/frontend/trip-regions.md#editing-the-catalog).
 *
 * `slug` is the product's id in the contract: `tickets=` names it, shared
 * links carry it, so it is unique per region and never changes after create,
 * and a row is archived (`active = false`), never deleted. The optimizer's
 * rules sit in one `rules` document, checked as a whole by
 * `validateTicketProduct` before every write: a rule the optimizer learns
 * later is a field there, not a column `synchronize` would add at boot.
 *
 * The table starts from `tickets.ts`, seeded once per region when it has no
 * row (`TicketCatalogService`); from then on the admin is the source.
 */
@Entity("ticket_products")
@Index("idx_ticket_products_region_slug", ["region", "slug"], { unique: true })
export class TicketProductRecord {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  /** The trip region (`orlando`). */
  @Column({ type: "text" })
  region: string;

  @Column({ type: "text" })
  slug: string;

  /** The operator's product name, not translated. */
  @Column({ type: "text" })
  name: string;

  @Column({ type: "text", nullable: true })
  description: string | null;

  @Column({ type: "boolean", default: true })
  active: boolean;

  @Column({ type: "int", name: "sort_order", default: 0 })
  sortOrder: number;

  /** Plan first tries a resort's products by it, lower first; null: never suggested. */
  @Column({ type: "int", name: "suggest_rank", nullable: true })
  suggestRank: number | null;

  /** The region group whose operator sells it. */
  @Column({ type: "text" })
  resort: string;

  /** Everything the optimizer reads: parks, days, windows, hopping, caps, allowance, sale dates. */
  @Column({ type: "jsonb" })
  rules: TicketRules;

  /** Where to buy it, in order; exactly one official. */
  @Column({ type: "jsonb", default: () => "'[]'::jsonb" })
  links: TicketLink[];

  @Column({ type: "jsonb", default: () => "'[]'::jsonb" })
  sources: TicketSource[];

  @Column({ type: "text", name: "blockout_url", nullable: true })
  blockoutUrl: string | null;

  @Column({ type: "text", name: "reservation_url", nullable: true })
  reservationUrl: string | null;

  /** high | medium | low; text, not an enum (docs/rules/the-entities-are-the-schema.md). */
  @Column({ type: "text" })
  confidence: string;

  @Column({ type: "text", nullable: true })
  notes: string | null;

  @CreateDateColumn({ type: "timestamptz", name: "created_at" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz", name: "updated_at" })
  updatedAt: Date;

  /** The admin user who wrote it last; null for the seed. */
  @Column({ type: "text", name: "updated_by", nullable: true })
  updatedBy: string | null;
}
