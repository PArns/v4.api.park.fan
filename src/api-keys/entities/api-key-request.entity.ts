import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from "typeorm";

/** Where a request stands. */
export const API_KEY_REQUEST_STATUSES = [
  "pending",
  "approved",
  "rejected",
  "withdrawn",
] as const;
export type ApiKeyRequestStatus = (typeof API_KEY_REQUEST_STATUSES)[number];

/**
 * Which key the terms allow: `free` for an open-source project that earns
 * nothing (donations aside), `paid` for everything else, which needs a
 * written agreement. Derived from the answers when the request is made.
 */
export type ApiKeyRequestTier = "free" | "paid";

/**
 * A developer's request for a data key, from the developer dashboard. The
 * owner approves it (which issues a key bound to the developer's account) or
 * turns it down with a reason the developer reads on the dashboard. Every
 * request is decided by hand; none issues a key by itself.
 */
@Entity("api_key_requests")
@Index("idx_api_key_requests_user", ["userId"])
@Index("idx_api_key_requests_status", ["status"])
export class ApiKeyRequest {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  /** The developer account that asked. */
  @Column({ name: "user_id", type: "uuid" })
  userId: string;

  @Column({ type: "text", default: "pending" })
  status: ApiKeyRequestStatus;

  @Column({ type: "text", default: "paid" })
  tier: ApiKeyRequestTier;

  @Column({ name: "project_name", type: "text" })
  projectName: string;

  /** The project's website or store page; where the backlink goes on a website. */
  @Column({ type: "text", nullable: true })
  website: string | null;

  /** `mobile-app`, `website`, `smart-home`, `chat-bot`, `research`, `internal-tool`, `other`. */
  @Column({ type: "text" })
  kind: string;

  /** `ios`, `android`, `web`, `desktop`, `server`, `other`. */
  @Column({ type: "text", array: true, default: () => "'{}'" })
  platforms: string[];

  /** The languages the project is in, as the applicant wrote them. */
  @Column({ type: "text", nullable: true })
  languages: string | null;

  @Column({ type: "text" })
  description: string;

  /** `open-source` or `closed-source`. */
  @Column({ type: "text" })
  source: string;

  @Column({ type: "text", nullable: true })
  repository: string | null;

  /** The licence the repository names (`MIT`, `GPL-3.0`, …), as written. */
  @Column({ type: "text", nullable: true })
  license: string | null;

  /** How the project earns money: `none`, `donations`, `ads`, `affiliate`, `paid`, `client-work`, `company`. */
  @Column({ type: "text", array: true, default: () => "'{}'" })
  revenue: string[];

  /** The permissions asked for (`permissions.ts`). */
  @Column({ type: "text", array: true, default: () => "'{}'" })
  permissions: string[];

  /** The routes the project will call, as the applicant listed them. */
  @Column({ type: "text", nullable: true })
  endpoints: string | null;

  /** Requests per day: `under-1k`, `under-10k`, `under-100k`, `more`. */
  @Column({ type: "text" })
  volume: string;

  /** The parks it needs, in the applicant's words; empty for all. */
  @Column({ type: "text", nullable: true })
  parks: string | null;

  /** The key stays on the applicant's server and never reaches a browser or an app. */
  @Column({ name: "server_side", type: "boolean", default: false })
  serverSide: boolean;

  /** How answers are cached, as the applicant described it. */
  @Column({ type: "text", nullable: true })
  caching: string | null;

  /** The page that carries the backlink to park.fan. */
  @Column({ name: "attribution_url", type: "text", nullable: true })
  attributionUrl: string | null;

  @Column({ type: "text", nullable: true })
  message: string | null;

  /** The portal language the request came from; the reply is written in it. */
  @Column({ type: "text", default: "en" })
  locale: string;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;

  @Column({ name: "decided_at", type: "timestamptz", nullable: true })
  decidedAt: Date | null;

  /** The admin who decided, by address; null while undecided. */
  @Column({ name: "decided_by", type: "text", nullable: true })
  decidedBy: string | null;

  /** Why it was turned down, or what the approval changed; the developer reads it. */
  @Column({ name: "decision_note", type: "text", nullable: true })
  decisionNote: string | null;

  /** The key issued for it. */
  @Column({ name: "key_id", type: "uuid", nullable: true })
  keyId: string | null;
}
