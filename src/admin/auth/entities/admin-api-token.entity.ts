import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from "typeorm";
import type { AdminRole } from "./admin-user.entity";
import type { AdminApiTokenKind } from "../admin-api-token.constants";

/**
 * A personal access token or an OAuth grant: what the account page lists and
 * revokes, one row per credential a person handed out.
 *
 * The secrets themselves live in `admin_api_token_secrets`, as hashes: a PAT
 * has one, a grant has a rotating access and refresh pair. Revoking this row
 * cuts all of them off, because every bearer lookup joins it.
 */
@Entity("admin_api_tokens")
@Index("idx_admin_api_tokens_user", ["userId", "createdAt"])
export class AdminApiToken {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ name: "user_id", type: "uuid" })
  userId: string;

  /** `pat` or `oauth`; text, not an enum, so a third kind adds no table rewrite. */
  @Column({ type: "text" })
  kind: AdminApiTokenKind;

  /** The label a person gave a PAT; null for a grant. */
  @Column({ type: "text", nullable: true })
  name: string | null;

  @Column({ name: "client_id", type: "text", nullable: true })
  clientId: string | null;

  /** Copied from the client registration, so the list still reads after it is gone. */
  @Column({ name: "client_name", type: "text", nullable: true })
  clientName: string | null;

  /** The first characters of the current token, enough to recognise it. */
  @Column({ type: "text" })
  prefix: string;

  @Column({ type: "text", default: "admin:read" })
  scope: string;

  /** The most this token may do; the account's own role caps it further. */
  @Column({ name: "role_cap", type: "text", default: "viewer" })
  roleCap: AdminRole;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;

  @Column({ name: "last_used_at", type: "timestamptz", nullable: true })
  lastUsedAt: Date | null;

  /** A PAT's chosen expiry, or a grant's refresh expiry; null never expires. */
  @Column({ name: "expires_at", type: "timestamptz", nullable: true })
  expiresAt: Date | null;

  @Column({ name: "revoked_at", type: "timestamptz", nullable: true })
  revokedAt: Date | null;

  /** `user`, `owner`, `sessions-reset`, `refresh-reuse`, `code-replay`, `client`. */
  @Column({ name: "revoked_reason", type: "text", nullable: true })
  revokedReason: string | null;
}
