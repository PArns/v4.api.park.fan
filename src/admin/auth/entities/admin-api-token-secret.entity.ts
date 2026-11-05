import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryColumn,
} from "typeorm";
import type { AdminApiTokenSecretKind } from "../admin-api-token.constants";

/**
 * One secret of an `admin_api_tokens` row, stored as the SHA-256 of the token
 * and never as the token.
 *
 * A refresh token is kept after it has been used (`consumed_at`) until it
 * would have expired: presenting it again means two parties hold the grant,
 * and that is only detectable while the old value is still on record.
 */
@Entity("admin_api_token_secrets")
@Index("idx_admin_api_token_secrets_token", ["tokenId"])
@Index("idx_admin_api_token_secrets_expires", ["expiresAt"])
export class AdminApiTokenSecret {
  /** Hex SHA-256 of the full token, prefix included. */
  @PrimaryColumn({ name: "token_hash", type: "text" })
  tokenHash: string;

  @Column({ name: "token_id", type: "uuid" })
  tokenId: string;

  /** `pat`, `access` or `refresh`. */
  @Column({ type: "text" })
  kind: AdminApiTokenSecretKind;

  /** Null only for a PAT created without an expiry. */
  @Column({ name: "expires_at", type: "timestamptz", nullable: true })
  expiresAt: Date | null;

  /** When a refresh token was exchanged; set once, by a conditional update. */
  @Column({ name: "consumed_at", type: "timestamptz", nullable: true })
  consumedAt: Date | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
