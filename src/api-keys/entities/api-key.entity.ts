import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from "typeorm";
import type { ApiKeyUsage } from "../api-key.constants";

/**
 * A key that unlocks the forecast routes for one data consumer, issued by
 * hand in the admin. The key itself is never stored: only its SHA-256, the way
 * admin tokens are, so a database backup holds nothing anyone can present.
 */
@Entity("api_keys")
@Index("idx_api_keys_hash", ["keyHash"], { unique: true })
export class ApiKey {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  /** Who or what the key is for, as the admin list shows it. */
  @Column({ type: "text" })
  name: string;

  /** An email or URL to reach the holder; null when the key was issued without one. */
  @Column({ type: "text", nullable: true })
  contact: string | null;

  @Column({ type: "text", default: "non-commercial" })
  usage: ApiKeyUsage;

  @Column({ type: "text", nullable: true })
  note: string | null;

  /**
   * How many requests the key may make in one clock hour; null means only the
   * API's general per-address limit applies.
   */
  @Column({ name: "requests_per_hour", type: "integer", nullable: true })
  requestsPerHour: number | null;

  /**
   * The parks the key may read forecasts for; null means every park. Ids, not
   * slugs, so a renamed park keeps its licence.
   */
  @Column({
    name: "allowed_park_ids",
    type: "uuid",
    array: true,
    nullable: true,
  })
  allowedParkIds: string[] | null;

  /**
   * When the key stops working; null means never. A key past it is refused
   * like a revoked one, but a new date brings it back.
   */
  @Column({ name: "expires_at", type: "timestamptz", nullable: true })
  expiresAt: Date | null;

  /** The first characters of the key, enough to recognise it. */
  @Column({ type: "text" })
  prefix: string;

  /** Hex SHA-256 of the full key, prefix included. */
  @Column({ name: "key_hash", type: "text" })
  keyHash: string;

  /** The admin account that issued it; null if that account is gone. */
  @Column({ name: "created_by", type: "uuid", nullable: true })
  createdBy: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;

  @Column({ name: "last_used_at", type: "timestamptz", nullable: true })
  lastUsedAt: Date | null;

  @Column({ name: "revoked_at", type: "timestamptz", nullable: true })
  revokedAt: Date | null;
}
