import { Column, CreateDateColumn, Entity, PrimaryColumn } from "typeorm";

/**
 * An OAuth client that registered itself (RFC 7591). Public clients only: no
 * secret exists, so a registration is just a name and the redirect URIs a
 * code may be sent to.
 *
 * Registration is anonymous by design, so a row proves nothing about who made
 * it; the consent page is where a person decides. Rows never used for a grant
 * are deleted after a day.
 */
@Entity("admin_oauth_clients")
export class AdminOAuthClient {
  @PrimaryColumn({ name: "client_id", type: "text" })
  clientId: string;

  @Column({ name: "client_name", type: "text" })
  clientName: string;

  @Column({ name: "redirect_uris", type: "jsonb" })
  redirectUris: string[];

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;

  /** Set when a grant is approved for it; null marks a registration nobody used. */
  @Column({ name: "last_used_at", type: "timestamptz", nullable: true })
  lastUsedAt: Date | null;
}
