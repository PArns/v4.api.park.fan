import { Column, CreateDateColumn, Entity, PrimaryColumn } from "typeorm";

/**
 * An OAuth client that registered itself (RFC 7591). Public clients only: no
 * secret exists, so a registration is just a name and the redirect URIs a
 * code may be sent to.
 *
 * Registration is anonymous by design, so a row proves nothing about who made
 * it; the consent page is where a person decides. Rows never used for a grant
 * are deleted after a week; beyond 1000 of them a registrant's own surplus and
 * then the oldest past their first hour give way.
 */
@Entity("admin_oauth_clients")
export class AdminOAuthClient {
  @PrimaryColumn({ name: "client_id", type: "text" })
  clientId: string;

  @Column({ name: "client_name", type: "text" })
  clientName: string;

  @Column({ name: "redirect_uris", type: "jsonb" })
  redirectUris: string[];

  /**
   * A hash of the registering address bucket (IPv4, IPv6 /64), so a flood of
   * registrations evicts its own before anybody else's. Null without one.
   */
  @Column({ name: "registrant", type: "text", nullable: true })
  registrant: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;

  /** Set when a grant is approved for it; null marks a registration nobody used. */
  @Column({ name: "last_used_at", type: "timestamptz", nullable: true })
  lastUsedAt: Date | null;
}
