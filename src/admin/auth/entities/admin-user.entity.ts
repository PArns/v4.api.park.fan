import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";

/**
 * The four things an admin account can be allowed to do.
 *
 * Ordered from most to least privileged, and read that way by
 * `roleAtLeast` — a check written as "editor or above" must not have to
 * enumerate the roles above editor, or adding a role later silently locks
 * somebody out of an endpoint they should reach.
 *
 * - `owner`   manages accounts and can run destructive maintenance (merges,
 *             retirements, cache resets). There is always at least one.
 * - `editor`  edits curated content: names, seasons, heights, ride profiles.
 *             The role a normal curation session runs as.
 * - `author`  edits blog posts and media metadata, but not park/ride data.
 * - `viewer`  reads dashboards. Cannot write anything.
 */
export const ADMIN_ROLES = ["owner", "editor", "author", "viewer"] as const;
export type AdminRole = (typeof ADMIN_ROLES)[number];

/** Rank of a role, higher = more privileged. */
export const ADMIN_ROLE_RANK: Record<AdminRole, number> = {
  owner: 30,
  editor: 20,
  author: 10,
  viewer: 0,
};

/** Whether `role` is at least as privileged as `minimum`. */
export function roleAtLeast(role: AdminRole, minimum: AdminRole): boolean {
  return ADMIN_ROLE_RANK[role] >= ADMIN_ROLE_RANK[minimum];
}

/**
 * Admin User Entity
 *
 * An actual person with an actual account, which the admin surface did not
 * have before: every administrative endpoint was gated on ONE shared `pass=`
 * query secret, checked by a Cloudflare rule rather than by this application —
 * so anything that reached the origin directly could merge parks, retire
 * attractions or flush every cache with no credential at all, and nothing that
 * happened could be attributed to anybody.
 *
 * Named accounts are the smaller half of the fix. The larger half is that a
 * curation record needs an author: `curated_*` columns state "a human decided
 * this", and until now there was no way to say which human, on what evidence,
 * or to ask them about it a year later. `admin_audit_log` rows point here.
 *
 * The password hash is never selected by default (`select: false`) so it
 * cannot leak through a `find()` that forgot to project — the login path asks
 * for it explicitly.
 */
@Entity("admin_users")
export class AdminUser {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  /**
   * Login identity, stored lower-cased and trimmed.
   *
   * Normalisation happens on write in the service rather than with a citext
   * column, because citext is another extension to require of the database and
   * this is the only place in the schema that would need it.
   */
  @Column({ type: "text", unique: true })
  email: string;

  /** Shown in the UI and written into audit rows. */
  @Column({ name: "display_name", type: "text" })
  displayName: string;

  @Column({ name: "password_hash", type: "text", select: false })
  passwordHash: string;

  @Column({ type: "text", default: "viewer" })
  @Index("idx_admin_user_role")
  role: AdminRole;

  /**
   * A deactivated account keeps its audit trail and its curation authorship
   * but can no longer log in, and its live sessions are dropped on the next
   * request. Deleting the row instead would orphan every edit it ever made.
   */
  @Column({ name: "is_active", type: "boolean", default: true })
  isActive: boolean;

  /**
   * Set when an owner creates or resets an account: the temporary password
   * works exactly once, and the next thing that account may do is choose its
   * own. Every other endpoint refuses the session until it is cleared.
   */
  @Column({ name: "must_change_password", type: "boolean", default: false })
  mustChangePassword: boolean;

  /**
   * Failed logins since the last success, and the lockout they earned.
   *
   * Rate limiting by IP is not enough on its own: the frontend proxies every
   * admin call server-side, so from the API's point of view a password-spraying
   * browser and a legitimate one share an address. The counter is per account
   * for that reason.
   */
  @Column({ name: "failed_login_count", type: "int", default: 0 })
  failedLoginCount: number;

  @Column({ name: "locked_until", type: "timestamptz", nullable: true })
  lockedUntil: Date | null;

  @Column({ name: "last_login_at", type: "timestamptz", nullable: true })
  lastLoginAt: Date | null;

  /** Truncated to /24 (IPv4) or /48 (IPv6) — enough to notice a new location,
   *  not a movement log of the person. */
  @Column({ name: "last_login_ip", type: "text", nullable: true })
  lastLoginIp: string | null;

  /**
   * Base32 TOTP secret (RFC 6238), null until the account enrols. Kept out of
   * default selects for the same reason as the password hash.
   */
  @Column({ name: "totp_secret", type: "text", nullable: true, select: false })
  totpSecret: string | null;

  @Column({ name: "totp_enabled", type: "boolean", default: false })
  totpEnabled: boolean;

  /**
   * The last TOTP step this account consumed.
   *
   * Without it a six-digit code stays valid for its whole window, so anyone who
   * reads it over a shoulder — or off a proxy log — can replay it within the
   * next 30 seconds. Storing the step number makes each code single-use.
   */
  @Column({ name: "totp_last_step", type: "bigint", nullable: true })
  totpLastStep: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ name: "updated_at", type: "timestamptz" })
  updatedAt: Date;
}
