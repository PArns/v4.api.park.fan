import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from "typeorm";

/**
 * Admin Audit Log Entity
 *
 * Every write an administrator makes, with what it was before.
 *
 * This exists because the curated columns are claims about the world and a
 * claim needs an author. `curated_minimum_height = 0` on Winni Splash means
 * "somebody read Phantasialand's Nutzungsbedingungen and found no minimum" —
 * that reasoning used to live in a commit message at best and in nobody's head
 * at worst. A row here carries the actor, the moment, the reason they typed,
 * and the previous value, which together are what makes a curation reviewable
 * and, when it turns out to be wrong, reversible.
 *
 * `before`/`after` hold only the fields that actually changed, not whole
 * entities: an audit row is read by a human comparing two values, and burying
 * those two in a 40-key dump of an attraction is how audit trails stop being
 * read. The undo path in AdminAuditService relies on the same shape.
 */
@Entity("admin_audit_log")
@Index("idx_admin_audit_created", ["createdAt"])
@Index("idx_admin_audit_entity", ["entityType", "entityId", "createdAt"])
@Index("idx_admin_audit_actor", ["actorId", "createdAt"])
export class AdminAuditLog {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  /**
   * The account that made the change. Nullable because the legacy shared-pass
   * path can still authenticate during the migration window and has no account
   * behind it — those rows say `actorEmail = 'legacy-pass'` so they are
   * visibly not attributable to a person.
   */
  @Column({ name: "actor_id", type: "uuid", nullable: true })
  actorId: string | null;

  /** Denormalised on purpose: an audit row must stay readable after the
   *  account it names is renamed or deactivated. */
  @Column({ name: "actor_email", type: "text" })
  actorEmail: string;

  /** Dotted verb, e.g. `attraction.curate`, `park.season.create`,
   *  `auth.login`, `user.deactivate`. */
  @Column({ type: "text" })
  @Index("idx_admin_audit_action")
  action: string;

  /** `park` | `attraction` | `park_season` | `ride_profile` | `admin_user` |
   *  `system` — the kind of thing `entityId` points at. */
  @Column({ name: "entity_type", type: "text" })
  entityType: string;

  @Column({ name: "entity_id", type: "text", nullable: true })
  entityId: string | null;

  /** Human-readable name of the target at the time of the change, so the log
   *  reads as "Taron" rather than as a UUID. */
  @Column({ name: "entity_label", type: "text", nullable: true })
  entityLabel: string | null;

  /** Changed fields only, as they were. Null for a create. */
  @Column({ type: "jsonb", nullable: true })
  before: Record<string, unknown> | null;

  /** Changed fields only, as they now are. Null for a delete. */
  @Column({ type: "jsonb", nullable: true })
  after: Record<string, unknown> | null;

  /**
   * Why, in the actor's own words, and the URL they established it from.
   *
   * Optional at the schema level and required by the curation endpoints: a
   * height correction without a source is a rumour, but an audit row for
   * `auth.login` has nothing to explain.
   */
  @Column({ type: "text", nullable: true })
  reason: string | null;

  @Column({ name: "source_url", type: "text", nullable: true })
  sourceUrl: string | null;

  @Column({ name: "actor_ip", type: "text", nullable: true })
  actorIp: string | null;

  /**
   * Set when this row has been undone, pointing at the audit row that undid
   * it. An undo is itself an edit and gets its own row; this back-pointer is
   * what stops the UI offering to undo the same change twice.
   */
  @Column({ name: "reverted_by", type: "uuid", nullable: true })
  revertedBy: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
