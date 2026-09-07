import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryColumn,
  UpdateDateColumn,
} from "typeorm";

/**
 * A visitor's plan, stored so it can outlive one browser.
 *
 * The planner has always lived in `localStorage`, which is the right default —
 * it needs no account, works offline and belongs to nobody but the visitor. It
 * is also invisible to this API, and two things a plan is asked for need the
 * server to have seen it: sharing a link, and a push notification that knows
 * what is next.
 *
 * **The id IS the credential.** There is no account system for visitors and none
 * is being built, so knowing the id is the whole of the authorisation — which is
 * why it is 96 bits of `randomBytes`, why it is never derived from anything
 * (a park slug and a date would be guessable in an afternoon), and why the UI
 * has to say so plainly rather than presenting the link as private.
 *
 * `payload` is the plan verbatim, as the browser holds it. Deliberately opaque
 * to this table: the planner's own shape is a frontend concern and versioning it
 * in two places would guarantee the two disagree. What this API does insist on
 * is that the payload IS a plan — see `isTripPayload` — because an
 * unauthenticated write endpoint that accepts any JSON is a free key-value
 * store, and it would be found.
 */
@Entity("trips")
@Index("idx_trips_expires_at", ["expiresAt"])
export class Trip {
  /**
   * URL-safe, unguessable, and short enough to be pasted into a message.
   * 16 base64url characters — see `TripsService.newId`.
   */
  @PrimaryColumn({ type: "varchar", length: 32 })
  id: string;

  /** The plan as the browser holds it. */
  @Column({ type: "jsonb" })
  payload: Record<string, unknown>;

  /**
   * When this row may be swept.
   *
   * Pushed forward on every write, so a trip somebody keeps editing never
   * expires and one abandoned after a weekend eventually goes. A plan is a
   * record of days already walked as well as days ahead, so the window is long.
   */
  @Column({ type: "timestamptz" })
  expiresAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
