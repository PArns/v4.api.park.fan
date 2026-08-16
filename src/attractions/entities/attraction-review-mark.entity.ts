import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
  Check,
  CreateDateColumn,
} from "typeorm";
import { Attraction } from "./attraction.entity";

/**
 * "A human already looked at this, and it is not a case."
 *
 * Both duplicate detection and retirement detection are behavioural: they ask
 * what the data looks like, not what is true. So every candidate they surface
 * comes back tomorrow, however carefully it was investigated today.
 *
 * That is not a small annoyance. On 2026-08-16 a research round established
 * that 57 of 73 silenced attractions are alive — Jurassic Park - The Ride runs
 * under an anniversary overlay name, Marvel Cave is central to a 2027 project,
 * Shock Wave is stored not scrapped — and that 5 of the remaining duplicate
 * pairs are genuinely different rides: Cedar Creek is a lazy river beside
 * Cedar Creek Mine Ride, KONDAALA is a kids' ride beside the KONDAA coaster.
 * Without somewhere to put that, the same 64 questions get re-asked until
 * somebody answers one carelessly and merges two real attractions.
 *
 * This table answers only that one question. It is deliberately NOT consulted
 * by detect-seasonal or reverse-reconciliation — those describe what the feed
 * is doing, and a human's verdict does not change that.
 */
@Entity("attraction_review_marks")
@Index(["kind", "attractionId", "otherAttractionId"], { unique: true })
// A pair fact has no direction: "A is not a duplicate of B" is the same
// statement as its mirror. Storing both would let a UNION-style read surface
// the pair twice — the bug that made the duplicate detector count 63 rows for
// 53 real pairs. The canonical order is enforced here rather than remembered.
@Check(
  `"other_attraction_id" IS NULL OR "attraction_id" < "other_attraction_id"`,
)
export class AttractionReviewMark {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  /**
   * - `not_a_duplicate`: these two rows are different attractions. Permanent —
   *   two rides do not become one later.
   * - `not_retired`: this attraction still exists despite its feed going quiet.
   *   Often temporary; see `recheckAfter`.
   */
  @Column({ name: "kind", type: "text" })
  kind: "not_a_duplicate" | "not_retired";

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attraction_id" })
  attraction: Attraction;

  @Column({ name: "attraction_id", type: "uuid" })
  attractionId: string;

  /** The other half of a pair mark; null for single-attraction marks. */
  @ManyToOne(() => Attraction, { onDelete: "CASCADE", nullable: true })
  @JoinColumn({ name: "other_attraction_id" })
  otherAttraction: Attraction | null;

  @Column({ name: "other_attraction_id", type: "uuid", nullable: true })
  otherAttractionId: string | null;

  /** What was established, and the URL it was established from. */
  @Column({ name: "reason", type: "text" })
  reason: string;

  /**
   * When to ask again. Null means never — right for a landmark that plainly
   * still operates, wrong for anything whose status is genuinely unsettled.
   *
   * Six Flags Over Texas' Shock Wave is the case that argues for this column:
   * standing but not running since March 2026, with no announcement either
   * way. A permanent mark would hide its eventual retirement forever.
   */
  @Column({ name: "recheck_after", type: "date", nullable: true })
  recheckAfter: Date | null;

  @CreateDateColumn({ name: "reviewed_at", type: "timestamptz" })
  reviewedAt: Date;
}
