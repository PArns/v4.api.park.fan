import { Column, CreateDateColumn, Entity, PrimaryColumn } from "typeorm";
import type { PlanDayUnavailableReason } from "../utils/plan-day-availability.util";

/**
 * One row per park per sweep: did `/plan/day` have anything to say about a day
 * a month out, and if not, why.
 *
 * **Why this is written down at all.** A park whose feed died still answers
 * 200, with an opening window, a crowd level and a drawn axis — only the ride
 * list is empty, and nothing anywhere counted those. The 26 % found on
 * 2026-09-14 was found by a verification run that was looking for something
 * else entirely (PAR-42), which is the definition of a number nobody watches.
 *
 * The row keeps the **reason**, not just the count, because the count alone
 * cannot go down for the right reason: a park that publishes no wait times and
 * a park whose feed broke last night both add one, and only the second is
 * worth waking up for. `isStructuralPlanDayReason` draws that line.
 *
 * Kept per park rather than pre-aggregated so a rise can be attributed the next
 * morning without a second sweep. One row is ~60 bytes and the sweep runs once
 * a day over the parks that are open, so the table gains on the order of 200
 * rows a day.
 */
@Entity("plan_day_coverage")
export class PlanDayCoverage {
  /**
   * The date the sweep ran, in UTC — the "with a date" half of the counter.
   *
   * Leading column of the primary key, which is why no separate index sits on
   * it: both reads this table has (`WHERE measured_on = …` and
   * `ORDER BY measured_on DESC`) use the PK, and `synchronize` does not drop an
   * index once it has made one.
   */
  @PrimaryColumn({ type: "date", name: "measured_on" })
  measuredOn: string;

  @PrimaryColumn({ type: "uuid", name: "park_id" })
  parkId: string;

  /**
   * The date that was planned. Stored rather than derived, because the lead
   * distance is a parameter of the sweep and a later change to it must not
   * silently reinterpret old rows.
   */
  @Column({ type: "date", name: "planned_date" })
  plannedDate: string;

  @Column({ type: "int", name: "lead_days" })
  leadDays: number;

  /** Rides the plan carried. Zero exactly when `reason` is set. */
  @Column({ type: "int", name: "ride_count" })
  rideCount: number;

  /**
   * Why the plan was empty, or `null` when it was not. Nullable because "this
   * park answered" is the normal case and has no reason to give.
   */
  @Column({ type: "varchar", length: 32, nullable: true })
  reason: PlanDayUnavailableReason | null;

  /**
   * Whether that reason is a property of the park rather than a gap in our
   * data. Denormalised on purpose: the alert reads one predicate, and the
   * classification of a reason can change without rewriting history.
   */
  @Column({ type: "boolean", name: "is_structural", default: false })
  isStructural: boolean;

  @CreateDateColumn({ type: "timestamptz", name: "created_at" })
  createdAt: Date;
}
