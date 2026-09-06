import { Column, Entity, Index, PrimaryColumn } from "typeorm";

/**
 * The denominator: one ride's minutes on one park operating day.
 *
 * **Minutes only, never an event.** That is a rule rather than an omission: a
 * table a reader could sum outages out of is a table that turns a three-day
 * outage back into three, which is the exact failure this whole model exists to
 * avoid. Events live in `attraction_outages` and nowhere else.
 *
 * ## What exposure is
 *
 * The ride's own OPERATING minutes inside a flattened park OPERATING window. Not
 * the park's opening hours — a risk set, not a schedule: a ride cannot break
 * while it is already broken, and a ride that is shut is not at risk.
 *
 * Every awkward case in the catalogue falls out of that one choice rather than
 * needing a rule:
 *
 * - A ride closed every Sunday (Wakobato at Phantasialand) reports CLOSED, never
 *   DOWN, so its Sunday contributes zero to both sides. Had the denominator been
 *   park-open hours, that Sunday would have added twelve hours of fictitious risk
 *   time and suppressed every rate in the park.
 * - A ride that opens at eleven contributes nothing before eleven.
 * - A ride out of season contributes nothing, with no seasonality predicate
 *   anywhere near the reconstruction.
 *
 * ## The invariant
 *
 * `operating + down + closed + refurbishment + absent + unobserved` equals
 * `parkOpenMinutes` to within a minute. A violation is a bug in the segment
 * arithmetic and raises a monitored alert; the row is marked `suspect` and kept,
 * because silently NULLing a day would hide the bug and shrink the denominator
 * at the same time.
 */
@Entity("attraction_exposure_days")
@Index(["parkId", "opDay"])
export class AttractionExposureDay {
  @PrimaryColumn("uuid")
  attractionId: string;

  /**
   * The park-local date the operating WINDOW opened on.
   *
   * Not the calendar date of the minutes themselves. A park closing at 02:00
   * would otherwise split its evening across two rows and lose it from both.
   */
  @PrimaryColumn({ type: "date", name: "op_day" })
  opDay: string;

  @Column("uuid")
  parkId: string;

  /** Length of the day's flattened OPERATING windows. The ceiling for the rest. */
  @Column({ type: "int", name: "park_open_minutes", default: 0 })
  parkOpenMinutes: number;

  /** The risk set: minutes the ride was reported OPERATING inside those windows. */
  @Column({ type: "int", name: "operating_minutes", default: 0 })
  operatingMinutes: number;

  @Column({ type: "int", name: "down_minutes", default: 0 })
  downMinutes: number;

  /** The part of `downMinutes` not backed by carried heartbeat rows. */
  @Column({ type: "int", name: "down_minutes_observed", default: 0 })
  downMinutesObserved: number;

  @Column({ type: "int", name: "closed_minutes", default: 0 })
  closedMinutes: number;

  @Column({ type: "int", name: "refurbishment_minutes", default: 0 })
  refurbishmentMinutes: number;

  /** Minutes covered by reverse-reconciliation rows: no source mentioned the ride. */
  @Column({ type: "int", name: "absent_minutes", default: 0 })
  absentMinutes: number;

  /**
   * Minutes inside the park's hours that no reading covers.
   *
   * A gap has four indistinguishable causes — nothing changed, the poll failed,
   * the ride left the feed, the park is shut and the safety-net heartbeat is off
   * by design — and none of them is evidence about the ride. The time leaves the
   * denominator, so the error is always LESS exposure and never invented
   * exposure.
   */
  @Column({ type: "int", name: "unobserved_minutes", default: 0 })
  unobservedMinutes: number;

  /**
   * Outages that STARTED on this day.
   *
   * Written from the interval table in the same transaction, so a multi-day
   * outage increments exactly one day. It is a convenience for per-day charts and
   * is never the source of an event count — that is `attraction_outages`.
   */
  @Column({ type: "smallint", name: "outage_starts", default: 0 })
  outageStarts: number;

  /** The invariant did not hold. Kept and flagged, never silently dropped. */
  @Column({ type: "boolean", default: false })
  suspect: boolean;

  @Column({ type: "timestamptz", name: "computed_at" })
  computedAt: Date;
}
