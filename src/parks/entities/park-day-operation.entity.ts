import { Column, Entity, Index, PrimaryColumn } from "typeorm";

/**
 * One row per park per park-local day: did measured ride activity say the park
 * operated, and what hours did it measure.
 *
 * **Why the verdict is stored rather than computed.** The rule itself
 * (`measured-operation.gate.ts`) has to look at raw `queue_data` for the day it
 * judges, because the three conditions that catch a feed artefact — variety,
 * block length, feed age — are not in any rollup. Evaluated in the read path,
 * that cost the ML-accuracy query a factor of 5.2 (1.4 s → 7.4 s) and a
 * set-based CTE timed out, so Patrick decided on 2026-10-07 (PAR-697) to
 * materialise it: the nightly job judges each finished day once, and the five
 * readers of the shut-day rule do a primary-key lookup.
 *
 * **A missing row is not a `false`.** `measuredOperation = false` is a verdict:
 * the day was looked at and the activity did not clear the gate. No row means
 * nobody has judged that day yet — the days before the fill job ran, and every
 * day for a park added since. Both end up leaving the operator's entry
 * standing, which is why `measuredOperationDayExists` asks for a row with
 * `measured_operation` rather than for the absence of a `false` one.
 *
 * Written by the `park-day-operation` queue: `calculate-yesterday-park-day-operation`
 * nightly at 4:45 (after the 4:30 hourly-history rollup the derived hours are
 * read from), and `backfill-park-day-operation` for an explicit range — the
 * one-time fill of the past, run in portions after the deploy (G-134).
 */
@Entity("park_day_operations")
@Index("idx_park_day_operations_day", ["day"])
export class ParkDayOperation {
  @PrimaryColumn({ type: "uuid", name: "park_id" })
  parkId: string;

  /**
   * The park-local calendar day, as `YYYY-MM-DD`.
   *
   * Park-local rather than UTC because that is the day the calendar and the
   * statistics ask about, and the conversion needs the park's timezone — a
   * reader that has to convert is a reader that can convert differently
   * (PAR-692 closed exactly that split).
   */
  @PrimaryColumn({ type: "date" })
  day: string;

  /**
   * Whether measured ride activity says the park operated on this day.
   *
   * The only column the readers look at. `true` lets the calendar publish an
   * operating day against a park-level `CLOSED` entry and the statistics count
   * it as a measuring day.
   */
  @Column({ type: "boolean", name: "measured_operation" })
  measuredOperation: boolean;

  /**
   * The reconstructed opening and closing time, park-local wall clock, as
   * `getDerivedHistoricalHours` produced them for this day — or `null` when the
   * day has none, which is the normal case for a day the gate refused.
   *
   * `timestamp`, not `timestamptz`, on purpose: these are wall-clock readings,
   * never instants. The slots are
   * built `AT TIME ZONE` the park's zone, so `derived_open` leaves the query as
   * a naive park-local time; it travels through a `Date` whose UTC fields hold
   * that wall clock, and a `timestamp` column stores those digits unchanged. A
   * `timestamptz` would turn them into an instant that is off by the park's
   * offset.
   *
   * Stored even though the calendar computes the same hours for its own window:
   * a verdict without the hours it was taken from cannot be checked after the
   * fact, and the statistics callers have no other way to see them.
   */
  @Column({ type: "timestamp", name: "derived_open", nullable: true })
  derivedOpen: Date | null;

  @Column({ type: "timestamp", name: "derived_close", nullable: true })
  derivedClose: Date | null;

  /**
   * When this verdict was taken. Not a `CreateDateColumn`: the row is upserted,
   * and what a reader needs to know is the age of the judgement rather than the
   * age of the row.
   */
  @Column({ type: "timestamptz", name: "computed_at" })
  computedAt: Date;
}
