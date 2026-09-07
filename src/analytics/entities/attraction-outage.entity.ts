import { Column, Entity, Index, PrimaryColumn } from "typeorm";

/**
 * Where an outage interval came from.
 *
 * Measured 2026-09-07: in parks whose feed emits DOWN, faults show up as DOWN
 * (45.74 per 1000 operating hours) and hardly ever as a closure gap (2.37), so
 * the two signals do not compete for the same events. In the 95 blind parks the
 * DOWN rate is 0.00 and the closure-gap rate is 5.78 — which is the only thing
 * that can be said there at all.
 */
export const OUTAGE_SIGNALS = ["down", "closed_gap"] as const;
export type OutageSignal = (typeof OUTAGE_SIGNALS)[number];

/**
 * Why an outage interval stopped, which is also whether its duration may be used.
 *
 * Only `recovered` and `reclassified` are OBSERVED ends. Everything else is the
 * interval being cut short by something that is not the ride starting again, and
 * a duration taken from one of those is a lower bound, not a measurement.
 */
export const DOWNTIME_END_REASONS = [
  /** A real OPERATING reading. The only end that is unambiguously a recovery. */
  "recovered",
  /** Turned into REFURBISHMENT. Observed, and not a lost sight — planned work. */
  "reclassified",
  /** Went CLOSED with no recovery. Censored: the estimand is time to recovery. */
  "closed",
  /** Still down at `as_of`. */
  "ongoing",
  /** The scan window ended mid-outage. Administrative censoring. */
  "window_edge",
  /** No reading for longer than the carry cap. We lost sight; not evidence. */
  "gap",
  /** Only carried heartbeats past the cap. Our own writer, not an observation. */
  "unconfirmed",
  /** Reverse reconciliation: no source mentioned the ride for 24 hours. */
  "source_absent",
] as const;

export type DowntimeEndReason = (typeof DOWNTIME_END_REASONS)[number];

/**
 * One reconstructed outage. **The only place an outage exists as an event.**
 *
 * The whole table hangs on one rule: an outage is an INTERVAL, never a day. A
 * ride standing still from Monday 16:00 to Wednesday 12:00 is one row with
 * `operatingDays = 3`, not three rows — everything per-day that this codebase
 * had before (`attraction_hourly_history.downCount`, `trackDowntime`'s Redis
 * counter) either cut such an outage into pieces or lost it entirely. The
 * denominator lives in `attraction_exposure_days` and holds minutes only, so
 * nothing there can be summed back into an event count.
 *
 * Rows are rewritten, not appended: the reconstruction deletes every outage of
 * the parks it covers from `scan_start` forward and re-inserts, so a multi-day
 * outage that has grown since yesterday replaces itself instead of leaving a
 * fragment behind.
 */
@Entity("attraction_outages")
@Index(["parkId", "startedAt"])
@Index("idx_attraction_outages_ride", ["attractionId", "startedAt"])
export class AttractionOutage {
  @PrimaryColumn("uuid")
  attractionId: string;

  /** First DOWN reading of the interval. */
  @PrimaryColumn({ type: "timestamptz", name: "started_at" })
  startedAt: Date;

  @Column("uuid")
  parkId: string;

  /** The reading that ended it, or null while it is still open. */
  @Column({ type: "timestamptz", name: "ended_at", nullable: true })
  endedAt: Date | null;

  /**
   * Minutes of the interval that fall inside a park OPERATING window.
   *
   * The published figure. A ride that breaks at 19:50 in a park closing at 20:00
   * and is still down at nine the next morning did not stand still for thirteen
   * hours of anybody's visit.
   */
  @Column({ type: "int", name: "operating_minutes", default: 0 })
  operatingMinutes: number;

  /**
   * The part of `operatingMinutes` backed by rows that are not carried
   * heartbeats.
   *
   * A heartbeat copies the previous status forward, so an outage can be mostly
   * our own writer repeating itself. A profile withholds duration when this
   * share drops below one half across its window.
   */
  @Column({ type: "int", name: "observed_operating_minutes", default: 0 })
  observedOperatingMinutes: number;

  /** Wall-clock length, opening hours ignored. Only used to spot works periods. */
  @Column({ type: "int", name: "wall_minutes", default: 0 })
  wallMinutes: number;

  /** How many park operating days the interval touches. 1 for the ordinary case. */
  @Column({ type: "smallint", name: "operating_days", default: 1 })
  operatingDays: number;

  /**
   * Which signal this interval was read from.
   *
   * `down` is a reported DOWN run — the operator's own feed saying the ride is
   * not running. `closed_gap` is inferred: the ride was OPERATING earlier that
   * day, went CLOSED inside opening hours, and came back the same day.
   *
   * They are stored together and must never be summed without looking, because
   * they are not equally strong evidence. A DOWN is a statement; a closure gap
   * is our reading of one. The wording a reader sees differs accordingly, and
   * `closed_gap` is only produced for parks whose feed never emits DOWN at all —
   * where the alternative is not a weaker signal but silence.
   */
  @Column({ type: "text", name: "signal", default: "down" })
  signal: OutageSignal;

  @Column({ type: "text", name: "end_reason" })
  endReason: DowntimeEndReason;

  /**
   * Whether this interval's length may be counted.
   *
   * True only for `recovered` and `reclassified`, and only when the carried
   * share is under half. Everything else is censored and contributes to the
   * event count alone.
   */
  @Column({ type: "boolean", name: "duration_usable", default: false })
  durationUsable: boolean;

  /**
   * Longer than seven wall-clock days, so almost certainly a works period rather
   * than a breakdown.
   *
   * Tokyo Disneyland's Dumbo carries one unchanged CLOSED row since 16 April,
   * five months out of a single row. Such an interval leaves the event count,
   * the duration set and the operating share entirely.
   */
  @Column({ type: "boolean", name: "likely_works_period", default: false })
  likelyWorksPeriod: boolean;

  /** DOWN readings behind the interval. One means it rests on a single row. */
  @Column({ type: "smallint", name: "rows_in_spell", default: 0 })
  rowsInSpell: number;

  /** How many of those were carried heartbeats. */
  @Column({ type: "smallint", name: "heartbeat_rows", default: 0 })
  heartbeatRows: number;

  /**
   * The interval already covered the oldest reading in the scan range.
   *
   * It began earlier than `startedAt`, so it counts as one event and leaves the
   * duration set. The scan start is chosen from the data rather than from a
   * calendar precisely to keep this rare.
   */
  @Column({ type: "boolean", name: "start_censored", default: false })
  startCensored: boolean;

  /**
   * Bumped when the reconstruction's rules change.
   *
   * The same device `FINGERPRINT_VERSION` uses in the content-change detector,
   * and for the same reason: editing the detector is not the world changing, so
   * a version bump adopts the new numbers instead of reporting a catalogue-wide
   * event.
   */
  @Column({ type: "smallint", name: "fingerprint_version", default: 1 })
  fingerprintVersion: number;

  @Column({ type: "timestamptz", name: "computed_at" })
  computedAt: Date;
}
