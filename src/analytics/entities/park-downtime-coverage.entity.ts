import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * What a park's downtime readings are worth, if anything.
 *
 * Published for EVERY park, including the ones that will never show a figure —
 * it is the reason a ride page shows nothing, and a reader is owed the reason
 * rather than an empty space.
 */
export const DOWNTIME_REGIMES = [
  /**
   * No source here can emit DOWN. Read from `parks.wiki_entity_id`.
   *
   * Only `themeparks-data-source` produces the status; Queue-Times maps
   * `is_open` onto OPERATING/CLOSED and wartezeiten collapses everything that is
   * not running onto CLOSED or REFURBISHMENT.
   */
  "not_capable",
  /**
   * The park is listed at themeparks.wiki, and its feed has still never said
   * DOWN.
   *
   * `wiki_entity_id IS NOT NULL` is necessary for a DOWN to be possible and it
   * is **not sufficient**: being known to the source says nothing about whether
   * that park's feed carries the status. Measured 2026-09-07, **102 of 182**
   * parks with a schedule had not produced one DOWN row in 180 days, among them
   * Phantasialand, Energylandia (19 026 observed operating hours), Hersheypark,
   * Alton Towers and Parc Asterix.
   *
   * They are not quiet. Taken together the never-DOWN parks have MORE observed
   * operating time than the reporting ones (736 738 h against 583 782 h) with
   * zero events. At the 10th-percentile rate of the parks that do report
   * (3.46 outages per 1000 operating hours), Energylandia alone would expect
   * ~66.
   *
   * This is read from the outcome, which the rest of this file refuses to do,
   * and the difference is the evidence threshold: `MIN_BLIND_EVIDENCE_HOURS` is
   * the point past which "we saw nothing" stops being compatible with "there
   * was nothing to see". A quiet quarter does not have 1500 observed operating
   * hours and no event; a feed that does not carry the status does.
   *
   * Without this regime those parks read `reports`, and a ride page would have
   * said „In den letzten 90 Tagen wurde keine Störung gemeldet" — our own
   * blindness rendered as an operator's clean record, which is the single worst
   * thing this feature could do.
   */
  "never_reports",
  /** Readings arrive and hold long enough to measure. */
  "reports",
  /**
   * Readings arrive and are erased again before the next poll.
   *
   * The signature is nearly every interval resting on a single reading. What a
   * duration would measure there is how fast the queue drained after
   * `ConflictResolverService` rewrote the DOWN away, not how long the ride stood
   * still.
   */
  "artefact",
  /** The park publishes no opening hours, so any denominator is circular. */
  "no_schedule",
] as const;

export type DowntimeRegime = (typeof DOWNTIME_REGIMES)[number];

/** One row per park, rewritten nightly. */
@Entity("park_downtime_coverage")
export class ParkDowntimeCoverage {
  @PrimaryColumn("uuid")
  parkId: string;

  @Column({ type: "text" })
  regime: DowntimeRegime;

  /**
   * Whether a source here could report an outage at all.
   *
   * From configuration, never from the outcome. A park with no `wiki_entity_id`
   * and a park with a quiet quarter are two different statements, and reading
   * capability off "did we see any outages" turns a blind park into a flawless
   * one.
   */
  @Column({ type: "boolean", name: "down_capable", default: false })
  downCapable: boolean;

  @Column({ type: "smallint", name: "rides_tracked", default: 0 })
  ridesTracked: number;

  @Column({ type: "smallint", name: "rides_with_outages", default: 0 })
  ridesWithOutages: number;

  @Column({ type: "int", default: 0 })
  outages: number;

  /**
   * Share of intervals resting on exactly one reading.
   *
   * Descriptive only, and NO LONGER the artefact test: measured over production
   * it correlates with "share of outages shorter than an hour" at r = 0.996,
   * because `queue_data` is a change log and an outage ending before the hourly
   * heartbeat writes exactly one row. Kept because it is a useful read on how
   * long a park's outages run.
   */
  @Column({
    type: "numeric",
    precision: 4,
    scale: 3,
    name: "one_interval_spell_share",
    default: 0,
  })
  oneIntervalSpellShare: string;

  /**
   * Share held by the single most common minute-of-hour across interval edges.
   *
   * The artefact test. An hourly feed puts every reading on one minute; taking
   * the most common value rather than minute zero keeps it timezone-independent.
   * Highest observed in production is 0.236, so no park is in that regime.
   */
  @Column({
    type: "numeric",
    precision: 4,
    scale: 3,
    name: "on_the_hour_share",
    default: 0,
  })
  onTheHourShare: string;

  @Column({ type: "smallint", name: "median_spell_minutes", nullable: true })
  medianSpellMinutes: number | null;

  @Column({ type: "timestamptz", name: "generated_at" })
  generatedAt: Date;
}

/**
 * Observed operating hours before "no DOWN ever" is read as blindness.
 *
 * Calibrated against production: among parks that do report, the 10th-percentile
 * rate is 3.46 outages per 1000 observed operating hours. At 1500 hours that
 * predicts 5.2 events, so seeing none has probability e^-5.2 = **0.0056** if the
 * park really were reporting. It catches 91 of the 102 blind parks; the other 11
 * have too little observation to say, stay `reports`, and are withheld by the
 * ride-level event floor anyway.
 */
export const MIN_BLIND_EVIDENCE_HOURS = 1500;
