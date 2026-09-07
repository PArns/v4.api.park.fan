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
