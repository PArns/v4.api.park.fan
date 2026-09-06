import { Column, Entity, Index, PrimaryColumn } from "typeorm";

/**
 * Why a ride shows no figures.
 *
 * Every one of these is a different sentence to a reader, and collapsing them
 * into "no data" is the mistake the whole model exists to avoid: a park whose
 * sources cannot report an outage and a ride that simply had none are opposite
 * statements, and only the first is about us.
 */
export const DOWNTIME_WITHHELD_REASONS = [
  /** No source in this park can emit DOWN. Configuration, not outcome. */
  "not_down_capable",
  /**
   * The feed publishes on the hour, so no duration can be read out of it.
   *
   * Measured as the share held by one minute-of-hour value, NOT as the share of
   * single-reading runs — that earlier test turned out to measure "outages
   * shorter than an hour" (r = 0.996) and threw out every well-covered park.
   */
  "artefact_regime",
  /** The park publishes no opening hours, so the denominator would be circular. */
  "no_schedule",
  /** Real outages, too few of them for anything but the count. */
  "thin_events",
  /** Too little observed operating time behind the window. */
  "thin_exposure",
  /** The two halves of the window disagree; one number would hide a change. */
  "inhomogeneous",
  /** The ride absorbed another one; its history is two interleaved series. */
  "recently_merged",
  /** Too new — the first weeks of a ride are not its steady state. */
  "new_ride",
  /**
   * Plenty of outages, but too few of them were seen to END.
   *
   * Its own reason rather than `thin_events`, because the two are opposite
   * statements: „too few outages" and „many outages whose end we did not see"
   * send a reader looking in different places, and only the second is about a
   * ride that breaks often. Censoring here is strongly seasonal — a run ending
   * because the park shut for the winter measured 89.8 % in March against
   * 20.5 % in September — so this reason appears and disappears with the
   * season, by design.
   */
  "heavily_censored",
] as const;

export type DowntimeWithheldReason = (typeof DOWNTIME_WITHHELD_REASONS)[number];

/**
 * The published aggregate, one row per ride, rewritten nightly.
 *
 * Nothing reads `attraction_outages` to serve a page. That is deliberate: the
 * gates below are the difference between a figure and a claim, and a surface
 * that computed its own aggregate would be a second place where they could be
 * forgotten.
 *
 * `publishable` is false for the overwhelming majority of the catalogue and that
 * is the expected state, not a failure.
 */
@Entity("attraction_downtime_profiles")
@Index(["parkId"])
export class AttractionDowntimeProfile {
  @PrimaryColumn("uuid")
  attractionId: string;

  @Column("uuid")
  parkId: string;

  @Column({ type: "smallint", name: "window_days" })
  windowDays: number;

  @Column({ type: "date", name: "window_from" })
  windowFrom: string;

  @Column({ type: "date", name: "window_to" })
  windowTo: string;

  /** Observed operating hours behind the window. The exposure, not the park's. */
  @Column({ type: "numeric", precision: 10, scale: 2, name: "exposure_hours" })
  exposureHours: string;

  /** Park operating days on which this ride was observed at all. */
  @Column({ type: "smallint", name: "observed_days", default: 0 })
  observedDays: number;

  /** Reported outages in the window, works periods excluded. */
  @Column({ type: "smallint", default: 0 })
  outages: number;

  /** How many of those have a usable length. */
  @Column({ type: "smallint", name: "usable_durations", default: 0 })
  usableDurations: number;

  /**
   * The empirical median of the usable durations, in minutes.
   *
   * Empirical, not Kaplan-Meier. Under censoring a KM median is not "half of the
   * observed outages", so the sentence beside it would be checkable against the
   * counts beside it and false.
   */
  @Column({ type: "smallint", name: "median_minutes", nullable: true })
  medianMinutes: number | null;

  @Column({ type: "int", name: "longest_minutes", nullable: true })
  longestMinutes: number | null;

  @Column({ type: "timestamptz", name: "longest_started_at", nullable: true })
  longestStartedAt: Date | null;

  /**
   * Down minutes over (down + operating) minutes.
   *
   * The ONE denominator on the card. A second one beside it is a division a
   * reader would perform and get a different answer from.
   */
  @Column({
    type: "numeric",
    precision: 5,
    scale: 4,
    name: "down_share",
    nullable: true,
  })
  downShare: string | null;

  /** Share of the window's intervals that are censored rather than observed. */
  @Column({
    type: "numeric",
    precision: 4,
    scale: 3,
    name: "censored_share",
    default: 0,
  })
  censoredShare: string;

  @Column({ type: "boolean", default: false })
  publishable: boolean;

  @Column({ type: "text", name: "withheld_reason", nullable: true })
  withheldReason: DowntimeWithheldReason | null;

  @Column({ type: "timestamptz", name: "generated_at" })
  generatedAt: Date;
}
