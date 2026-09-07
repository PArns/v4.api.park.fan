import { Column, Entity, Index, PrimaryColumn } from "typeorm";
import type { OutageSignal } from "./attraction-outage.entity";

/**
 * How long a running outage still has to go, as measured, never as predicted.
 *
 * ## What this answers, and why it is not the question §6 refuses
 *
 * `docs/analytics/ride-downtime.md` §6 refuses "when will this ride break next,
 * and for how long" on four independent grounds. This table answers a different
 * question — *it is broken right now; how long do outages like this one usually
 * last from here* — and the four grounds are why it is a separate table rather
 * than an extension of the profile:
 *
 * 1. **The arithmetic.** §6's is about a rate of p ≈ 0.006, which needs ~1300
 *    events per bin to pin down. Here the quantity is a conditional recovery
 *    share of p ≈ 0.15-0.5, measured on 5900-128000 intervals per bucket. At
 *    p = 0.15 and n = 5900 the standard error is 0.46 percentage points.
 * 2. **Stationarity.** The numerator is still "reported down", not "broken", so
 *    every sentence built on this says *gemeldet*. What changed is that the
 *    conditioning event is observed rather than forecast: we are not saying a
 *    ride will break, we are describing outages that already started.
 * 3. **Censoring.** This was the real objection, and measurement dissolved it.
 *    Read off raw `queue_data` runs, 68 % end without a recovery and the naive
 *    and Kaplan-Meier medians differ by a factor of five. Read off the
 *    reconstruction — which stitches across the night and counts *operating*
 *    minutes — censoring is **15.6 %**, because a park closing is a pause and no
 *    longer an ending. Same events, honest denominator.
 * 4. **Left truncation.** An outage that starts and ends between two polls
 *    writes no row, so the unconditional duration distribution is truncated at
 *    an unknown point. Conditioning on "already down for T operating minutes"
 *    removes it: a spell that survived to T is by construction not one that
 *    vanished between two polls. This is the reason the curve is only ever read
 *    at T > 0 and never as a plain average.
 *
 * ## Measured, not fitted
 *
 * No distribution is assumed. The hazard falls steeply — P(back up within 30
 * minutes) runs 50.5 % at T=5 down to 8.4 % at T=240 — so an exponential would
 * be wrong in the one direction that matters, telling a visitor at hour four
 * that it is nearly over. The rows are a Kaplan-Meier estimate over the
 * reconstructed intervals, with censored spells leaving the risk set at half the
 * band by the standard actuarial correction.
 *
 * Out-of-sample check, fit on 120 days and scored on the following 60: **mean
 * absolute calibration error 2.55 percentage points**, and under 2 points for
 * every bucket at T ≥ 60.
 *
 * ## Per park, never per ride
 *
 * Parks differ enough to matter — P(recover within 30 min | down 30 min) has a
 * median of 31.8 % and runs from 8.6 % to 52.4 %, with 12 of 48 parks more than
 * 10 points off the pooled value. Rides do not carry it: only 95 of 2285 have
 * 200 intervals behind them. So the curve is per park where the park has enough
 * (54 parks hold 98 % of all events) and pooled otherwise, and `parkId IS NULL`
 * is that pooled row.
 */
@Entity("downtime_recovery_curves")
@Index(["parkId", "signal", "elapsedMinutes"], { unique: true })
export class DowntimeRecoveryCurve {
  /**
   * Surrogate key, because the natural one contains a NULL.
   *
   * `parkId IS NULL` is the pooled curve, and a NULL inside a composite primary
   * key is not addressable in Postgres — `ON CONFLICT (parkId, elapsedMinutes)`
   * never matches it, so an upsert would insert a second pooled row on every
   * run. The unique index above carries a NULLS NOT DISTINCT clause applied by
   * the rebuild instead.
   */
  @PrimaryColumn("text")
  id: string;

  /** The park this curve describes, or NULL for the pooled curve. */
  @Column({ type: "uuid", name: "park_id", nullable: true })
  @Index()
  parkId: string | null;

  /**
   * Which signal's intervals this curve was built from.
   *
   * The two must not be pooled together. A `down` interval ends when the feed
   * says the ride runs again; a `closed_gap` ends when it stops reading CLOSED,
   * and by construction every one of them recovered — there is no censoring in
   * that population at all. Their quartiles differ (15/20/35 against 10/25/50)
   * and so does what a reader is being told, so a curve read against a
   * `closed_gap` outage is built from `closed_gap` intervals or it is not shown.
   */
  @Column({ type: "text", name: "signal", default: "down" })
  signal: OutageSignal;

  /**
   * Lower edge of the elapsed bucket, in the ride's own OPERATING minutes.
   *
   * Operating minutes, not wall-clock: the curve has to be read with the same
   * clock it was built with, and a spell that spans a night has hours of wall
   * time in which nothing could have been repaired or observed.
   */
  @Column({ type: "smallint", name: "elapsed_minutes" })
  elapsedMinutes: number;

  /** Intervals that reached this bucket. The precision of everything below. */
  @Column({ type: "int", name: "at_risk" })
  atRisk: number;

  /** P(reported running again within 30 more operating minutes). */
  @Column({
    type: "numeric",
    precision: 4,
    scale: 3,
    name: "recovery_within_30",
    nullable: true,
  })
  recoveryWithin30: string | null;

  /** P(reported running again within 60 more operating minutes). */
  @Column({
    type: "numeric",
    precision: 4,
    scale: 3,
    name: "recovery_within_60",
    nullable: true,
  })
  recoveryWithin60: string | null;

  /**
   * Remaining operating minutes at the 25th, 50th and 75th percentile.
   *
   * A median alone would be read as a promise. The pair is what makes the
   * spread visible, and past roughly four hours the upper one stops resolving
   * at all — which is itself the answer, and is stored as NULL rather than as a
   * large number that looks like knowledge.
   */
  @Column({ type: "int", name: "remaining_p25", nullable: true })
  remainingP25: number | null;

  @Column({ type: "int", name: "remaining_median", nullable: true })
  remainingMedian: number | null;

  @Column({ type: "int", name: "remaining_p75", nullable: true })
  remainingP75: number | null;

  @Column({ type: "timestamptz", name: "generated_at" })
  generatedAt: Date;
}

/**
 * Elapsed buckets the curve is measured at.
 *
 * Dense where the hazard moves fastest. The first bucket is 5 rather than 0
 * because `MIN_OUTAGE_OPERATING_MINUTES` is 5: there are no shorter intervals,
 * so a bucket at 0 would span half the width of the next one and read as a
 * non-monotonic jump that is an artefact of the floor.
 */
export const RECOVERY_ELAPSED_BUCKETS = [
  5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240,
] as const;

/** Horizons the recovery share is measured over, in operating minutes. */
export const RECOVERY_HORIZONS = [30, 60] as const;

/** Intervals a park needs at a bucket before its own curve is used there. */
export const MIN_PARK_CURVE_SAMPLE = 300;
