import type { DowntimeRecoveryCurve } from "../entities/downtime-recovery-curve.entity";
import {
  recoveryWindowFrom,
  type OperatingWindow,
  type RecoveryWindow,
} from "./operating-clock.util";

/**
 * What a running outage's remaining time looks like, or nothing.
 *
 * The decision is a pure function of the curve and the elapsed figure so it can
 * be tested against the measured numbers without a database, and so the one
 * place that decides to stay silent is one place.
 */
export interface OutageEstimate {
  /** Operating minutes already elapsed, the value the curve was read at. */
  elapsedMinutes: number;
  /** Bucket edge actually used. Never interpolated — see {@link estimateOutage}. */
  bucketMinutes: number;
  /** P(reported running again within 30 more operating minutes), 0-1. */
  recoveryWithin30: number;
  /** P(reported running again within 60 more operating minutes), 0-1. */
  recoveryWithin60: number;
  /**
   * Remaining operating minutes at the 25th/50th/75th percentile.
   *
   * `p75` is null once the curve stops resolving the upper quartile, which it
   * does past roughly two hours. It stays a null INSIDE the range rather than
   * collapsing the whole object, because "at least this long, no upper bound we
   * can measure" is the most useful thing anybody can say about a long outage —
   * and it renders as an open range, which carries the uncertainty visually.
   *
   * A median must never be rendered without this spread around it. At one hour
   * elapsed the quartiles are 25 and 255 minutes around a median of 70.
   */
  remaining?: { p25: number; median: number; p75: number | null };
  /**
   * The same quartiles as instants, once the park's calendar can place them.
   *
   * `remaining` is in operating minutes and therefore not a duration anybody can
   * add to a clock: an outage with two operating hours left, in a park shutting
   * in twenty minutes, ends tomorrow morning. Every renderer needs the calendar
   * to say so and no renderer has it — `AttractionCard` alone is drawn from
   * eight places, none of which passes a schedule — so the arithmetic is done
   * once, here, where the calendar already is.
   *
   * Absent whenever the lower bound cannot be placed: a park that publishes no
   * hours (there is no operating minute to project onto), or one whose calendar
   * does not reach far enough. Never a wall-clock fallback.
   *
   * **It recedes inside a bucket, and that is the conservative direction.**
   * `remaining` is read at the floored bucket edge, so between 120 and 179
   * elapsed minutes the same 50 minutes are added to a moving `now` and the
   * instant slides forward with it. Subtracting the minutes already served
   * inside the bucket would fix the sliding and import the exponential this
   * curve exists to refuse: the hazard falls, so an outage that survived to 175
   * has a LONGER remaining distribution than one at 120, not a shorter one by
   * the difference. Leaving it unshifted errs late rather than early, which is
   * the same direction the flooring itself takes. It is visible as a clock time
   * where it was invisible as "50 min", and it is the same arithmetic.
   */
  recoveryWindow?: RecoveryWindow;
  /** Whether this park carried its own curve or fell back to the pooled one. */
  basis: "park" | "pooled";
  /** Intervals behind the bucket. Diagnostic; never rendered. */
  sampleSize: number;
}

/**
 * Below this the estimate is withheld: too few spells behind the bucket.
 *
 * The pooled curve clears it everywhere by an order of magnitude; it exists so a
 * thin park-level bucket cannot slip through the `basis` selection.
 */
export const MIN_ESTIMATE_SAMPLE = 200;

/**
 * How long a running outage has left, read off the measured curve.
 *
 * ## Why the bucket is floored and never interpolated
 *
 * The curve is a step function of a hazard that falls steeply and unevenly;
 * interpolating between 120 and 180 minutes would invent a shape the data does
 * not have. Flooring to the bucket at or below the elapsed figure is also the
 * conservative direction: it reads the curve at a point the outage has provably
 * passed, so the recovery share it reports is if anything too optimistic by less
 * than one bucket's worth.
 *
 * ## Why an elapsed figure below the first bucket returns nothing
 *
 * `MIN_OUTAGE_OPERATING_MINUTES` is 5, so no reconstructed interval is shorter
 * and there is no measurement to read at two minutes. It also happens to be the
 * regime where a `queue_data` write is most likely to be a blip that resolves on
 * the next poll, and saying anything at all there would be the confident number
 * on the thinnest evidence.
 *
 * The caller passes curves already filtered to the outage's own signal. A
 * `closed_gap` outage read against a `down` curve would be answered from a
 * different population — one with censoring, a different end definition and
 * quartiles of 10/25/50 against 15/20/35 — so the filtering is the caller's job
 * and this function never mixes them.
 *
 * @param curves - The park's own rows and the pooled rows, both ascending, both
 *   already restricted to one signal.
 * @param elapsedMinutes - Operating minutes since the run started.
 * @param clock - The park's upcoming operating windows and the instant to
 *   project from. Optional, and its absence costs only `recoveryWindow`: a park
 *   that publishes no hours still gets its probabilities and its quartiles.
 * @returns The estimate, or undefined when the curve cannot answer.
 */
export function estimateOutage(
  curves: { park: DowntimeRecoveryCurve[]; pooled: DowntimeRecoveryCurve[] },
  elapsedMinutes: number,
  clock?: { windows: readonly OperatingWindow[]; asOf: Date },
): OutageEstimate | undefined {
  if (!Number.isFinite(elapsedMinutes) || elapsedMinutes < 0) return undefined;

  const pick = (rows: DowntimeRecoveryCurve[]) => {
    let best: DowntimeRecoveryCurve | undefined;
    for (const row of rows) {
      if (row.elapsedMinutes <= elapsedMinutes) best = row;
      else break;
    }
    return best;
  };

  // The bucket comes from the POOLED curve, which is the only one guaranteed to
  // span the whole range, and the park may then answer for that same bucket.
  //
  // Flooring within the park's own rows instead is a trap that costs a visitor
  // real time: a park whose curve stops at 30 minutes would answer a
  // four-hour outage with its 30-minute row, reporting a 52 % chance of
  // recovery where the measured figure is 8.5 %. The fallback is therefore per
  // bucket, not per park.
  const pooledRow = pick(curves.pooled);
  if (!pooledRow || pooledRow.atRisk < MIN_ESTIMATE_SAMPLE) return undefined;

  const parkRow = curves.park.find(
    (r) => r.elapsedMinutes === pooledRow.elapsedMinutes,
  );
  const row =
    parkRow && parkRow.atRisk >= MIN_ESTIMATE_SAMPLE ? parkRow : pooledRow;

  const p30 = numberOrNull(row.recoveryWithin30);
  const p60 = numberOrNull(row.recoveryWithin60);
  if (p30 === null || p60 === null) return undefined;

  const p25 = row.remainingP25;
  const median = row.remainingMedian;
  const p75 = row.remainingP75;

  const remaining =
    p25 !== null && median !== null ? { p25, median, p75 } : undefined;

  return {
    elapsedMinutes: Math.round(elapsedMinutes),
    bucketMinutes: row.elapsedMinutes,
    recoveryWithin30: p30,
    recoveryWithin60: p60,
    remaining,
    // Derived from the quartiles rather than from the median, so the pair that
    // is rendered is the pair that was measured. No `remaining`, no window:
    // there is nothing to place.
    recoveryWindow:
      remaining && clock
        ? recoveryWindowFrom(clock.windows, clock.asOf, remaining)
        : undefined,
    basis: row === parkRow ? "park" : "pooled",
    sampleSize: row.atRisk,
  };
}

function numberOrNull(value: string | null): number | null {
  if (value === null) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
