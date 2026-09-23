/**
 * When a ride opens, and how much watching that answer rests on.
 *
 * Produced by `AnalyticsService.getRideOpeningTimes` and served on `/plan/day`
 * as `opensAt` + `opensAtConfidence`. See `docs/frontend/plan-day-endpoint.md`
 * §9 for the shape the frontend reads.
 */
export interface RideOpening {
  /** Park-local `HH:mm`, floored to the quarter hour. */
  opensAt: string;
  /**
   * How many days this median was taken over, bucketed. The floor for an answer
   * to exist at all is 5 days, so `low` is a real answer on thin evidence and
   * never a placeholder.
   */
  confidence: RideOpeningConfidence;
}

export type RideOpeningConfidence = "high" | "medium" | "low";

/**
 * Observed days needed for each tier — the same boundaries `rope-drop.util.ts`
 * uses, because it counts the same thing: days that contributed to one ride's
 * own estimate.
 *
 * Measured against production on 2026-09-23 over every park open that day, at
 * that day's own park opening (2757 served values): 58.4% `high`, 22.5%
 * `medium`, 19.0% `low`. Three populated tiers, none empty and none crushing.
 */
export const RIDE_OPENING_CONFIDENCE_THRESHOLDS = {
  high: 40,
  medium: 20,
} as const;

/** Bucket a day count into the tier it earns. */
export function rideOpeningConfidence(
  sampleDays: number,
): RideOpeningConfidence {
  if (sampleDays >= RIDE_OPENING_CONFIDENCE_THRESHOLDS.high) return "high";
  if (sampleDays >= RIDE_OPENING_CONFIDENCE_THRESHOLDS.medium) return "medium";
  return "low";
}
